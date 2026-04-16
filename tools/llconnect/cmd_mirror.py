"""Mirror command: full sync of S3 bucket/prefix into a filespace directory."""

from typing import Set

from .paths import ensure_parent_dirs, join_filespace_path, parse_lucidlink_url
from .s3 import S3Client, S3Config
from .spinner import Spinner
from .store import iter_external_files_with_ids


def cmd_mirror(args, filespace, connect, config: S3Config, store_name: str) -> None:
    """Mirror S3 bucket/prefix into the filespace (full sync).

    Phase 1: Stream S3 objects page-by-page.  For each object, check whether
    it is already linked (via ``get_entry``).  Add new files, re-link changed
    ones, skip unchanged.  Collect a ``seen_ids`` set of file IDs that should
    remain.

    Phase 2: Page through linked files from the data store.  Any linked file
    under *base_path* whose ID is NOT in ``seen_ids`` is stale and gets removed.

    Neither phase loads all linked files into memory at once.
    """
    _, _, base_path = parse_lucidlink_url(args.path)
    dry_run = args.dry_run
    strip_prefix = not args.no_strip_prefix
    prefix = (getattr(args, "prefix", "") or "").strip("/")
    if prefix:
        prefix += "/"

    # Phase 1 -- stream S3 objects, add/update, collect seen file IDs
    print(f"Mirroring s3://{config.bucket}/{prefix} -> {base_path}")
    s3 = S3Client(config)
    spinner = Spinner("Mirroring")
    seen_ids, added, updated, skipped = _sync_from_s3(
        s3, filespace, connect, store_name, base_path, prefix,
        strip_prefix, dry_run, spinner,
    )

    # Phase 2 -- page through linked files, remove stale by file ID
    removed = _remove_stale_paged(
        connect, store_name, base_path, seen_ids, dry_run, spinner,
    )

    if not dry_run and (added or updated or removed):
        spinner.finish()
        filespace.sync_all()
    else:
        spinner.finish()

    action = "Would apply" if dry_run else "Applied"
    print(
        f"\n[OK] {action}: {added} added, {removed} removed, "
        f"{updated} updated, {skipped} unchanged "
        f"({spinner.errors} errors)"
    )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _sync_from_s3(
    s3: S3Client,
    filespace,
    connect,
    store_name: str,
    base_path: str,
    prefix: str,
    strip_prefix: bool,
    dry_run: bool,
    spinner: Spinner,
) -> tuple:
    """Stream S3 objects and add/update files.

    Returns ``(seen_ids, added, updated, skipped)`` where *seen_ids* is a
    ``Set[int]`` of external file IDs (~28 bytes each instead of ~200 for paths).
    """
    seen_ids: Set[int] = set()
    added = 0
    updated = 0
    skipped = 0

    for obj in s3.list_objects(prefix=prefix):
        key = obj["key"]
        if key.endswith("/"):
            continue

        rel = key[len(prefix):] if strip_prefix and prefix else key
        file_path = join_filespace_path(base_path, rel)

        entry = _get_entry_or_none(filespace, file_path)

        if entry is not None:
            if entry.size == obj["size"]:
                seen_ids.add(entry.file_id_external)
                skipped += 1
            else:
                _try_update_file(connect, store_name, file_path, key, obj, dry_run, spinner)
                _add_file_id(filespace, file_path, seen_ids)
                updated += 1
        else:
            _try_add_file(filespace, connect, store_name, file_path, key, obj, dry_run, spinner)
            _add_file_id(filespace, file_path, seen_ids)
            added += 1

        spinner.update()

    return seen_ids, added, updated, skipped


def _remove_stale_paged(
    connect,
    store_name: str,
    base_path: str,
    seen_ids: Set[int],
    dry_run: bool,
    spinner: Spinner,
) -> int:
    """Page through linked files, remove any whose file ID is not in *seen_ids*."""
    base_prefix = base_path.rstrip("/") + "/"
    removed = 0

    for file_id, fp in iter_external_files_with_ids(connect, store_name):
        if fp != base_path and not fp.startswith(base_prefix):
            continue
        if file_id in seen_ids:
            continue

        if dry_run:
            print(f"  [DRY] Would remove: {fp}")
        else:
            try:
                connect.unlink_file(fp)
            except Exception as e:
                print(f"\n  [ERROR] Remove {fp}: {e}")
                spinner.error()
        removed += 1
        spinner.update()

    return removed


def _get_entry_or_none(filespace, file_path: str):
    """Return the DirEntry if it exists, or None."""
    try:
        return filespace.fs.get_entry(file_path)
    except FileNotFoundError:
        return None


def _add_file_id(filespace, file_path: str, seen_ids: Set[int]) -> None:
    """Look up the current file ID for *file_path* and add it to *seen_ids*."""
    entry = _get_entry_or_none(filespace, file_path)
    if entry is not None:
        seen_ids.add(entry.file_id_external)


def _try_update_file(
    connect, store_name: str, file_path: str, key: str, obj: dict,
    dry_run: bool, spinner: Spinner,
) -> None:
    """Re-link a file whose size changed."""
    if dry_run:
        print(f"  [DRY] Would update: {file_path}")
        return

    try:
        connect.unlink_file(file_path)
        connect.link_file(file_path, store_name, key, size=obj["size"], checksum=obj["etag"])
    except Exception as e:
        print(f"\n  [ERROR] Update {file_path}: {e}")
        spinner.error()


def _try_add_file(
    filespace, connect, store_name: str, file_path: str, key: str, obj: dict,
    dry_run: bool, spinner: Spinner,
) -> None:
    """Link a new S3 object as a file in the filespace."""
    if dry_run:
        print(f"  [DRY] Would add: s3://.../{key} -> {file_path}")
        return

    try:
        ensure_parent_dirs(filespace, file_path)
        connect.link_file(file_path, store_name, key, size=obj["size"], checksum=obj["etag"])
    except Exception as e:
        print(f"\n  [ERROR] Add {file_path}: {e}")
        spinner.error()
