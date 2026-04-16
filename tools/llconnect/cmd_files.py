"""File operation commands: link, unlink."""

import sys
from typing import Optional

from .paths import ensure_parent_dirs, parse_lucidlink_url
from .s3 import S3Config
from .spinner import Spinner
from .store import iter_external_files
from .ui import confirm


def cmd_link(args, filespace, connect, config: S3Config, store_name: str) -> None:
    """Link a single S3 object as a file in the filespace."""
    _, _, file_path = parse_lucidlink_url(args.path)
    object_key = args.object_key

    if args.dry_run:
        print(f"[DRY] Would link s3://{config.bucket}/{object_key} -> {file_path}")
        return

    print(f"Linking s3://{config.bucket}/{object_key} -> {file_path}")
    ensure_parent_dirs(filespace, file_path)
    connect.link_file(file_path, store_name, object_key, size=args.size, checksum=args.checksum)
    filespace.sync_all()
    print(f"[OK] Linked: {file_path}")


def cmd_unlink(
    args, filespace, connect, config: Optional[S3Config], store_name: Optional[str],
) -> None:
    """Unlink linked files from the filespace."""
    _, _, file_path = parse_lucidlink_url(args.path)

    if not args.all:
        if not args.dry_run:
            if not confirm(f"Unlink '{file_path}'?", args.yes):
                print("Aborted.")
                return
        _unlink_single(filespace, connect, file_path, args.dry_run)
    else:
        if not args.dry_run:
            if not confirm(f"Unlink all files under '{file_path}' for store '{store_name}'?", args.yes):
                print("Aborted.")
                return
        _unlink_batch(filespace, connect, store_name, file_path, args.dry_run)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _unlink_single(filespace, connect, file_path: str, dry_run: bool) -> None:
    """Unlink one file, guarding against accidental directory deletion."""
    try:
        entry = filespace.fs.get_entry(file_path)
        if entry.is_dir():
            print(f"ERROR: '{file_path}' is a directory. Use --all to unlink all files under it.")
            sys.exit(1)
    except FileNotFoundError:
        pass

    if dry_run:
        print(f"[DRY] Would unlink: {file_path}")
        return

    connect.unlink_file(file_path)
    filespace.sync_all()
    print(f"[OK] Unlinked: {file_path}")


def _unlink_batch(
    filespace, connect, store_name: str, file_path: str, dry_run: bool,
) -> None:
    """Unlink all linked files under *file_path* for a data store (paged)."""
    base = file_path.rstrip("/")
    count = 0
    spinner = Spinner("Unlinking")

    for fp in iter_external_files(connect, store_name):
        if fp != base and not fp.startswith(base + "/"):
            continue

        if dry_run:
            print(f"  [DRY] Would unlink: {fp}")
        else:
            try:
                connect.unlink_file(fp)
            except Exception as e:
                print(f"\n  [ERROR] {fp}: {e}")
                spinner.error()
        count += 1
        spinner.update()

    if not dry_run and count:
        spinner.finish()
        filespace.sync_all()
    else:
        spinner.finish()

    if not count:
        print("Nothing to unlink.")
        return

    action = "Would unlink" if dry_run else "Unlinked"
    print(f"\n[OK] {action}: {count} files ({spinner.errors} errors)")
