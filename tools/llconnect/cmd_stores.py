"""Data store management commands: create, remove, list, cleanup, rekey."""

import sys
from typing import Optional

from .s3 import S3Client, S3Config
from .store import count_store_files
from .ui import confirm, print_progress, print_progress_done, verify_store_status


def cmd_create_store(args, filespace, connect, config, store_name) -> None:
    """Create a new data store."""
    s3_config = S3Config(
        bucket=args.bucket,
        region=args.region,
        access_key=args.access_key,
        secret_key=args.secret_key,
        endpoint=getattr(args, "endpoint", ""),
        path_style=getattr(args, "path_style", False),
        url_expiration=getattr(args, "url_expiration", 10080),
    )

    name = args.name if args.name else s3_config.store_id
    already_exists = connect.get_data_store(name) is not None

    if args.dry_run:
        if already_exists:
            print(f"[DRY] Data store '{name}' already exists — would fail.")
        else:
            print(f"[DRY] Would create data store '{name}' -> s3://{s3_config.bucket}")
        return

    if already_exists:
        print(f"ERROR: Data store '{name}' already exists.")
        sys.exit(1)

    if not args.no_verify:
        print(f"Verifying bucket s3://{s3_config.bucket}...")
        try:
            s3 = S3Client(s3_config)
            s3.verify_bucket()
        except Exception as e:
            print(f"ERROR: Cannot access bucket '{s3_config.bucket}': {e}")
            print("       Use --no-verify to skip this check.")
            sys.exit(1)

    from lucidlink import S3DataStoreConfig

    ds_config = S3DataStoreConfig(
        access_key=s3_config.access_key,
        secret_key=s3_config.secret_key,
        bucket_name=s3_config.bucket,
        region=s3_config.region,
        endpoint=s3_config.endpoint,
        use_virtual_addressing=not s3_config.path_style,
        url_expiration_minutes=s3_config.url_expiration,
    )

    print(f"Creating data store '{name}' -> s3://{s3_config.bucket}")
    connect.add_data_store(name, ds_config)
    filespace.sync_all()
    print(f"[OK] Created data store '{name}'")


def cmd_remove_store(args, filespace, connect, config, store_name) -> None:
    """Remove an empty data store."""
    name = args.store

    if connect.get_data_store(name) is None:
        print(f"ERROR: Data store '{name}' not found.")
        sys.exit(1)

    file_count = count_store_files(connect, name)
    if file_count > 0:
        print(f"ERROR: Data store '{name}' has {file_count} linked files.")
        print("       Remove all linked files first with 'unlink --all', then retry.")
        sys.exit(1)

    if args.dry_run:
        print(f"[DRY] Would remove data store '{name}'")
        return

    if not confirm(f"Remove data store '{name}'?", args.yes):
        print("Aborted.")
        return

    connect.remove_data_store(name)
    filespace.sync_all()
    print(f"[OK] Removed data store '{name}'")


def cmd_list_stores(args, filespace, connect, config, store_name) -> None:
    """List all data stores with info and optional accessibility check."""
    stores = connect.list_data_stores()
    if not stores:
        print("No data stores registered.")
        return

    no_verify = getattr(args, "no_verify", False)
    label = "Checking data stores" if not no_verify else "Listing data stores"

    rows = _scan_stores(connect, stores, label, verify=not no_verify)
    print(f"Showing {len(rows)} data stores")
    _print_store_table(rows, include_status=not no_verify)


def cmd_cleanup_stores(args, filespace, connect, config, store_name) -> None:
    """Remove empty stale stores; with --empty also remove empty healthy stores."""
    include_empty = getattr(args, "empty", False)
    dry_run = getattr(args, "dry_run", False)
    stores = connect.list_data_stores()
    if not stores:
        print("No data stores registered.")
        return

    to_remove, kept = _classify_stores_for_cleanup(connect, stores, include_empty)

    if not to_remove:
        print("Nothing to clean up.")
        return

    for name, reason in to_remove:
        prefix = "[DRY] Would remove" if dry_run else "Will remove"
        print(f"  {prefix} '{name}' ({reason})")

    if dry_run:
        print(f"\n[OK] Would remove {len(to_remove)} stores, {kept} kept")
        return

    if not confirm(f"Remove {len(to_remove)} stores?", args.yes):
        print("Aborted.")
        return

    for name, _reason in to_remove:
        connect.remove_data_store(name)

    filespace.sync_all()
    print(f"\n[OK] Removed {len(to_remove)} stores, {kept} kept")


def cmd_rekey_store(args, filespace, connect, config: S3Config, store_name: str) -> None:
    """Rotate S3 credentials for the data store."""
    existing = connect.get_data_store(store_name)

    if args.dry_run:
        print(f"[DRY] Would rekey data store '{store_name}' (bucket: {existing.bucket_name})")
        return

    print(f"Rekeying data store '{store_name}' (bucket: {existing.bucket_name})...")
    connect.rekey_data_store(store_name, new_access_key=args.access_key, new_secret_key=args.secret_key)
    filespace.sync_all()
    print(f"[OK] Credentials rotated for '{store_name}'")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _scan_stores(connect, stores, label: str, verify: bool) -> list:
    """Scan stores collecting name, bucket, region, file count, and optional status."""
    rows = []
    for i, store in enumerate(stores):
        print_progress(label, i, len(stores))
        file_count = count_store_files(connect, store.name)
        status = verify_store_status(connect, store) if verify else ""
        rows.append((store.name, store.bucket_name, store.region, file_count, status))
    print_progress_done(label)
    return rows


def _classify_stores_for_cleanup(connect, stores, include_empty: bool):
    """Scan stores and classify them for cleanup.

    Returns (to_remove, kept) where to_remove is a list of (name, reason).
    """
    label = "Scanning data stores"
    to_remove = []
    kept = 0

    for i, store in enumerate(stores):
        print_progress(label, i, len(stores))
        file_count = count_store_files(connect, store.name)

        if file_count > 0:
            if not include_empty:
                status = verify_store_status(connect, store)
                if status != "OK":
                    print(f"\r  WARNING: '{store.name}' has {file_count} files but is stale ({status})")
            kept += 1
            continue

        if include_empty:
            to_remove.append((store.name, "empty"))
        else:
            status = verify_store_status(connect, store)
            if status != "OK":
                to_remove.append((store.name, f"empty, {status}"))
            else:
                kept += 1

    print_progress_done(label)
    return to_remove, kept


def _print_store_table(rows: list, include_status: bool) -> None:
    """Print an aligned table of store information."""
    hdr = ("Name", "Bucket", "Region", "Files", "Status")
    w_name = max(len(hdr[0]), *(len(r[0]) for r in rows))
    w_bucket = max(len(hdr[1]), *(len(r[1]) for r in rows))
    w_region = max(len(hdr[2]), *(len(r[2]) for r in rows))
    w_files = max(len(hdr[3]), *(len(str(r[3])) for r in rows))

    if include_status:
        w_status = max(len(hdr[4]), *(len(r[4]) for r in rows))
        header = (
            f"{hdr[0]:<{w_name}}  {hdr[1]:<{w_bucket}}  "
            f"{hdr[2]:<{w_region}}  {hdr[3]:>{w_files}}  {hdr[4]:<{w_status}}"
        )
    else:
        header = f"{hdr[0]:<{w_name}}  {hdr[1]:<{w_bucket}}  {hdr[2]:<{w_region}}  {hdr[3]:>{w_files}}"

    sep = "\u2500" * len(header)
    print(f"\n{header}\n{sep}")

    for name, bucket, region, files, status in rows:
        line = f"{name:<{w_name}}  {bucket:<{w_bucket}}  {region:<{w_region}}  {files:>{w_files}}"
        if include_status:
            print(f"{line}  {status}")
        else:
            print(line)

    print()
