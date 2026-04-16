"""Argument parsing, LucidLink session setup, and CLI entry point."""

import argparse
import os
import sys
from typing import Tuple

from .cmd_files import cmd_link, cmd_unlink
from .cmd_mirror import cmd_mirror
from .cmd_stores import (
    cmd_cleanup_stores,
    cmd_create_store,
    cmd_list_stores,
    cmd_rekey_store,
    cmd_remove_store,
)
from .paths import parse_lucidlink_url
from .store import resolve_store_auto

# Ensure UTF-8 output on Windows (needed for braille spinner characters)
if sys.platform == "win32":
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")


_COMMANDS = {
    "create-store": cmd_create_store,
    "remove-store": cmd_remove_store,
    "list-stores": cmd_list_stores,
    "cleanup-stores": cmd_cleanup_stores,
    "rekey-store": cmd_rekey_store,
    "link": cmd_link,
    "unlink": cmd_unlink,
    "mirror": cmd_mirror,
}

# Commands that use --filespace fs.ws (no directory path needed)
_FILESPACE_COMMANDS = {"create-store", "remove-store", "list-stores", "cleanup-stores", "rekey-store"}

# Commands that use --path lucidlink://ws/fs/dir
_PATH_COMMANDS = {"link", "unlink", "mirror"}


def setup_lucidlink_by_path(token: str, path: str) -> Tuple:
    """Create daemon, authenticate, and link filespace from URL.

    Returns:
        ``(daemon, workspace, filespace)``
    """
    import lucidlink

    workspace_name, filespace_name, _ = parse_lucidlink_url(path)
    print(f"Connecting to {filespace_name}.{workspace_name}...")

    daemon = lucidlink.create_daemon(sandboxed=True)
    daemon.start()

    credentials = lucidlink.ServiceAccountCredentials(token=token)
    workspace = daemon.authenticate(credentials)
    filespace = workspace.link_filespace(name=filespace_name)

    print(f"[OK] Connected to filespace: {filespace.full_name}")
    return daemon, workspace, filespace


def setup_lucidlink_by_filespace(token: str, filespace_fqn: str) -> Tuple:
    """Connect to a filespace using fs.ws format.

    Returns:
        ``(daemon, workspace, filespace)``
    """
    import lucidlink

    dot_idx = filespace_fqn.find(".")
    if dot_idx == -1:
        print(f"ERROR: Invalid filespace format: '{filespace_fqn}'. Expected: fs.ws")
        sys.exit(1)
    filespace_name = filespace_fqn[:dot_idx]
    workspace_name = filespace_fqn[dot_idx + 1:]

    print(f"Connecting to {filespace_name}.{workspace_name}...")

    daemon = lucidlink.create_daemon(sandboxed=True)
    daemon.start()

    credentials = lucidlink.ServiceAccountCredentials(token=token)
    workspace = daemon.authenticate(credentials)
    filespace = workspace.link_filespace(name=filespace_name)

    print(f"[OK] Connected to filespace: {filespace.full_name}")
    return daemon, workspace, filespace


def _filespace_fqn_from_url(path: str) -> str:
    """Extract 'filespace.workspace' from a lucidlink:// URL."""
    workspace_name, filespace_name, _ = parse_lucidlink_url(path)
    return f"{filespace_name}.{workspace_name}"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="llconnect",
        description="LucidLink Connect CLI - manage external S3 data stores and linked files",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=_EPILOG,
    )

    sub = parser.add_subparsers(dest="command", required=True, help="Command to execute")

    # -- Shared parent parsers --
    token_parent = argparse.ArgumentParser(add_help=False)
    token_parent.add_argument("--token", required=True, help="LucidLink service account token")

    filespace_parent = argparse.ArgumentParser(add_help=False)
    filespace_parent.add_argument("--filespace", required=True,
                                  help="Filespace in fs.ws format (e.g., myfiles.myworkspace)")

    path_parent = argparse.ArgumentParser(add_help=False)
    path_parent.add_argument("--path", required=True,
                             help="LucidLink URL: lucidlink://workspace/filespace/dir")

    store_parent = argparse.ArgumentParser(add_help=False)
    store_parent.add_argument("--store", default="",
                              help="Data store name (auto-resolved if only one exists)")

    # -- Store management commands --

    p_create = sub.add_parser("create-store", parents=[token_parent, filespace_parent],
                              help="Create a new data store")
    p_create.add_argument("--bucket", required=True, help="S3 bucket name")
    p_create.add_argument("--region", required=True, help="AWS region (e.g., us-east-1)")
    p_create.add_argument("--access-key", required=True, help="S3 access key ID")
    p_create.add_argument("--secret-key", required=True, help="S3 secret access key")
    p_create.add_argument("--endpoint", default="",
                          help="Custom S3 endpoint URL with scheme (e.g., http://minio:9090)")
    p_create.add_argument("--path-style", action="store_true", help="Use path-style addressing")
    p_create.add_argument("--url-expiration", type=int, default=10080,
                          help="Presigned URL expiration in minutes (default: 10080 = 7 days)")
    p_create.add_argument("--name", default="", help="Custom store name (default: auto-generated)")
    p_create.add_argument("--no-verify", action="store_true", help="Skip S3 bucket verification")
    p_create.add_argument("--dry-run", action="store_true", help="Show what would happen without making changes")

    p_remove = sub.add_parser("remove-store", parents=[token_parent, filespace_parent],
                              help="Remove an empty data store")
    p_remove.add_argument("--store", required=True, help="Data store name")
    p_remove.add_argument("--dry-run", action="store_true", help="Show what would happen without making changes")
    p_remove.add_argument("--yes", "-y", action="store_true", help="Skip confirmation prompt")

    sub.add_parser("list-stores", parents=[token_parent, filespace_parent],
                   help="List all data stores") \
        .add_argument("--no-verify", action="store_true", help="Skip S3 accessibility check")

    p_cleanup = sub.add_parser("cleanup-stores", parents=[token_parent, filespace_parent],
                               help="Remove empty stale stores (add --empty to include healthy empty stores)")
    p_cleanup.add_argument("--empty", action="store_true",
                           help="Also remove empty stores with healthy (accessible) buckets")
    p_cleanup.add_argument("--dry-run", action="store_true", help="Show what would happen without making changes")
    p_cleanup.add_argument("--yes", "-y", action="store_true", help="Skip confirmation prompt")

    p_rekey = sub.add_parser("rekey-store", parents=[token_parent, filespace_parent, store_parent],
                             help="Rotate S3 credentials for a data store")
    p_rekey.add_argument("--access-key", required=True, help="New S3 access key ID")
    p_rekey.add_argument("--secret-key", required=True, help="New S3 secret access key")
    p_rekey.add_argument("--dry-run", action="store_true", help="Show what would happen without making changes")

    # -- File operation commands --

    p_link = sub.add_parser("link", parents=[token_parent, path_parent, store_parent],
                            help="Link a single S3 object as a file")
    p_link.add_argument("--object-key", required=True, help="S3 object key to link")
    p_link.add_argument("--size", type=int, default=None, help="Object size in bytes"
                        "(Access check of the object is skipped if provided, requires --checksum)")
    p_link.add_argument("--checksum", default="", help="Object ETag/checksum (requires --size)")
    p_link.add_argument("--dry-run", action="store_true", help="Show what would happen without making changes")

    p_unlink = sub.add_parser("unlink", parents=[token_parent, path_parent, store_parent],
                              help="Unlink linked files from the filespace")
    p_unlink.add_argument("--all", action="store_true", help="Unlink all linked files under the path")
    p_unlink.add_argument("--dry-run", action="store_true", help="Show what would happen without making changes")
    p_unlink.add_argument("--yes", "-y", action="store_true", help="Skip confirmation prompt")

    p_mirror = sub.add_parser("mirror", parents=[token_parent, path_parent, store_parent],
                              help="Mirror S3 bucket/prefix into filespace (full sync)")
    p_mirror.add_argument("--prefix", default="", help="S3 key prefix to mirror (default: entire bucket)")
    p_mirror.add_argument("--dry-run", action="store_true", help="Show what would happen without making changes")
    p_mirror.add_argument("--no-strip-prefix", action="store_true", help="Keep full S3 key as path")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    # Determine filespace identifier
    if args.command in _FILESPACE_COMMANDS:
        filespace_fqn = args.filespace
    elif args.command in _PATH_COMMANDS:
        parse_lucidlink_url(args.path)  # validate format
        filespace_fqn = _filespace_fqn_from_url(args.path)
    else:
        parser.error(f"Unknown command: {args.command}")
        return

    daemon = None
    try:
        # Connect to filespace
        if args.command in _FILESPACE_COMMANDS:
            daemon, _, filespace = setup_lucidlink_by_filespace(args.token, filespace_fqn)
        else:
            daemon, _, filespace = setup_lucidlink_by_path(args.token, args.path)

        connect = filespace.connect
        if not connect.are_data_stores_available():
            print("ERROR: Connect (external files) is not available on this filespace.")
            print("       Requires filespace version V9+ and the feature must be configured.")
            sys.exit(1)

        # Resolve store for commands that need it
        store_name = None
        config = None

        needs_store = args.command in {"link", "mirror", "rekey-store"}
        needs_store = needs_store or (args.command == "unlink" and getattr(args, "all", False))

        if needs_store:
            store_name, config = resolve_store_auto(
                connect, filespace_fqn, getattr(args, "store", ""),
            )

        _COMMANDS[args.command](args, filespace, connect, config, store_name)

    except KeyboardInterrupt:
        print("\nInterrupted.")
        sys.exit(130)
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        if daemon is not None:
            try:
                daemon.stop()
            except Exception:
                pass


_EPILOG = """\
Examples:
    # Create a data store (only time you provide S3 credentials)
    llconnect create-store --token sa_live:... \\
        --filespace myfiles.myworkspace \\
        --bucket my-bucket --region us-east-1 \\
        --access-key AKIA... --secret-key SECRET

    # List all data stores
    llconnect list-stores --token sa_live:... --filespace myfiles.myworkspace
    llconnect list-stores --token sa_live:... --filespace myfiles.myworkspace --no-verify

    # Remove empty stale stores (default: only inaccessible ones)
    llconnect cleanup-stores --token sa_live:... --filespace myfiles.myworkspace

    # Remove all empty stores (including healthy ones)
    llconnect cleanup-stores --token sa_live:... --filespace myfiles.myworkspace --empty

    # Rekey (rotate credentials)
    llconnect rekey-store --token sa_live:... \\
        --filespace myfiles.myworkspace \\
        --store ct-my-bucket-a1b2c3d4e5f6 \\
        --access-key NEW_KEY --secret-key NEW_SECRET

    # Remove a specific empty data store
    llconnect remove-store --token sa_live:... \\
        --filespace myfiles.myworkspace \\
        --store ct-my-bucket-a1b2c3d4e5f6

    # Link single object (auto-resolves store when only one exists)
    llconnect link --token sa_live:... \\
        --path lucidlink://ws/fs/data/file.csv \\
        --object-key path/in/s3/file.csv

    # Link with known size+checksum (skips access check of the object — faster for bulk)
    llconnect link --token sa_live:... \\
        --path lucidlink://ws/fs/data/file.csv \\
        --object-key path/in/s3/file.csv \\
        --size 1048576 --checksum "d41d8cd98f00b204e9800998ecf8427e"

    # Mirror entire bucket
    llconnect mirror --token sa_live:... \\
        --path lucidlink://ws/fs/datasets

    # Mirror prefix with explicit store and dry run
    llconnect mirror --token sa_live:... \\
        --path lucidlink://ws/fs/datasets \\
        --store ct-my-bucket-a1b2c3d4e5f6 \\
        --prefix data/2024/ --dry-run

    # Unlink all linked files under a path
    llconnect unlink --token sa_live:... \\
        --path lucidlink://ws/fs/datasets \\
        --store ct-my-bucket-a1b2c3d4e5f6 \\
        --all --dry-run
"""
