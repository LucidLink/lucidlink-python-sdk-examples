"""Data store lifecycle: resolve, validate, count, and list linked files."""

import sys
from typing import Iterator, Tuple

from .s3 import S3Config


def resolve_store(connect, store_name: str, filespace_fqn: str = "") -> Tuple[str, S3Config]:
    """Look up data store, return (name, S3Config) with decrypted credentials."""
    require_data_store(connect, store_name, filespace_fqn)
    info = connect.get_data_store(store_name)
    return store_name, S3Config.from_data_store_info(info)


def resolve_store_auto(
    connect, filespace_fqn: str, explicit_store: str = "",
) -> Tuple[str, S3Config]:
    """Resolve store: use explicit name, or auto-detect if only one exists.

    Stores are per-filespace -- only stores belonging to the connected
    filespace (identified by *filespace_fqn*) are considered.

    Returns (store_name, S3Config).
    """
    if explicit_store:
        return resolve_store(connect, explicit_store, filespace_fqn)

    stores = connect.list_data_stores()
    if len(stores) == 0:
        print(f"ERROR: No data stores found in filespace '{filespace_fqn}'.")
        print("       Create one first with 'create-store'.")
        sys.exit(1)
    if len(stores) == 1:
        name = stores[0].name
        print(f"  Using data store '{name}' (only store in {filespace_fqn})")
        return resolve_store(connect, name, filespace_fqn)

    names = ", ".join(s.name for s in stores)
    print(f"ERROR: Multiple data stores in filespace '{filespace_fqn}'.")
    print(f"       Use --store to specify one: {names}")
    sys.exit(1)


def require_data_store(connect, store_name: str, filespace_fqn: str = "") -> str:
    """Verify store exists in the connected filespace, return name. Exit on error."""
    if connect.get_data_store(store_name) is None:
        ctx = f" in filespace '{filespace_fqn}'" if filespace_fqn else ""
        print(f"ERROR: Data store '{store_name}' not found{ctx}.")
        sys.exit(1)
    return store_name


def count_store_files(connect, store_name: str) -> int:
    """Count all linked files for a data store (fast, no path resolution)."""
    return connect.count_external_files(store_name)


def _paginate(connect, store_name: str, page_size: int = 1000) -> Iterator:
    """Yield ``LinkedFilesResult`` pages one at a time."""
    cursor = ""
    while True:
        page = connect.list_external_files(store_name, limit=page_size, cursor=cursor)
        yield page
        if not page.has_more:
            break
        cursor = page.cursor


def iter_external_files(connect, store_name: str, page_size: int = 1000) -> Iterator[str]:
    """Yield linked file paths one page at a time (never loads all into memory)."""
    for page in _paginate(connect, store_name, page_size):
        yield from page.file_paths


def iter_external_files_with_ids(
    connect, store_name: str, page_size: int = 1000,
) -> Iterator[Tuple[int, str]]:
    """Yield ``(file_id, path)`` pairs, one page at a time."""
    for page in _paginate(connect, store_name, page_size):
        yield from zip(page.file_ids, page.file_paths)
