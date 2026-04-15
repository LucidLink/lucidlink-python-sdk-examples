"""
LucidLink Python SDK — fsspec Filesystem Operations

Demonstrates using LucidLink as an fsspec-compatible filesystem:
file I/O, directory operations, metadata, move/rename, and URL-style access.

Prerequisites:
    pip install lucidlink[fsspec]

Usage:
    export LUCIDLINK_SA_TOKEN="sa_live:your_key"
    export LUCIDLINK_WORKSPACE="my-workspace"
    export LUCIDLINK_FILESPACE="my-filespace"
    python 05_fsspec_operations.py
"""

import os
import fsspec


def _get_config():
    """Read common config from environment variables."""
    token = os.environ["LUCIDLINK_SA_TOKEN"]
    workspace = os.environ["LUCIDLINK_WORKSPACE"]
    filespace = os.environ["LUCIDLINK_FILESPACE"]

    # fsspec URLs require workspace/filespace format
    filespace_url = f"{workspace}/{filespace}"

    opts = {"token": token}

    return filespace_url, opts


def file_io(fs, base):
    """Write and read files."""
    print("=== File I/O ===")

    fs.makedirs(base, exist_ok=True)

    with fs.open(f"{base}/hello.txt", "wb") as f:
        f.write(b"Hello from fsspec!")

    with fs.open(f"{base}/hello.txt", "rb") as f:
        print(f"  Read: {f.read()}")


def metadata(fs, base):
    """File info, existence checks, and directory listing."""
    print("\n=== Metadata ===")

    info = fs.info(f"{base}/hello.txt")
    print(f"  Info: size={info['size']}, type={info['type']}")
    print(f"  Exists: {fs.exists(f'{base}/hello.txt')}")
    print(f"  Is file: {fs.isfile(f'{base}/hello.txt')}")
    print(f"  Is dir: {fs.isdir(base)}")

    contents = fs.ls(base)
    print(f"  Contents: {contents}")


def directory_ops(fs, base):
    """Create and remove directories."""
    print("\n=== Directory Operations ===")

    fs.makedirs(f"{base}/nested/deep", exist_ok=True)
    print(f"  Created nested dirs: {fs.isdir(f'{base}/nested/deep')}")

    fs.rm(f"{base}/nested", recursive=True)
    print(f"  Removed: exists={fs.exists(f'{base}/nested')}")


def move_rename(fs, base):
    """Move and rename files."""
    print("\n=== Move / Rename ===")

    with fs.open(f"{base}/src.txt", "wb") as f:
        f.write(b"move me")

    fs.mv(f"{base}/src.txt", f"{base}/dst.txt")
    print(f"  Moved: src={fs.exists(f'{base}/src.txt')}, dst={fs.exists(f'{base}/dst.txt')}")


def url_style_access(opts, base):
    """Use lucidlink:// URLs with fsspec.open() directly."""
    print("\n=== URL-style Access (fsspec.open) ===")

    url = f"{base}/url_test.txt"

    with fsspec.open(url, "wb", **opts) as f:
        f.write(b"Written via fsspec.open(url)")

    with fsspec.open(url, "rb", **opts) as f:
        print(f"  Read via URL: {f.read()}")


def main():
    filespace, opts = _get_config()
    base = f"lucidlink://{filespace}/fsspec_ops_demo"

    fs = fsspec.filesystem("lucidlink", **opts)

    try:
        file_io(fs, base)
        metadata(fs, base)
        directory_ops(fs, base)
        move_rename(fs, base)
        url_style_access(opts, base)

        # Cleanup
        fs.rm(base, recursive=True)
        print("\nCleaned up")
    finally:
        fs.close()


if __name__ == "__main__":
    main()
