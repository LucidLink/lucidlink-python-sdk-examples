"""
LucidLink Python SDK — File & Directory Operations

Demonstrates all file and directory operations available through
the Filesystem interface.

Prerequisites:
    pip install lucidlink

Usage:
    export LUCIDLINK_SA_TOKEN="sa_live:your_key"
    export LUCIDLINK_FILESPACE="my-filespace"
    python 02_file_operations.py
"""

import os
import lucidlink


DEMO_DIR = "/examples_demo"


def setup():
    """Create daemon, authenticate, and link to filespace."""
    token = os.environ["LUCIDLINK_SA_TOKEN"]
    filespace_name = os.environ["LUCIDLINK_FILESPACE"]

    daemon = lucidlink.create_daemon()
    daemon.start()

    credentials = lucidlink.ServiceAccountCredentials(token=token)
    workspace = daemon.authenticate(credentials)
    filespace = workspace.link_filespace(name=filespace_name)
    filespace.fs.create_dir(DEMO_DIR)

    return daemon, filespace


def directory_operations(fs):
    """Create, list, check, and delete directories."""
    print("=== Directory Operations ===")

    # Create directories (including nested)
    fs.create_dir(f"{DEMO_DIR}/subdir")
    print(f"Created {DEMO_DIR}/subdir")

    # Check existence
    print(f"{DEMO_DIR} exists: {fs.dir_exists(DEMO_DIR)}")
    print(f"/nonexistent exists: {fs.dir_exists('/nonexistent')}")

    # List directory contents (full metadata)
    entries = fs.read_dir(DEMO_DIR)
    for entry in entries:
        print(f"  {entry.name} (is_dir={entry.is_dir()})")

    # List directory contents (names only)
    names = fs.list_dir(DEMO_DIR)
    print(f"Names: {names}")

    # Delete empty directory
    fs.delete_dir(f"{DEMO_DIR}/subdir", recursive=False)
    print(f"Deleted {DEMO_DIR}/subdir")

    # Delete directory with contents
    fs.create_dir(f"{DEMO_DIR}/with_files")
    fs.write_file(f"{DEMO_DIR}/with_files/a.txt", b"content")
    fs.delete_dir(f"{DEMO_DIR}/with_files", recursive=True)
    print(f"Deleted {DEMO_DIR}/with_files recursively")


def file_read_write(fs):
    """Write and read files using convenience methods and streaming."""
    print("\n=== Write & Read ===")

    # Convenience methods (entire file at once)
    fs.write_file(f"{DEMO_DIR}/data.bin", b"binary content here")
    content = fs.read_file(f"{DEMO_DIR}/data.bin")
    print(f"read_file: {content}")

    # Streaming — binary mode
    with fs.open(f"{DEMO_DIR}/streamed.bin", "wb") as f:
        f.write(b"chunk 1 ")
        f.write(b"chunk 2 ")
        f.write(b"chunk 3")

    with fs.open(f"{DEMO_DIR}/streamed.bin", "rb") as f:
        print(f"Streamed read: {f.read()}")

    # Streaming — text mode (bare 'w'/'r' defaults to text with UTF-8)
    with fs.open(f"{DEMO_DIR}/text.txt", "w") as f:
        f.write("Hello, world! café 你好 🚀\n")

    with fs.open(f"{DEMO_DIR}/text.txt", "r") as f:
        text = f.read()
        print(f"Text read: {text!r}")
        assert isinstance(text, str)


def file_metadata(fs):
    """Get file/directory metadata and filespace statistics."""
    print("\n=== Metadata ===")

    entry = fs.get_entry(f"{DEMO_DIR}/data.bin")
    print(f"Entry: name={entry.name}, size={entry.size}, "
          f"is_file={entry.is_file()}, is_dir={entry.is_dir()}")

    print(f"file_exists: {fs.file_exists(f'{DEMO_DIR}/data.bin')}")
    print(f"dir_exists: {fs.dir_exists(DEMO_DIR)}")

    size_info = fs.get_size()
    print(f"Filespace size: {size_info}")

    stats = fs.get_statistics()
    print(f"Statistics: {stats}")


def move_and_truncate(fs):
    """Move/rename files and truncate."""
    print("\n=== Move & Truncate ===")

    fs.write_file(f"{DEMO_DIR}/original.txt", b"move me")

    # Rename
    fs.move(f"{DEMO_DIR}/original.txt", f"{DEMO_DIR}/renamed.txt")
    print(f"Moved. Exists at new path: {fs.file_exists(f'{DEMO_DIR}/renamed.txt')}")

    # Truncate
    fs.write_file(f"{DEMO_DIR}/trunc.txt", b"Hello, World!")
    fs.truncate(f"{DEMO_DIR}/trunc.txt", 5)
    print(f"After truncate to 5: {fs.read_file(f'{DEMO_DIR}/trunc.txt')}")


def cleanup(fs):
    """Remove all demo files."""
    fs.delete_dir(DEMO_DIR, recursive=True)
    print(f"\nCleaned up {DEMO_DIR}")


def main():
    daemon, filespace = setup()
    fs = filespace.fs

    try:
        directory_operations(fs)
        file_read_write(fs)
        file_metadata(fs)
        move_and_truncate(fs)
        cleanup(fs)
    finally:
        filespace.unlink()
        daemon.stop()


if __name__ == "__main__":
    main()
