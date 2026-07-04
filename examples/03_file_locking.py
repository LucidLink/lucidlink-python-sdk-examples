"""
LucidLink Python SDK — File Locking

Demonstrates shared and exclusive file locking for concurrent access control.

- Shared locks allow multiple readers, block writers
- Exclusive locks block all other access
- Locks are held for the lifetime of the file handle

Prerequisites:
    pip install lucidlink

Usage:
    export LUCIDLINK_SA_TOKEN="sa_live:your_key"
    export LUCIDLINK_FILESPACE="my-filespace"
    python 03_file_locking.py
"""

import os
import lucidlink


def shared_lock_example(fs):
    """Shared lock — multiple readers can hold simultaneously."""
    print("=== Shared Lock (Read) ===")

    fs.write_file("/locking_demo/shared.txt", b"shared content")

    with fs.open("/locking_demo/shared.txt", "rb", lock_type="shared") as f:
        data = f.read()
        print(f"Read with shared lock: {data}")
        # Other readers can also acquire shared locks on this file
        # Writers will block until all shared locks are released


def exclusive_lock_example(fs):
    """Exclusive lock — only one handle can access the file."""
    print("\n=== Exclusive Lock (Write) ===")

    with fs.open("/locking_demo/exclusive.txt", "wb", lock_type="exclusive") as f:
        f.write(b"exclusively written")
        print("Writing with exclusive lock held")
        # No other process can read or write this file until we exit

    with fs.open("/locking_demo/exclusive.txt", "rb") as f:
        print(f"After release: {f.read()}")


def safe_update_pattern(fs):
    """Common pattern: exclusive lock for read-modify-write."""
    print("\n=== Safe Update Pattern ===")

    # Create initial file
    fs.write_file("/locking_demo/counter.txt", b"0")

    # Read-modify-write with exclusive lock
    with fs.open("/locking_demo/counter.txt", "r+b", lock_type="exclusive") as f:
        value = int(f.read())
        print(f"Current value: {value}")

        new_value = value + 1
        f.seek(0)
        f.write(str(new_value).encode())
        f.truncate()  # Remove any leftover bytes if new value is shorter
        print(f"Updated to: {new_value}")

    # Verify
    with fs.open("/locking_demo/counter.txt", "rb") as f:
        print(f"Verified: {f.read()}")


def cleanup(fs):
    fs.delete_dir("/locking_demo", recursive=True)
    print("\nCleaned up /locking_demo")


def main():
    token = os.environ["LUCIDLINK_SA_TOKEN"]
    filespace_name = os.environ["LUCIDLINK_FILESPACE"]

    credentials = lucidlink.ServiceAccountCredentials(token=token)

    with lucidlink.Client() as client:
        client.login(credentials)

        workspace_info = client.list_workspaces()[0]
        workspace = client.get_workspace(workspace_info.id)

        filespaces = workspace.list_filespaces()
        filespace_id = next((fs.id for fs in filespaces if fs.name == filespace_name), None)
        if filespace_id is None:
            raise SystemExit(f"Filespace {filespace_name!r} not found")

        with workspace.link_filespace(id=filespace_id) as filespace:
            fs = filespace.fs
            fs.create_dir("/locking_demo")

            shared_lock_example(fs)
            exclusive_lock_example(fs)
            safe_update_pattern(fs)
            cleanup(fs)


if __name__ == "__main__":
    main()
