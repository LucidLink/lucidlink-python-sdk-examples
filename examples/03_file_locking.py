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


def setup():
    token = os.environ["LUCIDLINK_SA_TOKEN"]
    filespace_name = os.environ["LUCIDLINK_FILESPACE"]

    daemon = lucidlink.create_daemon()
    daemon.start()

    credentials = lucidlink.ServiceAccountCredentials(token=token)
    workspace = daemon.authenticate(credentials)
    filespace = workspace.link_filespace(name=filespace_name)

    return daemon, filespace


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
    daemon, filespace = setup()
    fs = filespace.fs

    try:
        fs.create_dir("/locking_demo")

        shared_lock_example(fs)
        exclusive_lock_example(fs)
        safe_update_pattern(fs)
        cleanup(fs)
    finally:
        filespace.unlink()
        daemon.stop()


if __name__ == "__main__":
    main()
