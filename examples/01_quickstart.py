"""
LucidLink Python SDK — Quickstart

Minimal end-to-end example: create a daemon, authenticate,
link to a filespace, read/write files, and clean up.

Prerequisites:
    pip install lucidlink

Usage:
    export LUCIDLINK_SA_TOKEN="sa_live:your_key"
    export LUCIDLINK_FILESPACE="my-filespace"
    python 01_quickstart.py
"""

import os
import lucidlink


def setup():
    """Create daemon, authenticate, and link to filespace."""
    token = os.environ["LUCIDLINK_SA_TOKEN"]
    filespace_name = os.environ["LUCIDLINK_FILESPACE"]

    daemon = lucidlink.create_daemon()
    daemon.start()

    credentials = lucidlink.ServiceAccountCredentials(token=token)
    workspace = daemon.authenticate(credentials)

    print(f"[1/4] Authenticated to workspace: {workspace.name}")

    filespaces = workspace.list_filespaces()
    print(f"[2/4] Available filespaces: {', '.join(fs.name for fs in filespaces)}")

    filespace = workspace.link_filespace(name=filespace_name)
    print(f"[3/4] Linked to filespace: {filespace_name}")

    return daemon, filespace


def run(fs):
    """Write a file, read it back, and list the root directory."""
    print("[4/4] Running demo...")

    with fs.open("/hello.txt", "w") as f:
        f.write("Hello from LucidLink Python SDK!")
    print("  Wrote /hello.txt")

    with fs.open("/hello.txt", "r") as f:
        print(f"  Read back: {f.read()}")

    entries = fs.read_dir("/")
    print(f"  Root directory ({len(entries)} entries):")
    for entry in entries:
        kind = "DIR " if entry.is_dir() else "FILE"
        print(f"    {kind} {entry.name} ({entry.size} bytes)")

    fs.delete("/hello.txt")
    print("  Cleaned up /hello.txt")


def main():
    daemon, filespace = setup()

    try:
        run(filespace.fs)
    finally:
        filespace.unlink()
        daemon.stop()


if __name__ == "__main__":
    main()
