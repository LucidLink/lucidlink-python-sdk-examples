"""
LucidLink Python SDK — Quickstart

Minimal end-to-end example: create a client, authenticate,
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
    token = os.environ["LUCIDLINK_SA_TOKEN"]
    filespace_name = os.environ["LUCIDLINK_FILESPACE"]

    credentials = lucidlink.ServiceAccountCredentials(token=token)

    with lucidlink.Client() as client:
        client.login(credentials)

        workspace_info = client.list_workspaces()[0]
        workspace = client.get_workspace(workspace_info.id)
        print(f"[1/4] Authenticated to workspace: {workspace.name}")

        filespaces = workspace.list_filespaces()
        print(f"[2/4] Available filespaces: {', '.join(fs.name for fs in filespaces)}")

        filespace_id = next((fs.id for fs in filespaces if fs.name == filespace_name), None)
        if filespace_id is None:
            raise SystemExit(f"Filespace {filespace_name!r} not found")

        with workspace.link_filespace(id=filespace_id) as filespace:
            print(f"[3/4] Linked to filespace: {filespace_name}")
            run(filespace.fs)


if __name__ == "__main__":
    main()
