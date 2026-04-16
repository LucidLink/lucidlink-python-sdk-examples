"""
LucidLink Python SDK — Connect (S3 Data Stores)

Demonstrates managing external S3 data stores and linking external
objects into the filespace as zero-copy references.

Prerequisites:
    pip install lucidlink

Usage:
    export LUCIDLINK_SA_TOKEN="sa_live:your_key"
    export LUCIDLINK_FILESPACE="my-filespace"
    export S3_ENDPOINT="https://s3.us-east-1.amazonaws.com"
    export S3_BUCKET="my-bucket"
    export S3_ACCESS_KEY="AKIA..."
    export S3_SECRET_KEY="..."
    export S3_REGION="us-east-1"
    python 04_connect_s3.py
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


def manage_data_stores(filespace):
    """Add, list, and inspect S3 data stores."""
    print("=== Data Store Management ===")

    connect = filespace.connect

    # Configure S3 data store
    config = lucidlink.S3DataStoreConfig(
        endpoint=os.environ["S3_ENDPOINT"],
        bucket_name=os.environ["S3_BUCKET"],
        access_key=os.environ["S3_ACCESS_KEY"],
        secret_key=os.environ["S3_SECRET_KEY"],
        region=os.environ.get("S3_REGION", "us-east-1"),
    )

    # Add data store
    store_name = "example-store"
    connect.add_data_store(store_name, config)
    print(f"Added data store: {store_name}")

    # List all data stores
    stores = connect.list_data_stores()
    for store in stores:
        print(f"  Store: {store.name}")

    # Get specific store info
    info = connect.get_data_store(store_name)
    if info:
        print(f"  Found: {info.name}")

    return store_name


def link_external_files(filespace, store_name):
    """Link S3 objects into the filespace."""
    print("\n=== Link External Files ===")

    connect = filespace.connect

    # Link an S3 object as a file in the filespace
    # The object is NOT copied — it's a zero-copy reference
    connect.link_file(
        file_path="/report.pdf",
        data_store_name=store_name,
        object_id="reports/2024/q1-report.pdf",
    )
    print("Linked /report.pdf")

    # Link another object
    connect.link_file(
        file_path="/dataset.csv",
        data_store_name=store_name,
        object_id="data/dataset.csv",
    )
    print("Linked /dataset.csv")

    # Synchronize changes before listing all files
    filespace.sync_all()

    # List all external file links
    linked = connect.list_external_files(data_store_name=store_name)
    for item in linked.file_paths:
        print(f"  {item}")


def cleanup(filespace, store_name):
    """Remove linked files and data store."""
    print("\n=== Cleanup ===")

    connect = filespace.connect

    connect.unlink_file("/report.pdf")
    connect.unlink_file("/dataset.csv")
    print("Unlinked external files")

    # Synchronize changes before removing the datastore
    filespace.sync_all()
    connect.remove_data_store(store_name)
    print(f"Removed data store: {store_name}")


def main():
    daemon, filespace = setup()

    try:
        store_name = manage_data_stores(filespace)
        link_external_files(filespace, store_name)
        cleanup(filespace, store_name)
    finally:
        filespace.unlink()
        daemon.stop()


if __name__ == "__main__":
    main()
