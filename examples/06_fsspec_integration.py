"""
LucidLink Python SDK — Pandas Integration

Demonstrates two ways to use Pandas with LucidLink:

1. Direct API — create a daemon, authenticate, link, and pass file handles
2. fsspec storage_options — just URLs and storage_options, no daemon management

Prerequisites:
    pip install lucidlink[fsspec] pandas pyarrow

Usage:
    export LUCIDLINK_SA_TOKEN="sa_live:your_key"
    export LUCIDLINK_WORKSPACE="my-workspace"
    export LUCIDLINK_FILESPACE="my-filespace"
    python 06_fsspec_integration.py
"""

import os

import fsspec
import pandas as pd

import lucidlink

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

DEMO_DIR = "/pandas_demo"


def sample_people():
    return pd.DataFrame({
        "name": ["Alice", "Bob", "Charlie"],
        "age": [30, 25, 35],
        "city": ["NYC", "London", "Tokyo"],
    })


def sample_timeseries():
    return pd.DataFrame({
        "timestamp": pd.date_range("2024-01-01", periods=100, freq="h"),
        "value": range(100),
        "category": ["A", "B", "C", "D"] * 25,
    })


def sample_events():
    return pd.DataFrame({
        "id": [1, 2, 3],
        "event": ["login", "purchase", "logout"],
        "amount": [None, 99.99, None],
    })


# ---------------------------------------------------------------------------
# Approach 1: Direct API — explicit daemon, file handles
# ---------------------------------------------------------------------------

def direct_api_examples():
    """Use the SDK directly: create daemon, authenticate, link, open files."""
    print("\n========== Direct API ==========\n")

    token = os.environ["LUCIDLINK_SA_TOKEN"]
    filespace_name = os.environ["LUCIDLINK_FILESPACE"]

    daemon = lucidlink.create_daemon()
    daemon.start()

    credentials = lucidlink.ServiceAccountCredentials(token=token)
    workspace = daemon.authenticate(credentials)

    with workspace.link_filespace(name=filespace_name) as filespace:
        fs = filespace.fs
        fs.create_dir(DEMO_DIR)

        # CSV
        print("=== CSV ===")
        df = sample_people()
        with fs.open(f"{DEMO_DIR}/people.csv", "wb") as f:
            df.to_csv(f, index=False)
        with fs.open(f"{DEMO_DIR}/people.csv", "rb") as f:
            print(pd.read_csv(f))

        # Parquet
        print("\n=== Parquet ===")
        df = sample_timeseries()
        with fs.open(f"{DEMO_DIR}/timeseries.parquet", "wb") as f:
            df.to_parquet(f, engine="pyarrow", compression="snappy")
        with fs.open(f"{DEMO_DIR}/timeseries.parquet", "rb") as f:
            df_read = pd.read_parquet(f, engine="pyarrow")
        print(f"{len(df_read)} rows, columns={list(df_read.columns)}")

        # JSON Lines
        print("\n=== JSON Lines ===")
        df = sample_events()
        with fs.open(f"{DEMO_DIR}/events.jsonl", "w") as f:
            df.to_json(f, orient="records", lines=True)
        with fs.open(f"{DEMO_DIR}/events.jsonl", "r") as f:
            print(pd.read_json(f, orient="records", lines=True))

        # Chunked CSV
        print("\n=== Chunked CSV ===")
        df = pd.DataFrame({"id": range(10000), "value": [i * 0.1 for i in range(10000)]})
        with fs.open(f"{DEMO_DIR}/large.csv", "wb") as f:
            df.to_csv(f, index=False)
        total_rows, total_sum = 0, 0.0
        with fs.open(f"{DEMO_DIR}/large.csv", "rb") as f:
            for chunk in pd.read_csv(f, chunksize=2000):
                total_rows += len(chunk)
                total_sum += chunk["value"].sum()
        print(f"Processed {total_rows} rows in chunks, sum={total_sum:.1f}")

        # Cleanup
        fs.delete_dir(DEMO_DIR, recursive=True)
        print("\nCleaned up")

    daemon.stop()


# ---------------------------------------------------------------------------
# Approach 2: fsspec storage_options — URLs only, no daemon management
# ---------------------------------------------------------------------------

def fsspec_examples():
    """Use Pandas storage_options with lucidlink:// URLs — no daemon code needed."""
    print("\n========== fsspec storage_options ==========\n")

    token = os.environ["LUCIDLINK_SA_TOKEN"]
    workspace = os.environ["LUCIDLINK_WORKSPACE"]
    filespace = os.environ["LUCIDLINK_FILESPACE"]

    storage_opts = {"token": token}

    base = f"lucidlink://{workspace}/{filespace}{DEMO_DIR}"

    # CSV
    print("=== CSV ===")
    df = sample_people()
    df.to_csv(f"{base}/people.csv", index=False, storage_options=storage_opts)
    print(pd.read_csv(f"{base}/people.csv", storage_options=storage_opts))

    # Parquet
    print("\n=== Parquet ===")
    df = sample_timeseries()
    df.to_parquet(f"{base}/timeseries.parquet", index=False, storage_options=storage_opts)
    df_read = pd.read_parquet(f"{base}/timeseries.parquet", storage_options=storage_opts)
    print(f"{len(df_read)} rows, columns={list(df_read.columns)}")

    # JSON Lines
    print("\n=== JSON Lines ===")
    df = sample_events()
    df.to_json(f"{base}/events.jsonl", orient="records", lines=True, storage_options=storage_opts)
    print(pd.read_json(f"{base}/events.jsonl", orient="records", lines=True, storage_options=storage_opts))

    # Cleanup
    fs = fsspec.filesystem("lucidlink", **storage_opts)
    fs.rm(base, recursive=True)
    fs.close()
    print("\nCleaned up")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    direct_api_examples()
    fsspec_examples()


if __name__ == "__main__":
    main()
