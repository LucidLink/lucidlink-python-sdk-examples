# LucidLink Python SDK Examples

Example scripts and tools for the [LucidLink Python SDK](https://pypi.org/project/lucidlink/).

## Prerequisites

- Python 3.10+
- A LucidLink filespace with a service account token

```bash
pip install -r requirements.txt
```

## Configuration

Set your credentials as environment variables:

```bash
export LUCIDLINK_SA_TOKEN="sa_live:your_key"
export LUCIDLINK_FILESPACE="my-filespace"
export LUCIDLINK_WORKSPACE="my-workspace"  # required by fsspec examples
```

## Running Examples

Start with the quickstart and work through the numbered tutorials:

```bash
python examples/01_quickstart.py
```

The `examples/` directory contains step-by-step tutorials.

## Links

- [LucidLink Python SDK on PyPI](https://pypi.org/project/lucidlink/)
- [SDK Documentation](https://lucidlink.github.io/lucidlink-python-sdk-examples/)
- [Support](https://support.lucidlink.com/)
