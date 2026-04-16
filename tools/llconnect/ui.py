"""Shared CLI helpers: confirmation prompts, progress display, S3 verification."""

from .s3 import S3Client, S3Config


def confirm(message: str, auto_yes: bool) -> bool:
    """Prompt user for confirmation. Returns True if confirmed."""
    if auto_yes:
        return True
    try:
        answer = input(f"{message} [y/N] ").strip().lower()
    except EOFError:
        return False
    return answer in ("y", "yes")


def print_progress(label: str, current: int, total: int) -> None:
    """Print an in-place percentage progress line."""
    pct = (current * 100) // total if total else 0
    print(f"\r{label}: {pct}%", end="", flush=True)


def print_progress_done(label: str) -> None:
    """Print the final 100% progress line."""
    print(f"\r{label}: 100%")


def verify_store_status(connect, store) -> str:
    """Verify S3 bucket accessibility for a store. Returns status string."""
    try:
        info = connect.get_data_store(store.name)
        s3_config = S3Config.from_data_store_info(info)
        s3 = S3Client(s3_config, connect_timeout=5, read_timeout=10)
        s3.verify_bucket()
        return "OK"
    except Exception as e:
        return _format_s3_error(e)


def _format_s3_error(e: Exception) -> str:
    """Extract a short error message from a botocore ClientError."""
    error_msg = str(e)
    if "Error" in error_msg and "Code" in error_msg:
        try:
            start = error_msg.index("(") + 1
            end = error_msg.index(")")
            return f"ERROR: {error_msg[start:end]}"
        except ValueError:
            pass
    return f"ERROR: {error_msg[:60]}"
