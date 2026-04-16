"""URL parsing and filespace path helpers."""

import posixpath
from typing import Tuple
from urllib.parse import urlparse


def parse_lucidlink_url(url: str) -> Tuple[str, str, str]:
    """Parse ``lucidlink://workspace/filespace/path`` into components.

    Returns:
        ``(workspace, filespace, file_path)`` where *file_path* starts with ``/``.
    """
    parsed = urlparse(url)
    if parsed.scheme != "lucidlink":
        raise ValueError(f"Expected lucidlink:// URL, got: {url}")
    workspace = parsed.netloc
    parts = parsed.path.strip("/").split("/", 1)
    if not parts or not parts[0]:
        raise ValueError(f"URL must include filespace: {url}")
    filespace = parts[0]
    file_path = "/" + parts[1] if len(parts) > 1 else "/"
    return workspace, filespace, file_path


def join_filespace_path(base: str, rel: str) -> str:
    """Join a base filespace path with a relative key, ensuring ``/`` prefix."""
    base = base.rstrip("/")
    rel = rel.lstrip("/")
    if not rel:
        return base
    return f"{base}/{rel}"


def ensure_parent_dirs(filespace, file_path: str) -> None:
    """Create parent directories for *file_path* if they don't exist (mkdir -p)."""
    parent = posixpath.dirname(file_path)
    if not parent or parent == "/":
        return
    parts = [p for p in parent.split("/") if p]
    current = ""
    for part in parts:
        current = current + "/" + part
        try:
            filespace.fs.create_dir(current)
        except FileExistsError:
            pass
