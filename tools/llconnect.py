#!/usr/bin/env python3
"""LucidLink Connect CLI Tool.

Thin wrapper — delegates to the ``llconnect`` package.
Run directly (``python llconnect.py ...``) or as a module (``python -m llconnect ...``).
"""

from llconnect.cli import main

if __name__ == "__main__":
    main()
