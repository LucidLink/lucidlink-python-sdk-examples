"""Braille spinner with counter for long-running operations."""

import time


class Spinner:
    """Animated counter: ``- Processing... 1234 files``"""

    FRAMES = "\u280b\u2819\u2839\u2838\u283c\u2834\u2826\u2827\u2807\u280f"
    _REFRESH_INTERVAL = 0.2  # seconds

    def __init__(self, label: str = "Processing"):
        self.label = label
        self.count = 0
        self.errors = 0
        self._frame = 0
        self._last_draw = 0.0

    def update(self, n: int = 1) -> None:
        self.count += n
        now = time.monotonic()
        if now - self._last_draw < self._REFRESH_INTERVAL:
            return
        self._last_draw = now
        self._draw()

    def error(self) -> None:
        self.errors += 1

    def finish(self) -> None:
        err = f" ({self.errors} errors)" if self.errors else ""
        print(f"\r  {self.label}: {self.count} files{err}     ")

    def _draw(self) -> None:
        char = self.FRAMES[self._frame % len(self.FRAMES)]
        self._frame += 1
        err = f" ({self.errors} errors)" if self.errors else ""
        print(f"\r  {char} {self.label}... {self.count} files{err}", end="", flush=True)
