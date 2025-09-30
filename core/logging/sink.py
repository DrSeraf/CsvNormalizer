# core/logging/sink.py
from __future__ import annotations
import io
import os


class LogSink:
    def __init__(self, path: str):
        self.path = path
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self._fh: io.TextIOWrapper = open(self.path, "w", encoding="utf-8")

    def write(self, text: str) -> None:
        self._fh.write(text)
        if not text.endswith("\n"):
            self._fh.write("\n")
        self._fh.flush()

    def close(self) -> None:
        try:
            self._fh.close()
        except Exception:
            pass
