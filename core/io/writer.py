# core/io/writer.py
from __future__ import annotations
import os
import pandas as pd


class CsvIncrementalWriter:
    def __init__(self, path: str):
        self.path = path
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        # очистим файл при создании
        open(self.path, "w", encoding="utf-8").close()

    def write_chunk(self, df: pd.DataFrame, *, header: bool) -> None:
        df.to_csv(
            self.path,
            mode="a",
            header=header,
            index=False,
            encoding="utf-8",
        )
