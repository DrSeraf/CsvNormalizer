# core/io/reader.py
from __future__ import annotations
from typing import Iterator, Optional
import pandas as pd


def read_csv_in_chunks(
    path: str,
    *,
    delimiter: str = ",",
    encoding: str = "auto",
    chunksize: int = 100_000,
) -> Iterator[pd.DataFrame]:
    encodings_to_try = ["utf-8", "cp1251"] if encoding == "auto" else [encoding]
    last_err: Optional[Exception] = None
    for enc in encodings_to_try:
        try:
            for chunk in pd.read_csv(
                path,
                sep=delimiter,
                encoding=enc,
                chunksize=chunksize,
                dtype=str,           # всё строками — безопасно для нормализации
                keep_default_na=False,
            ):
                yield chunk
            return
        except Exception as e:
            last_err = e
            continue
    raise last_err if last_err else RuntimeError("Failed to read CSV")
