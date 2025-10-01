# core/rules/lengths.py
from __future__ import annotations
from typing import Any, Dict, Tuple

import pandas as pd
import unicodedata
import re


def _sanitize_text(s: str) -> str:
    s = unicodedata.normalize("NFC", s)
    s = re.sub(r"[\u200B-\u200D\uFEFF]", "", s)  # zero-width
    s = re.sub(r"[\x00-\x1F\x7F]", "", s)        # control chars
    return s


def min_length_clear(series: pd.Series, *, min_len: int = 3, examples_limit: int = 25) -> Tuple[pd.Series, Dict[str, Any]]:
    """
    Если длина (после trim по краям и удаления невидимых символов) < min_len — очищаем ячейку.
    """
    original = series.copy()
    s = series.copy()

    def _apply(v):
        if pd.isna(v):
            return v
        x = str(v)
        x = _sanitize_text(x).strip()
        if x == "":
            return x
        if len(x) < int(min_len):
            return ""
        return v  # важно: сохраняем оригинал, чтобы не ломать регистр/формат после предыдущих правил

    s = s.apply(_apply)

    changed_mask = (s.astype("object") != original.astype("object"))
    cleared_mask = (s == "")

    stats: Dict[str, Any] = {
        "total": int(series.shape[0]),
        "changed": int(changed_mask.sum()),
        "cleared": int(cleared_mask.sum()),
        "examples": [],
    }

    # примеры только очищений (они самые показательные)
    for idx in list(s[cleared_mask].index)[:examples_limit]:
        stats["examples"].append({
            "row": int(idx) if isinstance(idx, (int, float)) and pd.notna(idx) else idx,
            "column": "min_length_clear",
            "before": "" if pd.isna(original.loc[idx]) else str(original.loc[idx]),
            "after": "",
            "note": f"cleared (< {min_len} chars)",
        })

    return s, stats
