# core/rules/phone.py
from __future__ import annotations
from typing import Any, Dict, Tuple
import re
import unicodedata
import pandas as pd

def _sanitize(s: str) -> str:
    s = unicodedata.normalize("NFC", s)
    s = re.sub(r"[\u200B-\u200D\uFEFF]", "", s)
    s = re.sub(r"[\x00-\x1F\x7F]", "", s)
    return s

def phone_digits_len(series: pd.Series, *, min_len: int = 9, max_len: int = 13, examples_limit: int = 25) -> Tuple[pd.Series, Dict[str, Any]]:
    original = series.copy()
    s = series.copy()

    def _apply(v):
        if pd.isna(v):
            return v
        x = _sanitize(str(v).strip())
        x = re.sub(r"\D+", "", x)  # оставляем только цифры
        if x == "":
            return x
        if not (min_len <= len(x) <= max_len):
            return ""
        return x

    s = s.apply(_apply)

    changed_mask = (s.astype("object") != original.astype("object"))
    cleared_mask = (s == "")

    stats: Dict[str, Any] = {
        "total": int(series.shape[0]),
        "changed": int(changed_mask.sum()),
        "cleared": int(cleared_mask.sum()),
        "examples": [],
    }

    for idx in list(s[changed_mask & ~cleared_mask].index)[:examples_limit]:
        stats["examples"].append({
            "row": int(idx) if isinstance(idx, (int, float)) and pd.notna(idx) else idx,
            "column": "phone",
            "before": "" if pd.isna(original.loc[idx]) else str(original.loc[idx]),
            "after": "" if pd.isna(s.loc[idx]) else str(s.loc[idx]),
            "note": "normalized",
        })
    if len(stats["examples"]) < examples_limit:
        for idx in list(s[cleared_mask].index)[:examples_limit - len(stats["examples"])]:
            stats["examples"].append({
                "row": int(idx) if isinstance(idx, (int, float)) and pd.notna(idx) else idx,
                "column": "phone",
                "before": "" if pd.isna(original.loc[idx]) else str(original.loc[idx]),
                "after": "",
                "note": f"cleared (length not in [{min_len},{max_len}])",
            })

    return s, stats
