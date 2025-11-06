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

    def _looks_like_romanized_east_asian_name(x: str) -> bool:
        # Keep short CJK strings outright
        if re.search(r"[\u4E00-\u9FFF\u3040-\u30FF\uAC00-\uD7AF]", x):
            return True

        y = x.lower()
        y = re.sub(r"[-']", " ", y)
        parts = [p for p in y.split() if p]
        if 1 <= len(parts) <= 3 and all(re.fullmatch(r"[a-z]{1,4}", p or "") for p in parts):
            # Two or three short latin tokens like "wo li", "lu an"
            if len(parts) >= 2 and all(1 <= len(p) <= 3 for p in parts):
                return True
            # Single-token common short surnames/given names
            common_single = {
                "li", "wu", "xu", "yu", "su", "hu", "ho", "lu", "lo", "ng",
                "do", "to", "ko", "ma", "an", "bo", "xi", "qi", "he", "le",
            }
            if len(parts) == 1 and parts[0] in common_single:
                return True
        return False

    def _apply(v):
        if pd.isna(v):
            return v
        x = str(v)
        x = _sanitize_text(x).strip()
        if x == "":
            return x
        if len(x) < int(min_len):
            # Do not clear probable romanized East-Asian short names (e.g., "wo li")
            if _looks_like_romanized_east_asian_name(x):
                return v
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
