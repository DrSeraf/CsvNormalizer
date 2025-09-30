# core/rules/birthdate.py
from __future__ import annotations
from typing import Any, Dict, List, Tuple
import re
import unicodedata
import pandas as pd

def _sanitize(v: Any) -> str:
    if pd.isna(v):
        return v
    s = str(v).strip()
    s = unicodedata.normalize("NFC", s)
    s = re.sub(r"[\u200B-\u200D\uFEFF]", "", s)
    s = re.sub(r"[\x00-\x1F\x7F]", "", s)
    return s

def _expand_year(yy: int, pivot: int) -> int:
    # 00–pivot-1 -> 2000..20(pivot-1); pivot..99 -> 1900..1999
    if yy < pivot:
        return 2000 + yy
    return 1900 + yy

def _parse_to_ddmmyyyy(s: str, pivot_year: int) -> str | None:
    if s == "":
        return s
    # если есть буквы — сразу невалидно
    if any(ch.isalpha() for ch in s):
        return None

    # выделим все группы цифр
    parts = re.findall(r"\d+", s)

    d = m = y = None

    if len(parts) == 3:
        a, b, c = parts
        if len(a) == 4:  # Y M D
            y, m, d = a, b, c
        else:            # D M Y
            d, m, y = a, b, c
    elif len(parts) == 1 and (len(parts[0]) in (6, 8)):
        g = parts[0]
        if len(g) == 8:
            if 1900 <= int(g[:4]) <= 2099:
                y, m, d = g[:4], g[4:6], g[6:]
            else:
                d, m, y = g[:2], g[2:4], g[4:]
        else:  # 6
            d, m, y = g[:2], g[2:4], g[4:]
    else:
        return None

    # нормализуем длины
    if len(d) == 1: d = d.zfill(2)
    if len(m) == 1: m = m.zfill(2)
    if len(y) == 2:
        y = str(_expand_year(int(y), pivot_year)).zfill(4)
    elif len(y) == 3:
        # странный случай, считаем невалидным
        return None
    elif len(y) == 4:
        pass
    else:
        return None

    # на этом этапе НЕ валидируем календарно
    # приведение к dd/mm/yyyy
    return f"{d.zfill(2)}/{m.zfill(2)}/{y}"

def birthdate_basic(series: pd.Series, *, pivot_year: int = 25, examples_limit: int = 25) -> Tuple[pd.Series, Dict[str, Any]]:
    original = series.copy()
    s = series.copy()

    def _apply(v):
        v2 = _sanitize(v)
        if pd.isna(v2) or v2 == "":
            return v2
        out = _parse_to_ddmmyyyy(v2, pivot_year=pivot_year)
        return out if out is not None else ""

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
            "column": "birthdate",
            "before": "" if pd.isna(original.loc[idx]) else str(original.loc[idx]),
            "after": "" if pd.isna(s.loc[idx]) else str(s.loc[idx]),
            "note": "normalized",
        })
    if len(stats["examples"]) < examples_limit:
        for idx in list(s[cleared_mask].index)[:examples_limit - len(stats["examples"])]:
            stats["examples"].append({
                "row": int(idx) if isinstance(idx, (int, float)) and pd.notna(idx) else idx,
                "column": "birthdate",
                "before": "" if pd.isna(original.loc[idx]) else str(original.loc[idx]),
                "after": "",
                "note": "cleared (invalid / letters present / unparsable)",
            })

    return s, stats
