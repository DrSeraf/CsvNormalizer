# core/rules/names.py
from __future__ import annotations
from typing import Any, Dict, List, Tuple
import re
import unicodedata
import pandas as pd

APOSTROPHES = "'’‘‛`´"
HYPHENS = "-–—"

def _sanitize_invisible_text(x: str) -> str:
    x = unicodedata.normalize("NFC", x)
    x = re.sub(r"[\u200B-\u200D\uFEFF]", "", x)      # zero-width + BOM
    x = re.sub(r"[\x00-\x1F\x7F]", "", x)            # control chars
    return x

def _normalize_name(value: Any, min_letters: int) -> str:
    if pd.isna(value):
        return value
    s = str(value)

    # обрезаем по краям, внутри пробелы НЕ трогаем
    s = s.strip()
    if s == "":
        return s

    # убираем невидимые
    s = _sanitize_invisible_text(s)

    # дефисы/апострофы → один пробел (НЕ схлопываем соседние пробелы)
    trans = str.maketrans({**{c: " " for c in APOSTROPHES}, **{c: " " for c in HYPHENS}})
    s = s.translate(trans)

    # удаляем всё, что не буквы/пробел
    s = "".join(ch for ch in s if (ch.isalpha() or ch == " "))

    # снова трим по краям (вдруг в начале/конце остались пробелы)
    s = s.strip()

    # Title Case (каждое слово с заглавной), пробелы внутри НЕ трогаем
    # split/join не используем, чтобы не схлопывать пробелы — применим по словам вручную
    def title_piece(piece: str) -> str:
        return piece[:1].upper() + piece[1:].lower() if piece else piece

    # обработаем подряд идущие пробелы, не меняя их кол-во
    out_chars: List[str] = []
    word: List[str] = []
    for ch in s:
        if ch == " ":
            if word:
                w = "".join(word)
                out_chars.append(title_piece(w))
                word = []
            out_chars.append(" ")
        else:
            word.append(ch)
    if word:
        out_chars.append(title_piece("".join(word)))
    s = "".join(out_chars)

    # считаем только буквы (без пробелов)
    letters_count = sum(1 for ch in s if ch.isalpha())
    if letters_count < min_letters:
        return ""

    return s

def name_basic(series: pd.Series, *, min_letters: int = 3, examples_limit: int = 25) -> Tuple[pd.Series, Dict[str, Any]]:
    original = series.copy()
    s = series.copy()

    def _apply(v): return _normalize_name(v, min_letters=min_letters)
    s = s.apply(_apply)

    changed_mask = (s.astype("object") != original.astype("object"))
    cleared_mask = (s == "")

    stats: Dict[str, Any] = {
        "total": int(series.shape[0]),
        "changed": int(changed_mask.sum()),
        "cleared": int(cleared_mask.sum()),
        "examples": [],
    }

    # Примеры: нормализованные и очищенные
    for idx in list(s[changed_mask & ~cleared_mask].index)[:examples_limit]:
        stats["examples"].append({
            "row": int(idx) if isinstance(idx, (int, float)) and pd.notna(idx) else idx,
            "column": "name",
            "before": "" if pd.isna(original.loc[idx]) else str(original.loc[idx]),
            "after": "" if pd.isna(s.loc[idx]) else str(s.loc[idx]),
            "note": "normalized",
        })
    if len(stats["examples"]) < examples_limit:
        for idx in list(s[cleared_mask].index)[:examples_limit - len(stats["examples"])]:
            stats["examples"].append({
                "row": int(idx) if isinstance(idx, (int, float)) and pd.notna(idx) else idx,
                "column": "name",
                "before": "" if pd.isna(original.loc[idx]) else str(original.loc[idx]),
                "after": "",
                "note": "cleared (too short / invalid)",
            })

    return s, stats
