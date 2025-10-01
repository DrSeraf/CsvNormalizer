# core/rules/email.py
from __future__ import annotations
from typing import Any, Dict, Tuple
import re
import unicodedata
import pandas as pd

# ────────────────── утилиты ──────────────────
# удаляем zero-width, BOM и управляющие
_ZW_RE = re.compile(r"[\u200B-\u200D\uFEFF]")
_CTRL_RE = re.compile(r"[\x00-\x1F\x7F]")
# удаление любых пробельных символов (пробел, таб, \r, \n, NBSP и т.п.)
_ALL_WS_RE = re.compile(r"\s+", flags=re.UNICODE)

def strip_whitespace(series: pd.Series) -> pd.Series:
    """Обрезать пробелы по краям (примитив, без статистики)."""
    return series.apply(lambda v: (str(v).strip() if pd.notna(v) else v))

def sanitize_invisible(series: pd.Series) -> pd.Series:
    """Удалить невидимые и управляющие символы (примитив)."""
    def _sanitize(v: Any) -> Any:
        if pd.isna(v):
            return v
        s = str(v)
        s = unicodedata.normalize("NFC", s)
        s = _ZW_RE.sub("", s)
        s = _CTRL_RE.sub("", s)
        return s
    return series.apply(_sanitize)

def to_lower(series: pd.Series) -> pd.Series:
    """Нижний регистр (примитив)."""
    return series.apply(lambda v: (str(v).lower() if pd.notna(v) else v))

# ────────────────── основное правило email ──────────────────
# ТВОЙ шаблон (ASCII, нижний регистр, подчёркивание/цифры/дефисы, сегменты через .)
DEFAULT_EMAIL_PATTERN = r'^[_a-z0-9-]+(\.[_a-z0-9-]+)*@[a-z0-9-]+(\.[a-z0-9-]+)*(\.[a-z]{2,})$'

def email_basic(
    series: pd.Series,
    *,
    pattern: str = DEFAULT_EMAIL_PATTERN,
    on_fail: str = "clear_cell",         # "clear_cell" | "keep"
    clear_value: str = "",
    examples_limit: int = 25,
) -> Tuple[pd.Series, Dict[str, Any]]:
    """
    Нормализация и валидация e-mail:
      1) NFC-нормализация, удаление невидимых/управляющих.
      2) УДАЛЕНИЕ ВСЕХ ПРОБЕЛОВ ВНУТРИ (и любых \s).
      3) Приведение к нижнему регистру.
      4) Проверка по заданному regex (по умолчанию — данная тобой регулярка).
         Невалидные — очищаем ячейку (или сохраняем как есть, если on_fail='keep').

    Возвращает: (Series, stats)
    """
    email_re = re.compile(pattern)

    original = series.copy()
    s = series.copy()

    def _normalize_email(v: Any) -> Any:
        if pd.isna(v):
            return v
        x = str(v)
        # NFC + убрать невидимые/управляющие
        x = unicodedata.normalize("NFC", x)
        x = _ZW_RE.sub("", x)
        x = _CTRL_RE.sub("", x)
        # удалить ВСЕ пробельные символы внутри
        x = _ALL_WS_RE.sub("", x)
        # нижний регистр
        x = x.lower()
        return x

    s = s.apply(_normalize_email)

    def _validate_or_clear(v: Any) -> Any:
        if pd.isna(v):
            return v
        x = str(v)
        if x == "":
            return x
        if email_re.match(x):
            return x
        return (clear_value if on_fail == "clear_cell" else x)

    validated = s.apply(_validate_or_clear)

    # статистика
    changed_mask = (validated.astype("object") != original.astype("object"))
    cleared_mask = (validated == clear_value) & (~original.isna())

    stats: Dict[str, Any] = {
        "total": int(series.shape[0]),
        "changed": int(changed_mask.sum()),
        "cleared": int(cleared_mask.sum()),
        "examples": [],
    }

    # примеры: нормализации
    for idx in list(validated[changed_mask & ~cleared_mask].index)[:examples_limit]:
        stats["examples"].append({
            "row": int(idx) if isinstance(idx, (int, float)) and pd.notna(idx) else idx,
            "column": "email",
            "before": "" if pd.isna(original.loc[idx]) else str(original.loc[idx]),
            "after": "" if pd.isna(validated.loc[idx]) else str(validated.loc[idx]),
            "note": "normalized (spaces removed; lower; regex ok)",
        })

    # примеры: очищенные
    if len(stats["examples"]) < examples_limit:
        for idx in list(validated[cleared_mask].index)[:examples_limit - len(stats["examples"])]:
            stats["examples"].append({
                "row": int(idx) if isinstance(idx, (int, float)) and pd.notna(idx) else idx,
                "column": "email",
                "before": "" if pd.isna(original.loc[idx]) else str(original.loc[idx]),
                "after": clear_value,
                "note": "cleared (regex not matched)",
            })

    return validated, stats
