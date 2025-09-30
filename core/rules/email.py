# core/rules/email.py
from __future__ import annotations

import re
import unicodedata
from typing import Dict, List, Tuple, Any

import pandas as pd


# ──────────────────────────────────────────────────────────────────────────────
# Вспомогательные «базовые» операции только для email-потока
# (позже можно вынести в core/rules/common.py)
# ──────────────────────────────────────────────────────────────────────────────

def strip_whitespace(s: pd.Series) -> pd.Series:
    """Обрезает пробелы и табы по краям (NaN не трогаем)."""
    s2 = s.copy()
    mask = s2.notna()
    s2.loc[mask] = s2.loc[mask].astype(str).str.strip()
    return s2


def sanitize_invisible(s: pd.Series) -> pd.Series:
    """
    Убирает невидимые/непечатаемые символы:
    - нулевой ширины (U+200B..U+200D, U+FEFF)
    - управляющие ASCII (0x00..0x1F, 0x7F)
    Также нормализует Unicode в NFC.
    """
    s2 = s.copy()

    def _clean(v: Any) -> Any:
        if pd.isna(v):
            return v
        x = str(v)
        # Unicode normalize
        x = unicodedata.normalize("NFC", x)
        # zero-width + BOM
        x = re.sub(r"[\u200B-\u200D\uFEFF]", "", x)
        # control chars
        x = re.sub(r"[\x00-\x1F\x7F]", "", x)
        return x

    return s2.apply(_clean)


def to_lower(s: pd.Series) -> pd.Series:
    """Приводит к нижнему регистру (NaN не трогаем)."""
    s2 = s.copy()
    mask = s2.notna()
    s2.loc[mask] = s2.loc[mask].astype(str).str.lower()
    return s2


# ──────────────────────────────────────────────────────────────────────────────
# Основное правило: email_basic
# ──────────────────────────────────────────────────────────────────────────────

def email_basic(
    series: pd.Series,
    *,
    pattern: str,
    on_fail: str = "clear_cell",
    clear_value: str = "",
    examples_limit: int = 25,
) -> Tuple[pd.Series, Dict[str, Any]]:
    """
    Нормализует столбец email:
      1) trim
      2) sanitize invisible + NFC
      3) lower
      4) validate regex; если не прошло — действуем по on_fail (по ТЗ: clear_cell)

    Возвращает:
      - нормализованный Series
      - stats dict с подсчётами и примерами изменений (для логов)

    Параметры:
      pattern      — regex из конфига
      on_fail      — поведение при провале: "clear_cell" (поддерживается сейчас)
      clear_value  — чем заполнять ячейку при очистке (по умолчанию пустая строка)
    """
    if on_fail not in {"clear_cell"}:
        raise ValueError(f"Unsupported on_fail='{on_fail}'. Use 'clear_cell'.")

    compiled = re.compile(pattern)

    # Сохраним оригинал для сравнения и примеров
    original = series.copy()

    # Шаги нормализации
    s = strip_whitespace(series)
    s = sanitize_invisible(s)
    s = to_lower(s)

    # Маски
    not_empty = s.notna() & (s.astype(str) != "")
    valid_mask = pd.Series(False, index=s.index)
    if not s.empty:
        # match только для непустых
        valid_mask.loc[not_empty] = s.loc[not_empty].astype(str).str.match(compiled)

    # Невалидные — очищаем ячейку
    invalid_mask = not_empty & (~valid_mask)
    cleared_count = int(invalid_mask.sum())

    if cleared_count:
        s.loc[invalid_mask] = clear_value

    # Подсчёт изменений (любые изменения значений)
    changed_mask = (s.astype("object") != original.astype("object"))
    changed_count = int(changed_mask.sum())

    # Примеры изменений (для лога)
    examples: List[Dict[str, Any]] = []
    # 1) примеры успешных исправлений (исправили формат, но оставили значение)
    fixed_mask = changed_mask & (~invalid_mask)
    if fixed_mask.any():
        for idx in list(s[fixed_mask].index)[:examples_limit]:
            examples.append({
                "row": int(idx) if isinstance(idx, (int, float)) and pd.notna(idx) else idx,
                "column": "email",
                "before": _safe_str(original.loc[idx]),
                "after": _safe_str(s.loc[idx]),
                "note": "normalized",
            })

    # 2) примеры очищенных (не прошли regex)
    if invalid_mask.any():
        for idx in list(s[invalid_mask].index)[:examples_limit - len(examples)]:
            examples.append({
                "row": int(idx) if isinstance(idx, (int, float)) and pd.notna(idx) else idx,
                "column": "email",
                "before": _safe_str(original.loc[idx]),
                "after": _safe_str(clear_value),
                "note": "cleared (regex not matched)",
            })

    stats: Dict[str, Any] = {
        "total": int(series.shape[0]),
        "changed": changed_count,
        "cleared_invalid": cleared_count,
        "examples": examples,
    }

    return s, stats


def _safe_str(v: Any) -> str:
    if pd.isna(v):
        return ""
    return str(v)
