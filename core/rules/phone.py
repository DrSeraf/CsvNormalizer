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

# ==========================
# 1) Номер телефона целиком
# ==========================
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
                "note": f"cleared (length out of range)",
            })

    return s, stats


# ==========================
# 2) Префикс (код страны)
# ==========================
def phone_prefix_basic(
    series: pd.Series,
    *,
    min_len: int = 1,
    max_len: int = 3,
    validate_cc: bool = False,          # строгая проверка против кодов стран (E.164)
    examples_limit: int = 25,
) -> Tuple[pd.Series, Dict[str, Any]]:
    """
    Очищает значение до цифр, проверяет длину [min_len, max_len].
    При validate_cc=True пытается проверить, что это валидный код страны (E.164).

    Для строгой проверки используется библиотека `phonenumbers` (если установлена).
    Если её нет — выполняется только проверка длины (без ошибки).
    """
    original = series.copy()
    s = series.copy()

    # подгрузим phonenumbers, если доступен
    cc_set = None
    cc_note = None
    if validate_cc:
        try:
            import phonenumbers
            # В libphonenumber есть мапа country_code -> [regions]
            cc_map = phonenumbers.COUNTRY_CODE_TO_REGION_CODE
            cc_set = {int(k) for k in cc_map.keys()}
        except Exception:
            cc_set = None
            cc_note = "validate_cc requested but phonenumbers not installed; length-only validation applied"

    def _apply(v):
        if pd.isna(v):
            return v
        x = _sanitize(str(v).strip())
        # оставляем только цифры
        x = re.sub(r"\D+", "", x)
        if x == "":
            return x
        # длина 1..3
        if not (min_len <= len(x) <= max_len):
            return ""
        # опциональная валидация по кодам стран
        if validate_cc and cc_set is not None:
            try:
                iv = int(x)
            except Exception:
                return ""
            if iv not in cc_set:
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
    if cc_note:
        stats["note"] = cc_note

    # Примеры
    for idx in list(s[changed_mask & ~cleared_mask].index)[:examples_limit]:
        stats["examples"].append({
            "row": int(idx) if isinstance(idx, (int, float)) and pd.notna(idx) else idx,
            "column": "phone_pfx",
            "before": "" if pd.isna(original.loc[idx]) else str(original.loc[idx]),
            "after": "" if pd.isna(s.loc[idx]) else str(s.loc[idx]),
            "note": "normalized",
        })
    if len(stats["examples"]) < examples_limit:
        for idx in list(s[cleared_mask].index)[:examples_limit - len(stats["examples"])]:
            stats["examples"].append({
                "row": int(idx) if isinstance(idx, (int, float)) and pd.notna(idx) else idx,
                "column": "phone_pfx",
                "before": "" if pd.isna(original.loc[idx]) else str(original.loc[idx]),
                "after": "",
                "note": ("cleared (length out of range or invalid calling code)" if validate_cc else "cleared (length out of range)"),
            })

    return s, stats
