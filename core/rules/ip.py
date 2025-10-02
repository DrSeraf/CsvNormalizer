# core/rules/ip.py
from __future__ import annotations
from typing import Any, Dict, Tuple
import re
import unicodedata
import pandas as pd


# удаляем zero-width и управляющие
_ZW_RE = re.compile(r"[\u200B-\u200D\uFEFF]")
_CTRL_RE = re.compile(r"[\x00-\x1F\x7F]")

# базовая маска IPv4
_IPV4_RE = re.compile(r"^\d{1,3}(?:\.\d{1,3}){3}$")


def _sanitize_base(s: str) -> str:
    s = unicodedata.normalize("NFC", s)
    s = _ZW_RE.sub("", s)
    s = _CTRL_RE.sub("", s)
    return s


def _strip_outer_quotes(s: str) -> str:
    # снимаем только ОБОЛОЧКУ, например "'1.2.3.4'" → 1.2.3.4
    if len(s) >= 2:
        if (s[0] == s[-1]) and s[0] in {"'", '"', "´", "`"}:
            return s[1:-1]
    return s


def ip_basic(
    series: pd.Series,
    *,
    strict_octets: bool = False,   # при True дополнительно проверяем диапазоны 0..255
    on_fail: str = "clear_cell",
    clear_value: str = "",
    examples_limit: int = 25,
) -> Tuple[pd.Series, Dict[str, Any]]:
    """
    Нормализация IPv4:
      1) NFC + удалить невидимые/управляющие;
      2) trim по краям;
      3) снять внешние кавычки ('...' или "..."), если есть;
      4) проверить IPv4-маску и (опц.) диапазоны октетов.
    Невалидные -> clear_value (по умолчанию "").
    """
    original = series.copy()

    def _norm(v: Any) -> Any:
        if pd.isna(v):
            return v
        x = str(v)
        x = _sanitize_base(x).strip()
        x = _strip_outer_quotes(x.strip())
        return x

    s = series.apply(_norm)

    def _validate(v: Any) -> Any:
        if pd.isna(v):
            return v
        x = str(v).strip()
        if x == "":
            return x
        if not _IPV4_RE.match(x):
            return clear_value if on_fail == "clear_cell" else x
        if strict_octets:
            try:
                parts = [int(p) for p in x.split(".")]
                if any(p < 0 or p > 255 for p in parts):
                    return clear_value if on_fail == "clear_cell" else x
            except Exception:
                return clear_value if on_fail == "clear_cell" else x
        return x

    validated = s.apply(_validate)

    # (статистику runner считает централизованно; оставим минимальную совместимость)
    changed_mask = (validated.astype("object") != original.astype("object"))
    cleared_mask = (validated == clear_value) & (~original.isna())
    stats: Dict[str, Any] = {
        "total": int(series.shape[0]),
        "changed": int(changed_mask.sum()),
        "cleared": int(cleared_mask.sum()),
        "examples": [],
    }

    # примеры (сдержанно)
    for idx in list(validated[changed_mask & ~cleared_mask].index)[:examples_limit]:
        stats["examples"].append({
            "row": int(idx) if isinstance(idx, (int, float)) and pd.notna(idx) else idx,
            "column": "ip_address",
            "before": "" if pd.isna(original.loc[idx]) else str(original.loc[idx]),
            "after": "" if pd.isna(validated.loc[idx]) else str(validated.loc[idx]),
            "note": "normalized",
        })
    if len(stats["examples"]) < examples_limit:
        for idx in list(validated[cleared_mask].index)[:examples_limit - len(stats["examples"])]:
            stats["examples"].append({
                "row": int(idx) if isinstance(idx, (int, float)) and pd.notna(idx) else idx,
                "column": "ip_address",
                "before": "" if pd.isna(original.loc[idx]) else str(original.loc[idx]),
                "after": clear_value,
                "note": "cleared (invalid IPv4)",
            })

    return validated, stats
