# core/rules/email.py
from __future__ import annotations
from typing import Any, Dict, Tuple
import re
import unicodedata
import pandas as pd

# ────────────────── служебные regex ──────────────────
_ZW_RE = re.compile(r"[\u200B-\u200D\uFEFF]")   # zero-width
_CTRL_RE = re.compile(r"[\x00-\x1F\x7F]")       # control chars
_ALL_WS_RE = re.compile(r"\s+", flags=re.UNICODE)
_DOTS_RE = re.compile(r"\.+")                   # множественные точки

# Разрешённые наборы
_LOCAL_ALLOWED = re.compile(r"[^_a-z0-9\.-]")   # для local-part
_DOMAIN_ALLOWED = re.compile(r"[^a-z0-9\.-]")   # для domain

# ТВОЙ шаблон (ASCII, нижний регистр)
DEFAULT_EMAIL_PATTERN = r'^[_a-z0-9-]+(\.[_a-z0-9-]+)*@[a-z0-9-]+(\.[a-z0-9-]+)*(\.[a-z]{2,})$'


# ────────────────── примитивы (оставил для совместимости) ──────────────────
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


# ────────────────── нормализация e-mail ──────────────────
def _strip_outer_wrappers(s: str) -> str:
    """Снять ОБОЛОЧКУ вида '...'/\"...\"/(...)/{...}/[...]/<...> по краям, если она симметрична."""
    if len(s) >= 2:
        pairs = {("'","'"), ('"','"'), ('(',')'), ('[',']'), ('{','}'), ('<','>'), ('`','`'), ('´','´')}
        first, last = s[0], s[-1]
        for lft, rgt in pairs:
            if first == lft and last == rgt:
                return s[1:-1]
    return s

def _normalize_email_text(raw: str) -> str:
    """
    1) NFC, убрать невидимые/управляющие
    2) trim + удалить ВСЕ пробелы внутри
    3) снять внешние кавычки/скобки
    4) выделить первый '@', всё правее — домен (доп. '@' удаляем)
    5) выбросить запрещённые символы в local/domain
    6) схлопнуть точки, убрать точки/дефисы по краям сегментов
    7) привести к нижнему регистру
    8) в TLD оставить только буквы
    """
    s = unicodedata.normalize("NFC", raw)
    s = _ZW_RE.sub("", s)
    s = _CTRL_RE.sub("", s)
    s = s.strip()
    s = _ALL_WS_RE.sub("", s)          # убрать все пробельные внутри
    s = _strip_outer_wrappers(s)

    if not s:
        return ""

    # найдём ПЕРВЫЙ '@'
    at = s.find("@")
    if at == -1:
        # нет '@' — просто зачистим до разрешённых и низкий регистр
        s = s.lower()
        s = _LOCAL_ALLOWED.sub("", s)  # допускаем только local-символы
        return s  # далее завалится на проверке regex, что нормально

    local_raw = s[:at]
    domain_raw = s[at+1:].replace("@", "")  # все остальные '@' удаляем

    # выбросить запрещённые символы
    local = _LOCAL_ALLOWED.sub("", local_raw.lower())
    domain = _DOMAIN_ALLOWED.sub("", domain_raw.lower())

    # схлопнуть точки, убрать точки/дефисы по краям сегментов
    local = _DOTS_RE.sub(".", local).strip(".-")
    domain = _DOTS_RE.sub(".", domain).strip(".-")

    # нормализуем TLD → только буквы
    if "." in domain:
        parts = [p for p in domain.split(".") if p != ""]
        if parts:
            tld = re.sub(r"[^a-z]", "", parts[-1])
            parts[-1] = tld
            # убрать пустые сегменты, ещё раз подчистить края
            parts = [p.strip(".-") for p in parts if p.strip(".-") != ""]
            domain = ".".join(parts)

    # если local/domain опустели — вернём то, что есть (regex отсеет)
    if local == "" or domain == "":
        return f"{local}@{domain}" if "@" in s else s

    return f"{local}@{domain}"


# ────────────────── основное правило ──────────────────
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
      • полная чистка (см. _normalize_email_text)
      • проверка по заданному regex (по умолчанию — твоя регулярка)
      • невалидные → clear_value (или оставить как есть при on_fail='keep')
    """
    email_re = re.compile(pattern)

    original = series.copy()

    def _normalize(v: Any) -> Any:
        if pd.isna(v):
            return v
        return _normalize_email_text(str(v))

    norm = series.apply(_normalize)

    def _validate_or_clear(v: Any) -> Any:
        if pd.isna(v):
            return v
        x = str(v)
        if x == "":
            return x
        return x if email_re.match(x) else (clear_value if on_fail == "clear_cell" else x)

    validated = norm.apply(_validate_or_clear)

    # (агрегированную статистику и примеры теперь собирает runner централизованно)
    changed_mask = (validated.astype("object") != original.astype("object"))
    cleared_mask = (validated == clear_value) & (~original.isna())

    stats: Dict[str, Any] = {
        "total": int(series.shape[0]),
        "changed": int(changed_mask.sum()),
        "cleared": int(cleared_mask.sum()),
        "examples": [],  # примеры формирует runner
    }
    return validated, stats
