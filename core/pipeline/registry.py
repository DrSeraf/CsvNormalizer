# core/pipeline/registry.py
from __future__ import annotations
from typing import Callable, Dict, Any
import pandas as pd

# ЯВНЫЕ ИМПОРТЫ ИЗ МОДУЛЕЙ ПРАВИЛ
from core.rules.email import strip_whitespace, sanitize_invisible, to_lower, email_basic
from core.rules.names import name_basic
from core.rules.birthdate import birthdate_basic
from core.rules.phone import phone_digits_len, phone_prefix_basic
from core.rules.ip import ip_basic
from core.rules.lengths import min_length_clear

RuleFunc = Callable[..., pd.Series]

# Примитивы (функции, возвращающие только Series — без stats)
_PRIMITIVES: Dict[str, Callable[..., pd.Series]] = {
    "strip_whitespace": strip_whitespace,
    "sanitize_invisible": sanitize_invisible,
    "to_lower": to_lower,
}

# Продвинутые шаги (возвращают кортеж: (Series, stats))
_ADVANCED: Dict[str, Callable[..., tuple]] = {
    # email
    "validate_regex": email_basic,  # для обратной совместимости
    "email_basic": email_basic,

    # names
    "name_basic": name_basic,

    # birthdate
    "birthdate_basic": birthdate_basic,

    # phone
    "phone_digits_len": phone_digits_len,
    "phone_prefix_basic": phone_prefix_basic,

    # ip
    "ip_basic": ip_basic,

    # generic
    "min_length_clear": min_length_clear,
}

def is_advanced(name: str) -> bool:
    return name in _ADVANCED

def get(name: str) -> Callable[..., Any]:
    if name in _PRIMITIVES:
        return _PRIMITIVES[name]
    if name in _ADVANCED:
        return _ADVANCED[name]
    raise KeyError(f"Unknown rule '{name}'")
