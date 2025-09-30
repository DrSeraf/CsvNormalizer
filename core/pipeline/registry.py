# core/pipeline/registry.py
from __future__ import annotations
from typing import Callable, Dict, Any
import pandas as pd

from core.rules import email as email_rules

# Тип функции-правила: принимает Series и **kwargs, возвращает Series и/или (Series, stats)
RuleFunc = Callable[..., pd.Series]

# Примитивные шаги, которые НЕ возвращают статистику
_PRIMITIVES: Dict[str, Callable[..., pd.Series]] = {
    "strip_whitespace": email_rules.strip_whitespace,
    "sanitize_invisible": email_rules.sanitize_invisible,
    "to_lower": email_rules.to_lower,
}

# Шаги, которые возвращают (Series, stats) — их статистику собираем
_ADVANCED: Dict[str, Callable[..., tuple]] = {
    "validate_regex": email_rules.email_basic,  # внутри себя делает всю логику валидации
}


def is_advanced(name: str) -> bool:
    return name in _ADVANCED


def get(name: str) -> Callable[..., Any]:
    if name in _PRIMITIVES:
        return _PRIMITIVES[name]
    if name in _ADVANCED:
        return _ADVANCED[name]
    raise KeyError(f"Unknown rule '{name}'")
