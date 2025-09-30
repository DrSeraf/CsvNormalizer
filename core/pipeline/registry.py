# core/pipeline/registry.py
from __future__ import annotations
from typing import Callable, Dict, Any
import pandas as pd

from core.rules import email as email_rules
from core.rules import names as name_rules
from core.rules import birthdate as birthdate_rules
from core.rules import phone as phone_rules
from core.rules import ip as ip_rules

RuleFunc = Callable[..., pd.Series]

# Примитивы (без статистики)
_PRIMITIVES: Dict[str, Callable[..., pd.Series]] = {
    "strip_whitespace": email_rules.strip_whitespace,      # пока переиспользуем
    "sanitize_invisible": email_rules.sanitize_invisible,
    "to_lower": email_rules.to_lower,
}

# Продвинутые шаги (возвращают (Series, stats))
_ADVANCED: Dict[str, Callable[..., tuple]] = {
    # email
    "validate_regex": email_rules.email_basic,   # для обратной совместимости
    "email_basic": email_rules.email_basic,

    # names
    "name_basic": name_rules.name_basic,

    # birthdate
    "birthdate_basic": birthdate_rules.birthdate_basic,

    # phone
    "phone_digits_len": phone_rules.phone_digits_len,

    # ip
    "ip_basic": ip_rules.ip_basic,
}

def is_advanced(name: str) -> bool:
    return name in _ADVANCED

def get(name: str) -> Callable[..., Any]:
    if name in _PRIMITIVES:
        return _PRIMITIVES[name]
    if name in _ADVANCED:
        return _ADVANCED[name]
    raise KeyError(f"Unknown rule '{name}'")
