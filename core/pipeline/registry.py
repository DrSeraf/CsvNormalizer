# core/pipeline/registry.py
from __future__ import annotations
from typing import Callable, Dict, Any
import pandas as pd

from core.rules.email import strip_whitespace, sanitize_invisible, to_lower, email_basic
from core.rules.names import name_basic
from core.rules.birthdate import birthdate_basic
from core.rules.phone import phone_digits_len
from core.rules.ip import ip_basic
from core.rules.lengths import min_length_clear  # NEW

RuleFunc = Callable[..., pd.Series]

_PRIMITIVES: Dict[str, Callable[..., pd.Series]] = {
    "strip_whitespace": strip_whitespace,
    "sanitize_invisible": sanitize_invisible,
    "to_lower": to_lower,
}

_ADVANCED: Dict[str, Callable[..., tuple]] = {
    "validate_regex": email_basic,
    "email_basic": email_basic,
    "name_basic": name_basic,
    "birthdate_basic": birthdate_basic,
    "phone_digits_len": phone_digits_len,
    "ip_basic": ip_basic,
    "min_length_clear": min_length_clear,  # NEW
}

def is_advanced(name: str) -> bool:
    return name in _ADVANCED

def get(name: str) -> Callable[..., Any]:
    if name in _PRIMITIVES:
        return _PRIMITIVES[name]
    if name in _ADVANCED:
        return _ADVANCED[name]
    raise KeyError(f"Unknown rule '{name}'")
