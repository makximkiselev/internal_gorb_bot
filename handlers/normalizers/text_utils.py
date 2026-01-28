# handlers/normalizers/text_utils.py
from __future__ import annotations

import re
from typing import List, Tuple, Optional


# ======================================================================
# CONFUSABLES (RU->EN lookalikes)
# ======================================================================

_CONFUSABLES_TABLE = str.maketrans({
    "А": "A", "Б": "B", "В": "B", "Е": "E", "К": "K", "М": "M", "Н": "H",
    "О": "O", "Р": "P", "С": "C", "Т": "T", "У": "Y", "Х": "X",
    "а": "a", "б": "b", "е": "e", "о": "o", "р": "p", "с": "c", "у": "y", "х": "x",
    "к": "k", "м": "m", "т": "t", "в": "b", "н": "h",
})

_RX_MULTI_SPACE = re.compile(r"\s+")
_RX_DASHES = re.compile(r"[–—]+")
_RX_PIPES = re.compile(r"[|•]+")


def fix_confusables(s: str) -> str:
    return (s or "").translate(_CONFUSABLES_TABLE)


def clean_generic_text(text: str) -> str:
    """Лёгкая нормализация строки без потери смысла."""
    if not text:
        return ""
    s = fix_confusables(text)
    s = s.replace("\u00A0", " ")
    s = _RX_DASHES.sub("-", s)
    s = _RX_PIPES.sub(" | ", s)
    s = _RX_MULTI_SPACE.sub(" ", s).strip()
    return s

def clean_spaces(s: str) -> str:
    """Схлопывает пробелы/неразрывные пробелы, тримит края."""
    if not s:
        return ""
    s = (s or "").replace("\u00A0", " ")
    return re.sub(r"\s+", " ", s).strip()

def remove_spans(text: str, spans: List[Tuple[int, int]]) -> str:
    """
    Alias к consume_spans (историческое имя).
    В entry.py ожидается remove_spans.
    """
    return consume_spans(text or "", spans or [])

def prefix_token_ok(prefix: str, full: str) -> bool:
    """
    Проверяет, что prefix встречается в full как последовательность токенов (не по подстроке внутри слов).
    Упрощённая, но стабильная версия для индексов/релакса.
    """
    p = norm_key_for_index(prefix)
    f = norm_key_for_index(full)
    if not p or not f:
        return False
    return f == p or f.startswith(p + " ")

def consume_spans(text: str, spans: List[Tuple[int, int]]) -> str:
    """
    Вырезаем найденные куски (по позициям) из текста, заменяя их пробелами,
    чтобы последующие экстракторы не переиспользовали эти токены.
    """
    if not text or not spans:
        return (text or "")
    spans = [(max(0, a), max(0, b)) for a, b in spans if a < b]
    if not spans:
        return text
    spans.sort()

    s = list(text)
    n = len(s)
    for a, b in spans:
        a2 = min(max(a, 0), n)
        b2 = min(max(b, 0), n)
        for i in range(a2, b2):
            s[i] = " "

    return re.sub(r"\s+", " ", "".join(s)).strip()


# ======================================================================
# FLAGS
# ======================================================================

FLAG_RE = re.compile(r"[\U0001F1E6-\U0001F1FF]{2}")


def strip_flags(text: str) -> str:
    return FLAG_RE.sub(" ", text or "").strip()


# ======================================================================
# KEY NORMALIZATION (for indexes)
# ======================================================================

def norm_key_for_index(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace("\u00A0", " ")
    s = s.replace("/", " ")
    s = strip_flags(s)
    s = re.sub(r"[\s\-_]+", " ", s)
    s = re.sub(r"[^a-z0-9а-яё ]+", "", s)
    return s.strip()


# ======================================================================
# TOKEN-LIKE REGEX BUILDER (shared)
# ======================================================================

def rx_token_like(k: str) -> re.Pattern:
    """
    Матч "токеноподобных" ключей:
      - НЕ матчим внутри латинских слов/чисел
      - плюс учитываем разделители . _ - как пробел (важно для space-gray, jet_black и т.д.)
    """
    k = (k or "").strip().lower()
    pat = re.escape(k).replace(r"\ ", r"[\s._-]+")
    return re.compile(rf"(?i)(?<![a-z0-9]){pat}(?![a-z0-9])")

def norm_key(s: str) -> str:
    """Back-compat alias (entry.py ожидает tu.norm_key)."""
    return norm_key_for_index(s)
