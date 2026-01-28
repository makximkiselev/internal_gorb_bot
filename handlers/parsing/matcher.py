# handlers/parsing/matcher.py
# Универсальное сопоставление эталонных и распарсенных товаров
#
# ✅ matcher отвечает ТОЛЬКО за:
#    - строгий match по параметрам (по полям из MATCH_FIELDS + дополнительные правила)
#    - агрегацию price/channel/raw
#    - I/O: читает parsed_etalon/parsed_goods -> пишет matched/unmatched + stats
#
# ❌ Никаких "инференсов", дефолтов, подстановок, align'ов, region-логики и т.п.
# ❌ region НЕ участвует в сравнении (вообще)
#
# 🔧 Смягчение: для "умные часы"
#    - band_color: сравниваем только если заполнено у обоих
#    - если в эталоне есть band_* , а в parsed их НЕТ — матч разрешаем
#    - relaxed-сигнатура без band_*

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional, Set


# ======================
# ✅ BOOTSTRAP IMPORT PATH
# ======================

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# ======================
# ✅ SHARED DICTS (colors canon/compat)
# ======================
try:
    from handlers.normalizers import entry_dicts as D
except Exception:
    D = None  # type: ignore


# ======================
# ✅ AFTER-MATCH PIPELINE (build parsed_data.json)
# ======================
try:
    from handlers.parsing import results as results_builder
except Exception:
    results_builder = None


# ======================
# ✅ ПУТИ (runner I/O)
# ======================

MODULE_DIR = Path(__file__).parent.resolve()
DATA_DIR = MODULE_DIR / "data"

ETALON_FILE = DATA_DIR / "parsed_etalon.json"
GOODS_FILE = DATA_DIR / "parsed_goods.json"

MATCHED_FILE = DATA_DIR / "parsed_matched.json"
UNMATCHED_ETALON_FILE = DATA_DIR / "unmatched_etalon.json"
UNMATCHED_PARSED_FILE = DATA_DIR / "unmatched_parsed.json"
MATCH_STATS_FILE = DATA_DIR / "match_stats.json"


# =========================
# === Карта полей мэтчинга
# =========================
MATCH_FIELDS: Dict[str, List[str]] = {
    # ✅ smartphones: connectivity сравниваем ТОЛЬКО если заполнено в эталоне
    "смартфоны": ["model", "storage", "color", "sim", "connectivity"],
    "smartphones": ["model", "storage", "color", "sim", "connectivity"],

    # ✅ FIX: diagonal -> screen_size
    "планшеты": ["model", "screen_size", "storage", "color", "connectivity"],

    # ✅ FIX: dial_size -> watch_size_mm
    "умные часы": ["model", "watch_size_mm", "band_type", "band_size", "connectivity", "color"],
    "приставки и игры": ["model", "storage", "drive", "color", "game"],
    "наушники": ["model", "color"],
    "ноутбуки": ["model", "ram", "storage", "chip"],  # + code (если заполнено у обоих — проверим ниже)
    "_default": ["model", "color", "storage"],
}

# ✅ Поля, которые должны ВСЕГДА совпадать:
# если заполнено хотя бы у одного -> у второго тоже должно быть и должно совпасть
ALWAYS_EQUAL_FIELDS = {
    "storage",
    "ram",
    "color",
    "sim",
    "band_size",
    "screen_size",
    "connectivity",
    "anc",
    "nano_glass",
    "no_watches",
    "game",
}

# ✅ Поля сравниваем ТОЛЬКО если заполнены у ОБОИХ
BOTH_FILLED_FIELDS = {
    "code",
    "band_color",
    "chip",    
}

# ✅ Поля, которые НЕ сравниваем строго в отдельных категориях
SOFT_IGNORE_FIELDS_BY_CAT: Dict[str, set] = {}

# ✅ Поля, которые могут быть заданы в эталоне, но если в parsed они отсутствуют — НЕ валим матч
SOFT_OPTIONAL_FIELDS_BY_CAT: Dict[str, set] = {
    "умные часы": {"band_type", "band_size", "band_color"},
}

# ✅ Для построения сигнатур: поля, которые можно исключить в relaxed-сигнатуре
RELAXED_SIG_DROP_FIELDS_BY_CAT: Dict[str, set] = {
    "умные часы": {"band_type", "band_size", "band_color"},
}


# ======================
# Utils
# ======================

def _norm_str(v: Any) -> str:
    return str(v or "").strip().lower()


def get_cat(item: dict) -> str:
    path = item.get("path")
    if isinstance(path, list) and path:
        return str(path[0] or "").strip().lower()
    return "_default"


def _pick_category(etalon_item: dict, parsed_item: dict) -> str:
    """
    Если у эталона категория "_default", но у parsed есть явная категория — берём parsed.
    """
    cat_e = get_cat(etalon_item)
    cat_p = get_cat(parsed_item)
    if cat_e == "_default" and cat_p != "_default":
        return cat_p
    return cat_e or cat_p or "_default"


def get_field(item: dict, key: str):
    """
    Поля могут лежать:
      - на верхнем уровне
      - либо в params
    """
    if not isinstance(item, dict):
        return None

    v = item.get(key)
    if v not in (None, "", []):
        return v

    params = item.get("params")
    if isinstance(params, dict):
        v2 = params.get(key)
        if v2 not in (None, "", []):
            return v2

    return None


def _normalize_connectivity(cat: str, v: Any) -> str:
    cat = (cat or "").strip().lower() or "_default"
    s = _norm_str(v)

    if not s:
        return "wi-fi" if cat == "планшеты" else ""

    # unify separators
    s = s.replace("_", " ")
    s = " ".join(s.replace("+", " + ").split())

    # common wifi tokens
    if s in {"wifi", "wi fi", "wi-fi", "wifionly", "wifi only", "wi fi only"}:
        s = "wi-fi"

    # common wifi+cellular tokens
    if s in {
        "wifi + cellular", "wi-fi + cellular", "wi-fi cellular", "wifi cellular",
        "wi fi + cellular", "wi fi cellular",
        "wifi+cellular", "wi-fi+cellular",
    }:
        s = "wi-fi+cellular"

    if cat == "планшеты":
        # ✅ ВСЕ СОТОВЫЕ ВАРИАНТЫ -> cellular
        if s in {
            "lte", "4g", "5g",
            "cellular", "cell", "sim",
            "wi-fi+cellular",
            "wifi+cellular",  # на всякий
        }:
            return "cellular"

        if s == "wi-fi":
            return "wi-fi"

        # если прилетело что-то странное — оставим как есть
        return s

    return s



def _norm_field_value(cat: str, field: str, raw_val: Any) -> str:
    if field == "connectivity":
        return _normalize_connectivity(cat, raw_val)
    s = _norm_str(raw_val)
    return s if s else ""


# ======================
# Colors canon/compat (matcher-level)
# ======================

# Если entry_dicts не импортнулся — работаем в режиме strict exact (без compat),
# чтобы matcher не падал.
_COLOR_CANON_LC: Dict[str, str] = {}
_COLOR_SYNONYMS_LC: Dict[str, str] = {}
_COLOR_COMPAT_GROUPS_LC: List[Set[str]] = []

if D is not None:
    try:
        _COLOR_CANON_LC = {_norm_str(k): _norm_str(v) for k, v in getattr(D, "COLOR_CANON_MAP", {}).items()}
        _COLOR_SYNONYMS_LC = {_norm_str(k): _norm_str(v) for k, v in getattr(D, "COLOR_SYNONYMS", {}).items()}
        _COLOR_COMPAT_GROUPS_LC = [{_norm_str(x) for x in g} for g in getattr(D, "COLOR_COMPAT_GROUPS", [])]
    except Exception:
        _COLOR_CANON_LC = {}
        _COLOR_SYNONYMS_LC = {}
        _COLOR_COMPAT_GROUPS_LC = []


def _canon_color(v: Any) -> str:
    """
    Канонизация цвета:
      1) synonyms (рус/сленг) -> базовый цвет (black/white/silver/…)
      2) canon family (silver -> white, starlight -> white, space gray -> gray, …)
    Всё в lower-case.
    """
    s = _norm_str(v)
    if not s:
        return ""
    s = " ".join(s.replace("-", " ").split())

    # 1) synonyms
    if _COLOR_SYNONYMS_LC:
        s = _COLOR_SYNONYMS_LC.get(s, s)
        s = " ".join(s.replace("-", " ").split())

    # 2) canon family
    if _COLOR_CANON_LC:
        s = _COLOR_CANON_LC.get(s, s)

    return s


def _colors_compatible(c1: str, c2: str) -> bool:
    """
    Совместимость по группам (например, silver/starlight/white в одной группе).
    На вход — уже lower.
    """
    if not c1 or not c2:
        return False
    if c1 == c2:
        return True
    for g in _COLOR_COMPAT_GROUPS_LC:
        if c1 in g and c2 in g:
            return True
    return False


def _norm_color_token(s: str) -> str:
    s = _norm_str(s)
    if not s:
        return ""
    return " ".join(s.replace("-", " ").split())


def _is_jet_black_color(s: str) -> bool:
    ns = _norm_color_token(s)
    return ns.replace(" ", "") == "jetblack"


def _is_black_color(s: str) -> bool:
    return _norm_color_token(s) == "black"


def _is_s25_ultra_item(item: dict) -> bool:
    model = _norm_str(get_field(item, "model"))
    return "s25 ultra" in model


def _is_s25_family_item(item: dict) -> bool:
    model = _norm_str(get_field(item, "model"))
    return "s25" in model


def _is_icy_blue_color(s: str) -> bool:
    ns = _norm_color_token(s)
    return ns.replace(" ", "") == "icyblue"


def _is_navy_color(s: str) -> bool:
    return _norm_color_token(s) == "navy"


def _is_apple_watch_item(item: dict) -> bool:
    brand = _norm_str(get_field(item, "brand"))
    if brand != "apple":
        return False
    series = _norm_str(get_field(item, "series"))
    model = _norm_str(get_field(item, "model"))
    return ("watch" in series) or ("watch" in model)


def _is_starlight_color(s: str) -> bool:
    ns = _norm_color_token(s)
    return ns == "starlight"


def _is_silver_color(s: str) -> bool:
    ns = _norm_color_token(s)
    return ns == "silver"


def _codes_equal(etalon_item: dict, parsed_item: dict, cat: str) -> bool:
    e_code = _norm_field_value(cat, "code", get_field(etalon_item, "code"))
    p_code = _norm_field_value(cat, "code", get_field(parsed_item, "code"))
    return bool(e_code and p_code and e_code == p_code)


def _code_is_strong_match(code: str) -> bool:
    """
    Treat "real" product codes as a strict match, but avoid short-circuiting
    on model-like codes (e.g. Dyson HS08/HD16/SV46).
    """
    code = _norm_str(code)
    if not code:
        return False
    if len(code) <= 4:
        return False
    if re.match(r"^(hs|hd|ht|ph|sv|v)\\d", code, flags=re.IGNORECASE):
        return False
    return True


# ======================
# ✅ Multi-color helpers
# ======================

def _extract_color_candidates(item: dict, cat: str) -> List[str]:
    """
    Возвращает список возможных цветов товара в порядке приоритета:
      1) color (главный)
      2) color_1, color_2
      3) colors[] (если есть)
    Все значения нормализуем для сравнения (lower/strip).
    """
    out: List[str] = []

    def _push(v: Any):
        s = _norm_field_value(cat, "color", v)
        if s and s not in out:
            out.append(s)

    _push(get_field(item, "color"))
    _push(get_field(item, "color_1"))
    _push(get_field(item, "color_2"))

    colors_list = get_field(item, "colors")
    if isinstance(colors_list, list):
        for x in colors_list[:5]:
            _push(x)

    return out


def _colors_match(cat: str, etalon_item: dict, parsed_item: dict, *, code_same: bool) -> bool:
    """
    Строгое сравнение цвета с поддержкой multi-color + совместимости:
      1) exact match (как раньше)
      2) canon/compat match (silver == white, starlight == white, space gray == gray, ...)
    """
    e_cols = _extract_color_candidates(etalon_item, cat)
    p_cols = _extract_color_candidates(parsed_item, cat)

    # если где-то цвет задан — требуем, чтобы у второго тоже было что сравнивать
    if (e_cols or p_cols) and (not e_cols or not p_cols):
        return False

    if not e_cols and not p_cols:
        return True

    # 1) exact match
    if any(ec == pc for ec in e_cols for pc in p_cols):
        return True

    # S25 Ultra: Jet Black and Black are distinct (no compat match).
    if _is_s25_ultra_item(etalon_item) and _is_s25_ultra_item(parsed_item):
        e_has_jet = any(_is_jet_black_color(c) for c in e_cols)
        p_has_jet = any(_is_jet_black_color(c) for c in p_cols)
        e_has_black = any(_is_black_color(c) for c in e_cols)
        p_has_black = any(_is_black_color(c) for c in p_cols)
        if (e_has_jet and p_has_black) or (e_has_black and p_has_jet):
            return False

    # S25 family: Navy and Icy Blue are distinct (no compat match).
    if _is_s25_family_item(etalon_item) and _is_s25_family_item(parsed_item):
        e_has_icy = any(_is_icy_blue_color(c) for c in e_cols)
        p_has_icy = any(_is_icy_blue_color(c) for c in p_cols)
        e_has_navy = any(_is_navy_color(c) for c in e_cols)
        p_has_navy = any(_is_navy_color(c) for c in p_cols)
        if (e_has_icy and p_has_navy) or (e_has_navy and p_has_icy):
            return False
    # Apple Watch: Starlight and Silver are distinct (no compat match).
    if cat == "умные часы" and _is_apple_watch_item(etalon_item) and _is_apple_watch_item(parsed_item):
        e_has_star = any(_is_starlight_color(c) for c in e_cols)
        p_has_star = any(_is_starlight_color(c) for c in p_cols)
        e_has_silver = any(_is_silver_color(c) for c in e_cols)
        p_has_silver = any(_is_silver_color(c) for c in p_cols)
        if (e_has_star and p_has_silver) or (e_has_silver and p_has_star):
            return False

    # 2) canon/compat match (работает и без code_same)
    # code_same оставляем в сигнатуре функции, но теперь он не ограничивает логику.
    e_can = {_canon_color(x) for x in e_cols if x}
    p_can = {_canon_color(x) for x in p_cols if x}

    if not e_can or not p_can:
        return False

    if e_can.intersection(p_can):
        return True

    for ec in e_can:
        for pc in p_can:
            if _colors_compatible(ec, pc):
                return True

    return False


# ======================
# Price helpers
# ======================

def _to_price_float(v) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        try:
            return float(v)
        except Exception:
            return None

    s = str(v).strip()
    if not s:
        return None

    s = s.replace("\u00A0", " ")
    for tok in ("₽", "руб", "р.", "rub", "rur", "$", "usd", "€", "eur"):
        s = s.replace(tok, " ").replace(tok.upper(), " ")

    s = s.strip().replace(" ", "").replace("_", "")
    s2 = []
    for ch in s:
        if ch.isdigit() or ch in (".", ","):
            s2.append(ch)
    s = "".join(s2).replace(",", ".")

    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _dedup_prices(prices: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    uniq = {}
    for p in prices:
        key = (
            _norm_str(p.get("channel")),
            float(p.get("price")) if p.get("price") is not None else None,
            _norm_str(p.get("raw")),
        )
        uniq[key] = p
    out = list(uniq.values())
    out.sort(key=lambda x: (x.get("price") is None, x.get("price")))
    return out


# =========================
# Строгая проверка совпадения
# =========================

def match_product(etalon_item: dict, parsed_item: dict) -> Tuple[bool, str]:
    """
    Правила:
    1) model: если заполнено хотя бы у одного -> у второго тоже и должно совпасть
    2) ALWAYS_EQUAL_FIELDS:
       если заполнено хотя бы у одного -> у второго тоже и должно совпасть
       (connectivity учитывает спец-правило планшетов LTE==5G)
    3) BOTH_FILLED_FIELDS:
       сравниваем только если заполнены у ОБОИХ
    4) Остальные поля из MATCH_FIELDS:
       сравниваем только то, что задано в эталоне (важно для смартфонов connectivity)
    5) region не участвует
    6) умные часы:
       - band_color: проверим только если заполнено у обоих
       - если в эталоне band_* есть, а в parsed нет -> матч разрешаем (soft_optional)
    """
    cat = _pick_category(etalon_item, parsed_item)
    fields = MATCH_FIELDS.get(cat, MATCH_FIELDS["_default"])

    soft_ignored = SOFT_IGNORE_FIELDS_BY_CAT.get(cat, set())
    soft_optional = SOFT_OPTIONAL_FIELDS_BY_CAT.get(cat, set())
    code_same = _codes_equal(etalon_item, parsed_item, cat)
    if code_same:
        code_val = _norm_field_value(cat, "code", get_field(etalon_item, "code"))
        if _code_is_strong_match(code_val):
            return True, ""

    # 0) model как обязательное совпадение (если где-то задано)
    e_model = _norm_field_value(cat, "model", get_field(etalon_item, "model"))
    p_model = _norm_field_value(cat, "model", get_field(parsed_item, "model"))
    if (e_model or p_model) and e_model != p_model:
        return False, "model не совпал"

    # 1) ALWAYS_EQUAL_FIELDS (глобально)
    # смартфоны: connectivity НЕ always-equal — только если задано в эталоне (см. ниже)
    for f in sorted(ALWAYS_EQUAL_FIELDS):
        if f == "year":
            continue
        if f in soft_ignored:
            continue
        if cat in ("смартфоны", "smartphones") and f == "connectivity":
            continue
        if cat == "ноутбуки" and f == "chip":
            continue

        # ✅ multi-color compare (+ compat)
        if f == "color":
            if not _colors_match(cat, etalon_item, parsed_item, code_same=code_same):
                return False, "color не совпал"
            continue

        e_v = _norm_field_value(cat, f, get_field(etalon_item, f))
        p_v = _norm_field_value(cat, f, get_field(parsed_item, f))

        if f == "ram" and cat in ("смартфоны", "smartphones", "планшеты", "tablets"):
            # RAM у смартфонов и планшетов сравниваем только если заполнено у обоих
            if not e_v or not p_v:
                continue
        if f == "band_size" and cat in ("умные часы", "watches"):
            # band_size у часов сравниваем только если заполнено у обоих
            if not e_v or not p_v:
                continue
        if f == "connectivity" and cat in ("умные часы", "watches"):
            # connectivity у часов сравниваем только если заполнено у обоих
            if not e_v or not p_v:
                continue

        # смягчение для часов
        if f in soft_optional and e_v and not p_v:
            continue

        if (e_v or p_v) and e_v != p_v:
            return False, f"{f} не совпал"

    # 2) BOTH_FILLED_FIELDS
    for f in sorted(BOTH_FILLED_FIELDS):
        if f in soft_ignored:
            continue
        if f == "band_color":
            e_v = _canon_color(get_field(etalon_item, f))
            p_v = _canon_color(get_field(parsed_item, f))
        else:
            e_v = _norm_field_value(cat, f, get_field(etalon_item, f))
            p_v = _norm_field_value(cat, f, get_field(parsed_item, f))
        if e_v and p_v and e_v != p_v:
            return False, f"{f} не совпал"

    # 3) Остальные поля из MATCH_FIELDS
    for f in fields:
        if f in soft_ignored:
            continue

        # connectivity у смартфонов сравниваем тут (только если задан в эталоне)
        if f in ALWAYS_EQUAL_FIELDS and not (cat in ("смартфоны", "smartphones") and f == "connectivity"):
            continue

        if f in BOTH_FILLED_FIELDS or f == "model":
            continue

        # color уже проверили выше (ALWAYS_EQUAL_FIELDS)
        if f == "color":
            continue

        e_v = _norm_field_value(cat, f, get_field(etalon_item, f))
        if not e_v:
            continue  # если в эталоне поле не задано — не сравниваем

        p_v = _norm_field_value(cat, f, get_field(parsed_item, f))

        if f in soft_optional and not p_v:
            continue

        if e_v != p_v:
            return False, f"{f} не совпал"

    return True, ""


# =========================
# === Сигнатуры (для индекса)
# =========================

def _sig_color_value(item: dict, cat: str) -> str:
    """
    Для сигнатуры используем один "главный" цвет, чтобы не ломать формат ключей.
    Приоритет:
      color -> color_1 -> first(colors[])
    """
    v = get_field(item, "color")
    s = _norm_field_value(cat, "color", v)
    if s:
        return s

    v = get_field(item, "color_1")
    s = _norm_field_value(cat, "color", v)
    if s:
        return s

    colors_list = get_field(item, "colors")
    if isinstance(colors_list, list) and colors_list:
        s = _norm_field_value(cat, "color", colors_list[0])
        if s:
            return s

    return ""


def _sig_for_category(item: dict, cat: str, *, relaxed: bool = False) -> str:
    cat = (cat or "").strip().lower() or "_default"
    fields = MATCH_FIELDS.get(cat, MATCH_FIELDS["_default"]).copy()

    soft_ignored = SOFT_IGNORE_FIELDS_BY_CAT.get(cat, set())
    relaxed_drop = RELAXED_SIG_DROP_FIELDS_BY_CAT.get(cat, set()) if relaxed else set()

    parts: List[str] = []
    for f in fields:
        if f in soft_ignored:
            parts.append("")
            continue
        if relaxed and f in relaxed_drop:
            parts.append("")
            continue

        if f == "color":
            parts.append(_sig_color_value(item, cat))
            continue

        v = _norm_field_value(cat, f, get_field(item, f))
        parts.append(v)

    # ноутбуки: code входит в сигнатуру
    if cat == "ноутбуки":
        parts.append(_norm_field_value(cat, "code", get_field(item, "code")))

    return "|".join(parts)


def _primary_keys(item: dict) -> List[str]:
    cat = get_cat(item) or "_default"
    keys: List[str] = []

    # базовая сигнатура
    sig = _sig_for_category(item, cat, relaxed=False)
    if sig.strip("|"):
        keys.append(f"sig:{cat}:{sig}")

    # relaxed-сигнатура (важно для часов)
    if cat in RELAXED_SIG_DROP_FIELDS_BY_CAT:
        sig2 = _sig_for_category(item, cat, relaxed=True)
        if sig2.strip("|") and sig2 != sig:
            keys.append(f"sig2:{cat}:{sig2}")

    # дополнительный ключ для multi-color:
    # если есть color_2, добавим альтернативную sig, где цвет = color_2
    c2 = _norm_field_value(cat, "color", get_field(item, "color_2"))
    if c2:
        # строим "alt sig" только в тех категориях, где color участвует
        if "color" in MATCH_FIELDS.get(cat, MATCH_FIELDS["_default"]):
            base_fields = MATCH_FIELDS.get(cat, MATCH_FIELDS["_default"])
            alt_parts: List[str] = []
            soft_ignored = SOFT_IGNORE_FIELDS_BY_CAT.get(cat, set())
            for f in base_fields:
                if f in soft_ignored:
                    alt_parts.append("")
                    continue
                if f == "color":
                    alt_parts.append(c2)
                else:
                    alt_parts.append(_norm_field_value(cat, f, get_field(item, f)))
            if cat == "ноутбуки":
                alt_parts.append(_norm_field_value(cat, "code", get_field(item, "code")))
            sigc2 = "|".join(alt_parts)
            if sigc2.strip("|") and sigc2 != sig:
                keys.append(f"sigc2:{cat}:{sigc2}")

    # модельный ключ
    m = _norm_field_value(cat, "model", get_field(item, "model"))
    if m:
        keys.append(f"model:{cat}:{m}")
        if cat != "_default":
            keys.append(f"model:_default:{m}")

    # ноутбуки: code-ключ
    if cat == "ноутбуки":
        c = _norm_field_value(cat, "code", get_field(item, "code"))
        if c:
            keys.append(f"code:{c}")

    out: List[str] = []
    seen = set()
    for k in keys:
        if k in seen:
            continue
        seen.add(k)
        out.append(k)
    return out


# =========================
# Этап: сопоставление эталона и пула
# =========================

def match_etalon_with_parsed(parsed_etalon: List[dict], parsed_pool: List[dict]):
    results: List[dict] = []
    unmatched_etalon: List[dict] = []
    stats = {"matched_etalon_items": 0, "unmatched_etalon_items": 0, "unmatched_parsed_items": 0, "channels": {}}

    parsed_index: Dict[str, List[int]] = {}
    parsed_used = [False] * len(parsed_pool)

    for i, p in enumerate(parsed_pool):
        if not isinstance(p, dict):
            continue
        for k in _primary_keys(p):
            parsed_index.setdefault(k, []).append(i)

    def _candidates_for(et: dict) -> List[int]:
        idx: List[int] = []
        for k in _primary_keys(et):
            arr = parsed_index.get(k)
            if arr:
                idx.extend(arr)
        if not idx:
            return []
        seen = set()
        out: List[int] = []
        for x in idx:
            if x in seen:
                continue
            seen.add(x)
            out.append(x)
        return out

    def _strip_etalon_runtime_fields(x: dict) -> dict:
        out = dict(x or {})
        for k in ("date", "message_id", "channel", "price"):
            out.pop(k, None)
        return out

    for e_item in parsed_etalon:
        if not isinstance(e_item, dict):
            continue

        cand_idx = _candidates_for(e_item)
        if not cand_idx:
            unmatched_etalon.append(
                {"raw_parsed": e_item.get("raw_parsed") or e_item.get("raw"), "reason": "нет кандидатов", "path": e_item.get("path")}
            )
            stats["unmatched_etalon_items"] += 1
            continue

        filtered_idx: List[int] = []
        for i in cand_idx:
            try:
                ok, _ = match_product(e_item, parsed_pool[i])
            except Exception:
                ok = False
            if ok:
                filtered_idx.append(i)

        if not filtered_idx:
            unmatched_etalon.append(
                {"raw_parsed": e_item.get("raw_parsed") or e_item.get("raw"), "reason": "кандидаты есть, но не прошли строгий match", "path": e_item.get("path")}
            )
            stats["unmatched_etalon_items"] += 1
            continue

        price_items: List[Dict[str, Any]] = []
        raw_lines: List[str] = []
        all_channels: List[str] = []

        for i in filtered_idx:
            p = parsed_pool[i] or {}
            parsed_used[i] = True

            raw_line = (p.get("raw_parsed") or p.get("raw") or "").strip()
            if raw_line:
                raw_lines.append(raw_line)

            ch = (p.get("channel") or "").strip()
            if ch:
                all_channels.append(ch)

            pv = _to_price_float(p.get("price"))
            if pv is None:
                continue
            price_items.append({"price": pv, "channel": ch, "raw": raw_line})

        if price_items:
            price_items = _dedup_prices(price_items)
            min_price = price_items[0]["price"]

            best_channels = sorted({
                (p.get("channel") or "").strip()
                for p in price_items
                if p.get("price") is not None
                and abs(float(p["price"]) - float(min_price)) <= 0.0001
                and (p.get("channel") or "").strip()
            })
            best_channel = best_channels
        else:
            min_price = None
            best_channel = []

        out_et = _strip_etalon_runtime_fields(e_item)

        out_et["raw_channels"] = sorted(set([x for x in all_channels if x]))
        out_et["raw_lines"] = sorted(set([x for x in raw_lines if x]))

        out_et["prices"] = price_items
        out_et["min_price"] = min_price
        out_et["best_channel"] = best_channel

        results.append(out_et)
        stats["matched_etalon_items"] += 1

        for ch in (best_channel or []):
            stats["channels"][ch] = stats["channels"].get(ch, 0) + 1

    unmatched_parsed: List[dict] = []
    for i, used in enumerate(parsed_used):
        if used:
            continue
        p = parsed_pool[i] or {}
        unmatched_parsed.append(
            {
                "raw_parsed": p.get("raw_parsed") or p.get("raw"),
                "channel": p.get("channel"),
                "reason": "не найдено соответствия в эталоне",
                "params": {
                    "path": p.get("path"),
                    "model": get_field(p, "model"),
                    "storage": get_field(p, "storage"),
                    "ram": get_field(p, "ram"),
                    "color": get_field(p, "color"),
                    "color_1": get_field(p, "color_1"),
                    "color_2": get_field(p, "color_2"),
                    "colors": get_field(p, "colors"),
                    "sim": get_field(p, "sim"),
                    "code": get_field(p, "code"),
                    "screen_size": get_field(p, "screen_size"),
                    "connectivity": get_field(p, "connectivity"),
                    "chip": get_field(p, "chip"),
                    "year": get_field(p, "year"),
                    "anc": get_field(p, "anc"),
                    "case": get_field(p, "case"),
                    "watch_size_mm": get_field(p, "watch_size_mm"),
                    "band_size": get_field(p, "band_size"),
                    "band_type": get_field(p, "band_type"),
                    "band_color": get_field(p, "band_color"),
                    "region": get_field(p, "region"),
                },
            }
        )

    stats["unmatched_parsed_items"] = len(unmatched_parsed)
    return results, stats, unmatched_etalon, unmatched_parsed


# =========================
# Runner (I/O)
# =========================

def _load_items(path: Path) -> List[dict]:
    if not path.exists():
        return []
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if isinstance(obj, dict):
        items = obj.get("items") or []
        return [x for x in items if isinstance(x, dict)]
    if isinstance(obj, list):
        return [x for x in obj if isinstance(x, dict)]
    return []


def _write_json(path: Path, obj: Any):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def run_matcher(
    *,
    etalon_path: Path = ETALON_FILE,
    goods_path: Path = GOODS_FILE,
    matched_path: Path = MATCHED_FILE,
    stats_path: Path = MATCH_STATS_FILE,
    unmatched_etalon_path: Path = UNMATCHED_ETALON_FILE,
    unmatched_parsed_path: Path = UNMATCHED_PARSED_FILE,
) -> dict:
    parsed_etalon = _load_items(etalon_path)
    parsed_pool = _load_items(goods_path)

    matched, stats, unmatched_etalon, unmatched_parsed = match_etalon_with_parsed(parsed_etalon, parsed_pool)

    _write_json(
        matched_path,
        {
            "items": matched,
            "items_count": len(matched),
            "source": {"etalon": str(etalon_path), "goods": str(goods_path)},
        },
    )

    try:
        if results_builder is not None:
            results_builder.rebuild_parsed_data_all()
        else:
            print("[matcher] ⚠️ results_builder import failed (skipped parsed_data rebuild)")
    except Exception as e:
        print(f"[matcher] ⚠️ parsed_data rebuild failed: {e}")

    _write_json(stats_path, stats)
    _write_json(unmatched_etalon_path, {"items": unmatched_etalon, "items_count": len(unmatched_etalon)})
    _write_json(unmatched_parsed_path, {"items": unmatched_parsed, "items_count": len(unmatched_parsed)})

    return {
        "status": "ok",
        "etalon": len(parsed_etalon),
        "goods": len(parsed_pool),
        "matched": len(matched),
        "unmatched_etalon": len(unmatched_etalon),
        "unmatched_parsed": len(unmatched_parsed),
        "out": {
            "matched": str(matched_path),
            "stats": str(stats_path),
            "unmatched_etalon": str(unmatched_etalon_path),
            "unmatched_parsed": str(unmatched_parsed_path),
        },
    }


def run() -> dict:
    return run_matcher()


def main() -> dict:
    return run_matcher()
