# handlers/normalizers/entry.py
# ============================================================
# ENTRY.PY (single-file pipeline orchestrator)
#
# 0) (optional) parser.py already produced parsed_messages.json
# 1) data.json(etalon) -> parsed_etalon.json (+ model/code indexes)
# 2) parsed_messages.json -> parsed_goods.json (+ unmatched)
# 3) matcher.py -> parsed_matched.json + stats + unmatched*
#
# ✅ Local run emulates parser.py behavior:
#    build etalon -> build goods -> run matcher
#
# NOTE:
# - results.py здесь НЕ вызываем.
# - Добавлен "title-search mode" для аксессуаров/ремешков/консолей:
#   чтобы строки типа "42mm Modern Buckle - M - 15000" и "Rugged Case ..."
#   НЕ матчились в iPhone-модель по короткому алиасу.
# - Добавлены исключения: Dyson HS08 "(Presentation case)" не должен триггерить.
#
# ✅ FIX (важное):
# - Если у модели НЕ сгенерировалось ни одного “безопасного” alias (из-за фильтров),
#   мы принудительно добавляем fallback-alias на базе full canonical name
#   (series+model / brand+series+model / cat+series+model) — чтобы model_index
#   никогда не “терял” модели.
#
# ✅ FIX (fallback match):
# - resolve_meta_for_line теперь пробует:
#   1) strict по остаткам (tail-consume)
#   2) normal match по ORIGINAL raw_line (важно для строк “17 256 Gb ...”)
#   3) normal match по остаткам
#   4) match по коду
# ============================================================

from __future__ import annotations

import sys
import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Iterable
from collections import Counter, defaultdict

# ===== project root bootstrap =====
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from handlers.normalizers import text_utils as tu
from handlers.normalizers import entry_dicts as D
from handlers.normalizers import entry_regex as R
from handlers.parsing import matcher as matcher_mod

logger = logging.getLogger("parsing.entry")

# ============================================================
# PATHS
# ============================================================

BASE_DIR = ROOT
DATA_DIR = BASE_DIR / "handlers" / "parsing" / "data"

ROOT_DATA_JSON = BASE_DIR / "data.json"

PARSED_ETALON_JSON = DATA_DIR / "parsed_etalon.json"
PARSED_MESSAGES_JSON = DATA_DIR / "parsed_messages.json"
PARSED_GOODS_JSON = DATA_DIR / "parsed_goods.json"

# debug artifacts
ETALON_STATS_JSON = DATA_DIR / "etalon_stats.json"
MODEL_ALIASES_JSON = DATA_DIR / "model_aliases.json"
MODEL_INDEX_JSON = DATA_DIR / "model_index.json"  # meta index for fast loading
CODE_INDEX_JSON = DATA_DIR / "code_index.json"    # code->model meta index
LEARNED_TOKENS_JSON = DATA_DIR / "etalon_learned_tokens.json"
ALIAS_COLLISIONS_JSON = DATA_DIR / "alias_collisions.json"

# pipeline targets (matcher/results)
PARSED_MATCHED_JSON = DATA_DIR / "parsed_matched.json"
UNMATCHED_PARSED_JSON = DATA_DIR / "unmatched_parsed.json"

MATCH_STATS_JSON = DATA_DIR / "match_stats.json"
UNMATCHED_ETALON_JSON = DATA_DIR / "unmatched_etalon.json"
UNMATCHED_PARSED_FROM_MATCHER_JSON = DATA_DIR / "unmatched_parsed_from_matcher.json"

SCOPE_ETALON = "etalon_all_categories_v1"
SCOPE_GOODS = "goods_from_messages_v1"

# ============================================================
# IO (atomic save)
# ============================================================

def _load_json(path: Path, default: Any):
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning("Failed to read json %s: %s", path, e)
        return default


def _save_json(path: Path, obj: Any) -> None:
    """
    Atomic save: write to .tmp then replace.
    Prevents half-written JSON on crash.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _build_id_for_file(path: Path) -> str:
    """
    Cheap build id to detect changes (mtime+size).
    """
    try:
        st = path.stat()
        return f"{int(st.st_mtime)}:{int(st.st_size)}"
    except Exception:
        return "0:0"


def _clean(s: str) -> str:
    return tu.clean_generic_text(tu.fix_confusables(s or ""))


def _nk(s: str) -> str:
    return tu.norm_key(s or "")


def _alias_key_safe(alias: str) -> str:
    """
    Key for alias without losing '+':
      'A9+' -> 'a9 plus'
    """
    a = tu.clean_spaces(alias or "")
    if not a:
        return ""
    a = a.replace("+", " plus ")
    a = tu.clean_spaces(a)
    return _nk(a)


# ============================================================
# ETALON iterator (data.json)
# ============================================================

def iter_raw_etalon_lines(db: Dict[str, Any]) -> Iterable[Tuple[List[str], str]]:
    """
    Expects:
      db["etalon"][cat][brand][series][model] = [lines]
    Returns: ([cat, brand, series, model], raw_line)
    """
    root = (db or {}).get("etalon")
    if not isinstance(root, dict):
        return
    for cat, brands in root.items():
        if not isinstance(brands, dict):
            continue
        for br, series_map in brands.items():
            if not isinstance(series_map, dict):
                continue
            for sr, models in series_map.items():
                if not isinstance(models, dict):
                    continue
                for model, lines in models.items():
                    if not model:
                        continue
                    if isinstance(lines, str):
                        lines = [lines]
                    if not isinstance(lines, list):
                        continue
                    for ln in lines:
                        if not isinstance(ln, str):
                            continue
                        raw = ln.strip()
                        if raw:
                            yield ([str(cat), str(br), str(sr), str(model)], raw)


# ============================================================
# Extractors
# ============================================================

def extract_price(text: str) -> Optional[int]:
    raw = text or ""

    cand: List[int] = []

    def _looks_like_model_storage_token(s: str) -> bool:
        m = re.fullmatch(r"\s*(\d{1,3})[ .,_](\d{3})\s*", s or "")
        if not m:
            return False
        a = int(m.group(1))
        b = int(m.group(2))
        if b in {64, 128, 256, 512, 1024, 2048} and 1 <= a <= 30:
            return True
        return False

    def to_int(tok: str) -> Optional[int]:
        t = (tok or "").strip()
        if R._RX_YEAR_20XX.fullmatch(t):
            return None
        t = t.replace(" ", "").replace("_", "").replace(",", "").replace(".", "")
        if not t.isdigit():
            return None
        v = int(t)
        if 1_000 <= v <= 1_000_000:
            return v
        return None

    for m in R._RX_MONEY.finditer(raw):
        if _looks_like_model_storage_token(m.group(1)):
            continue
        v = to_int(m.group(1))
        if v is not None:
            cand.append(v)

    if cand:
        return cand[-1]

    digits = re.findall(r"(?<!\d)(\d{4,7})(?!\d)", raw)
    for d in digits[::-1]:
        if R._RX_YEAR_20XX.fullmatch(d):
            continue
        v = int(d)
        if 1_000 <= v <= 1_000_000:
            return v

    # Heuristic: trailing short price like "- 15" means 15000 (no currency present).
    if not R.RX_PRICE_HINT.search(raw):
        m_short = re.search(r"(?:^|\s)[—–-]\s*(\d{1,3})\s*$", raw)
        if m_short:
            v = int(m_short.group(1))
            if 10 <= v <= 999:
                return v * 1000

    return None


def _canonicalize_storage(storage: Optional[str]) -> Optional[str]:
    if not storage:
        return storage
    s = storage.strip().upper()
    m = re.fullmatch(r"(\d+)\s*GB", s)
    if m:
        gb = int(m.group(1))
        if gb % 1024 == 0:
            tb = gb // 1024
            if tb >= 1:
                return f"{tb}TB"
        return f"{gb}GB"
    m = re.fullmatch(r"(\d+)\s*TB", s)
    if m:
        return f"{int(m.group(1))}TB"
    return storage


def extract_storage(text: str) -> Tuple[Optional[str], Optional[int]]:
    """
    Returns (storage_str, ram_int).
    Supports:
      "16GB 256GB", "(M4, 24GB, 512GB)", "8/256", "1TB", "256",
      "(M4, 16, 512GB)" (RAM without GB when storage present).
    """
    s = _clean(text)

    def _extract_bare_ram_when_storage_present(src: str) -> Optional[int]:
        nk2 = _nk(src)
        toks2 = nk2.split()
        allowed_storage_gb2 = {64, 128, 256, 512, 1024, 2048}
        ram_bare: List[int] = []
        for i, t in enumerate(toks2):
            if not t.isdigit():
                continue
            v = int(t)
            if v < 4:
                continue
            if 2000 <= v <= 2099:
                continue
            if i + 1 < len(toks2) and toks2[i + 1] in {"gb", "g", "tb", "t", "гб", "тб"}:
                continue
            if i + 1 < len(toks2) and toks2[i + 1] in {"sim", "esim"}:
                continue
            if i - 1 >= 0 and toks2[i - 1] in {"sim", "esim"}:
                continue
            if v in D.RAM_PLAUSIBLE and v not in allowed_storage_gb2:
                ram_bare.append(v)
        return min(ram_bare) if ram_bare else None

    allowed_gb = {64, 128, 256, 512, 1024, 2048}

    def _pick_best_slash(matches: List[Tuple[int, int, Optional[str]]]) -> Tuple[Optional[str], Optional[int]]:
        best = None
        for a, b, unit in matches:
            ram = a if a in D.RAM_PLAUSIBLE else None
            storage = None
            if unit in (None, "gb", "гб", "g"):
                if b in allowed_gb:
                    storage = f"{b}GB"
            elif unit in ("tb", "тб"):
                storage = f"{b}TB"
            if not storage:
                continue
            score = (1 if ram else 0, 1 if unit else 0, b)
            if best is None or score > best[0]:
                best = (score, storage, ram)
        if best:
            return best[1], best[2]
        return None, None

    slash_hits: List[Tuple[int, int, Optional[str]]] = []
    for m0 in R.RX_MEM_CONFIG_SLASH_NO_UNIT.finditer(s):
        a = int(m0.group("a"))
        b = int(m0.group("b"))
        slash_hits.append((a, b, None))
    for m in R.RX_MEM_CONFIG_SLASH.finditer(s):
        a = int(m.group("a"))
        b = int(m.group("b"))
        unit = (m.group("unit") or "").lower() or None
        slash_hits.append((a, b, unit))

    if slash_hits:
        storage, ram = _pick_best_slash(slash_hits)
        if storage or ram:
            return storage, ram

    mem_hits: List[Tuple[int, str]] = []
    for mm in R.RX_MEM_EXPLICIT_ALL.finditer(s):
        num = int(mm.group("num"))
        unit = (mm.group("unit") or "").lower()
        if unit in ("gb", "гб", "g"):
            if unit == "g" and num in {4, 5} and re.search(rf"(?i)\b{num}g\b", s):
                continue
            mem_hits.append((num, "GB"))
        elif unit in ("tb", "тб", "t"):
            mem_hits.append((num, "TB"))

    ram: Optional[int] = None
    storage: Optional[str] = None

    if mem_hits:
        def to_gb(n: int, u: str) -> int:
            return n * 1024 if u == "TB" else n

        storage_cands = [(n, u) for (n, u) in mem_hits if (u == "TB" or n in allowed_gb)]
        if storage_cands:
            n, u = max(storage_cands, key=lambda x: to_gb(x[0], x[1]))
            storage = f"{n}{u}"

        if len(mem_hits) >= 2:
            ram_cands = [
                n for (n, u) in mem_hits
                if u == "GB" and n in D.RAM_PLAUSIBLE and n not in allowed_gb
            ]
            if ram_cands:
                ram = min(ram_cands)

        if storage and ram is None:
            ram = _extract_bare_ram_when_storage_present(s)

        return storage, ram

    # ---------------------------------------------------------
    # ✅ FAST PATH: bare storage number (64/128/256/...) anywhere in text
    # (после slash-конфигов и явных единиц, чтобы не терять TB/GB)
    # ---------------------------------------------------------
    for m_fast in re.finditer(r"(?<!\d)(64|128|256|512|1024|2048)(?!\d)", s, flags=re.IGNORECASE):
        # пропускаем число, если это часть цены вида 128.400
        tail = s[m_fast.end(): m_fast.end() + 4]
        if re.match(r"[.,]\d{3}", tail):
            continue
        v = int(m_fast.group(1))
        ram0 = _extract_bare_ram_when_storage_present(s)
        return f"{v}GB", ram0

    # ---------------------------------------------------------
    # bare storage only before price/separator
    # ---------------------------------------------------------
    nk = _nk(s)
    toks = nk.split()

    cut_i = len(toks)

    # ✅ режем по явному сепаратору
    for sep in ("-", "—", "–"):
        if sep in toks:
            cut_i = min(cut_i, toks.index(sep))

    # ✅ режем по цене (часто без "-"): первое большое число
    for i, t in enumerate(toks):
        if t.isdigit():
            v = int(t)
            if v >= 10000:  # похоже на цену
                cut_i = min(cut_i, i)
                break

    toks_head = toks[:cut_i] if cut_i > 0 else toks

    for t in reversed(toks_head):
        m = re.fullmatch(r"(?i)(\d{2,4})(gb|g|tb|t)", t)
        if not m:
            continue
        num = int(m.group(1))
        unit = m.group(2).lower()
        if unit in ("tb", "t"):
            if num in (1, 2, 4):
                storage0 = f"{num}TB"
                ram0 = _extract_bare_ram_when_storage_present(" ".join(toks_head))
                return storage0, ram0
        else:
            if num in allowed_gb:
                storage0 = f"{num}GB"
                ram0 = _extract_bare_ram_when_storage_present(" ".join(toks_head))
                return storage0, ram0

    for t in reversed(toks_head):
        if t.isdigit():
            v = int(t)
            if v in allowed_gb:
                storage0 = f"{v}GB"
                ram0 = _extract_bare_ram_when_storage_present(" ".join(toks_head))
                return storage0, ram0

    return None, None



# -------------------------
# Colors
# -------------------------

_COLOR_MATCHERS: Optional[List[Tuple[re.Pattern, str]]] = None


def _rx_color_like_phrase(key: str) -> re.Pattern:
    raw = tu.fix_confusables(tu.clean_spaces(key or ""))
    if not raw:
        return re.compile(r"(?!)")
    toks = _nk(raw).split()
    if not toks:
        return re.compile(r"(?!)")
    sep = r"(?:[\s\-_\/]+)"
    pat = r"\b" + sep.join(re.escape(t) for t in toks) + r"\b"
    return re.compile(pat, re.IGNORECASE)


def _init_color_matchers():
    items: List[Tuple[str, str]] = []
    for k, v in (D.COLOR_SYNONYMS or {}).items():
        kk = (k or "").strip()
        vv = (v or "").strip()
        if kk and vv:
            items.append((kk, vv))
    for c in (D.BASE_COLORS or []):
        cc = (c or "").strip()
        if cc:
            items.append((cc, cc))
    items.sort(key=lambda x: len(x[0]), reverse=True)

    out: List[Tuple[re.Pattern, str]] = []
    seen = set()
    for key, canon in items:
        nk = _nk(key)
        if not nk or nk in seen:
            continue
        seen.add(nk)
        out.append((_rx_color_like_phrase(key), canon))
    return out


def extract_color(text: str) -> Optional[str]:
    global _COLOR_MATCHERS
    if _COLOR_MATCHERS is None:
        _COLOR_MATCHERS = _init_color_matchers()
    s = _clean(text)
    for rx, canon in _COLOR_MATCHERS:
        if rx.search(s):
            return canon
    return None


def extract_colors_all(text: str, limit: int = 3) -> List[str]:
    global _COLOR_MATCHERS
    if _COLOR_MATCHERS is None:
        _COLOR_MATCHERS = _init_color_matchers()
    s = _clean(text)
    out: List[str] = []
    seen = set()
    for rx, canon in _COLOR_MATCHERS:
        if rx.search(s):
            k = _nk(canon)
            if k and k not in seen:
                seen.add(k)
                out.append(canon)
                if len(out) >= max(1, limit):
                    break
    return out


def extract_region(text: str) -> Optional[str]:
    raw = text or ""

    # 1) emoji flags first
    for fl in tu.FLAG_RE.findall(raw):
        reg = D.REGION_FLAG_MAP.get(fl)
        if reg:
            return reg

    # 2) token-safe scan
    s_clean = _clean(raw)
    nk = _nk(s_clean)
    toks = nk.split()

    def _has_token(token: str) -> bool:
        token = (token or "").strip().lower()
        return bool(token) and token in toks

    for rx, reg in R.REGION_WORDS_STRICT:
        if not reg:
            continue
        reg_l = str(reg).lower().strip()
        if 2 <= len(reg_l) <= 3:
            if not _has_token(reg_l):
                continue
        if rx.search(s_clean):
            return reg_l

    return None


def extract_sim(text: str) -> Optional[str]:
    s_raw = tu.clean_spaces(text or "")
    if not s_raw:
        return None

    # 1) dual/2sim — самое специфичное
    if re.search(r"(?i)\b2\s*[- ]?\s*sim\b", s_raw) or re.search(r"(?i)\bdual\s*sim\b", s_raw):
        return "2sim"

    # 2) sim+esim — ДО одиночного esim
    if re.search(r"(?i)\bsim\s*\+\s*e\s*[- ]?\s*sim\b", s_raw) or re.search(r"(?i)\bnano\s*[- ]?\s*sim\s*\+\s*e\s*[- ]?\s*sim\b", s_raw):
        return "sim+esim"

    # 3) одиночный esim
    if re.search(r"(?i)\be\s*[- ]?\s*sim\b", s_raw):
        return "esim"

    # дальше твоя логика на cleaned (можно оставить как есть)
    s = _clean(s_raw)

    if R.RX_SIM_2SIM.search(s):
        return "2sim"
    if R.RX_SIM_NANO_ESIM.search(s) or R.RX_SIM_SIM_ESIM.search(s):
        return "sim+esim"
    if R.RX_SIM_ESIM.search(s):
        return "esim"

    if re.search(r"(?i)\bsim\b", s_raw):
        return "sim+esim"

    return None


# ============================================================
# iPhone SIM defaults (gen-aware)
# ============================================================

def _iphone_gen_from_text(brand: str, series: str, model: str) -> Optional[int]:
    s = " ".join([brand or "", series or "", model or ""])
    s = tu.clean_spaces(s)
    m = R._RX_IPHONE_GEN_NUM.search(s)
    if not m:
        return None
    g = m.group(1) or m.group(2)
    try:
        v = int(g)
        if 1 <= v <= 30:
            return v
    except Exception:
        pass
    return None


def _is_iphone(brand: str, series: str, model: str) -> bool:
    b = _nk(brand)
    s = _nk(series)
    m = _nk(model)
    return (b == "apple") and ("iphone" in s or "iphone" in m)


def apply_default_sim(*, brand: str, series: str, model: str, region: str, sim: str, cat: str = "") -> str:
    if sim:
        return sim
    if not _is_iphone(brand, series, model):
        if _nk(cat) in {"смартфоны", "smartphones", "phones"}:
            return "sim+esim"
        return ""
    if _iphone_gen_from_text(brand, series, model) == 17 and "air" in _nk(model):
        return "esim"
    reg = (region or "").strip().lower()
    if reg in {"cn", "china"}:
        reg = "ch"
    if reg in {"hk", "hongkong"}:
        reg = "hk"
    if reg in {"uae"}:
        reg = "ae"

    gen = _iphone_gen_from_text(brand, series, model)

    if gen is not None and gen <= 13:
        return "sim+esim"

    if gen == 14:
        if reg == "us":
            return "esim"
        if reg in {"ch", "hk"}:
            return "2sim"
        return "sim+esim"

    if gen == 17:
        if reg in {"us", "jp", "ae", "ca"}:
            return "esim"
        if reg == "ch":
            return "2sim"
        return "sim+esim"

    if reg == "us":
        return "esim"
    if reg in {"ch", "hk"}:
        return "2sim"
    return "sim+esim"


# -------------------------
# Watch: bands, sizes, screen, connectivity
# -------------------------

_BAND_TYPES: List[Tuple[re.Pattern, str]] = [
    (re.compile(r"(?i)\bmodern\s*buckle\b"), "Modern Buckle"),
    (re.compile(r"(?i)\bmagnetic\s*link\b"), "Magnetic Link"),
    (re.compile(r"(?i)\blink\s*bracelet\b|\blb\b"), "Link Bracelet"),
    (re.compile(r"(?i)\bsport\s*band\b|\bsb\b"), "Sport Band"),
    (re.compile(r"(?i)\bsport\s*loop\b|\bsl\b"), "Sport Loop"),
    (re.compile(r"(?i)\btrail\s*loop\b|\btl\b"), "Trail Loop"),
    (re.compile(r"(?i)\bcharcoal\s*loop\b"), "Trail Loop"),
    (re.compile(r"(?i)\balpine\s*loop\b|\balp\s*lp\b|\bal\b"), "Alpine Loop"),
    (re.compile(r"(?i)\balpine\b"), "Alpine Loop"),
    (re.compile(r"(?i)\bocean\s*band\b|\bob\b"), "Ocean Band"),
    (re.compile(r"(?i)\bocean\b"), "Ocean Band"),
    (re.compile(r"(?i)\bmilanese\s*loop\b"), "Milanese Loop"),
    (re.compile(r"(?i)\bmilanese\b"), "Milanese Loop"),
    (re.compile(r"(?i)\bbraided\s*solo\s*loop\b"), "Braided Solo Loop"),
    (re.compile(r"(?i)\bsolo\s*loop\b"), "Solo Loop"),
]


def _is_tablet_context(cat: str, brand: str, series: str, model: str, raw: str) -> bool:
    s = " ".join([cat or "", brand or "", series or "", model or "", raw or ""])
    s = tu.clean_spaces(s)

    nk = _nk(s)
    if "планшет" in nk or "tablet" in nk:
        return True
    if "ipad" in nk:
        return True

    if R._RX_TABLET_SIGNALS.search(s):
        return True

    return False


def _is_watch_context(cat: str, brand: str, series: str, model: str, raw: str) -> bool:
    s = " ".join([cat or "", brand or "", series or "", model or "", raw or ""])
    nk = _nk(s)
    if cat and cat.strip().lower() in {"смартфоны", "smartphones", "планшеты", "tablets", "ноутбуки", "laptops"}:
        return False
    if ("watch" in nk) or ("часы" in nk) or ("apple watch" in nk) or re.search(r"(?i)\baw\b", nk) is not None:
        return True
    if re.search(r"(?i)\b(ultra|series|se)\b", nk):
        if re.search(r"(?i)\b(watch|aw|apple|galaxy\s*watch)\b", nk):
            return True
    if re.search(r"(?i)\b(3[8-9]|4[0-9])\b", nk) and re.search(r"(?i)\b(mm)\b", nk):
        return True
    return False


def extract_band_type(text: str) -> str:
    s = _clean(text)
    for rx, canon in _BAND_TYPES:
        if rx.search(s):
            return canon
    if re.search(r"(?i)\bloop\b", s):
        return "Sport Loop"
    return ""


def extract_band_size(text: str, *, watch_context: bool) -> str:
    if not watch_context:
        return ""
    s = _clean(text).replace("\\", "/")
    m = R._RX_BAND_SIZE.search(s)
    if m:
        v = ((m.group(1) or m.group(2)) or "").upper().replace(" ", "")
        if v == "S/L":
            return "S/M"
        if v in {"S/M", "M/L"}:
            return v
        if v in {"S", "M", "L"}:
            return v
        return ""
    m2 = R._RX_BAND_SIZE_COMPACT.search(s)
    if not m2:
        return ""
    # avoid false-positive on Samsung model codes like SM-L320
    if re.search(r"(?i)\bsm-\w*\d{2,4}\b", s):
        return ""
    v2 = (m2.group(1) or "").upper()
    if v2 == "SM":
        return "S/M"
    if v2 == "ML":
        return "M/L"
    if v2 == "SL":
        return "S/M"
    if re.search(r"(?i)\bL\b", s):
        return "L"
    return ""


def _looks_like_tablet_line(raw: str, toks: List[str]) -> bool:
    s = tu.clean_spaces(raw or "")
    if not s:
        return False
    if R._RX_TABLET_SIGNALS.search(s):
        return True
    tset = set(toks or [])
    if "tab" in tset or ("galaxy" in tset and "tab" in tset):
        return True
    return False


def extract_screen_size(text: str, *, cat: str = "", brand: str = "", series: str = "", model: str = "") -> str:
    s = _clean(text)

    if _is_watch_context(cat, brand, series, model, text):
        m0 = R._RX_SCREEN_MM.search(s)
        if m0:
            mm = m0.group("mm")
            if mm:
                return str(int(mm))
        m1 = R._RX_WATCH_MM_BARE.search(s)
        if m1:
            mm = m1.group("mm")
            if mm:
                return str(int(mm))
        return ""

    m = R._RX_SCREEN_MM.search(s)
    if m:
        mm = m.group("mm")
        if mm:
            return str(int(mm))

    m2 = R._RX_INCH.search(s)
    if m2:
        inch = m2.group("inch")
        if inch:
            return str(int(inch))

    # Apple iPad/Mac bare inches fallback
    ctx = " ".join([cat, brand, series, model, text])
    nk = _nk(ctx)

    is_apple = ("apple" in nk)
    is_ipad = ("ipad" in nk)
    is_mac = ("macbook" in nk) or ("imac" in nk)

    if not is_apple or not (is_ipad or is_mac):
        return ""

    toks = nk.split()
    if not toks:
        return ""

    allowed = {"11", "12", "13", "14", "15", "16", "17"}

    def is_junk(tok: str) -> bool:
        if re.fullmatch(r"20\d{2}", tok):
            return True
        if re.fullmatch(r"m\d{1,2}", tok):
            return True
        if re.fullmatch(r"\d{2,3}mm", tok):
            return True
        if re.fullmatch(r"\d+(gb|tb|g|t)", tok):
            return True
        return False

    for i, t in enumerate(toks):
        if t in allowed and not is_junk(t):
            window = toks[max(0, i - 2): i + 3]
            if any(w in window for w in ("ipad", "macbook", "air", "pro", "mini")):
                return t

    for t in toks:
        if t in allowed and not is_junk(t):
            return t

    return ""


def extract_connectivity(text: str) -> str:
    s = _clean(text)
    if R._RX_WIFI_CELL.search(s):
        return "Wi-Fi+Cellular"
    if R._RX_CELLULAR.search(s):
        return "Wi-Fi+Cellular"
    if R._RX_WIFI.search(s):
        return "Wi-Fi"
    if R._RX_5G.search(s):
        return "5G"
    if R._RX_4G.search(s):
        return "4G"
    if R._RX_LTE.search(s):
        return "LTE"
    return ""


def extract_drive(text: str) -> str:
    s = _nk(text)
    if not s:
        return ""
    if re.search(r"(?i)\b(disc|disk)\b", s) or re.search(r"(?i)\bс\s*дисковод\w*\b", s):
        return "disc"
    if re.search(r"(?i)\b(digital)\b", s) or re.search(r"(?i)\b(цифров)\w*\b", s):
        return "digital"
    if re.search(r"(?i)\bбез\s*дисковод\w*\b", s):
        return "digital"
    return ""


def extract_nano_glass(text: str) -> bool:
    raw = text or ""
    s_clean = _clean(raw)
    if not raw and not s_clean:
        return False
    pattern = (
        r"(?i)\b("
        r"nano\s*glass|"
        r"nano[-\s]*texture|"
        r"nano[-\s]*textur|"
        r"nano[-\s]*textured|"
        r"nano[-\s]*glass|"
        r"нано[-\s]*текстур|"
        r"нано[-\s]*текстурн|"
        r"нанотекстурн|"
        r"нано[-\s]*стекл|"
        r"наностекл"
        r")\w*\b"
    )
    for s in (raw, s_clean):
        if s and re.search(pattern, s):
            return True
    return False


def _strip_suffix_tokens(text: str, suffix: str) -> str:
    t = tu.clean_spaces(text or "")
    s = tu.clean_spaces(suffix or "")
    if not t or not s:
        return t
    tt = _nk(t).split()
    ss = _nk(s).split()
    if not tt or not ss or len(ss) > len(tt):
        return t
    if tt[-len(ss):] != ss:
        return t
    kept = tt[:-len(ss)]
    return " ".join(kept)


def _extract_watch_band_color(raw: str) -> str:
    s = _clean(raw)
    if not s:
        return ""

    band_rx = re.compile(
        r"(?i)\b("
        r"sport\s*band|sport\s*loop|trail\s*loop|charcoal\s*loop|alpine\s*loop|alp\s*lp|alpine|"
        r"ocean\s*band|\bob\b|ocean|milanese\s*loop|milanese|link\s*bracelet|solo\s*loop|braided\s*solo\s*loop|"
        r"\bsb\b|\bsl\b|\btl\b|\bal\b|\blb\b"
        r")\b"
    )
    m = band_rx.search(s)
    if not m:
        return ""

    band_token = (m.group(0) or "").lower()
    tail = tu.clean_spaces(s[m.end():] or "")
    if tail:
        tail2 = R._RX_BAND_SIZE.sub(" ", tail)
        tail2 = R._RX_BAND_SIZE_COMPACT.sub(" ", tail2)
        tail2 = tu.clean_spaces(tail2)

        toks = tail2.split()
        if toks:
            cand3 = " ".join(toks[:3])
            cand2 = " ".join(toks[:2])
            c = extract_color(cand3) or extract_color(cand2) or extract_color(toks[0])
            if c:
                return c

    prefix = tu.clean_spaces(s[:m.start()] or "")
    if not prefix:
        return ""

    # Drop case color tokens for Ultra before picking band color.
    # Example: "Black Titanium Blue Ocean Band" => band color should be Blue.
    prefix = re.sub(r"(?i)\b(black|natural)\s+titanium\b", " ", prefix)
    prefix = re.sub(r"(?i)\btitanium\b", " ", prefix)
    prefix = tu.clean_spaces(prefix)

    ptoks = prefix.split()
    if not ptoks:
        return ""

    if "charcoal" in band_token:
        # explicit charcoal loop => prefer Black/Charcoal when black is present
        if re.search(r"(?i)\bblack\b", prefix):
            return "Black/Charcoal"
        return "Charcoal"

    cand1 = ptoks[-1]
    cand2 = " ".join(ptoks[-2:]) if len(ptoks) >= 2 else ""
    cand3 = " ".join(ptoks[-3:]) if len(ptoks) >= 3 else ""

    c = extract_color(cand3) or extract_color(cand2) or extract_color(cand1)
    if c and c.lower() in {"black titanium", "natural titanium", "natural"}:
        c = ""
    if c:
        return c

    # fallback: if multiple colors exist, prefer explicit multi-color (e.g. Black/Charcoal),
    # otherwise use the last one (band color is usually last).
    colors = extract_colors_all(prefix, limit=5)
    if colors:
        # If we still have Titanium variants alongside other colors, prefer non-Titanium.
        non_ti = [
            cc
            for cc in colors
            if cc.lower() not in {"black titanium", "natural titanium", "natural"}
        ]
        if non_ti:
            colors = non_ti
        for cc in colors:
            if "/" in cc or "Charcoal" in cc:
                return cc
        return colors[-1]
    return ""


def _extract_watch_case_color(raw: str, *, band_color: str) -> str:
    s = _clean(raw)
    if not s:
        return ""

    band_rx = re.compile(
        r"(?i)\b("
        r"sport\s*band|sport\s*loop|trail\s*loop|charcoal\s*loop|alpine\s*loop|alp\s*lp|alpine|"
        r"ocean\s*band|\bob\b|ocean|milanese\s*loop|milanese|link\s*bracelet|solo\s*loop|braided\s*solo\s*loop|"
        r"\bsb\b|\bsl\b|\btl\b|\bal\b|\blb\b"
        r")\b"
    )
    m = band_rx.search(s)

    prefix = tu.clean_spaces(s[:m.start()] if m else s)

    prefix = re.sub(r"(?i)\b\d{2,3}\s*mm\b", " ", prefix)
    prefix = re.sub(r"(?i)\b(3[8-9]|4[0-9])\b", " ", prefix)
    prefix = tu.clean_spaces(prefix)

    if band_color:
        prefix = _strip_suffix_tokens(prefix, band_color)
        prefix = tu.clean_spaces(prefix)

    colors = extract_colors_all(prefix, limit=3)
    if colors:
        return colors[0]
    return extract_color(prefix) or ""


# -------------------------
# Chip + Year
# -------------------------

def _is_apple_chip_context(cat: str, brand: str, series: str, model: str, raw: str) -> bool:
    s = " ".join([cat or "", brand or "", series or "", model or "", raw or ""])
    nk = _nk(s)

    is_apple_brand = (_nk(brand) == "apple")
    device_anchors = ("macbook", "imac", "mac mini", "macmini", "mac studio", "macstudio", "ipad")
    has_device_anchor = any(a in nk for a in device_anchors)
    if not is_apple_brand and not has_device_anchor:
        return False
    return has_device_anchor


def extract_chip(text: str, *, cat: str = "", brand: str = "", series: str = "", model: str = "") -> str:
    if not _is_apple_chip_context(cat, brand, series, model, text or ""):
        return ""

    s = _clean(text)
    m = R._RX_CHIP_APPLE.search(s)
    if not m:
        return ""

    if m.group("a"):
        a = int(m.group("a"))
        if not (4 <= a <= 99):
            return ""
        suffix = (m.group("a_suffix") or "").lower().strip()
        base = f"A{a}"
        if suffix == "pro":
            return f"{base} Pro"
        if suffix == "bionic":
            return f"{base} Bionic"
        return base

    if m.group("m"):
        mm = int(m.group("m"))
        if not (1 <= mm <= 99):
            return ""
        suffix = (m.group("m_suffix") or "").lower().strip()
        base = f"M{mm}"
        if suffix:
            return f"{base} {suffix.capitalize()}"
        return base

    return ""


def extract_year(text: str) -> str:
    s = _clean(text)
    m = R._RX_YEAR.search(s)
    if not m:
        return ""
    return str(int(m.group(1)))


# -------------------------
# AirPods
# -------------------------

def _is_airpods_context(cat: str, brand: str, series: str, model: str, raw: str) -> bool:
    s = " ".join([cat or "", brand or "", series or "", model or "", raw or ""])
    nk = _nk(s)
    return ("airpods" in nk) or ("air pods" in nk)


def extract_anc(text: str, *, airpods_context: bool) -> str:
    if not airpods_context:
        return ""
    s = _clean(text)
    return "anc" if R._RX_ANC.search(s) else ""


def extract_case(text: str, *, airpods_context: bool) -> str:
    if not airpods_context:
        return ""
    s = _clean(text)
    if R._RX_USBC.search(s):
        return "usb-c"
    if R._RX_MAGSAFE.search(s):
        return "magsafe"
    if R._RX_LIGHTNING.search(s):
        return "lightning"
    return ""


# -------------------------
# Product code
# -------------------------

def extract_code(text: str) -> Optional[str]:
    raw = text or ""
    s_up = tu.fix_confusables(raw).upper()

    storage, _ram = extract_storage(raw)
    candidates: List[str] = []

    # Samsung-style codes: SM-F966B/DS -> F966B, or bare F766B/S938B
    m_sm = re.search(r"(?i)\bSM[-\s]?([A-Z]\d{3,4}[A-Z])(?:/DS)?\b", s_up)
    if m_sm:
        candidates.append(m_sm.group(1).upper())
    else:
        m_bare = re.search(r"(?i)\b([A-Z]\d{3,4}[A-Z])\b", s_up)
        if m_bare:
            candidates.append(m_bare.group(1).upper())

    for m in R.RX_CODE_TOKEN.finditer(s_up):
        t = (m.group(1) or "").strip().upper()
        if not t:
            continue
        if not t.isalnum():
            continue

        has_a = any("A" <= ch <= "Z" for ch in t)
        has_d = any(ch.isdigit() for ch in t)
        if not (has_a and has_d):
            continue

        # filter obvious non-codes
        if re.fullmatch(r"\d{1,4}(GB|G|TB|T)", t):
            continue
        if re.fullmatch(r"\d{2,3}MM", t):
            continue
        if t in {"SIM", "ESIM", "2SIM", "DUALSIM"}:
            continue
        if re.fullmatch(r"A\d{1,2}(PRO|BIONIC)?", t):
            continue
        if re.fullmatch(r"M\d(PRO|MAX|ULTRA)?", t):
            continue
        if re.fullmatch(r"20\d{2}", t):
            continue
        if t in {"ANC", "MAGSAFE", "LIGHTNING", "USB", "USBC", "TYPEC"}:
            continue
        if storage and t == str(storage).upper():
            continue

        # vendor-ish patterns we don't want as SKU
        if re.fullmatch(r"\d{3,4}XM\d", t):
            continue
        if re.fullmatch(r"(FOLD|FLIP)\d+", t):
            continue
        if re.fullmatch(r"STUDIO\d+", t):
            continue
        if re.fullmatch(r"CH\d{3}N?", t):
            continue
        if re.fullmatch(r"S\d{2,3}(FE|ULTRA|PLUS|EDGE|PRO)?", t):
            continue
        if re.fullmatch(r"A\d{2,3}", t):
            continue

        if len(t) <= 3:
            continue
        if not (4 <= len(t) <= 7):
            continue

        letters = sum(1 for ch in t if "A" <= ch <= "Z")
        digits = sum(1 for ch in t if ch.isdigit())
        if letters < 2 or digits < 2:
            continue
        if len(t) == 7 and digits > 4:
            continue

        candidates.append(t)

    return candidates[-1] if candidates else None


# ============================================================
# Item contract
# ============================================================

def make_item(
    *,
    path: List[str],
    brand: str,
    series: str,
    model: str,
    raw: str,
    params: Dict[str, Any],
    price: Optional[int],
    date: str = "",
    message_id: Optional[int] = None,
    channel: str = "",
) -> Dict[str, Any]:
    return {
        "path": path,
        "brand": brand,
        "series": series,
        "model": model,
        "code": (params.get("code") or ""),
        "params": params,
        "date": date,
        "message_id": message_id,
        "raw_parsed": raw,
        "channel": channel,
        "price": price,
    }


# ============================================================
# Canonical model name
# ============================================================

_ANCHOR_WORDS = {
    "iphone", "ipad", "macbook", "imac", "watch",
    "airpods", "beats", "pixel", "galaxy", "redmi", "poco",
    "xiaomi", "oneplus", "nothing", "huawei", "honor", "oppo", "vivo", "realme",
}


def canonical_model_name(*, brand: str, series: str, model: str) -> str:
    b = tu.clean_spaces(brand or "")
    s = tu.clean_spaces(series or "")
    m = tu.clean_spaces(model or "")
    if not m:
        return ""

    mk = _nk(m)
    sk = _nk(s)
    bk = _nk(b)

    if s and sk and sk in mk:
        return m
    if b and bk and bk in mk:
        return m

    toks = mk.split()
    if not toks:
        return m

    starts_digit = toks[0][0].isdigit()
    is_single_token = len(toks) == 1
    shortish = is_single_token and (len(toks[0]) <= 4)

    # small fix: if series ends with a digit and model begins with same digit token, avoid duplication
    if s:
        stoks = sk.split()
        if stoks and toks:
            if stoks[-1].isdigit() and toks[0] == stoks[-1]:
                # drop first token from model
                m2 = " ".join(toks[1:]).strip()
                if m2:
                    return tu.clean_spaces(f"{s} {m2}")

    if s and (starts_digit or shortish):
        return tu.clean_spaces(f"{s} {m}")

    if s and len(toks) == 2 and toks[0].isdigit():
        return tu.clean_spaces(f"{s} {m}")

    return m


# ============================================================
# Aliases (safe learning)
# ============================================================

_ALIAS_STOP_1TOK = {
    "pro", "max", "plus", "mini", "ultra", "buds", "fe",
    "pm", "p", "m", "l", "xl", "se",
    "wifi", "cellular", "lte", "5g",
}
_ROMAN_TOKENS = {"i", "ii", "iii", "iv", "v", "vi", "vii", "viii", "ix", "x"}
_SAFE_SINGLE_TOKEN_ALIASES = {"dualsense"}


def _has_plus_marker(s: str) -> bool:
    x = (s or "").lower()
    return ("+" in x) or (" plus" in x) or x.endswith(" plus")


def _is_year_token(tok: str) -> bool:
    if not tok or not tok.isdigit():
        return False
    y = int(tok)
    return 2000 <= y <= 2099


def _alias_is_bad_global(k: str) -> bool:
    if not k:
        return True
    toks = k.split()
    if not toks:
        return True

    head_bad = {"pro", "buds", "fe", "se", "plus", "max", "ultra", "mini"}
    if toks and toks[0] in head_bad:
        return True

    if len(toks) == 1:
        t = toks[0]
        if t in _SAFE_SINGLE_TOKEN_ALIASES:
            return False
        if t in _ALIAS_STOP_1TOK:
            return True
        if t in _ROMAN_TOKENS:
            return True
        if t.isdigit():
            return True
        if _is_year_token(t):
            return True
        if len(t) >= 2 and t[0] == "m" and t[1:].isdigit():
            return True
        return True

    if k in {"pro max", "pro", "plus", "buds"}:
        return True

    if len(toks) == 2 and toks[0].isdigit() and toks[1] in {"pro", "max", "plus"}:
        return True

    if len(toks) == 2 and toks[1] in _ROMAN_TOKENS:
        return True

    return False


def _alias_requires_anchor_for_chip(k: str) -> bool:
    toks = k.split()
    if not toks:
        return False
    chip = any(t in {"m1", "m2", "m3", "m4", "m5"} for t in toks)
    if not chip:
        return False
    anchors = {"mac", "macbook", "imac", "ipad"}
    return not any(a in toks for a in anchors)


def _alias_requires_anchor_for_pro_variants(k: str) -> bool:
    toks = k.split()
    if len(toks) < 2:
        return False
    if toks[-2:] in (["pro", "xl"], ["pro", "fold"]):
        anchors = {"pixel", "galaxy"}
        return not any(a in toks for a in anchors)
    return False


def _alias_plus_rule_violation(model: str, alias: str) -> bool:
    canon_has_plus = _has_plus_marker(model)
    if not canon_has_plus:
        return False
    return not _has_plus_marker(alias)


def _alias_blacklisted_model_only(alias_k: str) -> bool:
    toks = alias_k.split()
    badset = {"pro", "max", "plus", "mini", "ultra", "buds", "fe", "se"}
    return toks and all(t in badset for t in toks)


def _alias_ok_for_index(*, alias: str, model: str) -> bool:
    a2 = tu.clean_spaces(alias or "")
    k = _alias_key_safe(a2)
    if not k:
        return False
    if _alias_is_bad_global(k):
        return False
    if _alias_blacklisted_model_only(k):
        return False
    if _alias_plus_rule_violation(model, a2):
        return False
    if _alias_requires_anchor_for_chip(k):
        return False
    if _alias_requires_anchor_for_pro_variants(k):
        return False
    return True


def _should_add_series_plus_model(series: str, model: str) -> bool:
    s = tu.clean_spaces(series or "")
    m = tu.clean_spaces(model or "")
    if not s or not m:
        return False

    sk = _nk(s)
    mk = _nk(m)

    if sk and sk in mk:
        return False

    stoks = sk.split()
    mtoks = mk.split()

    if any(t in mtoks for t in stoks):
        return False

    if mtoks and mtoks[0] in _ANCHOR_WORDS:
        return False

    return True


def gen_model_aliases(cat: str, brand: str, series: str, model: str) -> List[str]:
    out: List[str] = []
    m = tu.clean_spaces(model or "")
    if not m:
        return out

    out.append(m)

    # year suffix alias: "AirPods Max 2024" -> "AirPods Max"
    m_no_year = re.sub(r"\b20\d{2}\b$", "", m).strip()
    if m_no_year and m_no_year != m and len(m_no_year.split()) >= 2:
        out.append(m_no_year)

    # Marshall Major: roman numerals IV/V -> 4/5 (and back)
    if "major" in m.lower():
        roman_to_arabic = {
            "i": 1,
            "ii": 2,
            "iii": 3,
            "iv": 4,
            "v": 5,
            "vi": 6,
            "vii": 7,
            "viii": 8,
            "ix": 9,
            "x": 10,
        }
        arabic_to_roman = {v: k.upper() for k, v in roman_to_arabic.items()}
        m_roman = re.sub(
            r"(?i)\b(i|ii|iii|iv|v|vi|vii|viii|ix|x)\b",
            lambda mm: str(roman_to_arabic[mm.group(1).lower()]),
            m,
        )
        if m_roman != m:
            out.append(m_roman)
        m_arabic = re.sub(
            r"\b([1-9]|10)\b",
            lambda mm: arabic_to_roman.get(int(mm.group(1)), mm.group(1)),
            m,
        )
        if m_arabic != m:
            out.append(m_arabic)

    if "+" in m:
        out.append(tu.clean_spaces(m.replace("+", " plus ")))

    def drop_prefix(prefix: str, full: str) -> Optional[str]:
        p = tu.clean_spaces(prefix or "")
        if not p:
            return None
        if tu.prefix_token_ok(p, full):
            if full.lower().startswith(p.lower() + " "):
                return tu.clean_spaces(full[len(p):])
        return None

    m2 = drop_prefix(brand, m)
    if m2 and m2 != m:
        out.append(m2)

    m3 = drop_prefix(series, m)
    if m3 and m3 != m:
        out.append(m3)

    if _should_add_series_plus_model(series, m):
        out.append(f"{series} {m}")

    # -------------------------
    # iPhone shorthand aliases
    # -------------------------
    if R.RX_IPHONE_GEN.search(m) or ("iphone" in m.lower()):
        ml = m.lower()
        if "iphone" in ml:
            out.append(tu.clean_spaces(ml.replace("iphone", "").strip()))
        if "pro max" in ml:
            out.append(tu.clean_spaces(ml.replace("pro max", "pm")))
            out.append(tu.clean_spaces(ml.replace("pro max", "p m")))

    # -------------------------
    # ✅ Accessories: iPhone cases (EN aliases)
    # -------------------------
    cat_nk = _nk(cat)
    sr_nk = _nk(series)
    m_nk = _nk(m)

    looks_like_case_bucket = (
        ("аксесс" in cat_nk)
        or ("чехл" in sr_nk)
        or ("case" in sr_nk)
        or ("чехл" in m_nk)
        or ("case" in m_nk)
    )

    if looks_like_case_bucket:
        out += [
            "iphone case",
            "case for iphone",
            "iphone rugged case",
            "rugged case iphone",
            "iphone protective case",
            "protective case iphone",
        ]

    # -------------------------
    # iPhone Air shorthand: "iPhone Air" -> "iPhone 17 Air"
    # -------------------------
    if _nk(brand) == "apple" and "iphone" in _nk(series) and re.search(r"(?i)\bair\b", m):
        out.append("iPhone Air")

    # -------------------------
    # iPad Air shorthand: allow chip-less size alias ("iPad Air 11")
    # -------------------------
    if _nk(brand) == "apple" and "ipad air" in _nk(series + " " + m):
        m_size = re.search(r"(?i)\b(11|13)\b", m)
        if m_size:
            out.append(f"iPad Air {m_size.group(1)}")

    # -------------------------
    # Samsung Galaxy S shorthand: "Galaxy S25" alias for "S25"
    # -------------------------
    if _nk(brand) == "samsung" and "galaxy s" in _nk(series):
        m_short = re.match(r"(?i)^(s\d{2,3}(?:\s*(?:ultra|plus|edge|fe))?)$", m)
        if m_short:
            out.append(f"Galaxy {m_short.group(1)}")
        m_plus = re.match(r"(?i)^(s\\d{2,3})\\s+plus$", m)
        if m_plus:
            base = m_plus.group(1)
            out.append(f"{base}+")
            out.append(f"Galaxy {base}+")

    # -------------------------
    # Samsung Galaxy A shorthand: "Galaxy A06" alias for "A06"
    # -------------------------
    if _nk(brand) == "samsung" and _nk(series) == "galaxy a":
        m_short = re.match(r"(?i)^(a\d{2,3}[a-z]?)$", m)
        if m_short:
            out.append(f"Galaxy {m_short.group(1)}")

    # -------------------------
    # Samsung Galaxy Tab shorthand: "Tab S10 FE", "Tab S10+"
    # -------------------------
    if _nk(brand) == "samsung" and "galaxy tab" in _nk(series + " " + m):
        if "Galaxy Tab" in m:
            tab_short = m.replace("Galaxy Tab", "Tab").strip()
            out.append(tab_short)
            if "FE+" in tab_short:
                out.append(tab_short.replace("FE+", "FE Plus"))
            if "+" in tab_short:
                out.append(tab_short.replace("+", " Plus"))
        elif _nk(series).startswith("galaxy tab"):
            out.append(f"Tab {m}".strip())

    # -------------------------
    # Samsung Z Fold/Flip shorthand: "Fold 7" / "Flip 7" variants
    # -------------------------
    m_z = re.match(r"(?i)^z\s*(fold|flip)\s*(\d{1,2})$", m.replace(" ", ""))
    if m_z:
        fam = m_z.group(1).title()
        num = m_z.group(2)
        out.append(f"Z {fam} {num}")
        out.append(f"{fam} {num}")
        # compact variants: "Fold7", "Z Fold7", "Galaxy Z Fold7"
        out.append(f"{fam}{num}")
        out.append(f"Z {fam}{num}")
        if _nk(brand) == "samsung" and "galaxy" in _nk(series):
            out.append(f"Galaxy Z {fam} {num}")
            out.append(f"Galaxy {fam} {num}")

    # -------------------------
    # Sony PS5 shorthand: "PS 5", "PlayStation 5", Pro/Slim variants
    # -------------------------
    if _nk(brand) == "sony" and _nk(series) == "ps5":
        out += [
            "PS 5",
            "PlayStation 5",
            "Play Station 5",
            "Sony PS 5",
            "Sony PS5",
            "PS5 Pro",
            "PS 5 Pro",
            "PS5 Pro Digital",
            "PS 5 Pro Digital",
            "PS5 Slim",
            "PS 5 Slim",
            "PS5 Slim Disc",
            "PS 5 Slim Disc",
            "PS5 Slim Digital",
            "PS 5 Slim Digital",
            "PS5 Digital Slim",
            "PS 5 Digital Slim",
        ]

    # -------------------------
    # Meta Quest aliases: "Oculus Quest 3" -> "Meta Quest 3"
    # -------------------------
    if _nk(brand) == "meta" and "oculus quest" in _nk(model):
        m_meta = re.sub(r"(?i)oculus\\s+quest", "Meta Quest", m)
        if m_meta != m:
            out.append(m_meta)
        m_short = re.sub(r"(?i)oculus\\s+quest", "Quest", m)
        if m_short != m:
            out.append(m_short)

    # -------------------------
    # Yandex Station aliases: allow dropping the "Станция" token and dot variants
    # -------------------------
    if _nk(brand) == "яндекс" and "Яндекс.Станция" in m:
        m_dot = m.replace("Яндекс.Станция", "Яндекс Станция")
        if m_dot != m:
            out.append(m_dot)
        m_no_brand = m.replace("Яндекс.Станция", "Станция")
        if m_no_brand != m:
            out.append(m_no_brand)
        m_short = m.replace("Яндекс.Станция", "Яндекс")
        if m_short != m:
            out.append(m_short)
        m_drop_station = m.replace("Яндекс.Станция ", "Яндекс ")
        if m_drop_station != m:
            out.append(m_drop_station)
        if "Мини 3 Про" in m:
            out.append(m.replace("Мини 3 Про", "Мини Про"))
        if "Мини 3" in m:
            out.append(m.replace("Мини 3", "Мини"))

    # -------------------------
    # Poco shorthand: "Poco F7", "Poco X7", "Poco M7 Pro"
    # -------------------------
    if _nk(brand) == "poco":
        out.append(f"{brand} {m}")

    # -------------------------
    # Apple Pencil shorthand: "Apple Pencil"
    # -------------------------
    if _nk(brand) == "apple" and _nk(series) == "pencil":
        out.append(f"{brand} {series}")

    # -------------------------
    # AirPods spaced variant: "Air Pods"
    # -------------------------
    if _nk(brand) == "apple" and _nk(series) == "airpods":
        if "AirPods" in m:
            out.append(m.replace("AirPods", "Air Pods"))
        if "Airpods" in m:
            out.append(m.replace("Airpods", "Air Pods"))

    # -------------------------
    # Apple Watch shorthand: Series 11 -> S11, Watch SE 2 -> SE2
    # -------------------------
    if _nk(brand) == "apple" and "watch" in _nk(series + " " + m):
        ml = m.lower()
        if "series 11" in ml:
            out += [
                "Watch S11",
                "Apple Watch S11",
                "AW S11",
                "Series 11",
            ]
        if re.search(r"\\bse\\s*2\\b", ml) or "se2" in ml:
            out += [
                "Watch SE2",
                "Apple Watch SE2",
                "AW SE2",
                "Watch SE 2",
                "Apple Watch SE 2",
                "AW SE 2",
            ]

    # -------------------------
    # Whoop 5.0 shorthand: allow "Whoop Life/Peak/One"
    # -------------------------
    if _nk(brand) == "whoop" and _nk(series) == "whoop 5.0":
        for variant in ("Life", "Peak", "One"):
            out.append(f"Whoop {variant}")
        out.append(m.replace("Whoop 5.0", "Whoop"))

    # -------------------------
    # uniq + safe key
    # -------------------------
    uniq: List[str] = []
    seen = set()
    for a in out:
        a2 = tu.clean_spaces(a)
        if not a2:
            continue
        k = _alias_key_safe(a2)
        if not k or k in seen:
            continue
        seen.add(k)
        uniq.append(a2)

    return uniq


def _forced_fallback_aliases(cat: str, br: str, sr: str, model: str) -> List[str]:
    """
    Когда после фильтров не осталось НИ ОДНОГО alias — мы принудительно создаём
    безопасные длинные алиасы, чтобы модель НЕ исчезала из model_index.
    """
    cat_s = tu.clean_spaces(cat or "")
    br_s = tu.clean_spaces(br or "")
    sr_s = tu.clean_spaces(sr or "")
    m_s = tu.clean_spaces(model or "")

    cand: List[str] = []

    # 1) canonical (с учётом series-склейки)
    canon = canonical_model_name(brand=br_s, series=sr_s, model=m_s)
    if canon:
        cand.append(canon)

    # 2) series + model (если вдруг canonical вернул коротко)
    if sr_s and m_s:
        cand.append(f"{sr_s} {m_s}")

    # 3) brand + (series + model)
    if br_s:
        if sr_s and m_s:
            cand.append(f"{br_s} {sr_s} {m_s}")
        elif canon:
            cand.append(f"{br_s} {canon}")

    # 4) last resort: category anchor
    if cat_s and sr_s and m_s:
        cand.append(f"{cat_s} {sr_s} {m_s}")

    # uniq
    out: List[str] = []
    seen = set()
    for a in cand:
        a2 = tu.clean_spaces(a)
        k = _alias_key_safe(a2)
        if not k or k in seen:
            continue
        seen.add(k)
        out.append(a2)
    return out


def build_model_index_and_aliases(
    etalon_items: List[Dict[str, Any]]
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, str], Dict[str, List[Dict[str, Any]]]]:
    idx: Dict[str, Dict[str, Any]] = {}
    aliases_map: Dict[str, str] = {}
    collisions: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    def _add_collision(k: str, meta_a: Dict[str, Any], meta_b: Dict[str, Any]) -> None:
        def add(m: Dict[str, Any]):
            p = tuple(m.get("path") or [])
            for x in collisions[k]:
                if tuple(x.get("path") or []) == p:
                    return
            collisions[k].append(m)
        add(meta_a)
        add(meta_b)

    forced_added = 0

    for it in etalon_items:
        path = it.get("path") or []
        if not isinstance(path, list) or len(path) < 4:
            continue
        cat, br, sr, model = (path + ["", "", "", ""])[:4]
        cat = str(cat).strip()
        br = str(br).strip()
        sr = str(sr).strip()
        model = str(model).strip()
        if not model:
            continue

        canon_model = canonical_model_name(brand=br, series=sr, model=model)

        meta = {
            "path": [cat, br, sr, model],
            "brand": br,
            "series": sr,
            "model": model,
            "canonical_model": canon_model,
        }

        kept_any = False

        for a in gen_model_aliases(cat, br, sr, model):
            if not _alias_ok_for_index(alias=a, model=model):
                continue

            k = _alias_key_safe(a)
            if not k:
                continue

            kept_any = True

            if k in idx:
                if idx[k].get("path") != meta.get("path"):
                    _add_collision(k, idx[k], meta)
            else:
                idx[k] = meta
                aliases_map[k] = canon_model

        # ✅ hard fallback: если фильтры выкинули всё — принудительно добавляем длинный алиас
        if not kept_any:
            forced = _forced_fallback_aliases(cat, br, sr, model)
            for a in forced:
                k = _alias_key_safe(a)
                if not k:
                    continue
                if k in idx:
                    if idx[k].get("path") != meta.get("path"):
                        _add_collision(k, idx[k], meta)
                    continue
                idx[k] = meta
                aliases_map[k] = canon_model
                forced_added += 1
                break  # достаточно одного forced alias

    if forced_added:
        logger.info("Model aliases: forced fallback added=%d", forced_added)

    return idx, aliases_map, dict(collisions)


def build_code_index(etalon_items: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    idx: Dict[str, Dict[str, Any]] = {}
    for it in etalon_items:
        params = it.get("params") or {}
        code = (params.get("code") or "").strip().upper()
        if not code:
            continue
        path = it.get("path") or []
        if not isinstance(path, list) or len(path) < 4:
            continue
        cat, br, sr, model = (path + ["", "", "", ""])[:4]
        cat = str(cat).strip()
        br = str(br).strip()
        sr = str(sr).strip()
        model = str(model).strip()
        if not model:
            continue
        if code in idx and idx[code].get("path") != [cat, br, sr, model]:
            continue
        idx[code] = {
            "path": [cat, br, sr, model],
            "brand": br,
            "series": sr,
            "model": model,
        }
    return idx


def _load_code_index() -> Dict[str, Dict[str, Any]]:
    ci = _load_json(CODE_INDEX_JSON, {})
    if isinstance(ci, dict):
        idx = ci.get("index")
        if isinstance(idx, dict) and idx:
            return idx
    return {}


def _load_model_index() -> Dict[str, Dict[str, Any]]:
    mi = _load_json(MODEL_INDEX_JSON, {})
    if isinstance(mi, dict):
        idx = mi.get("index")
        if isinstance(idx, dict) and idx:
            return idx

    pe = _load_json(PARSED_ETALON_JSON, {})
    items = (pe.get("items") if isinstance(pe, dict) else None) or []
    if not isinstance(items, list):
        items = []
    idx, _aliases, _coll = build_model_index_and_aliases(items)
    return idx


# ============================================================
# Matching helpers
# ============================================================

def _looks_like_prod_code(tok: str) -> bool:
    if not tok:
        return False
    t = tok.strip()
    if not (4 <= len(t) <= 7):
        return False
    if re.fullmatch(r"(?i)(fold|flip)\d{1,2}", t):
        return False
    has_a = any("a" <= ch.lower() <= "z" for ch in t)
    has_d = any(ch.isdigit() for ch in t)
    if not (has_a and has_d):
        return False
    tl = t.lower()
    if re.fullmatch(r"m\d{1,2}", tl):
        return False
    if re.fullmatch(r"a\d{1,2}", tl):
        return False
    if re.fullmatch(r"v\d{1,2}s?", tl):
        return False
    return True


# ============================================================
# Title-search mode (аксессуары / ремешки / консоли)
# ============================================================

_TITLE_MODE_PATTERNS = [
    # generic accessories
    r"\bcase\b",
    r"\bcable\b",
    r"\badapter\b",
    r"\bethernet\b",
    r"\blan\b",
    r"\bwallet\b",
    r"\bairtag\s*loop\b",
    r"\bfolio\b",
    r"\bbumper\b",
    r"\bcorning\s*glass\b",
    r"\btempered\s*glass\b",
    r"\bscreen\s*protector\b",
    r"\bprotective\s*glass\b",
    r"\bglass\b",
    r"\bprotector\b",
    r"\bстекл\w*\b",
    r"\bкабел\w*\b",
    r"\bпереходник\w*\b",
    r"\bшнур\w*\b",

    # consoles
    r"\bps5\b",
    r"\bnsw2\b",
    r"\bnsw\b",
    r"\bcontroller\b",
    r"\bdualsense\b",
    r"\bjoycon\b",
    r"\bportal\b",
    r"\bvr\s*2\b",
    r"\bvr2\b",
    r"\bcamera\b",
    r"\bgame\b",
    r"\bигр\w*\b",

    # watch bands keywords (важно для строк без 'watch')
    r"\bmodern\s*buckle\b",
    r"\bmagnetic\s*link\b",
    r"\blink\s*bracelet(\s*kit)?\b",
    r"\bmilanese\s*loop\b",
    r"\bocean\s*band\b",
    r"\balpine\s*loop\b",
    r"\btrail\s*loop\b",
    r"\bsport\s*band\b",
    r"\bsport\s*loop\b",
    r"\bsolo\s*loop\b",
    r"\bbraided\s*solo\s*loop\b",
]
_RX_TITLE_MODE = re.compile("|".join(_TITLE_MODE_PATTERNS), re.IGNORECASE)

# exclude Dyson-like “Presentation case”
_RX_TITLE_MODE_EXCLUDE = re.compile(r"(?i)\bpresentation\s*case\b")
_RX_DYSON_HS_CODE = re.compile(r"(?i)\bhs\d{2}\b")

_ACCESSORY_HINT_TOKENS = {
    "case", "cable", "adapter", "ethernet", "lan", "wallet", "loop",
    "buckle", "magnetic", "link", "bracelet", "band",
    "milanese", "ocean", "alpine", "trail", "sport", "solo", "braided",
    "kit",
    "ps5", "nsw", "nsw2",
    "controller", "dualsense", "joycon", "portal", "vr", "vr2", "camera", "game", "игра", "игры", "игр",
    "glass", "corning", "protector", "screen", "tempered",
    "стекло", "пленка", "плёнка", "кабель", "шнур", "переходник",
    "folio", "cover", "keyboard", "bumper",
}


def _title_search_mode(raw_line: str) -> bool:
    """
    Если это аксессуар/ремешок/консоль — матчим осторожнее.
    Важное исключение: HS08 ... (Presentation case) не должно триггерить.
    """
    s = tu.clean_spaces(raw_line or "")
    if not s:
        return False

    if _RX_DYSON_HS_CODE.search(s) and _RX_TITLE_MODE_EXCLUDE.search(s):
        return False

    if _RX_TITLE_MODE_EXCLUDE.search(s):
        return False

    # watch-bands часто начинаются с размера мм: "42mm ..."
    if re.match(r"(?i)^\s*(3[8-9]|4[0-9])\s*mm\b", s):
        return True

    return bool(_RX_TITLE_MODE.search(s))


def _title_head(raw_line: str) -> str:
    """
    Сохраняем семантику до цены, включая размер ремешка (M/L, S/M),
    и цвет у кейсов (Everest Black).
    """
    s0 = tu.clean_spaces(raw_line or "")
    if not s0:
        return ""

    # split by ' - ' / ' — ' / ' – ' only when surrounded by spaces
    segs = re.split(r"\s+[—–-]\s+", s0)

    def _is_price_seg(x: str) -> bool:
        t = (x or "").strip()
        if not t:
            return False
        t2 = t.replace(" ", "").replace("_", "").replace(",", "").replace(".", "")
        return t2.isdigit() and 1000 <= int(t2) <= 1_000_000

    if segs and _is_price_seg(segs[-1]):
        segs = segs[:-1]

    head = " ".join(segs).strip()

    # also remove trailing glued price "...case)-37000"
    head = re.sub(r"(?<!\d)(\d{4,7})(?!\d)\s*$", " ", head).strip()

    head = _clean(head)
    head = re.sub(r"(?i)\baw\b", "apple watch", head)
    return _nk(head)


def _looks_like_watch_band_line(raw_line: str) -> bool:
    s = _clean(raw_line)
    if not s:
        return False
    if not re.search(r"(?i)\b(3[8-9]|4[0-9])\s*mm\b", s):
        return False
    return bool(
        re.search(
            r"(?i)\b("
            r"modern\s*buckle|magnetic\s*link|link\s*bracelet|milanese\s*loop|"
            r"sport\s*band|sport\s*loop|solo\s*loop|braided\s*solo\s*loop|"
            r"alpine\s*loop|trail\s*loop|ocean\s*band|"
            r"bracelet|buckle|link"
            r")\b",
            s,
        )
    )


def _meta_cat(meta: Optional[Dict[str, Any]]) -> str:
    if not meta:
        return ""
    path = meta.get("path") or []
    if isinstance(path, list) and path:
        return str(path[0] or "").strip()
    return ""


def _meta_is_phone_device(meta: Optional[Dict[str, Any]]) -> bool:
    """
    Очень грубая защита: если аксессуарная строка матчится в смартфоны/айфоны — это почти всегда плохо.
    """
    if not meta:
        return False
    path = meta.get("path") or []
    if not isinstance(path, list) or len(path) < 2:
        return False

    cat = _nk(str(path[0] or ""))
    br = _nk(str(path[1] or ""))
    sr = _nk(str(path[2] or "")) if len(path) >= 3 else ""
    mdl = _nk(str(path[3] or "")) if len(path) >= 4 else ""

    if cat in {"смартфоны", "smartphones", "phones"}:
        return True
    if br == "apple" and ("iphone" in sr or "iphone" in mdl):
        return True
    return False


def _meta_is_tablet_device(meta: Optional[Dict[str, Any]]) -> bool:
    if not meta:
        return False
    path = meta.get("path") or []
    if not isinstance(path, list) or len(path) < 2:
        return False
    cat = _nk(str(path[0] or ""))
    br = _nk(str(path[1] or ""))
    sr = _nk(str(path[2] or "")) if len(path) >= 3 else ""
    mdl = _nk(str(path[3] or "")) if len(path) >= 4 else ""
    if cat in {"планшеты", "tablets"}:
        return True
    if br == "apple" and ("ipad" in sr or "ipad" in mdl):
        return True
    if "pad" in sr or "pad" in mdl:
        return True
    return False


def _meta_is_console_device(meta: Optional[Dict[str, Any]]) -> bool:
    if not meta:
        return False
    path = meta.get("path") or []
    if not isinstance(path, list) or len(path) < 2:
        return False
    cat = _nk(str(path[0] or ""))
    if cat != "приставки и игры":
        return False
    sr = _nk(str(path[2] or "")) if len(path) >= 3 else ""
    mdl = _nk(str(path[3] or "")) if len(path) >= 4 else ""
    if "dualsense" in sr or "dualsense" in mdl:
        return False
    if "ps5" in sr or "ps5" in mdl:
        return True
    if "switch" in sr or "switch" in mdl:
        return True
    if "nintendo" in sr or "nintendo" in mdl:
        return True
    return False


def _looks_like_laptop_line(raw: str) -> bool:
    s = _clean(raw)
    if not s:
        return False

    _st, ram = extract_storage(raw)
    has_ram = bool(ram)

    has_diag = bool(R.RX_DIAGONAL.search(s) or R._RX_INCH.search(s))

    has_cpu = False
    m = R.RX_CHIP_APPLE.search(s)
    if m and m.group("m"):
        if re.search(r"(?i)\b(mac|macbook|imac|macmini|mac\s*mini|mac\s*studio)\b", s):
            has_cpu = True
        elif re.search(r"(?i)\b(air|pro)\b", s) and re.search(r"(?i)\b(1[3-6])\b", s):
            has_cpu = True
        elif re.search(r"\(\s*\d+\s*/\s*\d+\s*\)", s):
            has_cpu = True
    if not has_cpu and re.search(
        r"(?i)\b(i[3579]\s*[- ]?\s*\d{3,5}[a-z]{0,2}|ryzen\s*[3579]\b|celeron|pentium|athlon|xeon)\b",
        s,
    ):
        has_cpu = True

    if not has_diag and has_cpu and re.search(r"(?i)\b(air|pro)\b", s):
        if re.search(r"(?i)\b(1[3-6])\b", s):
            has_diag = True

    return has_diag and has_ram and has_cpu


def _reject_phone_meta_for_text(meta: Optional[Dict[str, Any]], raw: str) -> bool:
    if not meta:
        return False
    if not _meta_is_phone_device(meta):
        return False
    if _looks_like_laptop_line(raw):
        return True
    s = _clean(raw)
    if s:
        if R.RX_IPAD_CTX2.search(s) or R._RX_TABLET_SIGNALS.search(s):
            return True
        if re.search(r"(?i)\bwi[\s-]*fi\b", s) and re.search(r"(?i)\b(11|13)\b", s):
            return True
        if re.search(r"(?i)\b(lte|cellular)\b", s) and re.search(r"(?i)\b(11|13)\b", s):
            return True
        if re.search(r"(?i)\bair\b", s) and re.search(r"(?i)\b(11|13)\b", s) and not re.search(r"(?i)\biphone\b", s):
            return True
        if re.search(r"(?i)\bm\d\b", s) and re.search(r"(?i)\b(apple|ipad|mac|macbook|imac)\b", s):
            return True
    text_toks = set(_nk(raw).split())
    if not text_toks:
        return False
    if text_toks.intersection({
        "glass", "corning", "protector", "screen", "tempered", "bumper",
        "adapter", "ethernet", "lan", "cable",
        "стекло", "пленка", "плёнка", "кабель", "шнур", "переходник",
    }):
        return True
    path = meta.get("path") or []
    br = _nk(str(path[1] or "")) if isinstance(path, list) and len(path) >= 2 else ""
    if br == "samsung" and "jbl" in text_toks:
        return True
    sr = _nk(str(path[2] or "")) if isinstance(path, list) and len(path) >= 3 else ""
    mdl = _nk(str(path[3] or "")) if isinstance(path, list) and len(path) >= 4 else ""
    meta_toks = set((sr + " " + mdl).split())
    variant_toks = {"mini", "pro", "max", "plus", "ultra", "se", "fe"}
    for v in variant_toks:
        if v in text_toks and v not in meta_toks:
            return True
    return False


def _reject_tablet_meta_for_text(meta: Optional[Dict[str, Any]], raw: str) -> bool:
    if not meta or not _meta_is_tablet_device(meta):
        return False
    s = _nk(raw)
    if not s:
        return False
    if re.search(r"(?i)\b(folio|smart\s*folio|cover|case|keyboard|magic\s*keyboard)\b", s):
        return True
    if re.search(r"(?i)\b(чехол|обложк|клавиатур)\w*\b", s):
        return True
    return False


def _reject_console_meta_for_text(meta: Optional[Dict[str, Any]], raw: str) -> bool:
    if not meta or not _meta_is_console_device(meta):
        return False
    s = _nk(raw)
    if not s:
        return False
    if re.search(r"(?i)\b(dualsense|dual\s*sense|controller|joycon)\b", s):
        return True
    if re.search(r"(?i)\b(ps\s*portal|portal|vr\s*2|vr2|headset|camera|dock)\b", s):
        return True
    if re.search(r"(?i)\b(case|carrying|deluxe|glass|screen|protector)\b", s):
        return True
    if re.search(r"(?i)\b(игр\w*|game|edition\s*of)\b", s):
        return True
    if re.search(r"(?i)\bnsw\s*2\b|\bnsw2\b", s) and not re.search(r"(?i)\bswitch\b", s):
        return True
    # PS5 game-like titles without console keywords should not match consoles.
    if re.search(r"(?i)\bps\s*5\b|\bps5\b", s):
        console_kw = {
            "ps", "ps5", "playstation", "sony", "5",
            "slim", "digital", "disc", "disk", "pro", "edition", "anniversary", "bundle", "console",
        }
        toks = [t for t in _nk(s).split() if t.isalpha()]
        if toks and not any(t in console_kw for t in toks):
            return True
        if any(t for t in toks if t not in console_kw and len(t) >= 4):
            return True
    return False


def _meta_is_watch_device(meta: Optional[Dict[str, Any]]) -> bool:
    if not meta:
        return False
    path = meta.get("path") or []
    if not isinstance(path, list) or len(path) < 2:
        return False
    cat = _nk(str(path[0] or ""))
    br = _nk(str(path[1] or ""))
    sr = _nk(str(path[2] or "")) if len(path) >= 3 else ""
    mdl = _nk(str(path[3] or "")) if len(path) >= 4 else ""
    if "watch" in cat:
        return True
    if br == "apple" and ("watch" in sr or "watch" in mdl):
        return True
    return False


def _meta_is_pencil_device(meta: Optional[Dict[str, Any]]) -> bool:
    if not meta:
        return False
    path = meta.get("path") or []
    if not isinstance(path, list) or len(path) < 2:
        return False
    cat = _nk(str(path[0] or ""))
    br = _nk(str(path[1] or ""))
    sr = _nk(str(path[2] or "")) if len(path) >= 3 else ""
    mdl = _nk(str(path[3] or "")) if len(path) >= 4 else ""
    if cat == "аксессуары" and br == "apple" and ("pencil" in sr or "pencil" in mdl):
        return True
    return False


def _meta_is_airpods_device(meta: Optional[Dict[str, Any]]) -> bool:
    if not meta:
        return False
    path = meta.get("path") or []
    if not isinstance(path, list) or len(path) < 2:
        return False
    cat = _nk(str(path[0] or ""))
    br = _nk(str(path[1] or ""))
    sr = _nk(str(path[2] or "")) if len(path) >= 3 else ""
    mdl = _nk(str(path[3] or "")) if len(path) >= 4 else ""
    if cat == "наушники" and ("airpods" in sr or "airpods" in mdl):
        return True
    if br == "apple" and ("airpods" in sr or "airpods" in mdl):
        return True
    return False


def _reject_airpods_meta_for_text(meta: Optional[Dict[str, Any]], raw: str) -> bool:
    if not meta or not _meta_is_airpods_device(meta):
        return False
    s = _nk(raw)
    if not s:
        return False
    if any(t in s for t in ("garmin", "instinct", "solar")):
        return True
    if "⌚" in (raw or ""):
        return True
    if re.search(r"(?i)\b(case|кейc|кейс|чехол)\b", s):
        return True
    return False


def _reject_pencil_meta_for_text(meta: Optional[Dict[str, Any]], raw: str) -> bool:
    if not meta or not _meta_is_pencil_device(meta):
        return False
    s = _nk(raw)
    if not s:
        return False
    if re.search(r"(?i)\b(pencil|pen|stylus|penne)\b", s):
        return False
    if re.search(r"(?i)\b(пенсил|карандаш|стилус)\b", s):
        return False
    return True


def _reject_watch_meta_for_text(meta: Optional[Dict[str, Any]], raw: str) -> bool:
    if not meta:
        return False
    if not _meta_is_watch_device(meta):
        return False
    text_toks = set(_nk(raw).split())
    if not text_toks:
        return False
    path = meta.get("path") or []
    sr = _nk(str(path[2] or "")) if isinstance(path, list) and len(path) >= 3 else ""
    mdl = _nk(str(path[3] or "")) if isinstance(path, list) and len(path) >= 4 else ""
    meta_toks = set((sr + " " + mdl).split())
    # If text says Ultra/SE but meta doesn't include it, reject.
    watch_variants = {"ultra", "se"}
    for v in watch_variants:
        if v in text_toks and v not in meta_toks:
            return True
    return False


def _reject_plus_meta_for_text(meta: Optional[Dict[str, Any]], raw: str) -> bool:
    if not meta:
        return False
    path = meta.get("path") or []
    if not isinstance(path, list) or len(path) < 1:
        return False
    cat = _nk(str(path[0] or ""))
    if cat not in {"смартфоны", "smartphones", "phones"}:
        return False
    raw_s = raw or ""
    if not raw_s:
        return False
    # Ignore "+" coming from SIM+eSIM tokens.
    raw_no_sim = re.sub(r"(?i)\b(?:nano\s*)?sim\s*\+\s*esim\b", " ", raw_s)
    if not re.search(r"(?i)(\bplus\b|\+)", raw_no_sim):
        return False
    sr = _nk(str(path[2] or "")) if len(path) >= 3 else ""
    mdl = _nk(str(path[3] or "")) if len(path) >= 4 else ""
    meta_toks = set((sr + " " + mdl).split())
    model_raw = str(path[3] or "")
    if re.search(r"\+|\bplus\b", model_raw, flags=re.IGNORECASE):
        return False
    if "plus" in meta_toks:
        return False
    return True


def _tokenize_for_match(text: str, *, cut_at_dash: bool = True) -> List[str]:
    """
    Tokenize + normalize.
    """
    raw = text or ""
    s0 = _clean(raw)
    if "+" in raw:
        s0 = s0.replace("+", " plus ")
    nk0 = _nk(s0)
    toks0 = [t for t in nk0.split() if t]

    if not toks0:
        return []

    # ✅ cut trailing "price-like" token
    def _is_price_tail(tok: str) -> bool:
        if not tok or not tok.isdigit():
            return False
        if R._RX_YEAR_20XX.fullmatch(tok):
            return False
        v = int(tok)
        return 1_000 <= v <= 1_000_000

    currency_tail = {"rub", "rur", "руб", "р", "₽"}

    while toks0 and toks0[-1] in currency_tail:
        toks0.pop()

    if toks0 and _is_price_tail(toks0[-1]):
        toks0.pop()

    # optional: cut at dash separators
    if cut_at_dash:
        cut_i = len(toks0)
        for sep in ("-", "—", "–"):
            if sep in toks0:
                cut_i = min(cut_i, toks0.index(sep))
        toks0 = toks0[:cut_i] if cut_i > 0 else toks0

    # split tokens like "s25+" -> ["s25","plus"]
    toks: List[str] = []
    for t in toks0:
        if t.endswith("+") and len(t) > 1:
            toks.append(t[:-1])
            toks.append("plus")
            continue

        if t == "mi":
            toks.append("xiaomi")
            continue

        m_fe = re.fullmatch(r"(s\d{2})(fe)", t)
        if m_fe:
            toks.append(m_fe.group(1))
            toks.append(m_fe.group(2))
            continue

        m_pref = re.fullmatch(r"(ipad|pad|note|pixel|iphone|redmi|galaxy)(\d+)([a-z]+)?", t)
        if m_pref:
            prefix, num, tail = m_pref.groups()
            toks.append(prefix)
            toks.append(num)
            if tail:
                toks.append(tail)
            continue

        if re.fullmatch(r"[a-z]*\\d+promax", t):
            base = t[:-6]
            toks.append(base)
            toks.append("pro")
            toks.append("max")
            continue

        m_se = re.fullmatch(r"se([23])", t)
        if m_se:
            toks.append("se")
            toks.append(m_se.group(1))
            continue

        m = re.fullmatch(r"([a-z]*\\d+)(pro|plus|max|ultra|mini|lite)", t)
        if m:
            toks.append(m.group(1))
            toks.append(m.group(2))
            continue

        m = re.fullmatch(r"(\\d+)(pro|plus|max|ultra|mini|lite)", t)
        if m:
            toks.append(m.group(1))
            toks.append(m.group(2))
            continue

        if re.fullmatch(r"\\d+proxl", t):
            toks.append(t[:-5])
            toks.append("pro")
            toks.append("xl")
            continue

        toks.append(t)

    out: List[str] = []
    for t in toks:
        if re.fullmatch(r"20\d{2}", t):
            continue
        if _looks_like_prod_code(t):
            continue
        out.append(t)

    return out


def _looks_like_iphone_price_line(raw: str) -> bool:
    s = tu.clean_spaces(raw or "")
    if not s:
        return False
    s = tu.clean_spaces(tu.strip_flags(s))

    nk = _nk(s)
    if "iphone" in nk:
        return False

    m = R._RX_IPHONE_PRICESTYLE.search(s)
    if not m:
        return False

    storage, _ram = extract_storage(s)
    if storage:
        return True

    if (m.group("suffix") or "").strip():
        return True
    if (m.group("variant") or "").strip():
        return True
    if (m.group("variant2") or "").strip():
        return True

    return False


def _watch_tokens_from_shorthand(raw: str) -> Optional[List[str]]:
    s = tu.clean_spaces(raw or "")
    if not s:
        return None

    m = R._RX_WATCH_PRICESTYLE.search(s)
    if m:
        head = tu.clean_spaces(m.group("s") or "")
        mm = (m.group("mm") or "").strip()
        head_nk = _nk(head)

        if head_nk.startswith("ultra"):
            fam = ["ultra"]
            num = re.findall(r"\d{1,2}", head_nk)
            if num:
                fam.append(num[0])
            out = ["apple", "watch"] + fam
        elif head_nk.startswith("se"):
            fam = ["se"]
            num = re.findall(r"\d{1,2}", head_nk)
            if num:
                fam.append(num[0])
            out = ["apple", "watch"] + fam
        else:
            num = re.findall(r"\d{1,2}", head_nk)
            if not num:
                return None
            out = ["apple", "watch", "series", num[0]]

        if mm:
            try:
                out.append(f"{int(mm)}mm")
            except Exception:
                pass
        return out

    m2 = R._RX_WATCH_AW_PRICESTYLE.search(s)
    if not m2:
        return None

    family = (m2.group("family") or "").strip().lower()
    num = (m2.group("num") or "").strip()
    mm = (m2.group("mm") or "").strip()

    out = ["apple", "watch"]

    if family.isdigit():
        out += ["series", family]
    elif family in {"ultra", "se"}:
        out.append(family)
        if num.isdigit():
            out.append(num)
    else:
        return None

    if mm:
        try:
            out.append(f"{int(mm)}mm")
        except Exception:
            pass

    return out


def _looks_like_watch_price_line(raw: str) -> bool:
    s = tu.clean_spaces(raw or "")
    if not s:
        return False

    nk = _nk(s)

    if "watch" in nk:
        return False

    if R._RX_WATCH_AW_PRICESTYLE.search(s):
        return True

    m = R._RX_WATCH_PRICESTYLE.search(s)
    if not m:
        return False

    if re.search(r"(?i)\b(3[8-9]|4[0-9])\b", s):
        return True
    if R._RX_BAND_SIZE.search(_clean(s)):
        return True
    if extract_code(s):
        return True

    return True


def _is_watchish_tokens(toks: List[str]) -> bool:
    tset = set(toks or [])
    if "watch" in tset:
        return True
    if "apple" in tset and "watch" in tset:
        return True
    if any(R._RX_AW_TOKEN.fullmatch(t) for t in toks):
        return True
    if any(re.fullmatch(r"u\d{1,2}", t) for t in toks):
        return True

    has_family = any(t in tset for t in ("ultra", "se", "series"))
    has_mm = any(re.fullmatch(r"(3[8-9]|4[0-9])mm", t) for t in tset) or any(R._RX_WATCH_MM_BARE.fullmatch(t) for t in toks)
    has_band = any(t in tset for t in ("sb", "sl", "tl", "al", "lb", "sport", "loop", "bracelet", "band"))
    if has_family and (has_mm or has_band):
        return True
    return False


def _watch_normalize_tokens_for_match(toks: List[str], raw_text: str) -> List[List[str]]:
    variants: List[List[str]] = []
    base = list(toks)
    variants.append(base)

    if not base:
        return variants

    raw_nk = _nk(_clean(raw_text or ""))
    if _looks_like_tablet_line(raw_text, base):
        return variants

    watch_ctx = (
        _is_watchish_tokens(base)
        or ("apple watch" in raw_nk)
        or (" watch " in f" {raw_nk} ")
        or re.search(r"(?i)\baw\b", raw_nk) is not None
    )
    if not watch_ctx:
        return variants

    def expand_aw(v: List[str]) -> List[str]:
        out: List[str] = []
        expanded = False
        for t in v:
            if R._RX_AW_TOKEN.fullmatch(t) and not expanded:
                out.extend(["apple", "watch"])
                expanded = True
            else:
                out.append(t)
        return out

    v_aw = expand_aw(base)
    if v_aw != base:
        variants.append(v_aw)

    def expand_u3(v: List[str]) -> List[str]:
        out: List[str] = []
        changed = False
        for t in v:
            m = re.fullmatch(r"u(\d{1,2})", t)
            if m:
                out.extend(["ultra", m.group(1)])
                changed = True
            else:
                out.append(t)
        if not changed:
            return v
        if "apple" not in out and "watch" not in out:
            out = ["apple", "watch"] + out
        return out

    more = []
    for v in list(variants):
        vu = expand_u3(v)
        if vu != v:
            more.append(vu)
    variants.extend(more)

    def s_to_series(v: List[str]) -> List[str]:
        out: List[str] = []
        changed = False
        for t in v:
            m = R._RX_WATCH_S_SERIES.fullmatch(t)
            if m:
                changed = True
                out.extend(["series", m.group("num")])
            else:
                out.append(t)
        return out if changed else v

    more: List[List[str]] = []
    for v in list(variants):
        vv = s_to_series(v)
        if vv != v:
            more.append(vv)
    variants.extend(more)

    def aw_num_to_series(v: List[str]) -> List[str]:
        out = list(v)
        anchor_i = None
        for i in range(len(out) - 1):
            if out[i] == "apple" and out[i + 1] == "watch":
                anchor_i = i
                break
        if anchor_i is None:
            return v

        for j in range(anchor_i + 2, min(len(out), anchor_i + 6)):
            t = out[j]
            if t.isdigit():
                num = int(t)
                if 1 <= num <= 20:
                    if j > 0 and out[j - 1] == "series":
                        return v
                    return out[:j] + ["series", str(num)] + out[j + 1:]
        return v

    def watch_num_to_series(v: List[str]) -> List[str]:
        out = list(v)
        try:
            w_i = out.index("watch")
        except ValueError:
            return v
        for j in range(w_i + 1, min(len(out), w_i + 4)):
            t = out[j]
            if t.isdigit():
                num = int(t)
                if 1 <= num <= 20:
                    if j > 0 and out[j - 1] == "series":
                        return v
                    return out[:j] + ["series", str(num)] + out[j + 1:]
        return v

    more = []
    for v in list(variants):
        vv = aw_num_to_series(v)
        if vv != v:
            more.append(vv)
    variants.extend(more)

    more = []
    for v in list(variants):
        vv = watch_num_to_series(v)
        if vv != v:
            more.append(vv)
    variants.extend(more)

    def add_mm(v: List[str]) -> List[str]:
        if any(re.fullmatch(r"(3[8-9]|4[0-9])mm", x) for x in v):
            return v
        out = []
        added = False
        for x in v:
            m = R._RX_WATCH_MM_BARE.fullmatch(x)
            if m and not added:
                out.append(f"{m.group('mm')}mm")
                added = True
            else:
                out.append(x)
        return out

    more = []
    for v in list(variants):
        vmm = add_mm(v)
        if vmm != v:
            more.append(vmm)
    variants.extend(more)

    uniq: List[List[str]] = []
    seen = set()
    for v in variants:
        k = " ".join(v)
        if k not in seen:
            seen.add(k)
            uniq.append(v)
    return uniq


def match_model_from_text(text: str, model_index: Dict[str, Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Основной матч:
      - обычный режим: быстрые токены (head before dash)
      - title-search: токены из _title_head (не режем), затем выбираем лучший кандидат
    """
    if not text:
        return None

    m_se_year = R.RX_AW_SE_YEAR.search(text or "")
    if not m_se_year:
        m_se_year = re.search(r"(?i)\bse\b[^\n]{0,30}?(20\d{2})", text or "")
    if m_se_year:
        year = int(m_se_year.group(1))
        se_gen = 2 if year <= 2024 else 3
        for key_text in (
            f"watch se {se_gen} {year}",
            f"watch se {se_gen}",
            f"watch se {se_gen} 2024",
        ):
            k = _alias_key_safe(key_text)
            if not k:
                continue
            meta_se = model_index.get(k)
            if meta_se:
                return meta_se

    # "2nd Gen" shorthand for Watch SE 2
    if re.search(r"(?i)\bse\b", text or "") and re.search(r"(?i)\b2nd\s*gen\b", text or ""):
        for key_text in ("watch se 2", "watch se 2 2024", "watch se 2 2023", "watch se 2 2022"):
            k = _alias_key_safe(key_text)
            if not k:
                continue
            meta_se = model_index.get(k)
            if meta_se:
                return meta_se

    m_ipad_air_gen = re.search(r"(?i)\bair\s*(6|7)\b", text or "")
    if m_ipad_air_gen and re.search(r"(?i)\b(ipad|wi[\s-]*fi|lte|cellular)\b", text or ""):
        gen = int(m_ipad_air_gen.group(1))
        chip = "M3" if gen >= 7 else "M2"
        size_m = re.search(r"(?i)\b(11|13)\b", text or "")
        size = size_m.group(1) if size_m else ""
        key_texts = []
        if size:
            key_texts.append(f"ipad air {size} {chip}")
        key_texts.append(f"ipad air {chip}")
        for key_text in key_texts:
            k = _alias_key_safe(key_text)
            if not k:
                continue
            meta_air = model_index.get(k)
            if meta_air:
                return meta_air

    m_poco = re.search(r"(?i)\bpoco\s+([fxm]\d+)\b", text or "")
    if m_poco:
        fam_model = m_poco.group(1).lower()
        fam = fam_model[0]
        suffix = " pro" if re.search(r"(?i)\bpro\b", text or "") else ""
        k = _alias_key_safe(f"poco {fam} {fam_model}{suffix}")
        if k:
            meta_p = model_index.get(k)
            if meta_p and not _reject_phone_meta_for_text(meta_p, text) and not _reject_plus_meta_for_text(meta_p, text):
                return meta_p

    m_whoop = re.search(r"(?i)\bwhoop\b", text or "")
    if m_whoop and re.search(r"(?i)\b(life|peak|one)\b", text or ""):
        k = _alias_key_safe("Whoop 5.0")
        if k:
            meta_whoop = model_index.get(k)
            if meta_whoop:
                return meta_whoop

    # Title-search mode
    if _title_search_mode(text):
        head_nk = _title_head(text)
        toks = [t for t in head_nk.split() if t]
        if not toks:
            return None

        candidates: List[Tuple[Tuple[int, int, int, int], Dict[str, Any]]] = []

        tset = set(toks)
        has_acc_hint = any(t in _ACCESSORY_HINT_TOKENS for t in tset)
        has_mm_head = bool(re.match(r"^(3[8-9]|4[0-9])mm$", toks[0])) if toks else False

        max_len = min(12, len(toks))
        for n in range(max_len, 1, -1):
            for i in range(0, len(toks) - n + 1):
                phrase = " ".join(toks[i:i + n])
                k = _alias_key_safe(phrase)
                if not k:
                    continue
                meta = model_index.get(k)
                if not meta:
                    continue
                if _reject_phone_meta_for_text(meta, text):
                    continue
                if _reject_tablet_meta_for_text(meta, text):
                    continue
                if _reject_console_meta_for_text(meta, text):
                    continue
                if _reject_watch_meta_for_text(meta, text):
                    continue
                if _reject_airpods_meta_for_text(meta, text):
                    continue
                if _reject_pencil_meta_for_text(meta, text):
                    continue
                if _reject_plus_meta_for_text(meta, text):
                    continue

                not_phone = 1 if not _meta_is_phone_device(meta) else 0
                not_watch_device = 1 if not _meta_is_watch_device(meta) else 0
                phrase_toks = phrase.split()
                acc_bonus = 1 if any(t in _ACCESSORY_HINT_TOKENS for t in phrase_toks) else 0
                mm_bonus = 1 if (has_mm_head and not _meta_is_watch_device(meta)) else 0

                score = (not_phone, not_watch_device, n, acc_bonus + mm_bonus)
                candidates.append((score, meta))

        if not candidates:
            return None

        candidates.sort(key=lambda x: x[0], reverse=True)
        _best_score, best_meta = candidates[0]

        if has_acc_hint and _meta_is_phone_device(best_meta):
            for _sc, m in candidates:
                if not _meta_is_phone_device(m):
                    return m
            return None

        if has_acc_hint and _meta_is_tablet_device(best_meta):
            for _sc, m in candidates:
                if not _meta_is_tablet_device(m):
                    return m
            return None

        if has_mm_head and _meta_is_watch_device(best_meta):
            for _sc, m in candidates:
                if not _meta_is_watch_device(m):
                    return m
            return None

        return best_meta

    # Normal mode
    toks = _tokenize_for_match(text, cut_at_dash=True)
    if not toks:
        return None
    if len(toks) <= 2 and all(t.isdigit() for t in toks):
        return None

    def _try_match(toks_: List[str]) -> Optional[Dict[str, Any]]:
        max_len = min(8, len(toks_))
        for n in range(max_len, 0, -1):
            for i in range(0, len(toks_) - n + 1):
                phrase = " ".join(toks_[i:i + n])
                k = _alias_key_safe(phrase)
                if not k:
                    continue
                meta = model_index.get(k)
                if not meta:
                    continue
                if _reject_phone_meta_for_text(meta, text):
                    continue
                if _reject_tablet_meta_for_text(meta, text):
                    continue
                if _reject_console_meta_for_text(meta, text):
                    continue
                if _reject_watch_meta_for_text(meta, text):
                    continue
                if _reject_airpods_meta_for_text(meta, text):
                    continue
                if _reject_pencil_meta_for_text(meta, text):
                    continue
                if _reject_plus_meta_for_text(meta, text):
                    continue
                return meta
        return None

    meta = _try_match(toks)
    if meta:
        if _meta_is_phone_device(meta):
            nk_text = _nk(text)
            if re.match(r"(?i)^[a-z]{1,3}\d+", nk_text) and "iphone" not in nk_text and "apple" not in nk_text:
                meta = None
        if meta:
            return meta

    # Yandex Station fallback (no explicit brand in line)
    tset = set(_nk(text).split())
    if any(t in tset for t in ("станция", "миди", "мини", "лайт", "макс")):
        y_candidates: List[str] = []
        if "дуо" in tset and "макс" in tset:
            y_candidates.append("Яндекс.Станция Дуо Макс")
        if "макс" in tset:
            y_candidates.append("Яндекс.Станция Макс")
        if "миди" in tset:
            y_candidates.append("Яндекс.Станция Миди")
        if "лайт" in tset:
            y_candidates.append("Яндекс.Станция Лайт 2")
        if "мини" in tset:
            if "про" in tset:
                y_candidates.append("Яндекс.Станция Мини 3 Про")
            elif "3" in tset:
                y_candidates.append("Яндекс.Станция Мини 3")
            else:
                y_candidates.append("Яндекс.Станция Мини 3")
        if "станция" in tset and "3" in tset:
            y_candidates.append("Яндекс.Станция 3")
        if "станция" in tset and "2" in tset:
            y_candidates.append("Яндекс.Станция 2")
        for name in y_candidates:
            k = _alias_key_safe(name)
            if not k:
                continue
            meta_ya = model_index.get(k)
            if meta_ya:
                return meta_ya

    if "ipad" in toks or R.RX_IPAD_CTX2.search(text or "") or R._RX_TABLET_SIGNALS.search(text or ""):
        if not hasattr(match_model_from_text, "_color_token_set"):
            color_tokens: set[str] = set()
            for c in D.BASE_COLORS:
                for t in _nk(c).split():
                    color_tokens.add(t)
            for k, v in D.COLOR_SYNONYMS.items():
                for t in _nk(k).split():
                    color_tokens.add(t)
                for t in _nk(v).split():
                    color_tokens.add(t)
            setattr(match_model_from_text, "_color_token_set", color_tokens)
        color_tokens = getattr(match_model_from_text, "_color_token_set")
        drop_tokens = color_tokens | {"wifi", "wi", "fi", "lte", "cellular", "5g", "gb", "tb"}
        toks_focus = [t for t in toks if t not in drop_tokens]
        meta_focus = _try_match(toks_focus)
        if meta_focus:
            return meta_focus

    toks_no_brand = toks[1:] if toks and toks[0] == "samsung" else toks
    if toks_no_brand != toks:
        meta_nb = _try_match(toks_no_brand)
        if meta_nb:
            return meta_nb

    for v in _watch_normalize_tokens_for_match(toks, text):
        if v == toks:
            continue
        meta_w = _try_match(v)
        if meta_w:
            return meta_w

    if toks and toks[0] == "poco" and len(toks) >= 2:
        m_fam = re.match(r"(?i)^([fxm])\\d+", toks[1])
        if m_fam:
            fam = m_fam.group(1).lower()
            k = _alias_key_safe(f"poco {fam} {toks[1]}")
            if k:
                meta_poco = model_index.get(k)
                if meta_poco and not _reject_phone_meta_for_text(meta_poco, text) and not _reject_plus_meta_for_text(meta_poco, text):
                    return meta_poco
            toks_poco = [toks[0], fam, toks[1]] + toks[2:]
            meta_poco = _try_match(toks_poco)
            if meta_poco:
                return meta_poco

    def _starts_with_alnum_model(raw: str) -> bool:
        s = _nk(tu.strip_flags(tu.clean_spaces(raw or "")))
        if not s:
            return False
        first = s.split()[0]
        if first in {"iphone", "apple"}:
            return False
        return re.fullmatch(r"[a-z]{1,3}\d+[a-z]*", first) is not None

    if _looks_like_iphone_price_line(text) and not _starts_with_alnum_model(text):
        meta2 = _try_match(["iphone"] + toks)
        if meta2:
            return meta2

    def _looks_like_iphone_implicit(raw: str, toks_: List[str]) -> bool:
        if not toks_:
            return False
        if _starts_with_alnum_model(raw):
            return False
        if any(t in toks_ for t in {"galaxy", "poco", "redmi", "note", "xiaomi", "samsung", "pixel"}):
            return False
        if not toks_[0].isdigit():
            return False
        try:
            gen = int(toks_[0])
        except Exception:
            return False
        if gen < 12 or gen > 20:
            return False
        storage, _ram = extract_storage(raw)
        if not storage:
            return False
        if any(t in toks_ for t in {"pro", "max", "plus", "mini", "e", "esim", "2sim"}):
            return True
        return True

    if _looks_like_iphone_implicit(text, toks):
        meta3 = _try_match(["iphone"] + toks)
        if meta3:
            return meta3

    def _looks_like_watch_implicit(raw: str, toks_: List[str]) -> bool:
        if not toks_:
            return False
        if any(t in toks_ for t in {"iphone", "ipad", "macbook"}):
            return False
        if not toks_[0].isdigit():
            return False
        try:
            num = int(toks_[0])
        except Exception:
            return False
        if not (1 <= num <= 20):
            return False
        if re.search(r"(?i)\b(3[8-9]|4[0-9])mm\b", raw or ""):
            return True
        if any(t in toks_ for t in {"sm", "ml", "sl"}):
            return True
        if any(t in toks_ for t in {"ultra", "se", "series"}):
            return True
        return False

    if _looks_like_watch_implicit(text, toks):
        meta_w = _try_match(["watch"] + toks)
        if meta_w:
            return meta_w
        meta_w = _try_match(["apple", "watch"] + toks)
        if meta_w:
            return meta_w
        if toks and toks[0].isdigit():
            meta_w = _try_match(["watch", "series"] + toks)
            if meta_w:
                return meta_w
            meta_w = _try_match(["apple", "watch", "series"] + toks)
            if meta_w:
                return meta_w

    def _looks_like_watch_family(raw: str, toks_: List[str]) -> bool:
        if not toks_:
            return False
        if any(t in toks_ for t in {"ultra", "se", "series"}):
            return True
        return False

    if _looks_like_watch_family(text, toks):
        meta_w = _try_match(["watch"] + toks)
        if meta_w:
            return meta_w
        meta_w = _try_match(["apple", "watch"] + toks)
        if meta_w:
            return meta_w

    _RX_WIFI_TOKEN = re.compile(r"(?i)\bwi[\s-]*fi\b")

    def _looks_like_tablet_line(raw: str, toks_: List[str]) -> bool:
        if not raw:
            return False
        tset = set(toks_ or [])
        if R.RX_IPAD_CTX2.search(raw) or R._RX_TABLET_SIGNALS.search(raw):
            return True
        if _RX_WIFI_TOKEN.search(raw) or any(t in tset for t in {"wifi", "wi-fi", "lte", "cellular"}):
            return True
        if any(re.fullmatch(r"m\d+", t) for t in tset):
            return True
        if any(t in tset for t in {"11", "13"}) and (
            _RX_WIFI_TOKEN.search(raw) or any(t in tset for t in {"wifi", "wi-fi", "lte", "cellular"})
        ):
            return True
        return False

    tablet_hint = _looks_like_tablet_line(text, toks)

    # iPhone Air shorthand: "Air 256 ... eSim"
    if not tablet_hint and toks and toks[0] == "air" and not any(t in toks for t in {"ipad", "macbook", "airpods"}):
        if extract_sim(text) or re.search(r"(?i)\\besim\\b", text or ""):
            for pref in (["17"], ["iphone", "17"]):
                meta_air = _try_match(pref + toks)
                if meta_air:
                    return meta_air

    def _has_chip_token(toks_: List[str]) -> bool:
        return any(re.fullmatch(r"m\d+", t) for t in toks_)

    def _has_tablet_conn(raw: str, toks_: List[str]) -> bool:
        if _RX_WIFI_TOKEN.search(raw or ""):
            return True
        return any(t in {"wifi", "wi-fi", "lte", "cellular", "5g"} for t in toks_)

    def _has_ipad_size(toks_: List[str]) -> bool:
        return any(t in {"11", "13"} for t in toks_)

    # iPad Pro order fix: "iPad 11 Pro ..." -> "iPad Pro 11 ..."
    if "ipad" in toks and "pro" in toks and _has_ipad_size(toks):
        size_tok = "11" if "11" in toks else "13" if "13" in toks else None
        if size_tok:
            t2 = ["ipad", "pro", size_tok] + [t for t in toks if t not in {"ipad", "pro", size_tok}]
            meta_ipad_pro = _try_match(t2)
            if meta_ipad_pro:
                return meta_ipad_pro

    # iPad Air shorthand: "Air 11 M3 256 WiFi ..."
    if toks and toks[0] == "air" and _has_ipad_size(toks) and _has_chip_token(toks) and _has_tablet_conn(text, toks):
        toks_no_air = toks[1:]
        meta_ipad_air = _try_match(["ipad", "air"] + toks_no_air)
        if meta_ipad_air:
            return meta_ipad_air
    # iPad Air: allow "Air 11 ..." without chip token when tablet context is clear
    if ("air" in toks) and _has_ipad_size(toks) and _has_tablet_conn(text, toks):
        size_tok = "11" if "11" in toks else "13" if "13" in toks else None
        if size_tok:
            chip_tok = next((t for t in toks if re.fullmatch(r"m\d+", t)), None)
            key_toks = ["ipad", "air", size_tok]
            if chip_tok:
                key_toks.append(chip_tok)
            meta_ipad_air = _try_match(key_toks)
            if meta_ipad_air:
                return meta_ipad_air

    # iPad Pro shorthand: "Pro 11 M5 256 WiFi ..."
    if toks and toks[0] == "pro" and _has_ipad_size(toks) and _has_chip_token(toks) and _has_tablet_conn(text, toks):
        toks_no_pro = toks[1:]
        meta_ipad_pro = _try_match(["ipad", "pro"] + toks_no_pro)
        if meta_ipad_pro:
            return meta_ipad_pro
    # iPad Pro: allow "2024 Pro 11 ... M4" (pro not first token)
    if ("pro" in toks) and _has_ipad_size(toks) and _has_tablet_conn(text, toks):
        size_tok = "11" if "11" in toks else "13" if "13" in toks else None
        if size_tok:
            chip_tok = next((t for t in toks if re.fullmatch(r"m\d+", t)), None)
            key_toks = ["ipad", "pro", size_tok]
            if chip_tok:
                key_toks.append(chip_tok)
            meta_ipad_pro = _try_match(key_toks)
            if meta_ipad_pro:
                return meta_ipad_pro

    # iPhone Air shorthand: "Air 256 Black ..." (no esim)
    if not tablet_hint and toks and toks[0] == "air" and not any(t in toks for t in {"ipad", "macbook", "airpods"}):
        if extract_storage(text)[0]:
            toks_no_air = toks[1:]
            for pref in (["iphone", "17", "air"], ["17", "air"]):
                meta_air = _try_match(pref + toks_no_air)
                if meta_air:
                    return meta_air

    def _looks_like_android_price_line(raw: str) -> bool:
        s0 = tu.clean_spaces(raw or "")
        if not s0:
            return False
        st, rr = extract_storage(s0)
        return bool(st or rr)

    if _looks_like_android_price_line(text):
        t0 = toks[0] if toks else ""
        prefix_variants: List[List[str]] = []

        if t0 == "note":
            prefix_variants += [
                ["redmi"],
                ["xiaomi", "redmi"],
                ["redmi", "note"],
                ["xiaomi", "redmi", "note"],
            ]
        if t0 == "poco":
            prefix_variants += [["xiaomi"]]
        if re.fullmatch(r"(x|f|m|c)\d{1,2}[a-z]?", t0):
            prefix_variants += [["poco"], ["xiaomi", "poco"]]
        if toks and re.fullmatch(r"a\d{1,3}\+?", toks[0]):
            prefix_variants += [["galaxy", "a"], ["samsung", "galaxy", "a"]]

        for pref in prefix_variants:
            meta_a = _try_match(pref + toks)
            if meta_a:
                return meta_a

    def _looks_like_samsung_short_model(toks_: List[str]) -> bool:
        if not toks_:
            return False
        t0 = toks_[0]
        if re.fullmatch(r"s\d{1,2}\+?", t0):
            return True
        if re.fullmatch(r"a\d{1,3}", t0):
            return True
        if t0 in {"z", "fold", "flip"}:
            return True
        if t0 == "galaxy":
            return True
        return False

    def _has_any(toks_: List[str], *words: str) -> bool:
        sset = set(toks_)
        return any(w in sset for w in words)

    if _looks_like_android_price_line(text) and _looks_like_samsung_short_model(toks_no_brand):
        prefix_variants: List[List[str]] = []
        if not _has_any(toks_no_brand, "samsung", "galaxy"):
            prefix_variants += [["galaxy"], ["samsung", "galaxy"], ["samsung"]]
        if _has_any(toks_no_brand, "samsung") and ("galaxy" not in set(toks_no_brand)):
            prefix_variants += [["galaxy"]]
        if toks_no_brand and toks_no_brand[0] in {"z", "fold", "flip"} and ("galaxy" not in set(toks_no_brand)):
            prefix_variants += [["galaxy", "z"], ["samsung", "galaxy", "z"]]

        for pref in prefix_variants:
            meta_s = _try_match(pref + toks_no_brand)
            if meta_s:
                return meta_s

    if _looks_like_watch_price_line(text):
        prefix = _watch_tokens_from_shorthand(text)
        if prefix:
            meta3 = _try_match(prefix + toks)
            if meta3:
                return meta3
            meta4 = _try_match(prefix)
            if meta4:
                return meta4
            if prefix[:2] == ["apple", "watch"]:
                meta5 = _try_match(prefix[1:] + toks)
                if meta5:
                    return meta5

    return None


def match_model_from_text_strict(text: str, model_index: Dict[str, Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    STRICT match:
    - Никаких эвристик.
    - Никаких под-нграмм.
    - Матчим ТОЛЬКО всю фразу целиком (после токенизации).
    """
    if not text:
        return None

    toks = _tokenize_for_match(text, cut_at_dash=True)
    if not toks:
        return None

    phrase = " ".join(toks)
    k = _alias_key_safe(phrase)
    if not k:
        return None

    meta = model_index.get(k)
    if _reject_phone_meta_for_text(meta, text):
        return None
    if _reject_pencil_meta_for_text(meta, text):
        return None
    return meta


# ============================================================
# Shared: build params + price (single source of truth)
# ============================================================

def build_params_and_price(
    raw_line: str,
    *,
    cat: str,
    brand: str,
    series: str,
    model: str,
    is_etalon: bool = False,
) -> Tuple[Dict[str, Any], Optional[int]]:
    price = extract_price(raw_line)

    storage, ram = extract_storage(raw_line)
    storage = _canonicalize_storage(storage)

    region = extract_region(raw_line) or ""

    # Colors (compound first)
    s_clean = _clean(raw_line)
    colors: List[str] = []

    titanium_m = getattr(R, "RX_TITANIUM_COLOR", None)
    if titanium_m:
        m = titanium_m.search(s_clean)
        if m:
            tail = (m.group(2) or "").strip()
            tail_norm = tail.lower()
            tail_map = {
                "grey": "Gray",
                "gray": "Gray",
                "black": "Black",
                "white": "White",
                "blue": "Blue",
                "violet": "Violet",
                "purple": "Purple",
                "yellow": "Yellow",
                "green": "Green",
                "red": "Red",
                "pink": "Pink",
                "orange": "Orange",
                "natural": "Natural",
            }
            tail_canon = tail_map.get(tail_norm, tail[:1].upper() + tail[1:])
            compound = f"Titanium {tail_canon}"
            colors = [compound]

    if not colors:
        colors = extract_colors_all(raw_line, limit=5)

    color_1 = colors[0] if len(colors) > 0 else ""
    color_2 = colors[1] if len(colors) > 1 else ""

    watch_ctx = _is_watch_context(cat, brand, series, model, raw_line)

    band_type = extract_band_type(raw_line) if watch_ctx else ""
    band_size = extract_band_size(raw_line, watch_context=watch_ctx) if watch_ctx else ""
    screen_size = extract_screen_size(raw_line, cat=cat, brand=brand, series=series, model=model)
    watch_size_mm = screen_size if watch_ctx else ""
    if watch_ctx:
        screen_size = ""

    connectivity = extract_connectivity(raw_line)
    if not connectivity and _is_tablet_context(cat, brand, series, model, raw_line):
        connectivity = "Wi-Fi"
    if _is_tablet_context(cat, brand, series, model, raw_line):
        if re.search(r"(?i)\bsim\b", raw_line or ""):
            connectivity = "LTE"
        if connectivity in {"5G", "4G", "LTE", "Wi-Fi+Cellular"}:
            connectivity = "LTE"
    if not connectivity and not is_etalon and _nk(cat) in {"смартфоны", "smartphones"} and _nk(brand) not in {"", "apple"}:
        connectivity = "4G"

    band_color = ""
    color = ""
    if watch_ctx and band_type:
        band_color = _extract_watch_band_color(raw_line) or ""
        color = _extract_watch_case_color(raw_line, band_color=band_color) or ""
    else:
        color = color_1 or ""

    if watch_ctx and re.search(r"(?i)\bti\b|\btitanium\b", raw_line or ""):
        if color in {"Black", "White", "Blue", "Natural"}:
            color = f"{color} Titanium"
        if color:
            if colors:
                colors[0] = color
            else:
                colors = [color]
            color_1 = colors[0]
    elif watch_ctx and "ultra" in _nk(model):
        # Ultra models are titanium even if "Ti" is omitted in the line.
        if not color:
            nk_line = _nk(raw_line)
            if "natural" in nk_line:
                color = "Natural"
            elif "black" in nk_line:
                color = "Black"
        if color in {"Black", "Natural"}:
            color = f"{color} Titanium"
            if colors:
                colors[0] = color
            else:
                colors = [color]
            color_1 = colors[0]

    if watch_ctx:
        if color:
            colors = [color]
            color_1 = color
            color_2 = ""
        elif band_color and colors:
            colors = [c for c in colors if _nk(c) != _nk(band_color)]
            color_1 = colors[0] if colors else ""
            color_2 = colors[1] if len(colors) > 1 else ""

    sim0 = extract_sim(raw_line) or ""
    sim = apply_default_sim(brand=brand, series=series, model=model, region=region, sim=sim0, cat=cat)
    if _is_tablet_context(cat, brand, series, model, raw_line):
        sim = ""

    drive = ""
    if _nk(cat) == "приставки и игры":
        drive = extract_drive(raw_line)

    chip = extract_chip(raw_line, cat=cat, brand=brand, series=series, model=model)
    year = extract_year(raw_line)

    code = extract_code(raw_line) or ""

    airpods_ctx = _is_airpods_context(cat, brand, series, model, raw_line)
    anc = extract_anc(raw_line, airpods_context=airpods_ctx)
    case_type = extract_case(raw_line, airpods_context=airpods_ctx)
    nano_glass = extract_nano_glass(raw_line)
    no_watches = False
    if _nk(brand) == "яндекс":
        if re.search(r"(?i)\bбез\s*часов\b", raw_line or ""):
            no_watches = True
    game = False
    if _nk(cat) == "приставки и игры":
        tset = set(_nk(raw_line).split())
        if tset.intersection(getattr(D, "GAME_TOKENS", set())):
            game = True
        if re.search(r"(?i)\bигр\w*\b", raw_line or ""):
            game = True

    params: Dict[str, Any] = {
        "storage": storage or "",
        "ram": ram or "",

        "color": color,

        "colors": colors[:3],
        "color_1": color_1,
        "color_2": color_2,

        "region": region,
        "sim": sim,
        "code": code,

        "band_type": band_type,
        "band_color": band_color,
        "band_size": band_size,

        "screen_size": screen_size,
        "watch_size_mm": watch_size_mm,
        "connectivity": connectivity,

        "drive": drive,

        "chip": chip,
        "year": year,

        "anc": anc,
        "case": case_type,
        "nano_glass": nano_glass,
        "no_watches": no_watches,
        "game": game,
    }

    return params, price


# ============================================================
# Tail-consume for model matching (right-to-left)
# ============================================================

def _rm_span(s: str, a: int, b: int) -> str:
    if not s:
        return s
    a = max(0, a); b = min(len(s), b)
    return tu.clean_spaces((s[:a] + " " + s[b:]).strip())


def _consume_price_tail(s: str) -> Tuple[Optional[int], str]:
    raw = s or ""
    last = None
    for m in R._RX_MONEY.finditer(raw):
        last = m
    if not last:
        m2 = None
        for mm in re.finditer(r"(?<!\d)(\d{4,7})(?!\d)", raw):
            if R._RX_YEAR_20XX.fullmatch(mm.group(1)):
                continue
            m2 = mm
        if not m2:
            return None, tu.clean_spaces(raw)

        v = int(m2.group(1))
        a, b = m2.span(1)
        left = raw[:a]
        msep = re.search(r"\s+[—–-]\s*$", left)
        if msep:
            a = msep.start()
        out = _rm_span(raw, a, b)
        out = re.sub(r"(?i)\b(?:rub|rur|руб|р\.?)\b|₽", " ", out)
        return v, tu.clean_spaces(out)

    v = extract_price(raw)
    a, b = last.span(1)
    left = raw[:a]
    msep = re.search(r"\s+[—–-]\s*$", left)
    if msep:
        a = msep.start()
    out = _rm_span(raw, a, b)
    out = re.sub(r"(?i)\b(?:rub|rur|руб|р\.?)\b|₽", " ", out)
    return v, tu.clean_spaces(out)


def _consume_region_tail(s: str) -> Tuple[str, str]:
    raw = s or ""
    region = ""

    # flags
    flags = tu.FLAG_RE.findall(raw)
    if flags:
        for fl in flags:
            reg = D.REGION_FLAG_MAP.get(fl)
            if reg and not region:
                region = reg
            raw = raw.replace(fl, " ")

    s_clean = tu.clean_spaces(raw)
    # words (strict, token-safe)
    for rx, reg in R.REGION_WORDS_STRICT:
        if not reg:
            continue
        reg_l = str(reg).lower().strip()
        if 2 <= len(reg_l) <= 3:
            toks = _nk(s_clean).split()
            if reg_l not in toks:
                continue
        m = rx.search(s_clean)
        if not m:
            continue
        if not region:
            region = reg_l
        s_clean = _rm_span(s_clean, m.start(), m.end())

    return (region or ""), tu.clean_spaces(s_clean)


def _consume_sim_tail(s: str) -> Tuple[str, str]:
    raw = tu.clean_spaces(s or "")
    sim = extract_sim(raw) or ""
    if not sim:
        return "", raw

    raw2 = raw
    raw2 = re.sub(r"(?i)\b2\s*[- ]?\s*sim\b", " ", raw2)
    raw2 = re.sub(r"(?i)\bdual\s*sim\b", " ", raw2)
    raw2 = re.sub(r"(?i)\bnano\s*[- ]?\s*sim\s*\+\s*e\s*[- ]?\s*sim\b", " ", raw2)
    raw2 = re.sub(r"(?i)\bsim\s*\+\s*e\s*[- ]?\s*sim\b", " ", raw2)
    raw2 = re.sub(r"(?i)\be\s*[- ]?\s*sim\b", " ", raw2)
    raw2 = re.sub(r"(?i)\bsim\b", " ", raw2)

    return sim, tu.clean_spaces(raw2)


def _consume_color_tail(s: str) -> Tuple[str, List[str], str]:
    raw = s or ""
    color = extract_color(raw) or ""
    if not color:
        return "", [], tu.clean_spaces(raw)

    global _COLOR_MATCHERS
    if _COLOR_MATCHERS is None:
        _COLOR_MATCHERS = _init_color_matchers()

    s_clean = _clean(raw)
    for rx, canon in _COLOR_MATCHERS:
        m = rx.search(s_clean)
        if not m:
            continue
        raw2 = _rm_span(raw, m.start(), m.end())
        colors = extract_colors_all(raw, limit=3)
        c0 = colors[0] if colors else canon
        return c0, colors, tu.clean_spaces(raw2)

    return "", [], tu.clean_spaces(raw)


def _consume_storage_tail(s: str) -> Tuple[str, Any, str]:
    raw = s or ""
    storage, ram = extract_storage(raw)
    storage = _canonicalize_storage(storage)

    if not storage and not ram:
        return "", "", tu.clean_spaces(raw)

    raw2 = raw
    raw2 = re.sub(R.RX_MEM_CONFIG_SLASH_NO_UNIT, " ", raw2)
    raw2 = re.sub(R.RX_MEM_CONFIG_SLASH, " ", raw2)
    raw2 = re.sub(R.RX_MEM_EXPLICIT_ALL, " ", raw2)

    if storage:
        num = storage.replace("GB", "").replace("TB", "").strip()
        if num.isdigit():
            raw2 = re.sub(rf"(?<!\d)\b{re.escape(num)}\b(?!\d)", " ", raw2)

    # cleanup dangling unit tokens like "Gb" after "1TB"
    raw2 = re.sub(r"(?i)\b(gb|гб|tb|тб|g)\b", " ", raw2)

    return storage or "", ram or "", tu.clean_spaces(raw2)


def _rest_for_model_from_tail(raw_line: str) -> str:
    """
    Right-to-left consume:
      price -> region -> sim -> color -> storage/ram
    Return remainder for model matching.
    """
    s = tu.clean_spaces(raw_line or "")
    _p, s = _consume_price_tail(s)
    _r, s = _consume_region_tail(s)
    _sim, s = _consume_sim_tail(s)
    _c, _cs, s = _consume_color_tail(s)
    _st, _ram, s = _consume_storage_tail(s)
    return tu.clean_spaces(s)


def resolve_meta_for_line(
    raw_line: str,
    *,
    model_index: Dict[str, Dict[str, Any]],
    code_index: Dict[str, Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    """
    ✅ Новый порядок:
      0) by code
      1) strict по остатку (tail-consume)
      2) normal match по ORIGINAL raw_line (важно для “17 256 Gb ...”)
      3) normal match по остатку
    """
    def _reject_for_line(m: Optional[Dict[str, Any]]) -> bool:
        if not m:
            return False
        if _reject_plus_meta_for_text(m, raw_line):
            return True
        if _reject_phone_meta_for_text(m, raw_line):
            return True
        if _reject_tablet_meta_for_text(m, raw_line):
            return True
        if _reject_watch_meta_for_text(m, raw_line):
            return True
        if _reject_airpods_meta_for_text(m, raw_line):
            return True
        if _reject_pencil_meta_for_text(m, raw_line):
            return True
        return False

    # 0) by code
    if code_index:
        code = (extract_code(raw_line) or "").strip().upper()
        if code:
            meta = code_index.get(code)
            if meta:
                if not _reject_for_line(meta):
                    return meta

    # 1) strict by remainder
    rest = _rest_for_model_from_tail(raw_line)
    meta = match_model_from_text_strict(rest, model_index)
    if meta:
        if not _reject_for_line(meta):
            return meta

    # 2) normal by ORIGINAL string (там есть storage/цвет/сим и т.д.)
    meta = match_model_from_text(raw_line, model_index)
    if meta:
        if not _reject_for_line(meta):
            return meta

    # 3) normal by remainder
    meta = match_model_from_text(rest, model_index)
    if meta:
        if not _reject_for_line(meta):
            return meta

    # 4) accessories fallback: watch bands
    if _looks_like_watch_band_line(raw_line):
        k = _alias_key_safe("Ремешки для Apple Watch")
        if k and model_index.get(k):
            return model_index.get(k)
        k2 = _alias_key_safe("Ремешки Apple Watch")
        if k2 and model_index.get(k2):
            return model_index.get(k2)

    return None


# ============================================================
# Google one-line helper (kept)
# ============================================================

def normalize_text_as_etalon_item(
    text: str,
    *,
    prefer_clean: bool = True,
) -> Dict[str, Any]:
    raw_in = (text or "").strip()
    if not raw_in:
        return {"empty": True}

    raw = _clean(raw_in) if prefer_clean else raw_in

    if not PARSED_ETALON_JSON.exists():
        run_build_parsed_etalon(root_data_path=ROOT_DATA_JSON, out_path=PARSED_ETALON_JSON)

    model_index = _load_model_index()
    code_index = _load_code_index()

    meta = resolve_meta_for_line(raw, model_index=model_index, code_index=code_index)
    if not meta:
        return {"empty": True, "raw_etalon": raw_in, "raw_clean": raw}

    path = meta.get("path") or ["", "", "", ""]
    cat_s, br_s, sr_s, model_s = (path + ["", "", "", ""])[:4]
    cat_s = str(cat_s).strip()
    br_s = str(br_s).strip()
    sr_s = str(sr_s).strip()
    model_s = str(model_s).strip()

    params, price = build_params_and_price(raw, cat=cat_s, brand=br_s, series=sr_s, model=model_s, is_etalon=True)

    it = make_item(
        path=[cat_s, br_s, sr_s, model_s],
        brand=br_s,
        series=sr_s,
        model=model_s,
        raw=raw,
        params=params,
        price=price,
    )

    it["raw_etalon"] = raw
    it["google_original"] = raw_in
    it["google_clean"] = raw

    return it


def normalize_text_as_goods_item(
    text: str,
    *,
    channel: str | None = None,
    message_id: Optional[int] = None,
    date: str | None = None,
    path: List[str] | None = None,
    prefer_clean: bool = True,
) -> Dict[str, Any]:
    raw_in = (text or "").strip()
    if not raw_in:
        return {"empty": True}

    raw = _clean(raw_in) if prefer_clean else raw_in

    if not PARSED_ETALON_JSON.exists():
        run_build_parsed_etalon(root_data_path=ROOT_DATA_JSON, out_path=PARSED_ETALON_JSON)

    model_index = _load_model_index()
    code_index = _load_code_index()

    meta = resolve_meta_for_line(raw, model_index=model_index, code_index=code_index)
    if not meta:
        return {"empty": True, "raw_parsed": raw_in, "raw_clean": raw}

    path_meta = meta.get("path") or ["", "", "", ""]
    cat_s, br_s, sr_s, model_s = (path_meta + ["", "", "", ""])[:4]
    cat_s = str(cat_s).strip()
    br_s = str(br_s).strip()
    sr_s = str(sr_s).strip()
    model_s = str(model_s).strip()

    params, price = build_params_and_price(raw, cat=cat_s, brand=br_s, series=sr_s, model=model_s, is_etalon=True)

    it = make_item(
        path=[cat_s, br_s, sr_s, model_s],
        brand=br_s,
        series=sr_s,
        model=model_s,
        raw=raw,
        params=params,
        price=price,
        date=(date or ""),
        message_id=message_id,
        channel=(channel or ""),
    )

    if path:
        it["path"] = path

    it["raw_original"] = raw_in
    it["raw_clean"] = raw

    return it


# ============================================================
# Build parsed_etalon.json from data.json
# ============================================================

def run_build_parsed_etalon(
    *,
    root_data_path: Path = ROOT_DATA_JSON,
    out_path: Path | None = None,
) -> Dict[str, Any]:
    if out_path is None:
        out_path = PARSED_ETALON_JSON
    db = _load_json(root_data_path, {})
    if not isinstance(db, dict):
        db = {}

    build_id = _build_id_for_file(root_data_path)

    items: List[Dict[str, Any]] = []

    cnt_brand = Counter()
    cnt_series = Counter()
    cnt_model = Counter()
    cnt_cat = Counter()

    cnt_color = Counter()
    cnt_region = Counter()
    cnt_sim = Counter()
    cnt_storage = Counter()
    cnt_ram = Counter()
    cnt_code = Counter()
    cnt_band_type = Counter()
    cnt_band_color = Counter()
    cnt_band_size = Counter()
    cnt_screen_size = Counter()
    cnt_connectivity = Counter()
    cnt_chip = Counter()
    cnt_year = Counter()

    cnt_anc = Counter()
    cnt_case = Counter()

    for path4, raw_line in iter_raw_etalon_lines(db):
        cat, br, sr, model = (path4 + ["", "", "", ""])[:4]
        raw = raw_line.strip()

        cat_s = str(cat).strip()
        br_s = str(br).strip()
        sr_s = str(sr).strip()
        model_s = str(model).strip()

        params, price = build_params_and_price(raw, cat=cat_s, brand=br_s, series=sr_s, model=model_s, is_etalon=True)

        items.append(
            make_item(
                path=[cat_s, br_s, sr_s, model_s],
                brand=br_s,
                series=sr_s,
                model=model_s,
                raw=raw,
                params=params,
                price=price,
            )
        )

        if cat_s: cnt_cat[cat_s] += 1
        if br_s: cnt_brand[br_s] += 1
        if sr_s: cnt_series[sr_s] += 1
        if model_s: cnt_model[model_s] += 1

        color = (params.get("color") or "").strip()
        region = (params.get("region") or "").strip()
        sim = (params.get("sim") or "").strip()
        storage = (params.get("storage") or "").strip()
        ram = params.get("ram") or ""
        code = (params.get("code") or "").strip()

        band_type = (params.get("band_type") or "").strip()
        band_color = (params.get("band_color") or "").strip()
        band_size = (params.get("band_size") or "").strip()
        screen_size = (params.get("screen_size") or "").strip()
        connectivity = (params.get("connectivity") or "").strip()
        chip = (params.get("chip") or "").strip()
        year = (params.get("year") or "").strip()
        anc = (params.get("anc") or "").strip()
        case_type = (params.get("case") or "").strip()

        if color: cnt_color[color] += 1
        if region: cnt_region[region] += 1
        if sim: cnt_sim[sim] += 1
        if storage: cnt_storage[storage] += 1
        if ram:
            try:
                cnt_ram[int(ram)] += 1
            except Exception:
                pass
        if code: cnt_code[code] += 1

        if band_type: cnt_band_type[band_type] += 1
        if band_color: cnt_band_color[band_color] += 1
        if band_size: cnt_band_size[band_size] += 1
        if screen_size: cnt_screen_size[screen_size] += 1
        if connectivity: cnt_connectivity[connectivity] += 1
        if chip: cnt_chip[chip] += 1
        if year: cnt_year[year] += 1

        if anc: cnt_anc[anc] += 1
        if case_type: cnt_case[case_type] += 1

    out = {
        "source": str(root_data_path),
        "build_id": build_id,
        "items": items,
        "items_count": len(items),
        "scope": SCOPE_ETALON,
    }
    _save_json(out_path, out)

    model_index, aliases_map, collisions = build_model_index_and_aliases(items)
    code_index = build_code_index(items)

    _save_json(MODEL_INDEX_JSON, {
        "scope": SCOPE_ETALON,
        "build_id": build_id,
        "index_count": len(model_index),
        "index": model_index,
    })

    _save_json(MODEL_ALIASES_JSON, {
        "scope": SCOPE_ETALON,
        "build_id": build_id,
        "aliases_count": len(aliases_map),
        "aliases": aliases_map,
    })

    _save_json(ALIAS_COLLISIONS_JSON, {
        "scope": SCOPE_ETALON,
        "build_id": build_id,
        "collisions_count": len(collisions),
        "collisions": collisions,
    })

    _save_json(CODE_INDEX_JSON, {
        "scope": SCOPE_ETALON,
        "build_id": build_id,
        "index_count": len(code_index),
        "index": code_index,
    })

    learned = {
        "scope": SCOPE_ETALON,
        "build_id": build_id,
        "counts": {
            "categories": len(cnt_cat),
            "brands": len(cnt_brand),
            "series": len(cnt_series),
            "models": len(cnt_model),
            "aliases": len(aliases_map),
            "alias_collisions": len(collisions),
            "codes_index": len(code_index),
        },
        "top": {
            "categories": cnt_cat.most_common(50),
            "brands": cnt_brand.most_common(50),
            "series": cnt_series.most_common(50),
            "models": cnt_model.most_common(50),
            "colors": cnt_color.most_common(80),
            "regions": cnt_region.most_common(80),
            "sim": cnt_sim.most_common(20),
            "storage": cnt_storage.most_common(50),
            "ram": cnt_ram.most_common(30),
            "codes": cnt_code.most_common(50),
            "band_type": cnt_band_type.most_common(30),
            "band_color": cnt_band_color.most_common(30),
            "band_size": cnt_band_size.most_common(20),
            "screen_size": cnt_screen_size.most_common(30),
            "connectivity": cnt_connectivity.most_common(30),
            "chip": cnt_chip.most_common(50),
            "year": cnt_year.most_common(30),
            "anc": cnt_anc.most_common(10),
            "case": cnt_case.most_common(10),
        }
    }
    _save_json(LEARNED_TOKENS_JSON, learned)

    stats = {
        "scope": SCOPE_ETALON,
        "build_id": build_id,
        "items": len(items),
        "unique": {
            "categories": len(cnt_cat),
            "brands": len(cnt_brand),
            "series": len(cnt_series),
            "models": len(cnt_model),
        },
        "coverage": {
            "price": sum(1 for it in items if it.get("price") is not None),
            "storage": sum(1 for it in items if (it.get("params") or {}).get("storage")),
            "ram": sum(1 for it in items if (it.get("params") or {}).get("ram")),
            "color": sum(1 for it in items if (it.get("params") or {}).get("color")),
            "region": sum(1 for it in items if (it.get("params") or {}).get("region")),
            "sim": sum(1 for it in items if (it.get("params") or {}).get("sim")),
            "code": sum(1 for it in items if (it.get("params") or {}).get("code")),
            "band_type": sum(1 for it in items if (it.get("params") or {}).get("band_type")),
            "band_color": sum(1 for it in items if (it.get("params") or {}).get("band_color")),
            "band_size": sum(1 for it in items if (it.get("params") or {}).get("band_size")),
            "screen_size": sum(1 for it in items if (it.get("params") or {}).get("screen_size")),
            "connectivity": sum(1 for it in items if (it.get("params") or {}).get("connectivity")),
            "chip": sum(1 for it in items if (it.get("params") or {}).get("chip")),
            "year": sum(1 for it in items if (it.get("params") or {}).get("year")),
            "anc": sum(1 for it in items if (it.get("params") or {}).get("anc")),
            "case": sum(1 for it in items if (it.get("params") or {}).get("case")),
        },
        "aliases": {"count": len(aliases_map), "collisions": len(collisions)},
        "code_index": {"count": len(code_index)},
    }
    _save_json(ETALON_STATS_JSON, stats)

    return {
        "items_count": len(items),
        "aliases_count": len(aliases_map),
        "collisions_count": len(collisions),
        "code_index_count": len(code_index),
        "build_id": build_id,
        "out": str(out_path),
    }


def ensure_etalon_ready() -> None:
    """
    Ensure parsed_etalon + indexes are built and up-to-date with data.json.
    """
    build_id = _build_id_for_file(ROOT_DATA_JSON)

    mi = _load_json(MODEL_INDEX_JSON, {})
    ci = _load_json(CODE_INDEX_JSON, {})
    pe = _load_json(PARSED_ETALON_JSON, {})

    def ok(meta: Any) -> bool:
        return isinstance(meta, dict) and (meta.get("build_id") == build_id)

    if ok(mi) and ok(ci) and ok(pe):
        return

    logger.info("Etalon/index build_id mismatch -> rebuilding etalon (build_id=%s)", build_id)
    run_build_parsed_etalon(root_data_path=ROOT_DATA_JSON, out_path=PARSED_ETALON_JSON)


# ============================================================
# Build parsed_goods.json from parsed_messages.json
# ============================================================

def run_build_parsed_goods(
    *,
    messages_path: Path | None = None,
    out_path: Path | None = None,
    ensure_etalon: bool = True,
    run_matcher: bool = True,
) -> Dict[str, Any]:
    if messages_path is None:
        messages_path = PARSED_MESSAGES_JSON
    if out_path is None:
        out_path = PARSED_GOODS_JSON
    if ensure_etalon:
        ensure_etalon_ready()

    model_index = _load_model_index()
    if not model_index:
        raise RuntimeError("model_index is empty (check etalon build / MODEL_INDEX_JSON)")

    code_index = _load_code_index()

    db = _load_json(messages_path, [])
    if not isinstance(db, list):
        raise RuntimeError(f"parsed_messages.json must be a list, got: {type(db).__name__}")

    goods: List[Dict[str, Any]] = []
    unmatched: List[Dict[str, Any]] = []

    cnt_msgs = 0
    cnt_lines = 0
    cnt_empty = 0
    cnt_matched = 0
    cnt_unmatched = 0
    cnt_exceptions = 0

    for msg in db:
        if not isinstance(msg, dict):
            continue
        cnt_msgs += 1

        channel = str(msg.get("channel") or "").strip()
        message_id = msg.get("message_id")
        try:
            message_id_int = int(message_id) if message_id is not None else None
        except Exception:
            message_id_int = None

        date = str(msg.get("date") or "").strip()

        message_text = str(msg.get("message") or "")
        orig_lines = [tu.clean_spaces(l) for l in message_text.splitlines()]
        orig_lines_nk = [_nk(tu.strip_flags(l)) for l in orig_lines]
        deleted_rows = msg.get("deleted_rows") or []
        deleted_set = {
            _nk(tu.strip_flags(tu.clean_spaces(r)))
            for r in deleted_rows
            if isinstance(r, str)
        }
        last_line_idx = -1

        lines = msg.get("lines") or []
        if isinstance(lines, str):
            lines = [lines]
        if not isinstance(lines, list):
            continue

        for ln in lines:
            if not isinstance(ln, str):
                continue
            cnt_lines += 1

            raw_line = ln.strip()
            if not raw_line:
                cnt_empty += 1
                continue

            try:
                raw_nk = _nk(tu.strip_flags(tu.clean_spaces(raw_line)))
                line_idx = None
                if orig_lines_nk:
                    for i in range(last_line_idx + 1, len(orig_lines_nk)):
                        if orig_lines_nk[i] == raw_nk:
                            line_idx = i
                            break
                    if line_idx is None:
                        for i in range(len(orig_lines_nk)):
                            if orig_lines_nk[i] == raw_nk:
                                line_idx = i
                                break

                header = None
                if line_idx is not None:
                    last_line_idx = line_idx
                    for j in range(line_idx - 1, -1, -1):
                        if orig_lines_nk[j] and orig_lines_nk[j] in deleted_set:
                            header = orig_lines[j]
                            break

                meta = resolve_meta_for_line(
                    raw_line,
                    model_index=model_index,
                    code_index=code_index,
                )
                parse_line = raw_line

                if header:
                    header_nk = _nk(tu.strip_flags(tu.clean_spaces(header)))
                    header_brand = None
                    for tok in header_nk.split():
                        if tok in {"xiaomi", "poco", "redmi", "mi"}:
                            header_brand = "xiaomi"
                            break
                        if tok in {"google", "pixel"}:
                            header_brand = "google"
                            break
                        if tok in {"samsung"}:
                            header_brand = "samsung"
                            break
                    if header_brand and meta:
                        meta_path = meta.get("path") or []
                        meta_brand = _nk(str(meta_path[1] or "")) if isinstance(meta_path, list) and len(meta_path) > 1 else ""
                        if meta_brand and meta_brand != header_brand:
                            meta = None

                if not meta and header:
                    parse_line = f"{header} {raw_line}"
                    meta = resolve_meta_for_line(
                        parse_line,
                        model_index=model_index,
                        code_index=code_index,
                    )

                if not meta:
                    cnt_unmatched += 1
                    unmatched.append({
                        "channel": channel,
                        "message_id": message_id_int,
                        "date": date,
                        "raw": raw_line,
                        "reason": "no_etalon_match",
                        "rest": _rest_for_model_from_tail(raw_line),
                    })
                    continue

                path = meta.get("path") or ["", "", "", ""]
                cat_s, br_s, sr_s, model_s = (path + ["", "", "", ""])[:4]
                cat_s = str(cat_s).strip()
                br_s = str(br_s).strip()
                sr_s = str(sr_s).strip()
                model_s = str(model_s).strip()

                params, price = build_params_and_price(
                    parse_line,
                    cat=cat_s,
                    brand=br_s,
                    series=sr_s,
                    model=model_s,
                )

                goods.append(
                    make_item(
                        path=[cat_s, br_s, sr_s, model_s],
                        brand=br_s,
                        series=sr_s,
                        model=model_s,
                        raw=raw_line,
                        params=params,
                        price=price,
                        date=date,
                        message_id=message_id_int,
                        channel=channel,
                    )
                )
                cnt_matched += 1

            except Exception as e:
                cnt_exceptions += 1
                cnt_unmatched += 1
                unmatched.append({
                    "channel": channel,
                    "message_id": message_id_int,
                    "date": date,
                    "raw": raw_line,
                    "reason": "exception",
                    "error": f"{type(e).__name__}: {e}",
                })
                continue

    out = {
        "source": str(messages_path),
        "items": goods,
        "items_count": len(goods),
        "unmatched_count": len(unmatched),
        "scope": SCOPE_GOODS,
        "code_index_loaded": bool(code_index),
        "code_index_count": len(code_index) if code_index else 0,
        "debug": {
            "msgs": cnt_msgs,
            "lines_total": cnt_lines,
            "lines_empty": cnt_empty,
            "matched": cnt_matched,
            "unmatched": cnt_unmatched,
            "exceptions": cnt_exceptions,
        }
    }
    _save_json(out_path, out)
    _save_json(UNMATCHED_PARSED_JSON, {
        "source": str(messages_path),
        "items": unmatched,
        "items_count": len(unmatched),
        "scope": "unmatched_goods_lines_v1",
    })

    matcher_res = None
    if run_matcher:
        matcher_res = run_matcher_stage()

    return {
        "goods_count": len(goods),
        "unmatched_count": len(unmatched),
        "out": str(out_path),
        "unmatched_out": str(UNMATCHED_PARSED_JSON),
        "code_index_loaded": bool(code_index),
        "code_index_count": len(code_index) if code_index else 0,
        "matcher_ran": bool(run_matcher),
        "matcher_result": matcher_res,
        "debug": out.get("debug", {}),
    }


# ============================================================
# External stage: matcher
# ============================================================

def run_matcher_stage():
    if not PARSED_GOODS_JSON.exists() or PARSED_GOODS_JSON.stat().st_size < 20:
        raise RuntimeError(f"parsed_goods.json not ready: {PARSED_GOODS_JSON}")

    return matcher_mod.run_matcher(
        etalon_path=PARSED_ETALON_JSON,
        goods_path=PARSED_GOODS_JSON,
        matched_path=PARSED_MATCHED_JSON,
        stats_path=MATCH_STATS_JSON,
        unmatched_etalon_path=UNMATCHED_ETALON_JSON,
        unmatched_parsed_path=UNMATCHED_PARSED_FROM_MATCHER_JSON,
    )


# ============================================================
# Local run: emulate parser.py behavior
# (etalon -> goods -> matcher) WITHOUT results.py
# ============================================================

def run_as_parser() -> Dict[str, Any]:
    logger.info("ENTRY: run_as_parser() start")

    et = run_build_parsed_etalon(
        root_data_path=ROOT_DATA_JSON,
        out_path=PARSED_ETALON_JSON,
    )

    gd = run_build_parsed_goods(
        messages_path=PARSED_MESSAGES_JSON,
        out_path=PARSED_GOODS_JSON,
        ensure_etalon=True,
        run_matcher=True,
    )

    res = {"etalon": et, "goods": gd}
    logger.info("ENTRY: run_as_parser() done")
    return res


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    out = run_as_parser()
    print(json.dumps(out, ensure_ascii=False, indent=2))
