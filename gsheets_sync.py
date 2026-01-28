# gsheets_sync.py
from __future__ import annotations

import asyncio
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone, timedelta

import gspread
from google.oauth2.service_account import Credentials

from handlers.normalizers import entry as entry_mod  # ✅ единый entry.py
from handlers.parsing import matcher

DEBUG_GSHEETS = False


# ---------------- PATHS ----------------

BASE_DIR = Path(__file__).resolve().parent

SERVICE_ACCOUNT_FILE = BASE_DIR / "config" / "google_service_account.json"
PARSING_DATA_DIR = BASE_DIR / "handlers" / "parsing" / "data"

# ✅ candidates from Telegram prices/messages (entry.py writes dict with "items")
GOODS_FILE = PARSING_DATA_DIR / "parsed_goods.json"

# ✅ output like parsed_data
GOOGLE_PARSED_FILE = PARSING_DATA_DIR / "google_parsed.json"

LOG_DIR = BASE_DIR / "google_sheet_update"
LOG_DIR.mkdir(parents=True, exist_ok=True)

SHEET_ID = "1DhDdf5FbjIXShOhWN3g_xZdIjnLiMZUPkIAei-hG3Bc"
WORKSHEET_GID = 0

# read G, write BB/BC/BD
COL_SRC_NAME = "G"
COL_MATCHED_RAW = "BB"  # что реально сматчилось (raw из сообщений)
COL_PRICE = "BC"
COL_CHANNEL = "BD"

MOSCOW_TZ = timezone(timedelta(hours=3))


# ======================================================================
# CLEAN GOOGLE NAME (light clean before entry.py)
# ======================================================================

_RE_MULTI_SPACES = re.compile(r"\s+")
_RE_PREFIXES = re.compile(r"(?i)^\s*(смартфон|телефон|планшет|ноутбук|часы|watch)\s+")
_RE_BRAND_PREFIX = re.compile(r"(?i)^\s*apple\s+")
_RE_GB_FIX = re.compile(r"(?i)\b(\d+)\s*g(?:b)?\b")  # 256Gb / 256GB / 256g -> 256GB
_RE_PAREN = re.compile(r"\(([^)]{1,80})\)")

# ✅ FIX: Google colors like "Desert Titanium" -> "Desert"
_RE_IPHONE_TITANIUM_COLOR = re.compile(r"(?i)\b(desert|natural|black|white)\s+titanium\b")
_RE_TITANIUM_WORD = re.compile(r"(?i)\btitanium\b")

_GARBAGE_PAREN_WORDS = {
    "global", "eu", "europe",
    "ростест", "рст", "eac",
    "серый", "сертиф", "сертификация",
    "гарантия", "официал", "official",
    "актив", "active", "open", "opened", "распак", "вскрыт",
}


def clean_google_name_for_entry(text: str) -> str:
    """
    Лёгкая чистка строки из Google:
    - убрать префиксы типа "Смартфон"
    - убрать Apple в начале (если есть)
    - выкинуть мусорные скобки (Global/Ростест/...)
    - привести память к 256GB
    - ✅ привести iPhone-цвета типа "Desert Titanium" -> "Desert"
    """
    s = (text or "").replace("\xa0", " ").strip()
    if not s:
        return ""

    s = _RE_PREFIXES.sub("", s)
    s = _RE_BRAND_PREFIX.sub("", s)

    def _paren_repl(m: re.Match) -> str:
        inside = (m.group(1) or "").strip().lower()
        inside_norm = re.sub(r"[^a-zа-я0-9]+", " ", inside).strip()
        parts = set(inside_norm.split())
        if parts & _GARBAGE_PAREN_WORDS:
            return " "
        return m.group(0)

    s = _RE_PAREN.sub(_paren_repl, s)

    # memory
    s = _RE_GB_FIX.sub(lambda m: f"{m.group(1)}GB", s)

    # separators
    s = s.replace("—", "-").replace("–", "-")

    # titanium color normalization (iPhone-only)
    if re.search(r"(?i)\biphone\b", s):
        s = _RE_IPHONE_TITANIUM_COLOR.sub(lambda m: m.group(1), s)
        s = _RE_TITANIUM_WORD.sub(" ", s)

    s = _RE_MULTI_SPACES.sub(" ", s).strip()
    return s


# ======================================================================
# LOGGING
# ======================================================================

def _make_log_file() -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return LOG_DIR / f"update_{ts}.log"


def _write_log(log_file: Path, msg: str) -> None:
    if not DEBUG_GSHEETS:
        return
    try:
        with log_file.open("a", encoding="utf-8") as f:
            f.write(msg.rstrip() + "\n")
    except Exception:
        pass


# ======================================================================
# GOOGLE SHEETS
# ======================================================================

def _authorize_gspread():
    if not SERVICE_ACCOUNT_FILE.exists():
        raise RuntimeError(f"Файл сервисного аккаунта не найден: {SERVICE_ACCOUNT_FILE}")

    creds = Credentials.from_service_account_file(
        str(SERVICE_ACCOUNT_FILE),
        scopes=["https://www.googleapis.com/auth/spreadsheets"],
    )
    return gspread.authorize(creds)


def _open_worksheet(gc):
    sh = gc.open_by_key(SHEET_ID)
    ws = sh.get_worksheet_by_id(WORKSHEET_GID)
    if not ws:
        ws = sh.sheet1
    return ws


# ======================================================================
# LOAD parsed_goods (candidates)
# ======================================================================

def _load_goods_list(log_file: Optional[Path] = None) -> List[Dict[str, Any]]:
    """
    entry.py пишет parsed_goods.json как dict:
      {"items":[...], "items_count":...}
    но оставляем совместимость со старым форматом list.
    """
    goods_list: List[Dict[str, Any]] = []
    try:
        if GOODS_FILE.exists():
            raw = json.loads(GOODS_FILE.read_text(encoding="utf-8"))
            if isinstance(raw, dict) and isinstance(raw.get("items"), list):
                goods_list = raw["items"]
            elif isinstance(raw, dict) and isinstance(raw.get("parsed_pool"), list):
                goods_list = raw["parsed_pool"]  # legacy
            elif isinstance(raw, list):
                goods_list = raw
    except Exception as e:
        if log_file:
            _write_log(log_file, f"[gsheets_sync] Ошибка чтения {GOODS_FILE}: {e}")
        goods_list = []

    if log_file:
        _write_log(log_file, f"[gsheets_sync] parsed_goods loaded: {len(goods_list)}")
    return goods_list


# ======================================================================
# HELPERS
# ======================================================================

def _norm_str(v: Any) -> str:
    return str(v or "").strip().lower()


def _best_channel_to_str(v: Any) -> str:
    if isinstance(v, list):
        return (v[0] if v else "") or ""
    return str(v or "")


def _extract_price_channel_from_good(good: Dict[str, Any]) -> Tuple[Optional[float], Optional[str], Optional[str]]:
    """
    Возвращаем (price, channel, raw_line).
    """
    price = good.get("price")
    channel = good.get("channel")

    if price is None and "min_price" in good:
        price = good.get("min_price")

    if not channel and "best_channel" in good:
        channel = _best_channel_to_str(good.get("best_channel"))

    raw_line = (
        good.get("raw_line")
        or good.get("raw_text")
        or good.get("raw")
        or good.get("raw_parsed")
        or ""
    )

    try:
        price_f = float(price) if price is not None else None
    except Exception:
        price_f = None

    return (price_f, (channel or None), (raw_line or None))


def _build_parsed_item_from_good(item: Dict[str, Any]) -> Dict[str, Any]:
    """
    Собираем dict "как parsed_item" из goods.
    matcher.match_product() ожидает плоский dict + params.
    """
    parsed: Dict[str, Any] = {}

    base = None
    for key in ("parsed", "parsed_raw", "normalized", "item", "data"):
        v = item.get(key)
        if isinstance(v, dict):
            base = v
            break
    if base is None:
        base = item

    if isinstance(base, dict):
        parsed.update(base)

    params = item.get("params")
    if isinstance(params, dict):
        parsed.update(params)

    if "category" not in parsed and item.get("category"):
        parsed["category"] = item["category"]
    if "path" not in parsed and isinstance(item.get("path"), list):
        parsed["path"] = item["path"]

    if "raw" not in parsed:
        parsed["raw"] = (
            item.get("raw")
            or item.get("raw_line")
            or item.get("raw_text")
            or item.get("raw_parsed")
            or ""
        )

    return parsed


# ======================================================================
# GOOGLE ETALON BUILD
# ======================================================================

def _ensure_entry_indexes(log_file: Path) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    """
    Гарантируем наличие etalon-индексов entry.py (MODEL_INDEX_JSON + CODE_INDEX_JSON).
    Возвращаем (model_index, code_index).
    """
    try:
        if not entry_mod.PARSED_ETALON_JSON.exists():
            _write_log(log_file, "[gsheets_sync] parsed_etalon.json отсутствует -> build etalon")
            entry_mod.run_build_parsed_etalon(
                root_data_path=entry_mod.ROOT_DATA_JSON,
                out_path=entry_mod.PARSED_ETALON_JSON,
            )
    except Exception as e:
        _write_log(log_file, f"[gsheets_sync] build etalon failed: {e}")
        raise

    model_index = entry_mod._load_model_index()
    code_index = entry_mod._load_code_index()
    _write_log(log_file, f"[gsheets_sync] model_index={len(model_index)} code_index={len(code_index)}")
    return model_index, code_index


def _extract_params_best_effort(
    raw: str,
    *,
    cat: str = "",
    brand: str = "",
    series: str = "",
    model: str = "",
) -> Dict[str, Any]:
    """
    Best-effort извлечение параметров из строки (даже если модель не сматчилась в индексе).
    """
    raw = (raw or "").strip()
    if not raw:
        return {
            "storage": "", "ram": "", "color": "", "region": "", "sim": "", "code": "",
            "band_type": "", "band_color": "", "band_size": "",
            "screen_size": "", "connectivity": "", "chip": "", "year": "",
            "anc": "", "case": "", "nano_glass": False,
        }

    storage, ram = entry_mod.extract_storage(raw)
    region = entry_mod.extract_region(raw) or ""
    sim0 = entry_mod.extract_sim(raw) or ""

    # Без meta apply_default_sim может не отработать идеально, но если бренд/серия/модель пустые — вернёт как есть
    sim = entry_mod.apply_default_sim(brand=brand, series=series, model=model, region=region, sim=sim0, cat=cat)

    # цвета
    colors = entry_mod.extract_colors_all(raw, limit=5)
    color = colors[0] if colors else ""

    # прочее
    code = entry_mod.extract_code(raw) or ""
    year = entry_mod.extract_year(raw)
    chip = entry_mod.extract_chip(raw, cat=cat, brand=brand, series=series, model=model)
    # watch/airpods параметры — только если контекст очевиден
    watch_ctx = False
    try:
        watch_ctx = entry_mod._is_watch_context(cat, brand, series, model, raw)
    except Exception:
        watch_ctx = False

    connectivity = entry_mod.extract_connectivity(raw)
    screen_size = entry_mod.extract_screen_size(raw, cat=cat, brand=brand, series=series, model=model)
    watch_size_mm = screen_size if watch_ctx else ""
    if watch_ctx:
        screen_size = ""

    band_type = entry_mod.extract_band_type(raw) if watch_ctx else ""
    band_size = entry_mod.extract_band_size(raw, watch_context=watch_ctx) if watch_ctx else ""
    band_color = entry_mod._extract_watch_band_color(raw) if (watch_ctx and band_type) else ""
    if watch_ctx and band_type:
        try:
            case_color = entry_mod._extract_watch_case_color(raw, band_color=band_color or "") or ""
        except Exception:
            case_color = ""
        # если кейс-цвет определился — предпочитаем его
        if case_color:
            color = case_color
    if watch_ctx and re.search(r"(?i)\bti\b|\btitanium\b", raw or ""):
        if color in {"Black", "White", "Blue", "Natural"}:
            color = f"{color} Titanium"
        if band_color in {"Black", "White", "Blue", "Natural"}:
            band_color = f"{band_color} Titanium"

    airpods_ctx = False
    try:
        airpods_ctx = entry_mod._is_airpods_context(cat, brand, series, model, raw)
    except Exception:
        airpods_ctx = False

    anc = entry_mod.extract_anc(raw, airpods_context=airpods_ctx)
    case_type = entry_mod.extract_case(raw, airpods_context=airpods_ctx)
    nano_glass = entry_mod.extract_nano_glass(raw)

    return {
        "storage": storage or "",
        "ram": ram or "",
        "color": color or "",
        "region": region or "",
        "sim": sim or "",
        "code": code or "",
        "band_type": band_type or "",
        "band_color": band_color or "",
        "band_size": band_size or "",
        "watch_size_mm": watch_size_mm or "",
        "screen_size": screen_size or "",
        "connectivity": connectivity or "",
        "chip": chip or "",
        "year": year or "",
        "anc": anc or "",
        "case": case_type or "",
        "nano_glass": bool(nano_glass),
    }


def _google_line_to_etalon_or_stub(
    cleaned: str,
    *,
    model_index: Dict[str, Dict[str, Any]],
    code_index: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Всегда возвращает dict для строки Google:
      - если сматчилась модель в индексах -> полноценный etalon (category/brand/series/model/path)
      - если не сматчилась -> stub с пустыми category/brand/series/model, но params будут (best-effort)
    """
    raw = (cleaned or "").strip()
    if not raw:
        return {
            "etalon_matched": False,
            "category": "", "brand": "", "series": "", "model": "",
            "path": ["", "", "", ""],
            "raw": "",
            "raw_etalon": "",
            **_extract_params_best_effort(""),
        }

    meta = entry_mod.match_model_from_text(raw, model_index)
    if not meta and code_index:
        code = (entry_mod.extract_code(raw) or "").strip().upper()
        if code:
            meta = code_index.get(code)

    if meta:
        path = meta.get("path") or ["", "", "", ""]
        cat_s, br_s, sr_s, model_s = (path + ["", "", "", ""])[:4]
        cat_s, br_s, sr_s, model_s = str(cat_s).strip(), str(br_s).strip(), str(sr_s).strip(), str(model_s).strip()
        params = _extract_params_best_effort(raw, cat=cat_s, brand=br_s, series=sr_s, model=model_s)
        return {
            "etalon_matched": True,
            "category": cat_s,
            "brand": br_s,
            "series": sr_s,
            "model": model_s,
            "path": [cat_s, br_s, sr_s, model_s],
            "raw": raw,
            "raw_etalon": raw,
            **params,
        }

    # stub
    params = _extract_params_best_effort(raw)
    return {
        "etalon_matched": False,
        "category": "",
        "brand": "",
        "series": "",
        "model": "",
        "path": ["", "", "", ""],
        "raw": raw,
        "raw_etalon": raw,
        **params,
    }


def _build_google_rows(
    ws,
    log_file: Path,
) -> Tuple[List[Optional[Dict[str, Any]]], List[int], int, int]:
    """
    Читаем G со 2 строки.
    Возвращаем:
      rows[] длины rows_total, где элемент либо dict по строке, либо None если строка пустая,
      empty_rows[], start_row, rows_total
    """
    model_index, code_index = _ensure_entry_indexes(log_file)

    colG_vals = ws.col_values(7)  # G
    start_row = 2
    if len(colG_vals) < start_row:
        return [], [], start_row, 0

    g_values = colG_vals[start_row - 1:]
    rows_total = len(g_values)

    rows: List[Optional[Dict[str, Any]]] = [None] * rows_total
    empty_rows: List[int] = []

    for offset, raw_name in enumerate(g_values):
        row_idx = start_row + offset
        raw_name = (raw_name or "").strip()
        if not raw_name:
            empty_rows.append(row_idx)
            rows[offset] = None
            continue

        cleaned = clean_google_name_for_entry(raw_name)

        try:
            et_or_stub = _google_line_to_etalon_or_stub(
                cleaned,
                model_index=model_index,
                code_index=code_index,
            )
        except Exception as e:
            _write_log(log_file, f"[google_row] row={row_idx} error: {e} | text='{cleaned}'")
            et_or_stub = {
                "etalon_matched": False,
                "category": "", "brand": "", "series": "", "model": "",
                "path": ["", "", "", ""],
                "raw": cleaned,
                "raw_etalon": cleaned,
                **_extract_params_best_effort(cleaned),
            }

        et_or_stub["google_row"] = row_idx
        et_or_stub["google_name"] = raw_name
        et_or_stub["google_clean"] = cleaned

        rows[offset] = et_or_stub

    _write_log(log_file, f"[google_rows] rows_total={rows_total} non_empty={rows_total - len(empty_rows)}")
    return rows, empty_rows, start_row, rows_total


# ======================================================================
# MATCH: GOODS -> GOOGLE ETALONS (Google is etalon)
# ======================================================================

def _match_goods_to_google_rows(
    goods_list: List[Dict[str, Any]],
    google_rows: List[Optional[Dict[str, Any]]],
    log_file: Path,
) -> Dict[int, Dict[str, Any]]:
    """
    Возвращает агрегатор по google_row:
      {
        row: {
          "prices": [...],
          "min_price": float,
          "best_channel": str,
          "raw_best": str,
          "raw_lines": [...],
          "raw_channels": [...],
        }
      }
    """
    agg: Dict[int, Dict[str, Any]] = {}
    if not goods_list or not google_rows:
        return agg

    # индекс по category (ускорение)
    et_by_cat: Dict[str, List[Dict[str, Any]]] = {}
    for et in google_rows:
        if not et:
            continue
        cat = _norm_str(et.get("category"))
        et_by_cat.setdefault(cat, []).append(et)

    for good in goods_list:
        price, channel, raw_line = _extract_price_channel_from_good(good)
        if price is None:
            continue

        parsed_item = _build_parsed_item_from_good(good)

        good_cat = _norm_str(parsed_item.get("category"))
        candidates = et_by_cat.get(good_cat) if (good_cat and good_cat in et_by_cat) else [
            e for e in google_rows if e
        ]

        good_model = _norm_str(parsed_item.get("model") or parsed_item.get("display_model"))
        good_storage = _norm_str(parsed_item.get("storage"))
        for et in candidates:
            # если у строки google нет model/path (stub) — не делаем жёстких отсечений по model
            et_model = _norm_str(et.get("model") or et.get("display_model"))
            if et_model and good_model and et_model != good_model:
                continue

            et_storage = _norm_str(et.get("storage"))
            if et_storage and good_storage and et_storage != good_storage:
                continue

            try:
                ok, _reason = matcher.match_product(et, parsed_item)
            except Exception as e:
                _write_log(log_file, f"[match] matcher error: {e}")
                continue

            if not ok:
                continue

            row = int(et.get("google_row") or 0)
            if not row:
                continue

            st = agg.get(row)
            if st is None:
                st = {
                    "prices": [],
                    "min_price": float("inf"),
                    "best_channel": "",
                    "raw_best": "",
                    "raw_lines": [],
                    "raw_channels": [],
                }
                agg[row] = st

            entry = {
                "price": float(price),
                "channel": channel,
                "raw": raw_line,
                "source_channel": good.get("channel"),
                "source_message_id": good.get("message_id"),
            }
            st["prices"].append(entry)

            if raw_line:
                st["raw_lines"].append(raw_line)
            if channel:
                st["raw_channels"].append(channel)

            if float(price) < float(st["min_price"]):
                st["min_price"] = float(price)
                st["best_channel"] = channel or ""
                st["raw_best"] = raw_line or ""

    return agg


# ======================================================================
# MAIN UPDATE
# ======================================================================

async def update_prices_in_gsheet() -> int:
    """
    Google (G) = эталон/витрина.
    parsed_goods = кандидаты с ценами.
    Результат пишем в BB/BC/BD и сохраняем google_parsed.json (витрина/обучение).
    """
    def _work() -> int:
        log_file = _make_log_file()
        _write_log(log_file, f"=== Google Sheet update started: {datetime.now().isoformat()} ===")

        goods_list = _load_goods_list(log_file=log_file)
        if not goods_list:
            msg = (
                "parsed_goods пуст или не найден. "
                "Сначала запусти pipeline entry.py, "
                "чтобы сформировать handlers/parsing/data/parsed_goods.json."
            )
            _write_log(log_file, msg)
            raise RuntimeError(msg)

        gc = _authorize_gspread()
        ws = _open_worksheet(gc)

        google_rows, empty_google_rows, start_row, rows_total = _build_google_rows(ws, log_file)

        if rows_total <= 0:
            _write_log(log_file, "[gsheets_sync] Нет данных в колонке G.")
            return 0

        agg = _match_goods_to_google_rows(goods_list, google_rows, log_file)

        # готовим колонки BB/BC/BD (на весь диапазон G2:G...)
        matched_raw_col: List[str] = [""] * rows_total
        price_col: List[Any] = [""] * rows_total
        channel_col: List[str] = [""] * rows_total

        items_for_json: List[Dict[str, Any]] = []
        rows_with_prices = 0
        rows_non_empty = 0
        rows_etalon_matched = 0

        for i in range(rows_total):
            row_idx = start_row + i
            row_obj = google_rows[i]

            if row_obj is None:
                # пустая строка в G
                items_for_json.append({
                    "google_row": row_idx,
                    "google_name": "",
                    "google_clean": "",
                    "etalon_matched": False,
                    "path": ["", "", "", ""],
                    "raw_etalon": "",
                    "params": {},
                    "matched": False,
                    "min_price": None,
                    "best_channel": "",
                    "matched_raw": "",
                    "prices": [],
                    "raw_lines": [],
                    "raw_channels": [],
                })
                continue

            rows_non_empty += 1
            if bool(row_obj.get("etalon_matched")):
                rows_etalon_matched += 1

            st = agg.get(row_idx)
            matched = bool(st and st.get("prices"))

            min_price: Optional[float] = None
            best_channel = ""
            raw_best = ""

            if matched:
                min_price = float(st["min_price"])
                best_channel = st.get("best_channel") or ""
                raw_best = st.get("raw_best") or ""
                matched_raw_col[i] = raw_best
                price_col[i] = min_price
                channel_col[i] = best_channel
                rows_with_prices += 1

            # ✅ params кладём отдельно (для обучения)
            params = {
                "storage": row_obj.get("storage", "") or "",
                "ram": row_obj.get("ram", "") or "",
                "color": row_obj.get("color", "") or "",
                "region": row_obj.get("region", "") or "",
                "sim": row_obj.get("sim", "") or "",
                "code": row_obj.get("code", "") or "",
                "band_type": row_obj.get("band_type", "") or "",
                "band_color": row_obj.get("band_color", "") or "",
                "band_size": row_obj.get("band_size", "") or "",
                "screen_size": row_obj.get("screen_size", "") or "",
                "connectivity": row_obj.get("connectivity", "") or "",
                "chip": row_obj.get("chip", "") or "",
                "year": row_obj.get("year", "") or "",
                "anc": row_obj.get("anc", "") or "",
                "case": row_obj.get("case", "") or "",
            }

            items_for_json.append({
                "google_row": row_idx,
                "google_name": row_obj.get("google_name", "") or "",
                "google_clean": row_obj.get("google_clean", "") or "",
                "etalon_matched": bool(row_obj.get("etalon_matched")),
                "path": row_obj.get("path") or ["", "", "", ""],
                "category": row_obj.get("category", "") or "",
                "brand": row_obj.get("brand", "") or "",
                "series": row_obj.get("series", "") or "",
                "model": row_obj.get("model", "") or "",
                "raw_etalon": row_obj.get("raw_etalon") or row_obj.get("google_clean") or "",
                "params": params,

                # match result
                "matched": matched,
                "min_price": min_price,
                "best_channel": best_channel,
                "matched_raw": raw_best,
                "prices": (st.get("prices") if matched else []) if st else [],
                "raw_lines": (st.get("raw_lines") if matched else []) if st else [],
                "raw_channels": (st.get("raw_channels") if matched else []) if st else [],
            })

        payload = {
            "timestamp": datetime.now().isoformat(),
            "sheet_id": SHEET_ID,
            "worksheet_gid": WORKSHEET_GID,
            "rows_total": rows_total,
            "rows_empty": len(empty_google_rows),
            "rows_non_empty": rows_non_empty,
            "rows_etalon_matched": rows_etalon_matched,
            "rows_with_prices": rows_with_prices,
            "items": items_for_json,
            "empty_rows": sorted(set(empty_google_rows)),
        }

        try:
            GOOGLE_PARSED_FILE.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            _write_log(log_file, f"[gsheets_sync] saved {GOOGLE_PARSED_FILE} items={len(items_for_json)}")
        except Exception as e:
            _write_log(log_file, f"[gsheets_sync] write google_parsed.json error: {e}")

        # ---- update BB:BD in chunks (всегда пишем; если нет цены — будут пустые ячейки)
        values_block: List[List[Any]] = [
            [matched_raw_col[i], price_col[i], channel_col[i]] for i in range(rows_total)
        ]

        CHUNK_ROWS = 400
        end_row = start_row + rows_total - 1

        for i in range(0, rows_total, CHUNK_ROWS):
            chunk_values = values_block[i:i + CHUNK_ROWS]
            if not chunk_values:
                continue
            chunk_start_row = start_row + i
            chunk_end_row = chunk_start_row + len(chunk_values) - 1
            chunk_range = f"{COL_MATCHED_RAW}{chunk_start_row}:{COL_CHANNEL}{chunk_end_row}"

            try:
                ws.update(chunk_range, chunk_values, value_input_option="USER_ENTERED")
            except Exception as e:
                _write_log(log_file, f"[gsheets_sync] ws.update error range={chunk_range}: {e}")
                raise

        summary = (
            f"[gsheets_sync] Google rows: {rows_total} ({start_row}-{end_row}). "
            f"Non-empty: {rows_non_empty}. Etalon-matched: {rows_etalon_matched}. "
            f"With prices: {rows_with_prices}. Items saved: {len(items_for_json)}."
        )
        print(summary)
        _write_log(log_file, summary)
        _write_log(log_file, f"=== Google Sheet update finished: {datetime.now().isoformat()} ===")

        return rows_with_prices

    return await asyncio.to_thread(_work)


# ======================================================================
# SCHEDULER
# ======================================================================

async def schedule_gsheet_updates() -> None:
    """
    Запуски: каждый час с 11:00 до 19:00 (МСК)
    """
    while True:
        now_utc = datetime.now(timezone.utc)
        now_msk = now_utc.astimezone(MOSCOW_TZ)

        if now_msk.hour < 11:
            next_run = now_msk.replace(hour=11, minute=0, second=0, microsecond=0)
        elif 11 <= now_msk.hour < 19:
            next_run = now_msk.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        else:
            next_day = (now_msk + timedelta(days=1)).date()
            next_run = datetime(
                year=next_day.year,
                month=next_day.month,
                day=next_day.day,
                hour=11,
                minute=0,
                second=0,
                microsecond=0,
                tzinfo=MOSCOW_TZ,
            )

        delay = (next_run - now_msk).total_seconds()
        if delay < 0:
            delay = 0

        await asyncio.sleep(delay)

        try:
            await update_prices_in_gsheet()
        except Exception:
            # Ошибка не должна останавливать вечный цикл
            continue
