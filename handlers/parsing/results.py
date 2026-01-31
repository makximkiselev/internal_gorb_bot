# handlers/parsing/results.py
# Строит финальный каталог с ценами:
# storage.data["etalon"] (структура) + parsed_matched.json (цены) -> parsed_data.json

from __future__ import annotations

import json
from pathlib import Path
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple, Optional

# ✅ чтобы main.py мог сделать dp.include_router(results.router)
try:
    from aiogram import Router
except Exception:  # pragma: no cover - fallback for parsing env without aiogram
    class Router:  # type: ignore
        def __init__(self, *args, **kwargs):
            pass

from storage import load_data
from handlers.normalizers.entry import extract_region


# =========================
# AIROGRAM ROUTER (stub)
# =========================
# ВАЖНО: этот модуль не про UI, но main.py ожидает router.
# Хендлеров тут можно не держать — достаточно объявить Router.
router = Router(name="parsing_results")


MODULE_DIR = Path(__file__).parent.resolve()
DATA_DIR = MODULE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

MATCHED_FILE = DATA_DIR / "parsed_matched.json"
PARSED_FILE = DATA_DIR / "parsed_data.json"


# -------------------------
# I/O
# -------------------------

def _read_json(path: Path, default):
    try:
        if not path.exists():
            return default
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _price_to_float(v) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def _norm_key(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace("\u00A0", " ")
    while "  " in s:
        s = s.replace("  ", " ")
    return s


def _strip_ram(s: str) -> str:
    """
    "12/256gb" -> "256gb" (для моделей, где RAM не указана в каталоге).
    """
    return re.sub(r"\b\d{1,2}\s*/\s*(\d{2,4}\s*(?:gb|tb))\b", r"\1", s)


# -------------------------
# Read sources
# -------------------------

def _get_catalog_and_etalon() -> Tuple[dict, dict]:
    db = load_data() or {}
    cat = db.get("catalog") or {}
    et = db.get("etalon") or {}
    cat = cat if isinstance(cat, dict) else {}
    et = et if isinstance(et, dict) else {}
    return cat, et


def _read_matched_items() -> List[dict]:
    data = _read_json(MATCHED_FILE, {})
    if isinstance(data, dict):
        items = data.get("items") or []
        return [x for x in items if isinstance(x, dict)]
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    return []


def _get_etalon_variant_list_for_model(etalon: Dict[str, Any], path_to_model: List[str]) -> Optional[List[str]]:
    cur: Any = etalon
    for p in path_to_model:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(p)
    if isinstance(cur, list):
        out: List[str] = []
        for x in cur:
            s = "" if x is None else str(x)
            out.append(s)
        return out
    return None


# -------------------------
# Detect "model leaf" in catalog
# -------------------------

def _is_variant_value(v: Any) -> bool:
    """
    Значение варианта (leaf-value) допустимо в 2 формах:
      1) {}                                   (цены нет)
      2) {"min_price": number|None, "best_channels": list[str]}  (цена есть/или None)
    """
    if not isinstance(v, dict):
        return False
    if not v:
        return True

    allowed = {"min_price", "best_channels", "region", "region_min"}
    if any(k not in allowed for k in v.keys()):
        return False

    mp = v.get("min_price", None)
    if mp is not None and _price_to_float(mp) is None:
        return False

    bch = v.get("best_channels", [])
    if bch is None:
        bch = []
    if not isinstance(bch, list):
        return False
    # элементы списка — строки (пустые отфильтруем при записи)
    for x in bch:
        if not isinstance(x, str):
            return False

    return True


def _is_model_leaf(node: Any) -> bool:
    """
    Лист модели = dict, где ключи — названия вариантов, а значения — variant_value.
    ВАЖНО: после сборки parsed_data.json значения уже НЕ пустые, поэтому
    детекция должна принимать оба вида: {} и {"min_price":..., "best_channels":[...]}.
    """
    if not isinstance(node, dict):
        return False
    if not node:
        return True
    return all(_is_variant_value(v) for v in node.values())


# -------------------------
# Build index from matched
# -------------------------

def _build_index(matched: List[dict]) -> Dict[Tuple[Tuple[str, ...], str], Dict[str, Any]]:
    """
    Ключ: (tuple(model_path), variant_title/raw_parsed) -> {min_price, best_channels}
    """
    idx: Dict[Tuple[Tuple[str, ...], str], Dict[str, Any]] = {}
    stripped_bucket: Dict[Tuple[Tuple[str, ...], str], List[Dict[str, Any]]] = {}

    def _regions_for_min_price(item: dict, min_price: Optional[float]) -> List[str]:
        out: List[str] = []
        if min_price is None:
            return out
        prices = item.get("prices")
        if isinstance(prices, list):
            for p in prices:
                if not isinstance(p, dict):
                    continue
                mp = _price_to_float(p.get("price"))
                if mp is None or mp != min_price:
                    continue
                raw = (p.get("raw") or "").strip()
                reg = (extract_region(raw) or "").strip().lower()
                if reg and reg not in out:
                    out.append(reg)
        if not out:
            params = item.get("params") or {}
            if isinstance(params, dict):
                reg = (params.get("region") or "").strip().lower()
                if reg:
                    out.append(reg)
        return out

    for it in matched:
        path = it.get("path") or []
        raw = (it.get("raw_parsed") or "").strip()

        if not isinstance(path, list) or not path:
            continue
        if not raw:
            continue  # без raw_parsed не сможем положить цену в вариант

        mp = _price_to_float(it.get("min_price"))

        # matcher гарантирует best_channel: list[str], но страхуемся
        bch = it.get("best_channel")
        if bch is None:
            bch = []
        if isinstance(bch, str):
            bch = [bch]
        if not isinstance(bch, list):
            bch = []
        params = it.get("params") or {}
        if not isinstance(params, dict):
            params = {}
        region = (params.get("region") or "").strip().lower()

        # нормализация каналов
        best_channels: List[str] = []
        seen = set()
        for x in bch:
            s = str(x or "").strip()
            if s and s not in seen:
                seen.add(s)
                best_channels.append(s)

        raw_norm = _norm_key(raw)
        if not raw_norm:
            continue

        regions = _regions_for_min_price(it, mp)
        info = {
            "min_price": mp,
            "best_channels": best_channels,
            "region_min": regions,
        }
        key = (tuple(path), raw_norm)
        existing = idx.get(key)
        if existing is None:
            idx[key] = info
        else:
            ex_mp = existing.get("min_price")
            if ex_mp is None or (mp is not None and mp < ex_mp):
                idx[key] = info
            elif mp is not None and ex_mp is not None and mp == ex_mp:
                ex_regions = existing.get("region_min") or []
                if not isinstance(ex_regions, list):
                    ex_regions = [str(ex_regions)]
                for r in regions:
                    if r not in ex_regions:
                        ex_regions.append(r)
                existing["region_min"] = ex_regions

        raw_stripped = _strip_ram(raw_norm)
        if raw_stripped != raw_norm:
            stripped_bucket.setdefault((tuple(path), raw_stripped), []).append(info)

    for key, items in stripped_bucket.items():
        if len(items) == 1 and key not in idx:
            idx[key] = items[0]

    return idx


# -------------------------
# Merge into catalog structure
# -------------------------

def _merge_catalog_with_prices(
    catalog_node: dict,
    idx: Dict[Tuple[Tuple[str, ...], str], Dict[str, Any]],
    cur_path: List[str],
    etalon_tree: Dict[str, Any],
) -> dict:
    """
    Возвращает новый узел каталога, где в листьях-вариантах:
      - {}  (нет цены)
      - {"min_price": ..., "best_channels": [...]}  (есть цена)
    """
    out: dict = {}

    for k, v in (catalog_node or {}).items():
        key = str(k)

        # v — словарь вариантов модели (leaf) или список вариантов из etalon
        if isinstance(v, list):
            model_path = tuple(cur_path + [key])
            variants_out: Dict[str, Any] = {}

            for variant_title in v:
                vt = str(variant_title).strip()
                if not vt:
                    continue
                vt_norm = _norm_key(vt)
                info = idx.get((model_path, vt_norm))
                if info is None:
                    vt_stripped = _strip_ram(vt_norm)
                    if vt_stripped != vt_norm:
                        info = idx.get((model_path, vt_stripped))

                if info and info.get("min_price") is not None:
                    variants_out[variant_title] = {
                        "min_price": info["min_price"],
                        "best_channels": info.get("best_channels") or [],
                        "region_min": info.get("region_min"),
                    }
                else:
                    variants_out[variant_title] = {}

            out[key] = variants_out
            continue

        if isinstance(v, dict) and _is_model_leaf(v):
            model_path = tuple(cur_path + [key])
            variants_out: Dict[str, Any] = {}

            variant_items = list(v.items())
            if not variant_items:
                et_list = _get_etalon_variant_list_for_model(etalon_tree, list(model_path))
                if et_list:
                    variant_items = [(x, {}) for x in et_list if str(x).strip()]

            for variant_title, vv in variant_items:
                vt = str(variant_title).strip()
                vt_norm = _norm_key(vt)
                info = idx.get((model_path, vt_norm))
                if info is None:
                    vt_stripped = _strip_ram(vt_norm)
                    if vt_stripped != vt_norm:
                        info = idx.get((model_path, vt_stripped))

                if info and info.get("min_price") is not None:
                    variants_out[variant_title] = {
                        "min_price": info["min_price"],
                        "best_channels": info.get("best_channels") or [],
                        "region_min": info.get("region_min"),
                    }
                else:
                    # сохраняем пустым (даже если vv уже был с ценой — rebuild всегда строит заново)
                    variants_out[variant_title] = {}

            out[key] = variants_out
            continue

        # обычная ветка дерева
        if isinstance(v, dict):
            out[key] = _merge_catalog_with_prices(v, idx, cur_path + [key], etalon_tree)
        else:
            out[key] = v

    return out


# -------------------------
# Public API
# -------------------------

def rebuild_parsed_data_all() -> dict:
    _catalog, etalon = _get_catalog_and_etalon()
    catalog = etalon
    matched = _read_matched_items()
    idx = _build_index(matched)

    catalog_with_prices = _merge_catalog_with_prices(catalog, idx, [], etalon)

    payload = {
        "timestamp": _utcnow_iso(),
        "catalog": catalog_with_prices,
        "stats": {
            "matched_items": len(matched),
            "priced_variants": sum(1 for v in idx.values() if v.get("min_price") is not None),
        },
    }

    _write_json(PARSED_FILE, payload)
    return payload
