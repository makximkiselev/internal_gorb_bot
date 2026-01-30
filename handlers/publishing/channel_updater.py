# handlers/publishing/channel_updater.py
from __future__ import annotations

from typing import Union, Dict, Optional, Tuple, List, Any

import asyncio
import hashlib
import json
import re
from pathlib import Path
from datetime import datetime, timezone, timedelta

from telethon import utils
from telethon.client.telegramclient import TelegramClient
from telethon.tl.types import Message
from telethon.errors import FloodWaitError

from aiogram import Bot as AiogramBot
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup

from storage import load_data, save_data
from handlers.publishing.storage import (
    load_channel_posts,
    save_channel_posts,
    load_channel_menu_state,
    save_channel_menu_state,
    load_status_extra,
)


# ====================== Настройки темпа публикаций (мягкий rate-limit) ======================
THROTTLE_SECS = 1.4
GROUP_PAUSE_SECS = 3.5

# Сколько последних сообщений считаем "живыми" без доп. проверки
RECENT_MESSAGES_LIMIT = 2000
# Размер батча для проверки кэша по ids
VERIFY_CACHE_CHUNK = 100

# Лимиты для склейки моделей (больше не используются — 1 сообщение = 1 модель)
SHORT_MODEL_LINES = 20   # legacy
MAX_GROUP_LINES = 40     # legacy

# ВЕРБОЗНЫЙ ЛОГИНГ
VERBOSE_MENU_LOG = True

# Фраза статусного сообщения
STATUS_TITLE = "Цены и наличие обновлены."

# Московское время
MOSCOW_TZ = timezone(timedelta(hours=3))

# Признаки управляемых меню (посты с этими заголовками мы удаляем и пересоздаём)
MANAGED_TITLES_PREFIXES = ("📱 Модели ", "🏷️ Бренды в ", "🧭 Навигация по категориям:")

# project root
ROOT = Path(__file__).resolve().parents[2]

# publish paths cfg
PUBLISH_PATHS_FILE = Path(__file__).resolve().parent / "channel_publish_paths.json"

# cover images cfg
COVER_IMAGES_FILE = Path(__file__).resolve().parent / "channel_cover_images.json"

# ====================== RETAIL: всегда MEDIA (placeholder если нет обложки) ======================
RETAIL_ALWAYS_MEDIA = True
DEFAULT_PLACEHOLDER_REL = "covers/_placeholder.jpg"  # лежит рядом: handlers/publishing/covers/_placeholder.jpg


def _log(msg: str) -> None:
    print(f"[channel_updater] {msg}")


async def _throttle(base: float = THROTTLE_SECS) -> None:
    await asyncio.sleep(base)


# --------------------------- Вспомогательные утилиты ---------------------------

def _first_line(s: str) -> str:
    s = (s or "").strip()
    return s.split("\n", 1)[0].strip() if s else ""


def _ensure_dict(d: dict, key: str, default):
    if key not in d:
        d[key] = default
    return d[key]


def _strip_markup_title(s: str) -> str:
    s = (s or "").strip()
    if s.startswith("<b>") and s.endswith("</b>"):
        return s[3:-4].strip()
    return s


def _chunked(seq: list[int], size: int) -> list[list[int]]:
    return [seq[i:i + size] for i in range(0, len(seq), size)]


async def _prune_missing_messages(
    client: TelegramClient,
    entity,
    *,
    existing_index: dict[str, dict],
    recent_ids: set[str],
) -> int:
    """
    Удаляем из кэша ТОЛЬКО реально удалённые сообщения.
    recent_ids — ids, которые мы точно видели в свежей выборке.
    """
    if not existing_index:
        return 0

    to_check = [int(mid) for mid in existing_index.keys() if mid not in recent_ids]
    if not to_check:
        return 0

    removed = 0
    for chunk in _chunked(to_check, VERIFY_CACHE_CHUNK):
        try:
            msgs = await client.get_messages(entity, ids=chunk)
        except Exception as ex:
            _log(f"[prune_cache] Ошибка проверки ids: {ex}")
            continue

        if not isinstance(msgs, list):
            msgs = [msgs]

        for mid, msg in zip(chunk, msgs):
            if not msg:
                existing_index.pop(str(mid), None)
                removed += 1

    return removed


def _read_json(paths: Tuple[Path, ...]) -> tuple[dict, Optional[str]]:
    for p in paths:
        try:
            if p.exists():
                with p.open("r", encoding="utf-8") as f:
                    data = json.load(f)
                return data, str(p.resolve())
        except Exception:
            pass
    return {}, None


def _load_parsed_data() -> tuple[dict, Optional[str]]:
    """
    parsed_data.json — источник цен (catalog) и channel_pricing.
    """
    here = Path(__file__).resolve()
    candidates = tuple(dict.fromkeys([
        Path("parsed_data.json"),
        Path("./parsed_data.json"),
        Path("./data/parsed_data.json"),
        Path("/mnt/data/parsed_data.json"),
        Path("../parsed_data.json"),
        # варианты внутри проекта
        here.parents[2] / "handlers" / "parsing" / "parsed_data.json",
        here.parents[2] / "handlers" / "parsing" / "data" / "parsed_data.json",
        here.parents[1] / "parsing" / "parsed_data.json",
        here.parents[1] / "parsing" / "data" / "parsed_data.json",
    ]))
    data, src = _read_json(candidates)
    if src:
        _log(f"parsed_data.json source: {src}")
    return data, src


def _debug_parsed_shape(parsed: dict) -> None:
    try:
        keys = list(parsed.keys())[:60]
        _log(f"parsed_data top-level keys: {keys}")
        for k in ("etalon", "catalog", "channel_pricing", "data", "parsed", "payload", "result", "stats"):
            if k in parsed:
                _log(f"parsed_data['{k}'] type: {type(parsed.get(k)).__name__}")
    except Exception:
        pass


def _extract_prices_catalog_from_parsed(parsed: dict) -> Optional[dict]:
    """
    ✅ Цены/лучшие каналы — parsed_data.json["catalog"].
    """
    if not isinstance(parsed, dict):
        return None

    cat = parsed.get("catalog")
    if isinstance(cat, dict) and cat:
        return cat

    for container_key in ("data", "parsed", "payload", "result"):
        c = parsed.get(container_key)
        if isinstance(c, dict):
            cat2 = c.get("catalog")
            if isinstance(cat2, dict) and cat2:
                return cat2

    return None


def _looks_like_models_map(d: dict) -> bool:
    """
    Эвристика: если хотя бы у одного значения есть признаки "узла модели",
    считаем, что d это map model_name -> model_node.
    """
    if not isinstance(d, dict) or not d:
        return False

    sample_vals = list(d.values())[:5]
    for v in sample_vals:
        if isinstance(v, dict):
            if any(k in v for k in ("variants", "items", "rows", "lines", "min_price", "price", "prices", "best_channels")):
                return True
        elif isinstance(v, list):
            return True
    return False


def _count_models_in_catalog(catalog: dict) -> int:
    if not isinstance(catalog, dict):
        return 0
    cnt = 0
    for _cat, brands in catalog.items():
        if not isinstance(brands, dict):
            continue
        for _br, block in brands.items():
            if not isinstance(block, dict):
                continue
            if _looks_like_models_map(block):
                cnt += len(block)
            else:
                for _series, models in block.items():
                    if isinstance(models, dict):
                        cnt += len(models)
    return cnt


def _is_retail_mode(channel_pricing: Union[str, dict], channel_mode_fallback: str = "opt") -> bool:
    """
    retail = любой режим, НЕ opt.
    """
    mode = channel_mode_fallback
    if isinstance(channel_pricing, dict):
        mode = (channel_pricing.get("mode") or channel_mode_fallback or "opt").lower()
    elif isinstance(channel_pricing, str):
        mode = channel_pricing.strip().lower() or channel_mode_fallback
    return mode != "opt"


# --------------------------- cover images cfg ---------------------------

def _load_cover_images_cfg() -> dict:
    try:
        if COVER_IMAGES_FILE.exists():
            with COVER_IMAGES_FILE.open("r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                return data
    except Exception as e:
        _log(f"⚠️ Failed to read cover_images cfg: {e}")
    return {}


def _resolve_rel_or_abs_path(rel_or_abs: str) -> Path:
    p = Path(str(rel_or_abs))
    if p.is_absolute():
        return p

    publishing_dir = Path(__file__).resolve().parent  # .../handlers/publishing
    candidates = [
        publishing_dir / p,                    # handlers/publishing/<...>
        publishing_dir / "covers" / p,          # handlers/publishing/covers/<...> если указали без "covers/"
        ROOT / p,                               # fallback
        ROOT / "handlers" / "publishing" / p,   # fallback
    ]
    return next((c for c in candidates if c.exists()), candidates[0])


def _resolve_placeholder_cover(cover_cfg: dict, peer_id_short: str) -> Optional[Path]:
    """
    Плейсхолдер можно переопределить в channel_cover_images.json:
    {
      "3021670017": {
        "placeholder": "covers/3021670017/_placeholder.jpg"
      }
    }
    Иначе берём DEFAULT_PLACEHOLDER_REL.
    """
    rel = DEFAULT_PLACEHOLDER_REL
    try:
        peer_block = (cover_cfg or {}).get(peer_id_short)
        if isinstance(peer_block, dict):
            rel2 = peer_block.get("placeholder")
            if rel2:
                rel = str(rel2)
    except Exception:
        pass

    p = _resolve_rel_or_abs_path(rel)
    if p.exists():
        return p

    _log(f"⚠️ PLACEHOLDER cover not found on disk: {p}")
    return None


def _resolve_model_cover(
    cover_cfg: dict,
    peer_id_short: str,
    model_path: List[str],
) -> Optional[Path]:
    """
    Ищем картинку по точному пути:
      "Категория|Бренд|Серия|Модель"  или "Категория|Бренд|Модель"

    channel_cover_images.json:
    {
      "3021670017": {
        "by_path": {
          "Смартфоны|Apple|iPhone 17|iPhone 17 Pro Max": "covers/3021670017/....jpg"
        }
      }
    }
    """
    if not cover_cfg or not peer_id_short or not model_path:
        return None

    peer_block = cover_cfg.get(peer_id_short)
    if not isinstance(peer_block, dict):
        return None

    by_path = peer_block.get("by_path") or {}
    if not isinstance(by_path, dict):
        return None

    key = "|".join(model_path)
    rel = by_path.get(key)
    if not rel:
        return None

    p = _resolve_rel_or_abs_path(str(rel))

    if p.exists():
        return p

    _log(f"⚠️ COVER path not found on disk: {p}")
    return None


# --------------------------- publish paths cfg ---------------------------

def _load_publish_paths_cfg() -> dict:
    try:
        if PUBLISH_PATHS_FILE.exists():
            with PUBLISH_PATHS_FILE.open("r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                return data
    except Exception as e:
        _log(f"⚠️ Failed to read publish_paths cfg: {e}")
    return {}


def _parse_publish_spec(raw: list) -> list[list[str]]:
    out: list[list[str]] = []
    if not isinstance(raw, list):
        return out
    for item in raw:
        s = str(item).strip()
        if not s:
            continue
        parts = [p for p in s.split("|") if p]
        if parts:
            out.append(parts)
    return out


def _load_publish_spec_for_peer(peer_id: str, peer_id_short: str) -> list[list[str]]:
    cfg = _load_publish_paths_cfg()
    raw = cfg.get(peer_id_short) or cfg.get(peer_id) or []
    spec = _parse_publish_spec(raw)
    if spec:
        _log(f"Publish spec loaded for channel {peer_id_short}: {len(spec)} paths")
    else:
        _log(f"Publish spec for channel {peer_id_short}: <none> (публикуем всё)")
    return spec


def _model_path_matches_any(model_path: List[str], spec: list[list[str]]) -> bool:
    if not spec or not model_path:
        return True
    for p in spec:
        n = min(len(p), len(model_path))
        if p[:n] == model_path[:n]:
            return True
    return False


def _apply_publish_spec_filter(
    cat_list: list[str],
    cat_brands_order: dict[str, list[str]],
    brand_models: dict[tuple[str, str], list[str]],
    prices_tree: dict,
    spec: list[list[str]],
) -> tuple[list[str], dict[str, list[str]], dict[tuple[str, str], list[str]]]:
    """
    Фильтруем (cat_list, cat_brands_order, brand_models) по spec.
    model_path проверяем ПО prices_tree (parsed_data.json), потому что именно там реальная структура.
    model_path может быть:
      [cat, brand, model] или [cat, brand, series, model]
    """
    new_cat_list: list[str] = []
    new_cat_brands: dict[str, list[str]] = {}
    new_brand_models: dict[tuple[str, str], list[str]] = {}

    for cat in cat_list:
        kept_brands: list[str] = []
        for br in (cat_brands_order.get(cat) or []):
            kept_models: list[str] = []
            for m in (brand_models.get((cat, br)) or []):
                mp = _find_first_model_path_in_catalog(prices_tree, cat, br, m)
                if not mp:
                    continue
                if _model_path_matches_any(mp, spec):
                    kept_models.append(m)

            if kept_models:
                kept_brands.append(br)
                new_brand_models[(cat, br)] = kept_models

        if kept_brands:
            new_cat_list.append(cat)
            new_cat_brands[cat] = kept_brands

    _log(
        f"Publish filter applied (prices-order): categories {len(cat_list)}→{len(new_cat_list)}, "
        f"brand_keys {len(brand_models)}→{len(new_brand_models)}"
    )
    return new_cat_list, new_cat_brands, new_brand_models


# --------------------------- order from PRICES tree (parsed_data.json) ---------------------------

def _order_from_prices_catalog(prices_catalog: dict) -> tuple[list[str], dict[str, list[str]], dict[tuple[str, str], list[str]]]:
    """
    Порядок публикации берём из parsed_data.json["catalog"]:
      prices[category][brand][model] = model_node
    или
      prices[category][brand][series][model] = model_node

    Важно:
    - сохраняем insertion-order dict'ов как есть;
    - если модель встречается внутри серии — добавляем в порядке обхода серий;
    - если повтор — игнорируем (берём первое появление).
    """
    cat_list: list[str] = []
    cat_brands_order: dict[str, list[str]] = {}
    brand_models: dict[tuple[str, str], list[str]] = {}

    if not isinstance(prices_catalog, dict):
        return cat_list, cat_brands_order, brand_models

    for cat, brands in prices_catalog.items():
        if not isinstance(brands, dict):
            continue
        cat = str(cat)
        cat_list.append(cat)

        br_list: list[str] = []
        for br, maybe_series_or_models in brands.items():
            if not isinstance(maybe_series_or_models, dict):
                continue
            br = str(br)
            br_list.append(br)

            models_order: list[str] = []
            seen: set[str] = set()

            # A: cat/brand/model
            if _looks_like_models_map(maybe_series_or_models):
                for model_name in maybe_series_or_models.keys():
                    mn = str(model_name)
                    if mn not in seen:
                        seen.add(mn)
                        models_order.append(mn)
            else:
                # B: cat/brand/series/model
                for _series_name, models in maybe_series_or_models.items():
                    if not isinstance(models, dict):
                        continue
                    for model_name in models.keys():
                        mn = str(model_name)
                        if mn not in seen:
                            seen.add(mn)
                            models_order.append(mn)

            brand_models[(cat, br)] = models_order

        cat_brands_order[cat] = br_list

    return cat_list, cat_brands_order, brand_models


def _is_series_container(node: Any) -> bool:
    """
    Серия: dict, где значения преимущественно dict (подмодели),
    и при этом это НЕ leaf с ценой/вариантами.
    """
    if not isinstance(node, dict) or not node:
        return False

    # если на верхнем уровне есть признаки "модельного" leaf — это НЕ серия
    if any(k in node for k in ("min_price", "price", "best_channels", "variants", "items", "rows", "lines")):
        return False

    vals = list(node.values())[:10]
    dict_vals = sum(1 for v in vals if isinstance(v, dict))
    list_vals = sum(1 for v in vals if isinstance(v, list))

    # серия обычно почти целиком из dict'ов (подмоделей)
    return dict_vals >= max(1, (len(vals) * 6) // 10) and list_vals == 0


def _find_first_model_path_in_catalog(catalog: dict, cat: str, br: str, model: str) -> Optional[List[str]]:
    """
    Ищем путь модели в дереве catalog:
      A) cat/brand/model
      B) cat/brand/series/model
      C) спецкейс: cat/brand/series/model, где series == model (как в data.json: "iPhone 17": {"iPhone 17":[...]})
    """
    if not isinstance(catalog, dict):
        return None

    brands = catalog.get(cat)
    if not isinstance(brands, dict):
        return None

    block = brands.get(br)
    if not isinstance(block, dict):
        return None

    # 0) Спецкейс: модель есть как ключ верхнего уровня, но это серия-контейнер
    if model in block and isinstance(block.get(model), dict):
        maybe_series = block.get(model)

        # если внутри серии есть ключ модели (частый случай: series == model)
        if isinstance(maybe_series, dict) and model in maybe_series:
            # data.json: ...["iPhone 17"]["iPhone 17"] -> [list]
            return [cat, br, model, model]

        # если это серия (контейнер подмоделей)
        if _is_series_container(maybe_series):
            for m2 in maybe_series.keys():
                if str(m2) == model:
                    return [cat, br, model, model]
            # иначе продолжаем поиск ниже

    # 1) Формат A: cat/brand/model (но только если это НЕ серия)
    if model in block:
        node = block.get(model)
        if not _is_series_container(node):
            return [cat, br, model]

    # 2) Формат B: cat/brand/series/model
    for series_name, models in block.items():
        if not isinstance(models, dict):
            continue
        # спецкейс: series == model и внутри series есть dict{model: ...}
        if str(series_name) == model and model in models:
            return [cat, br, str(series_name), model]
        if model in models:
            return [cat, br, str(series_name), model]

    return None


# --------------------------- Канальные правила наценки (из parsed_data.json) ---------------------------

def _load_channel_pricing_config_from_parsed(parsed: dict) -> dict:
    """
    Берём только из parsed_data.json.
    Ожидаемый формат:
    {
      "channel_pricing": {
        "default": {"mode":"opt","pct":0.0,"flat":0.0},
        "by_peer_id": {"-100...": {...}},
        "by_username": {"mychannel": {...}}
      }
    }
    """
    if not isinstance(parsed, dict):
        return {}
    cfg = parsed.get("channel_pricing")
    if isinstance(cfg, dict):
        return cfg

    for k in ("data", "parsed", "payload", "result"):
        c = parsed.get(k)
        if isinstance(c, dict):
            cfg2 = c.get("channel_pricing")
            if isinstance(cfg2, dict):
                return cfg2

    return {}


def _resolve_channel_pricing(entity, cfg: dict, fallback_mode: Optional[str] = None) -> dict:
    peer_id = str(utils.get_peer_id(entity))
    username = (getattr(entity, "username", None) or "").lower()

    by_peer = (cfg.get("by_peer_id") or {})
    by_user = (cfg.get("by_username") or {})
    default = (cfg.get("default") or {})

    rule = {}
    if peer_id in by_peer:
        rule = by_peer[peer_id]
    elif username and username in by_user:
        rule = by_user[username]
    else:
        rule = default

    mode = (rule.get("mode") or default.get("mode") or fallback_mode or "opt").lower()
    pct = float(rule.get("pct") or 0.0)
    flat = float(rule.get("flat") or 0.0)
    return {"mode": mode, "pct": pct, "flat": flat}


# --------------------------- Наценка/форматирование ЦЕН ---------------------------

_PRICE_KEYS = ("min_price", "price", "min", "best_price", "best", "value", "amount", "rub", "rur")


def _fmt_price_int(n: int | float) -> str:
    try:
        return f"{int(round(float(n))):,}".replace(",", " ")
    except Exception:
        return str(n)


def _extract_price_any(v: Any) -> Optional[float]:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)

    if isinstance(v, str):
        s = v.strip()
        m = re.search(r"(\d[\d\s]{2,})", s)
        if m:
            try:
                return float(int(m.group(1).replace(" ", "")))
            except Exception:
                return None
        return None

    if isinstance(v, dict):
        for k in _PRICE_KEYS:
            if k in v and v[k] is not None:
                try:
                    return float(v[k])
                except Exception:
                    pass

        for ck in ("price", "prices", "best_offer", "offer", "value", "min", "best"):
            if ck in v:
                p = _extract_price_any(v.get(ck))
                if p is not None:
                    return p

    return None


def _apply_channel_markup(base: int | float, channel_pricing: Optional[Union[str, dict]]) -> int:
    """
    channel_pricing:
      - str: "opt" | "retail" (обратная совместимость)
      - dict: {"mode": "...", "pct": 0.03, "flat": 1000}
    """
    p = float(base)

    def _opt_step(x: float) -> float:
        if x < 20_000:
            return 300.0
        if x < 150_000:
            return 500.0
        if x < 250_000:
            return 1000.0
        return 2000.0

    # legacy
    if isinstance(channel_pricing, str):
        mode = channel_pricing.strip().lower()
        if mode == "opt":
            return int(round(p + _opt_step(p)))

        if p <= 10_000:
            inc = 1500
        elif p <= 25_000:
            inc = 2000
        elif p <= 70_000:
            inc = 3000
        elif p <= 100_000:
            inc = 4000
        elif p <= 200_000:
            inc = 5000
        else:
            inc = 20000
        return int(round(p + inc))

    rule = (channel_pricing or {})
    mode = (rule.get("mode") or "opt").lower()
    pct = float(rule.get("pct") or 0.0)
    flat = float(rule.get("flat") or 0.0)

    if mode != "absolute":
        if mode == "opt":
            p = p + _opt_step(p)
        else:
            if p <= 10_000:
                inc = 1500
            elif p <= 25_000:
                inc = 2000
            elif p <= 70_000:
                inc = 3000
            elif p <= 100_000:
                inc = 4000
            elif p <= 200_000:
                inc = 5000
            else:
                inc = 20000
            p = p + inc

    if pct:
        p = p * (1.0 + pct)
    if flat:
        p = p + flat

    return int(round(p))


# --------------------------- Template access / model leaf ---------------------------

def _get_node_by_path(root: dict, path: List[str]) -> Optional[Any]:
    cur: Any = root
    for key in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur


def _unwrap_same_key(node: Any, name: str, max_depth: int = 3) -> Any:
    """
    Бывает структура вида:
      model_node = {"iPhone 16": {...реальные данные...}}
    или даже несколько раз.
    Разворачиваем такие "оболочки".
    """
    cur = node
    for _ in range(max_depth):
        if isinstance(cur, dict) and name in cur and len(cur) == 1:
            cur = cur[name]
            continue
        break
    return cur


def _get_template_list(template_root: dict, model_path: Optional[List[str]]) -> Optional[List[str]]:
    """
    template leaf обычно list[str] (где "" = разделитель).
    Но иногда leaf завернут повторно ключом модели:
      ...["iPhone 16"] -> {"iPhone 16": [ ... ]}
    """
    if not model_path:
        return None

    leaf = _get_node_by_path(template_root or {}, model_path)
    if leaf is None:
        return None

    model_name = model_path[-1] if model_path else ""
    leaf = _unwrap_same_key(leaf, model_name)

    # иногда ещё и внутри лежит {"items":[...]} и т.п.
    if isinstance(leaf, dict):
        for ck in ("items", "variants", "rows", "lines"):
            v = leaf.get(ck)
            if isinstance(v, list):
                return [str(x) for x in v]

    if isinstance(leaf, list):
        return [str(x) for x in leaf]

    return None


def _get_model_leaf(catalog: dict, model_path: List[str]) -> Optional[Any]:
    """
    model_path:
      [cat, brand, model] OR [cat, brand, series, model]
    Возвращаем leaf модели с вариантами/ценами.

    ФИКС: если модельный leaf завернут повторно ключом модели — разворачиваем.
    ПЛЮС: если передали 3-уровневый путь, но в дереве есть серии — ищем модель по сериям.
    """
    if not model_path or len(model_path) < 3:
        return None

    cat = model_path[0]
    br = model_path[1]
    model = model_path[-1]

    brands = catalog.get(cat)
    if not isinstance(brands, dict):
        return None

    block = brands.get(br)
    if not isinstance(block, dict):
        return None

    # A: cat/brand/model
    if len(model_path) == 3:
        leaf = block.get(model)
        if leaf is not None:
            return _unwrap_same_key(leaf, model)

        # fallback: если в ценах есть series/*/model
        for _series_name, models in block.items():
            if isinstance(models, dict) and model in models:
                leaf2 = models.get(model)
                return _unwrap_same_key(leaf2, model)
        return None

    # B: cat/brand/series/model
    series = model_path[2]
    models = block.get(series)
    if isinstance(models, dict):
        leaf = models.get(model)
        leaf = _unwrap_same_key(leaf, model)
        if isinstance(leaf, dict) and series in leaf and len(leaf) == 1:
            leaf = leaf[series]
            leaf = _unwrap_same_key(leaf, model)
        if leaf is not None:
            return leaf

    # fallback: вдруг модель лежит напрямую
    if model in block:
        leaf3 = block.get(model)
        return _unwrap_same_key(leaf3, model)

    return None


def _choose_effective_template_list(price_leaf: dict, template_list: Optional[List[str]]) -> Optional[List[str]]:
    """
    ВАЖНО: разделители "" из data.json должны сохраняться всегда.
    Даже если ключи в price_leaf не совпали идеально — мы НЕ падаем в list(price_leaf.keys()),
    потому что тогда теряем "" и красивую структуру.
    """
    if not isinstance(price_leaf, dict) or not price_leaf:
        return template_list

    # нет шаблона вообще — тогда да, просто порядок из price_leaf
    if not template_list:
        return [str(k) for k in price_leaf.keys()]

    # 1) Берём шаблон как есть (он содержит "" разделители)
    out = [str(x) for x in template_list]

    # 2) Добавляем "лишние" ключи из price_leaf, которых нет в шаблоне,
    #    чтобы новые варианты не пропадали, но структура сохранялась.
    existing = {t.strip() for t in out if t.strip() != ""}
    extras: List[str] = []
    for k in price_leaf.keys():
        ks = str(k).strip()
        if not ks:
            continue
        if ks not in existing:
            extras.append(ks)

    if extras:
        # аккуратно отделяем добавленные хвосты пустой строкой
        if out and out[-1].strip() != "":
            out.append("")
        out.extend(extras)

    return out


def _render_model_body_from_prices_and_template(
    prices_tree: dict,
    prices_path: List[str],
    template_list: Optional[List[str]],
    channel_pricing: Optional[Union[str, dict]],
) -> Optional[str]:
    # режим
    if isinstance(channel_pricing, dict):
        mode = (channel_pricing.get("mode") or "opt").lower()
    elif isinstance(channel_pricing, str):
        mode = channel_pricing.strip().lower()
    else:
        mode = "opt"
    wholesale = (mode == "opt")

    price_leaf = _get_model_leaf(prices_tree or {}, prices_path)
    if not isinstance(price_leaf, dict) or not price_leaf:
        return None if wholesale else "—"

    template_list = _choose_effective_template_list(price_leaf, template_list)

    out: List[str] = []
    last_empty = False
    any_prices = False

    for raw in (template_list or []):
        t = (raw or "").strip()

        # разделитель
        if t == "":
            if out and not last_empty:
                out.append("")
                last_empty = True
            continue

        payload = price_leaf.get(t)
        price = _extract_price_any(payload)
        if price is None:
            if not wholesale:
                out.append(t)
                last_empty = False
            continue

        any_prices = True
        adj = _apply_channel_markup(price, channel_pricing)
        out.append(f"{t} - {_fmt_price_int(adj)}")
        last_empty = False

    while out and out[-1] == "":
        out.pop()

    if wholesale and not any_prices:
        return None
    if not out:
        return None if wholesale else "—"
    return "\n".join(out)


# --------------------------- Brand reorder / reset logic ---------------------------

def _plan_brand_units(
    models: List[str],
    *,
    cat: str,
    br: str,
    prices_tree: dict,
    template_tree: dict,
    channel_pricing: Union[str, dict],
) -> List[List[str]]:
    """
    Возвращает список "юнитов" публикации: всегда 1 модель = 1 сообщение.
    """
    return [[m] for m in models]


def _unit_mid(unit: List[str], model_to_mid: Dict[str, str]) -> Optional[int]:
    """
    Возвращает mid юнита, если все модели юнита указывают на один mid.
    """
    mids: List[int] = []
    for m in unit:
        mid = model_to_mid.get(m)
        if not mid:
            return None
        try:
            mids.append(int(mid))
        except Exception:
            return None

    if len(set(mids)) != 1:
        return None
    return mids[0]


def _brand_needs_reset(units: List[List[str]], model_to_mid: Dict[str, str]) -> bool:
    """
    Проверяем, соответствует ли текущий порядок постов в канале нашему units-плану.

    Важно про порядок Telegram:
      - чем больше message_id, тем НОВЕЕ сообщение;
      - в истории канала новые сообщения оказываются НИЖЕ старых;
      - значит корректный "сверху-вниз" порядок для units == mid должны быть строго ВОЗРАСТАЮЩИМИ.
        (unit[0] — верхний/старый => самый маленький mid)
    """
    mids: List[int] = []
    for u in units:
        mid = _unit_mid(u, model_to_mid)
        if mid is None:
            return True
        mids.append(mid)

    # Должно быть строго возрастающе: mid0 < mid1 < mid2 < ...
    for i in range(1, len(mids)):
        if mids[i - 1] >= mids[i]:
            return True
    return False


def _brand_mid_range(models: List[str], model_to_mid: Dict[str, str]) -> Optional[Tuple[int, int]]:
    mids: List[int] = []
    for m in models:
        mid = model_to_mid.get(m)
        if not mid:
            continue
        try:
            mids.append(int(mid))
        except Exception:
            continue
    if not mids:
        return None
    return min(mids), max(mids)


def _find_section_reset_point(
    cat_list: List[str],
    cat_brands_order: Dict[str, List[str]],
    brand_models: Dict[Tuple[str, str], List[str]],
    model_to_mid: Dict[str, str],
) -> Optional[Tuple[str, str]]:
    """
    Находим первую секцию (cat/brand), после которой порядок нарушен
    или секция новая (нет ни одного опубликованного поста).
    Тогда нужно удалить всё после этой секции и перепубликовать.
    """
    last_max_mid: Optional[int] = None
    for cat in cat_list:
        brands = cat_brands_order.get(cat, []) or []
        for br in brands:
            models = brand_models.get((cat, br), []) or []
            rng = _brand_mid_range(models, model_to_mid)
            if rng is None:
                return cat, br

            min_mid, max_mid = rng
            if last_max_mid is not None and min_mid <= last_max_mid:
                return cat, br

            last_max_mid = max_mid
    return None


def _collect_models_from_section(
    cat_list: List[str],
    cat_brands_order: Dict[str, List[str]],
    brand_models: Dict[Tuple[str, str], List[str]],
    start_cat: str,
    start_brand: str,
) -> List[str]:
    out: List[str] = []
    started_cat = False
    started_brand = False
    for cat in cat_list:
        if not started_cat:
            if cat != start_cat:
                continue
            started_cat = True
        brands = cat_brands_order.get(cat, []) or []
        for br in brands:
            if started_cat and not started_brand and cat == start_cat:
                if br != start_brand:
                    continue
                started_brand = True
            out.extend(brand_models.get((cat, br), []) or [])
    return out


async def _reset_brand_posts(
    client,
    entity,
    *,
    existing_index: Dict[str, dict],
    model_to_mid: Dict[str, str],
    models: List[str],
) -> int:
    """
    Удаляет все сообщения (mid) которые связаны с models (у данного бренда),
    чистит existing_index и model_to_mid.
    """
    mids: List[int] = []
    for m in models:
        mid = model_to_mid.get(m)
        if mid:
            try:
                mids.append(int(mid))
            except Exception:
                pass

    mids = sorted(set(mids), reverse=True)
    if not mids:
        return 0

    _log(f"🧨 BRAND RESET: delete {len(mids)} model posts to rebuild order")
    await safe_delete(client, entity, mids)
    await _throttle()

    for mid in mids:
        existing_index.pop(str(mid), None)

    for m in models:
        model_to_mid.pop(m, None)

    return len(mids)


def _build_model_text(
    prices_tree: dict,
    template_tree: dict,
    model: str,
    prices_path: List[str],
    template_path: Optional[List[str]],
    channel_pricing: Optional[Union[str, dict]],
) -> Optional[str]:
    template_list = _get_template_list(template_tree, template_path)
    body = _render_model_body_from_prices_and_template(
        prices_tree=prices_tree,
        prices_path=prices_path,
        template_list=template_list,
        channel_pricing=channel_pricing,
    )
    if body is None:
        return None
    body = body or "—"
    return f"<b>{model}</b>\n\n{body}"


# --------------------------- FloodWait-safe wrappers -----------------------------------------

async def safe_send(client, entity, *args, **kwargs):
    while True:
        try:
            msg = await client.send_message(entity, *args, **kwargs)
            return msg
        except FloodWaitError as e:
            _log(f"⏳ FloodWait on send: {e.seconds}s")
            await asyncio.sleep(e.seconds + 1)
        except Exception as ex:
            _log(f"[safe_send] Ошибка: {ex}")
            return None


async def safe_send_file(client, entity, *, file: Path, caption: str):
    while True:
        try:
            msg = await client.send_file(
                entity,
                file=str(file),
                caption=caption,
                parse_mode="HTML",
            )
            return msg
        except FloodWaitError as e:
            _log(f"⏳ FloodWait on send_file: {e.seconds}s")
            await asyncio.sleep(e.seconds + 1)
        except Exception as ex:
            _log(f"[safe_send_file] Ошибка: {ex}")
            return None


async def safe_edit(client, entity, message_id: int, *args, **kwargs):
    while True:
        try:
            msg = await client.edit_message(entity, int(message_id), *args, **kwargs)
            return msg or True
        except FloodWaitError as e:
            _log(f"⏳ FloodWait on edit: {e.seconds}s")
            await asyncio.sleep(e.seconds + 1)
        except Exception as ex:
            s = str(ex).lower()
            if "not modified" in s or "message is not modified" in s:
                return True
            _log(f"[safe_edit] Ошибка: {ex}")
            return None


async def safe_edit_media(client, entity, message_id: int, *, file: Path, caption: str):
    """
    Пытаемся заменить медиа у существующего media-сообщения, сохраняя message_id.
    Если Telegram/Telethon не даст — вернём None, дальше будет fallback delete+recreate.
    """
    while True:
        try:
            msg = await client.edit_message(
                entity,
                int(message_id),
                caption,
                file=str(file),
                parse_mode="HTML",
            )
            return msg or True
        except FloodWaitError as e:
            _log(f"⏳ FloodWait on edit_media: {e.seconds}s")
            await asyncio.sleep(e.seconds + 1)
        except Exception as ex:
            s = str(ex).lower()
            if "not modified" in s or "message is not modified" in s:
                return True
            _log(f"[safe_edit_media] Ошибка: {ex}")
            return None


async def safe_delete(client, entity, ids):
    if not ids:
        return None
    if not isinstance(ids, (list, tuple, set)):
        ids = [ids]
    ids = [int(i) for i in ids]
    while True:
        try:
            return await client.delete_messages(entity, ids)
        except FloodWaitError as e:
            _log(f"⏳ FloodWait on delete: {e.seconds}s")
            await asyncio.sleep(e.seconds + 1)
        except Exception as ex:
            _log(f"[safe_delete] Ошибка: {ex}")
            return None


# --------------------------- Логирование меню --------------------------------------

def _log_menu_buttons(title: str, btns: List[InlineKeyboardButton]) -> None:
    if not VERBOSE_MENU_LOG:
        return
    _log(f"┌ MENU '{title}' buttons={len(btns)}")
    for i, b in enumerate(btns, 1):
        _log(f"│  [{i:02d}] '{b.text}' → {getattr(b, 'url', '—')}")
    _log("└ end of buttons")


def _buttons_fingerprint(btns: List[InlineKeyboardButton]) -> str:
    payload = "|".join(f"{b.text}→{getattr(b, 'url', '')}" for b in btns)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _same_menu(prev_text: str, prev_fp: str, new_text: str, new_fp: str) -> bool:
    return (prev_text or "") == (new_text or "") and (prev_fp or "") == (new_fp or "")


def _aiogram_markup(btns: List[InlineKeyboardButton]) -> InlineKeyboardMarkup:
    rows = [[b] for b in btns]
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _ensure_menu_message(
    client,
    entity,
    *,
    title: str,
    btns: List[InlineKeyboardButton],
    existing_index: dict[str, dict],
    old_mid: Optional[int],
    aio_bot: Optional[AiogramBot],
    chat_ref_for_bot: Union[str, int, None],
) -> Optional[int]:
    """
    Создать/обновить меню.
    Если есть bot_api — используем inline keyboard.
    Иначе — fallback: Telethon + HTML-ссылки в тексте.
    """
    if not btns:
        if old_mid and str(old_mid) in existing_index:
            _log(f"🗑 DELETE EMPTY MENU mid={old_mid}: {title}")
            await safe_delete(client, entity, int(old_mid))
            existing_index.pop(str(old_mid), None)
            await _throttle()
        else:
            _log(f"⏭ SKIP MENU (no buttons): {title}")
        return None

    _log_menu_buttons(title, btns)
    new_fp = _buttons_fingerprint(btns)
    html_title = f"<b>{title}</b>"

    use_bot_api = (aio_bot is not None and chat_ref_for_bot)
    if use_bot_api:
        kb = _aiogram_markup(btns)
        try:
            if old_mid and str(old_mid) in existing_index:
                prev = existing_index[str(old_mid)]
                prev_text = prev.get("text") or ""
                prev_fp = prev.get("kb_fp") or ""
                if _same_menu(prev_text, prev_fp, html_title, new_fp):
                    _log(f"↔️ SKIP MENU via Bot mid={old_mid}: no changes")
                    return int(old_mid)

                _log(f"✏️ EDIT MENU via Bot mid={old_mid}")
                await aio_bot.edit_message_text(
                    chat_id=chat_ref_for_bot,
                    message_id=int(old_mid),
                    text=html_title,
                    reply_markup=kb,
                    parse_mode="HTML",
                )
                await _throttle()

                prev["text"] = html_title
                prev["kb_fp"] = new_fp
                prev["date"] = datetime.now(timezone.utc).isoformat()
                _log(f"✅ MENU (bot) updated mid={old_mid}")
                return int(old_mid)

            _log(f"➕ CREATE MENU via Bot: '{_first_line(html_title)}' (buttons={len(btns)})")
            msg = await aio_bot.send_message(
                chat_id=chat_ref_for_bot,
                text=html_title,
                reply_markup=kb,
                parse_mode="HTML",
            )
            await _throttle()
            if msg:
                existing_index[str(msg.message_id)] = {
                    "text": html_title,
                    "kb_fp": new_fp,
                    "date": datetime.now(timezone.utc).isoformat(),
                }
                _log(f"✅ MENU (bot) created mid={msg.message_id}")
                return int(msg.message_id)

        except Exception as e:
            s = str(e).lower()
            if "message is not modified" in s:
                _log(f"↔️ MENU via Bot mid={old_mid}: already up-to-date")
                if old_mid and str(old_mid) in existing_index:
                    prev = existing_index[str(old_mid)]
                    prev["text"] = html_title
                    prev["kb_fp"] = new_fp
                    prev["date"] = datetime.now(timezone.utc).isoformat()
                return int(old_mid) if old_mid else None

            _log(f"⚠️ Bot menu failed, fallback to Telethon: {e}")

    # ---- Telethon fallback: HTML links
    links_block_lines: List[str] = []
    for b in btns:
        url = getattr(b, "url", None)
        if not url:
            continue
        links_block_lines.append(f"• <a href=\"{url}\">{b.text}</a>")
    links_block = "\n".join(links_block_lines)
    full_text = html_title + ("\n\n" + links_block if links_block else "")

    if old_mid and str(old_mid) in existing_index:
        prev = existing_index[str(old_mid)]
        prev_text = prev.get("text") or ""
        prev_fp = prev.get("kb_fp") or ""
        if _same_menu(prev_text, prev_fp, html_title, new_fp):
            _log(f"↔️ SKIP MENU mid={old_mid}: no changes")
            return int(old_mid)

        _log(f"✏️ EDIT MENU mid={old_mid}")
        ok = await safe_edit(client, entity, int(old_mid), full_text, parse_mode="HTML")
        await _throttle()
        if ok:
            prev["text"] = html_title
            prev["kb_fp"] = new_fp
            prev["date"] = datetime.now(timezone.utc).isoformat()
            _log(f"✅ MENU updated mid={old_mid}")
            return int(old_mid)

        _log(f"❌ MENU edit failed mid={old_mid}; will try create new")

    _log(f"➕ CREATE MENU: '{_first_line(html_title)}' (buttons={len(btns)})")
    msg = await safe_send(client, entity, full_text, parse_mode="HTML")
    await _throttle()
    if msg:
        existing_index[str(msg.id)] = {
            "text": html_title,
            "kb_fp": new_fp,
            "date": datetime.now(timezone.utc).isoformat(),
        }
        _log(f"✅ MENU created mid={msg.id}: '{_first_line(html_title)}'")
        return int(msg.id)

    _log(f"❌ MENU create failed: '{_first_line(html_title)}'")
    return old_mid


# --------------------------- Посты моделей/группы -----------------------------

def _resolve_paths_for_model(
    prices_tree: dict,
    template_tree: dict,
    cat: str,
    brand: str,
    model: str,
) -> tuple[Optional[List[str]], Optional[List[str]]]:
    """
    Возвращает (prices_path, template_path).
    prices_path всегда пытаемся взять из parsed_data (prices_tree) — это критично.
    template_path берём из data.json (template_tree) только для шаблонных строк/разделителей.
    """
    prices_path = _find_first_model_path_in_catalog(prices_tree, cat, brand, model)
    template_path = _find_first_model_path_in_catalog(template_tree, cat, brand, model)
    return prices_path, template_path


async def _ensure_model_post(
    client,
    entity,
    *,
    cat: str,
    brand: str,
    model: str,
    existing_index: dict[str, dict],
    model_to_mid: dict[str, str],
    prices_tree: dict,
    template_tree: dict,
    channel_pricing: Union[str, dict],
    cover_cfg: dict,
    peer_id_short: str,
) -> tuple[bool, Optional[int], bool]:
    prices_path, template_path = _resolve_paths_for_model(prices_tree, template_tree, cat, brand, model)
    if not prices_path:
        _log(f"⚠️ MODEL '{model}' not found in PRICES under {cat}/{brand}")
        return False, None, False

    new_text = _build_model_text(prices_tree, template_tree, model, prices_path, template_path, channel_pricing)

    # Нет цен для opt → удаляем старый пост (если был)
    if not new_text:
        mid = model_to_mid.get(model)
        if mid and str(mid) in existing_index:
            _log(f"🗑 REMOVE MODEL '{model}' mid={mid}: no priced variants for this channel")
            await safe_delete(client, entity, int(mid))
            await _throttle()
            existing_index.pop(str(mid), None)
            model_to_mid.pop(model, None)
        else:
            _log(f"⚠️ MODEL '{model}' has no priced variants for this channel, skip")
        return False, None, False

    is_retail = _is_retail_mode(channel_pricing)
    placeholder = _resolve_placeholder_cover(cover_cfg, peer_id_short) if (RETAIL_ALWAYS_MEDIA and is_retail) else None
    cover_real = _resolve_model_cover(cover_cfg, peer_id_short, prices_path)

    # В retail: всегда MEDIA (обложка или placeholder)
    cover = cover_real or placeholder

    mid = model_to_mid.get(model)
    prev = existing_index.get(str(mid)) if mid else None
    prev_has_media = bool(prev and prev.get("has_media"))
    prev_media_path = (prev.get("media_path") if isinstance(prev, dict) else None) if prev_has_media else None

    # =================== MEDIA PATH (retail always, opt if cover exists) ===================
    if cover:
        # если был текстовый пост — удаляем и пересоздаём с картинкой (по месту не конвертнуть)
        if mid and prev and not prev_has_media:
            _log(f"🗑 SWITCH TEXT→MEDIA for '{model}' mid={mid}")
            await safe_delete(client, entity, int(mid))
            await _throttle()
            existing_index.pop(str(mid), None)
            model_to_mid.pop(model, None)
            mid = None
            prev = None
            prev_has_media = False
            prev_media_path = None

        # если был media и файл поменялся — стараемся заменить media без смены mid
        if mid and prev_has_media and prev_media_path and str(prev_media_path) != str(cover):
            _log(f"🖼 REPLACE MEDIA (keep mid) for '{model}' mid={mid}")
            okm = await safe_edit_media(client, entity, int(mid), file=cover, caption=new_text)
            await _throttle()
            if okm:
                existing_index[str(mid)]["text"] = new_text
                existing_index[str(mid)]["date"] = datetime.now(timezone.utc).isoformat()
                existing_index[str(mid)]["has_media"] = True
                existing_index[str(mid)]["media_path"] = str(cover)
                existing_index[str(mid)]["model"] = model
                existing_index[str(mid)]["hidden"] = False
                return True, int(mid), True

            # fallback: delete+recreate
            _log(f"🗑 REPLACE MEDIA fallback delete+recreate for '{model}' mid={mid}")
            await safe_delete(client, entity, int(mid))
            await _throttle()
            existing_index.pop(str(mid), None)
            model_to_mid.pop(model, None)
            mid = None
            prev = None
            prev_has_media = False
            prev_media_path = None

        if mid and prev_has_media:
            _log(f"✏️ EDIT MEDIA MODEL '{model}' mid={mid}: caption update")
            ok = await safe_edit(client, entity, int(mid), new_text, parse_mode="HTML")
            await _throttle()
            if ok:
                existing_index[str(mid)]["text"] = new_text
                existing_index[str(mid)]["date"] = datetime.now(timezone.utc).isoformat()
                existing_index[str(mid)]["has_media"] = True
                existing_index[str(mid)]["media_path"] = str(cover)
                existing_index[str(mid)]["model"] = model
                existing_index[str(mid)]["hidden"] = False
                return True, int(mid), True
            _log(f"❌ FAILED EDIT MEDIA MODEL '{model}' mid={mid}")
            return False, int(mid), True

        _log(f"🖼 CREATE MEDIA MODEL '{model}' cover={cover.name}")
        msg = await safe_send_file(client, entity, file=cover, caption=new_text)
        await _throttle()
        if msg:
            model_to_mid[model] = str(msg.id)
            existing_index[str(msg.id)] = {
                "text": new_text,
                "date": datetime.now(timezone.utc).isoformat(),
                "has_media": True,
                "media_path": str(cover),
                "model": model,
                "hidden": False,
            }
            _log(f"✅ CREATED MEDIA MODEL '{model}' mid={msg.id}")
            return True, int(msg.id), False

        _log(f"❌ FAILED CREATE MEDIA MODEL '{model}'")
        return False, None, False

    # =================== TEXT CASE (ТОЛЬКО для opt) ===================
    # если раньше была картинка, а теперь нет — удаляем медиа и создаём текст
    if mid and prev_has_media:
        _log(f"🗑 SWITCH MEDIA→TEXT for '{model}' mid={mid}")
        await safe_delete(client, entity, int(mid))
        await _throttle()
        existing_index.pop(str(mid), None)
        model_to_mid.pop(model, None)
        mid = None
        prev = None
        prev_has_media = False

    if mid and str(mid) in existing_index:
        _log(f"✏️ EDIT MODEL '{model}' mid={mid}: force rewrite")
        ok = await safe_edit(client, entity, int(mid), new_text, parse_mode="HTML")
        await _throttle()
        if ok:
            existing_index[str(mid)]["text"] = new_text
            existing_index[str(mid)]["date"] = datetime.now(timezone.utc).isoformat()
            existing_index[str(mid)]["has_media"] = False
            existing_index[str(mid)].pop("media_path", None)
            existing_index[str(mid)]["model"] = model
            existing_index[str(mid)]["hidden"] = False
            return True, int(mid), True
        _log(f"❌ FAILED EDIT MODEL '{model}' mid={mid}")
        return False, int(mid), True

    _log(f"➕ CREATE MODEL '{model}' (no existing mid)")
    msg = await safe_send(client, entity, new_text, parse_mode="HTML")
    await _throttle()
    if msg:
        model_to_mid[model] = str(msg.id)
        existing_index[str(msg.id)] = {
            "text": new_text,
            "date": datetime.now(timezone.utc).isoformat(),
            "has_media": False,
            "model": model,
            "hidden": False,
        }
        _log(f"✅ CREATED MODEL '{model}' mid={msg.id}")
        return True, int(msg.id), False

    _log(f"❌ FAILED CREATE MODEL '{model}'")
    return False, None, False


async def _ensure_group_post(
    client,
    entity,
    *,
    cat: str,
    brand: str,
    models_group: List[str],
    existing_index: dict[str, dict],
    model_to_mid: dict[str, str],
    prices_tree: dict,
    template_tree: dict,
    channel_pricing: Union[str, dict],
    cover_cfg: dict,
    peer_id_short: str,
) -> tuple[bool, Optional[int], bool]:
    segment_texts: List[str] = []
    actual_models: List[str] = []

    for m in models_group:
        prices_path, template_path = _resolve_paths_for_model(prices_tree, template_tree, cat, brand, m)
        if not prices_path:
            _log(f"⚠️ GROUP: MODEL '{m}' not found in PRICES under {cat}/{brand}")
            continue

        text = _build_model_text(prices_tree, template_tree, m, prices_path, template_path, channel_pricing)
        if text:
            segment_texts.append(text)
            actual_models.append(m)

    if not actual_models:
        for m in models_group:
            mid = model_to_mid.get(m)
            if mid and str(mid) in existing_index:
                _log(f"🗑 GROUP: remove stale model '{m}' mid={mid} (no priced variants)")
                await safe_delete(client, entity, int(mid))
                await _throttle()
                existing_index.pop(str(mid), None)
            model_to_mid.pop(m, None)
        return False, None, False

    if len(actual_models) == 1:
        return await _ensure_model_post(
            client, entity,
            cat=cat,
            brand=brand,
            model=actual_models[0],
            existing_index=existing_index,
            model_to_mid=model_to_mid,
            prices_tree=prices_tree,
            template_tree=template_tree,
            channel_pricing=channel_pricing,
            cover_cfg=cover_cfg,
            peer_id_short=peer_id_short,
        )

    group_text = "\n\n".join(segment_texts)

    # retail: групповые посты тоже держим MEDIA (placeholder)
    is_retail = _is_retail_mode(channel_pricing)
    placeholder = _resolve_placeholder_cover(cover_cfg, peer_id_short) if (RETAIL_ALWAYS_MEDIA and is_retail) else None
    group_cover = placeholder  # для группы используем только placeholder (без попыток "угадывать" обложку)

    target_mid: Optional[str] = None
    extra_mids: List[str] = []

    for m in actual_models:
        mid = model_to_mid.get(m)
        if mid and str(mid) in existing_index:
            if target_mid is None:
                target_mid = mid
            elif mid != target_mid:
                extra_mids.append(mid)

    # если делаем media-группу, и target_mid есть, но он текстовый — проще пересоздать в media
    if group_cover and target_mid and str(target_mid) in existing_index and not existing_index[str(target_mid)].get("has_media"):
        _log(f"🗑 GROUP: target_mid={target_mid} is text; delete & rebuild as MEDIA group")
        await safe_delete(client, entity, int(target_mid))
        await _throttle()
        existing_index.pop(str(target_mid), None)
        target_mid = None

    for mid in extra_mids:
        _log(f"🗑 GROUP: delete extra mid={mid} while merging models {actual_models}")
        await safe_delete(client, entity, int(mid))
        await _throttle()
        existing_index.pop(str(mid), None)

    changed = False
    was_existing = False
    result_mid: Optional[int] = None

    # ========= MEDIA GROUP (retail) =========
    if group_cover:
        if target_mid and str(target_mid) in existing_index:
            was_existing = True
            prev = existing_index[str(target_mid)]
            prev_media_path = prev.get("media_path") if prev.get("has_media") else None

            # если медиа уже есть и файл тот же — просто правим caption
            if prev.get("has_media") and prev_media_path and str(prev_media_path) == str(group_cover):
                _log(f"✏️ EDIT MEDIA GROUP mid={target_mid}: caption update")
                ok = await safe_edit(client, entity, int(target_mid), group_text, parse_mode="HTML")
                await _throttle()
                if ok:
                    prev["text"] = group_text
                    prev["date"] = datetime.now(timezone.utc).isoformat()
                    prev["has_media"] = True
                    prev["media_path"] = str(group_cover)
                    changed = True
                result_mid = int(target_mid)

            else:
                # медиа другое/неизвестно — пробуем заменить медиа сохраняя mid
                _log(f"🖼 REPLACE MEDIA GROUP (keep mid) mid={target_mid}")
                okm = await safe_edit_media(client, entity, int(target_mid), file=group_cover, caption=group_text)
                await _throttle()
                if okm:
                    prev["text"] = group_text
                    prev["date"] = datetime.now(timezone.utc).isoformat()
                    prev["has_media"] = True
                    prev["media_path"] = str(group_cover)
                    changed = True
                    result_mid = int(target_mid)
                else:
                    # fallback: delete+recreate
                    _log(f"🗑 REPLACE MEDIA GROUP fallback delete+recreate mid={target_mid}")
                    await safe_delete(client, entity, int(target_mid))
                    await _throttle()
                    existing_index.pop(str(target_mid), None)
                    target_mid = None

        if result_mid is None:
            _log(f"🖼 CREATE MEDIA GROUP for models={actual_models} placeholder={group_cover.name}")
            msg = await safe_send_file(client, entity, file=group_cover, caption=group_text)
            await _throttle()
            if msg:
                result_mid = int(msg.id)
                existing_index[str(msg.id)] = {
                    "text": group_text,
                    "date": datetime.now(timezone.utc).isoformat(),
                    "has_media": True,
                    "media_path": str(group_cover),
                }
                changed = True
                _log(f"✅ CREATED MEDIA GROUP mid={msg.id} for models={actual_models}")

        if result_mid is not None:
            for m in actual_models:
                model_to_mid[m] = str(result_mid)

        return changed, result_mid, was_existing

    # ========= TEXT GROUP (opt, как раньше) =========
    if target_mid and str(target_mid) in existing_index:
        # если target_mid — это media-пост (картинка модели), мы НЕ хотим превращать его в групповик в opt:
        if existing_index[str(target_mid)].get("has_media"):
            _log(f"🗑 GROUP: target_mid={target_mid} is media; delete & rebuild as text group")
            await safe_delete(client, entity, int(target_mid))
            await _throttle()
            existing_index.pop(str(target_mid), None)
            target_mid = None

    if target_mid and str(target_mid) in existing_index:
        was_existing = True
        _log(f"✏️ EDIT GROUP models={actual_models} mid={target_mid}: force rewrite")
        ok = await safe_edit(client, entity, int(target_mid), group_text, parse_mode="HTML")
        await _throttle()
        if ok:
            prev = existing_index[str(target_mid)]
            prev["text"] = group_text
            prev["date"] = datetime.now(timezone.utc).isoformat()
            prev["has_media"] = False
            prev.pop("media_path", None)
            changed = True
        result_mid = int(target_mid)
    else:
        _log(f"➕ CREATE GROUP for models={actual_models}")
        msg = await safe_send(client, entity, group_text, parse_mode="HTML")
        await _throttle()
        if msg:
            result_mid = int(msg.id)
            existing_index[str(msg.id)] = {
                "text": group_text,
                "date": datetime.now(timezone.utc).isoformat(),
                "has_media": False,
            }
            changed = True
            _log(f"✅ CREATED GROUP mid={msg.id} for models={actual_models}")

    if result_mid is not None:
        for m in actual_models:
            model_to_mid[m] = str(result_mid)

    return changed, result_mid, was_existing


async def _ensure_unit_post(
    client,
    entity,
    *,
    cat: str,
    brand: str,
    unit: List[str],
    existing_index: dict[str, dict],
    model_to_mid: dict[str, str],
    prices_tree: dict,
    template_tree: dict,
    channel_pricing: Union[str, dict],
    cover_cfg: dict,
    peer_id_short: str,
) -> tuple[bool, Optional[int], bool]:
    """
    Публикует один unit (одна модель или группа).
    Возвращает (changed, mid, was_existing)
    """
    if not unit:
        return False, None, False

    if len(unit) == 1:
        return await _ensure_model_post(
            client, entity,
            cat=cat,
            brand=brand,
            model=unit[0],
            existing_index=existing_index,
            model_to_mid=model_to_mid,
            prices_tree=prices_tree,
            template_tree=template_tree,
            channel_pricing=channel_pricing,
            cover_cfg=cover_cfg,
            peer_id_short=peer_id_short,
        )

    _log(f"📦 GROUP POST for models={unit} in {cat}/{brand}")
    return await _ensure_group_post(
        client, entity,
        cat=cat,
        brand=brand,
        models_group=unit,
        existing_index=existing_index,
        model_to_mid=model_to_mid,
        prices_tree=prices_tree,
        template_tree=template_tree,
        channel_pricing=channel_pricing,
        cover_cfg=cover_cfg,
        peer_id_short=peer_id_short,
    )


# --------------------------- Определение моделей в существующих постах -----------------------------

def _collect_all_model_titles(catalog: dict) -> set[str]:
    titles: set[str] = set()
    for _cat, brands in (catalog or {}).items():
        if not isinstance(brands, dict):
            continue
        for _br, block in (brands or {}).items():
            if not isinstance(block, dict):
                continue
            if _looks_like_models_map(block):
                for m in block.keys():
                    titles.add(str(m))
            else:
                for _sn, models in (block or {}).items():
                    if isinstance(models, dict):
                        for m in models.keys():
                            titles.add(str(m))
    return titles


def _extract_models_from_message_text(text: str) -> list[str]:
    if not text:
        return []

    titles: list[str] = []
    for m in re.finditer(r"<b>([^<\n]+)</b>", text):
        title = m.group(1).strip()
        if not title:
            continue
        title = re.sub(r"\s+", " ", title)
        titles.append(title)

    if not titles:
        first = _first_line(text)
        if first:
            first = re.sub(r"\s+", " ", first.strip())
            titles.append(first)

    return titles


# --------------------------- Статусное сообщение --------------------------------------

def _collect_status_msg_ids(existing: dict[str, dict]) -> List[int]:
    ids: List[int] = []
    for mid, meta in existing.items():
        raw = meta.get("text") or ""
        if STATUS_TITLE in raw:
            try:
                ids.append(int(mid))
            except Exception:
                pass
    return ids


async def _delete_all_status_messages(client, entity, *, existing_index: dict[str, dict]) -> int:
    ids = _collect_status_msg_ids(existing_index)
    if not ids:
        return 0
    await safe_delete(client, entity, sorted(ids, reverse=True))
    await _throttle()
    for oid in ids:
        existing_index.pop(str(oid), None)
    return len(ids)


def _load_status_extra_for_channel(peer_id: str, username: Optional[str]) -> str:
    cfg = load_status_extra() or {}
    if not isinstance(cfg, dict):
        return ""

    extra = cfg.get(peer_id)
    if not extra and username:
        extra = cfg.get(username.lower())
    if not extra:
        extra = cfg.get("_default")

    return (extra or "").strip()


def _format_status_text(ts: datetime, extra: str) -> str:
    date_str = ts.strftime("%d.%m.%Y %H:%M")
    base = f"<b>{date_str}</b>\n{STATUS_TITLE}"
    extra = (extra or "").strip()
    if extra:
        base += "\n\n" + extra
    return base


async def _refresh_status_message(
    client,
    entity,
    *,
    existing_index: dict[str, dict],
    menu_state: dict,
    tz: timezone,
    peer_id: str,
    username: Optional[str],
) -> Optional[int]:
    await _delete_all_status_messages(client, entity, existing_index=existing_index)

    now = datetime.now(tz).astimezone(tz)
    extra_text = _load_status_extra_for_channel(peer_id, username)
    text = _format_status_text(now, extra_text)

    msg = await safe_send(client, entity, text, parse_mode="HTML")
    await _throttle()
    if msg:
        existing_index[str(msg.id)] = {
            "text": text,
            "date": datetime.now(timezone.utc).isoformat(),
            "has_media": False,
        }
        menu_state["status"] = int(msg.id)
        _log(f"✅ STATUS created mid={msg.id}")
        return int(msg.id)

    _log("❌ STATUS create failed")
    return None


# --------------------------- Синхронизация канала -------------------------------------------

async def sync_channel(
    client: TelegramClient,
    channel_ref: Union[str, int],
    *,
    channel_mode: str = "opt",
    aio_bot: Optional[AiogramBot] = None
) -> dict:
    # ====== parsed_data.json: источник цен + channel_pricing ======
    parsed, _parsed_src = _load_parsed_data()
    _debug_parsed_shape(parsed)

    prices_tree = _extract_prices_catalog_from_parsed(parsed)
    if not prices_tree:
        _log("Nothing to publish: empty catalog(prices) in parsed_data.json")
        return {"created": 0, "edited": 0, "skipped": 0, "removed": 0, "model_to_mid": {}}

    _log(f"prices(models by count): {_count_models_in_catalog(prices_tree)}")

    # ====== data.json: TEMPLATE (разделители/варианты) ======
    db = load_data()
    template_tree = db.get("etalon") or {}
    if not isinstance(template_tree, dict) or not template_tree:
        _log("Nothing to publish: empty catalog(template) in data.json")
        return {"created": 0, "edited": 0, "skipped": 0, "removed": 0, "model_to_mid": {}}

    # ====== entity / peer_id ======
    entity = await client.get_entity(channel_ref)
    peer_id = str(utils.get_peer_id(entity))          # "-100123..."
    peer_id_short = peer_id.replace("-100", "")
    username = getattr(entity, "username", None)

    # ====== cover cfg (для картинок моделей) ======
    cover_cfg = _load_cover_images_cfg()
    if cover_cfg:
        _log(f"Cover cfg loaded: peers={len(cover_cfg)}")

    # chat_id для Bot API
    bot_chat_ref: Optional[Union[int, str]] = None
    if aio_bot is not None:
        if username:
            bot_chat_ref = f"@{username}"
        else:
            try:
                bot_chat_ref = int(peer_id)
            except Exception:
                bot_chat_ref = None

    # ====== Порядок: ИЗ prices_tree (parsed_data.json) ======
    cat_list, cat_brands_order, brand_models = _order_from_prices_catalog(prices_tree)

    # ====== Фильтрация publish-spec (ПО prices_tree) ======
    publish_spec = _load_publish_spec_for_peer(peer_id, peer_id_short)
    if publish_spec:
        cat_list, cat_brands_order, brand_models = _apply_publish_spec_filter(
            cat_list,
            cat_brands_order,
            brand_models,
            prices_tree,
            publish_spec,
        )

    # модельные заголовки берём из prices_tree (реально публикуемые)
    all_model_titles = _collect_all_model_titles(prices_tree)

    desired_models: set[str] = set()
    for (_cat, _br), models in brand_models.items():
        for m in models:
            desired_models.add(m)

    # ====== кеш ======
    channel_posts = load_channel_posts(peer_id)
    menu_state_all = load_channel_menu_state(peer_id)

    # pricing cfg: ТОЛЬКО из parsed_data.json
    pricing_cfg = _load_channel_pricing_config_from_parsed(parsed)
    channel_pricing = _resolve_channel_pricing(entity, pricing_cfg, fallback_mode=channel_mode)
    _log(f"Pricing rules resolved: {channel_pricing}")

    if RETAIL_ALWAYS_MEDIA and _is_retail_mode(channel_pricing):
        ph = _resolve_placeholder_cover(cover_cfg, peer_id_short)
        _log(f"Retail MEDIA mode: placeholder={'OK' if ph else 'MISSING'}")

    def _url(mid: int | str) -> str:
        return f"https://t.me/{username}/{mid}" if username else f"https://t.me/c/{peer_id_short}/{mid}"

    menu_state = menu_state_all if isinstance(menu_state_all, dict) and menu_state_all else {
        "brand_models": {},  # key "cat|br" -> mid
        "brands": {},        # cat -> mid
        "categories": None,  # global menu mid
        "status": None,      # status mid
    }

    # ====== существующие сообщения ======
    msgs = await client.get_messages(entity, limit=RECENT_MESSAGES_LIMIT)

    existing = channel_posts if isinstance(channel_posts, dict) else {}
    actual_ids = {str(m.id) for m in msgs if isinstance(m, Message)}

    removed = await _prune_missing_messages(client, entity, existing_index=existing, recent_ids=actual_ids)
    if removed:
        _log(f"Garbage collected {removed} vanished messages from cache")

    # обновляем кеш существующих постов (важно: has_media)
    for msg in msgs:
        if isinstance(msg, Message):
            prev_meta = existing.get(str(msg.id)) if isinstance(existing.get(str(msg.id)), dict) else {}
            existing[str(msg.id)] = {
                "text": msg.message or "",
                "date": msg.date.isoformat() if getattr(msg, "date", None) else None,
                "kb_fp": (prev_meta.get("kb_fp") if isinstance(prev_meta, dict) else None),
                "has_media": bool(getattr(msg, "media", None)),
                "media_path": (prev_meta.get("media_path") if isinstance(prev_meta, dict) else None),
                "model": (prev_meta.get("model") if isinstance(prev_meta, dict) else None),
                "hidden": bool(prev_meta.get("hidden")) if isinstance(prev_meta, dict) else False,
            }

    # ====== Очистка: меню/статус ======
    mids_to_delete: List[int] = []
    for mid, meta in list(existing.items()):
        text = meta.get("text") or ""
        first = _first_line(text)
        stripped = _strip_markup_title(first)

        if stripped.startswith(MANAGED_TITLES_PREFIXES):
            try:
                mids_to_delete.append(int(mid))
                continue
            except Exception:
                continue

        if STATUS_TITLE in text:
            try:
                mids_to_delete.append(int(mid))
            except Exception:
                pass

    if mids_to_delete:
        _log(f"🧹 Delete old menus/status: {len(mids_to_delete)} messages")
        await safe_delete(client, entity, sorted(mids_to_delete, reverse=True))
        await _throttle()
        for oid in mids_to_delete:
            existing.pop(str(oid), None)
        removed += len(mids_to_delete)

    # ====== Очистка: устаревшие модели (которые были в канале, но теперь не в publish-spec) ======
    obsolete_mids: List[int] = []
    for mid, meta in list(existing.items()):
        text = meta.get("text") or ""
        if not text:
            continue

        titles = _extract_models_from_message_text(text)
        if not titles:
            continue

        has_known_model = any(t in all_model_titles for t in titles)
        if not has_known_model:
            continue

        keep = any(t in desired_models for t in titles)
        if not keep:
            try:
                obsolete_mids.append(int(mid))
            except Exception:
                continue

    if obsolete_mids:
        _log(f"🧹 Delete obsolete model posts (not in publish-spec): {len(obsolete_mids)} messages")
        await safe_delete(client, entity, sorted(obsolete_mids, reverse=True))
        await _throttle()
        for oid in obsolete_mids:
            existing.pop(str(oid), None)
        removed += len(obsolete_mids)

    # ====== Очистка: старые групповые посты (несколько моделей в одном сообщении) ======
    group_mids: List[int] = []
    for mid, meta in list(existing.items()):
        if not isinstance(meta, dict):
            continue
        text = meta.get("text") or ""
        titles = [t for t in _extract_models_from_message_text(text) if t in desired_models]
        if len(titles) > 1:
            try:
                group_mids.append(int(mid))
            except Exception:
                pass
    if group_mids:
        _log(f"🧹 Delete grouped model posts: {len(group_mids)} messages")
        await safe_delete(client, entity, sorted(set(group_mids), reverse=True))
        await _throttle()
        for oid in group_mids:
            existing.pop(str(oid), None)
        removed += len(group_mids)

    # ====== model_to_mid только по актуальным ======
    model_to_mid: Dict[str, str] = {}
    for mid, meta in existing.items():
        if not isinstance(meta, dict):
            continue

        model_name = meta.get("model")
        if model_name in desired_models:
            model_to_mid[model_name] = mid
            continue

        text = meta.get("text") or ""
        titles = _extract_models_from_message_text(text)
        for title in titles:
            if title in desired_models:
                model_to_mid[title] = mid
                meta["model"] = title
                break

    created = edited = skipped = 0

    # ====== Если добавились новые секции (cat/brand) — удаляем всё после них ======
    reset_point = _find_section_reset_point(cat_list, cat_brands_order, brand_models, model_to_mid)
    if reset_point:
        reset_cat, reset_brand = reset_point
        models_after = _collect_models_from_section(
            cat_list, cat_brands_order, brand_models, reset_cat, reset_brand
        )
        mids_after: List[int] = []
        for m in models_after:
            mid = model_to_mid.get(m)
            if mid:
                try:
                    mids_after.append(int(mid))
                except Exception:
                    pass
        mids_after = sorted(set(mids_after), reverse=True)
        if mids_after:
            _log(f"🧹 SECTION RESET from {reset_cat}/{reset_brand}: delete {len(mids_after)} posts")
            await safe_delete(client, entity, mids_after)
            await _throttle()
            for mid in mids_after:
                existing.pop(str(mid), None)
            for m in models_after:
                model_to_mid.pop(m, None)
            removed += len(mids_after)

    # ================= Публикация =================
    for cat in cat_list:
        brands = cat_brands_order.get(cat, []) or []
        _log(f"CATEGORY '{cat}' — brands={len(brands)}")

        for br in brands:
            models = brand_models.get((cat, br), [])
            _log(f"  BRAND '{cat}/{br}' — models={len(models)}")

            # ====== PLAN + ORDER CHECK ======
            units_plan = _plan_brand_units(
                models,
                cat=cat,
                br=br,
                prices_tree=prices_tree,
                template_tree=template_tree,
                channel_pricing=channel_pricing,
            )

            did_reset = False
            if _brand_needs_reset(units_plan, model_to_mid):
                removed_here = await _reset_brand_posts(
                    client,
                    entity,
                    existing_index=existing,
                    model_to_mid=model_to_mid,
                    models=models,
                )
                removed += removed_here
                did_reset = True

            published_here = 0

            if did_reset:
                # Публикуем в порядке units_plan (m1 затем m2...), чтобы mid шли по возрастанию
                for unit in units_plan:
                    changed, mid, was_existing = await _ensure_unit_post(
                        client, entity,
                        cat=cat,
                        brand=br,
                        unit=unit,
                        existing_index=existing,
                        model_to_mid=model_to_mid,
                        prices_tree=prices_tree,
                        template_tree=template_tree,
                        channel_pricing=channel_pricing,
                        cover_cfg=cover_cfg,
                        peer_id_short=peer_id_short,
                    )
                    if changed and mid is not None:
                        edited += 1 if was_existing else 0
                        created += 0 if was_existing else 1
                        published_here += 1
                    else:
                        skipped += 1

            else:
                # 1 сообщение = 1 модель (без склейки)
                for unit in units_plan:
                    changed, mid, was_existing = await _ensure_unit_post(
                        client, entity,
                        cat=cat,
                        brand=br,
                        unit=unit,
                        existing_index=existing,
                        model_to_mid=model_to_mid,
                        prices_tree=prices_tree,
                        template_tree=template_tree,
                        channel_pricing=channel_pricing,
                        cover_cfg=cover_cfg,
                        peer_id_short=peer_id_short,
                    )
                    if changed and mid is not None:
                        edited += 1 if was_existing else 0
                        created += 0 if was_existing else 1
                        published_here += 1
                    else:
                        skipped += 1

            _log(
                f"    MODELS published_here={published_here}, "
                f"total_created={created}, total_edited={edited}, total_skipped={skipped}"
            )
            await _throttle(GROUP_PAUSE_SECS)

            # ====== меню бренда ======
            btns: List[InlineKeyboardButton] = []
            resolved = 0

            i = 0
            while i < len(models):
                m = models[i]
                mid = model_to_mid.get(m)
                if not mid:
                    i += 1
                    continue

                group_models_for_btn = [m]
                j = i + 1
                while j < len(models) and model_to_mid.get(models[j]) == mid:
                    group_models_for_btn.append(models[j])
                    j += 1

                resolved += len(group_models_for_btn)
                label = "/".join(group_models_for_btn)
                btns.append(InlineKeyboardButton(text=label, url=_url(mid)))
                i = j

            _log(f"   MENU '{cat}/{br}': models_total={len(models)} resolved_mids={resolved} buttons={len(btns)}")

            title = f"📱 Модели {cat} / {br}:"
            key = f"{cat}|{br}"
            old_mid = menu_state["brand_models"].get(key)
            new_mid = await _ensure_menu_message(
                client, entity,
                title=title,
                btns=btns,
                existing_index=existing,
                old_mid=int(old_mid) if old_mid else None,
                aio_bot=aio_bot,
                chat_ref_for_bot=bot_chat_ref,
            )
            if new_mid:
                menu_state["brand_models"][key] = int(new_mid)
                _log(f"   MENU '{cat}/{br}' mid={new_mid} saved to state")
            else:
                menu_state["brand_models"].pop(key, None)
                _log(f"   MENU '{cat}/{br}' not created (no buttons)")

            await _throttle(GROUP_PAUSE_SECS)

        # ====== меню категории ======
        brand_links: List[InlineKeyboardButton] = []
        linked = 0
        for br in brands:
            key = f"{cat}|{br}"
            mid_menu = menu_state["brand_models"].get(key)
            if mid_menu:
                linked += 1
                brand_links.append(InlineKeyboardButton(text=br, url=_url(mid_menu)))

        _log(f"  CATEGORY MENU '{cat}': brands_total={len(brands)} linked={linked} -> buttons={len(brand_links)}")

        title = f"🏷️ Бренды в {cat}:"
        old_mid = menu_state["brands"].get(cat)
        new_mid = await _ensure_menu_message(
            client, entity,
            title=title,
            btns=brand_links,
            existing_index=existing,
            old_mid=int(old_mid) if old_mid else None,
            aio_bot=aio_bot,
            chat_ref_for_bot=bot_chat_ref,
        )
        if new_mid:
            menu_state["brands"][cat] = int(new_mid)
            _log(f"  CATEGORY MENU '{cat}' mid={new_mid} saved to state")
        else:
            menu_state["brands"].pop(cat, None)
            _log(f"  CATEGORY MENU '{cat}' not created (no buttons)")

        await _throttle(GROUP_PAUSE_SECS)

    # ====== глобальная навигация ======
    cat_btns: List[InlineKeyboardButton] = []
    linked_cats = 0
    for cat in cat_list:
        mid_brand_menu = menu_state["brands"].get(cat)
        if mid_brand_menu:
            linked_cats += 1
            cat_btns.append(InlineKeyboardButton(text=cat, url=_url(mid_brand_menu)))

    _log(f"GLOBAL MENU: categories_total={len(cat_list)} linked={linked_cats} -> buttons={len(cat_btns)}")

    title = "🧭 Навигация по категориям:"
    old_mid = menu_state.get("categories")
    new_mid = await _ensure_menu_message(
        client, entity,
        title=title,
        btns=cat_btns,
        existing_index=existing,
        old_mid=int(old_mid) if old_mid else None,
        aio_bot=aio_bot,
        chat_ref_for_bot=bot_chat_ref,
    )
    if new_mid:
        menu_state["categories"] = int(new_mid)
        _log(f"GLOBAL MENU mid={new_mid} saved to state")
    else:
        menu_state["categories"] = None
        _log("GLOBAL MENU not created (no buttons)")

    # ====== статус ======
    await _refresh_status_message(
        client,
        entity,
        existing_index=existing,
        menu_state=menu_state,
        tz=MOSCOW_TZ,
        peer_id=peer_id,
        username=username,
    )

    # ====== save ======
    save_channel_posts(peer_id, existing)
    save_channel_menu_state(peer_id, menu_state)

    _log(f"DONE: created={created}, edited={edited}, skipped={skipped}, removed={removed}")
    return {
        "created": created,
        "edited": edited,
        "skipped": 0 if skipped < 0 else skipped,
        "removed": removed,
        "model_to_mid": model_to_mid,
    }


async def hide_opt_models(
    client: TelegramClient,
    channel_ref: Union[str, int],
    *,
    channel_mode: str = "opt",
) -> int:
    """
    Для opt-каналов скрывает все модельные посты, заменяя текст на ".".
    Возвращает количество обновлённых сообщений.
    """
    if str(channel_mode or "").lower() != "opt":
        return 0

    db = load_data()
    template_tree = db.get("etalon") or {}
    if not isinstance(template_tree, dict) or not template_tree:
        return 0

    model_titles = _collect_all_model_titles(template_tree)
    if not model_titles:
        return 0

    entity = await client.get_entity(channel_ref)
    peer_id = str(utils.get_peer_id(entity))

    existing = load_channel_posts(peer_id)
    if not isinstance(existing, dict) or not existing:
        return 0

    updated = 0
    for mid, meta in list(existing.items()):
        if not isinstance(meta, dict):
            continue

        text = meta.get("text") or ""
        first = _strip_markup_title(_first_line(text))
        if first.startswith(MANAGED_TITLES_PREFIXES) or STATUS_TITLE in text:
            continue

        model_name = meta.get("model")
        if not model_name:
            titles = _extract_models_from_message_text(text)
            for title in titles:
                if title in model_titles:
                    model_name = title
                    meta["model"] = title
                    break

        if model_name not in model_titles:
            continue

        if (text or "").strip() == ".":
            meta["hidden"] = True
            continue

        ok = await safe_edit(client, entity, int(mid), ".", parse_mode="HTML")
        await _throttle()
        if ok:
            meta["text"] = "."
            meta["hidden"] = True
            updated += 1

    save_channel_posts(peer_id, existing)
    return updated
