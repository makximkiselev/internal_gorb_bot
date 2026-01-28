# handlers/view_prices.py
# UI просмотра цен (aiogram v3):
# parsed_data.json (+ data.json etalon separators) -> tree navigation + "👀 Посмотреть цены" (per-branch, per-model pagination)
from __future__ import annotations

import json
from collections import OrderedDict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from aiogram import Router, F
from aiogram.filters import Command
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

router = Router(name="view_prices")

# ---------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------
MODULE_DIR = Path(__file__).resolve().parent                 # .../handlers
PROJECT_ROOT = MODULE_DIR.parent                             # .../Under_price_final

DATA_JSON = PROJECT_ROOT / "data.json"                       # etalon source
PARSED_DIR = MODULE_DIR / "parsing" / "data"                 # handlers/parsing/data
PARSED_DATA_JSON = PARSED_DIR / "parsed_data.json"

# ---------------------------------------------------------------------
# Navigation pagination (children buttons)
# ---------------------------------------------------------------------
PAGE_CHILDREN = 14

# ---------------------------------------------------------------------
# Path token cache (callback_data limit safety)
# ---------------------------------------------------------------------
_PATH_CACHE: Dict[str, List[str]] = {}
_PATH_ORDER: List[str] = []
_PATH_MAX = 8000
_PATH_SEQ = 0


def _cache_put(path: List[str]) -> str:
    global _PATH_SEQ
    _PATH_SEQ += 1
    token = f"{_PATH_SEQ:x}"
    _PATH_CACHE[token] = list(path)
    _PATH_ORDER.append(token)
    if len(_PATH_ORDER) > _PATH_MAX:
        old = _PATH_ORDER.pop(0)
        _PATH_CACHE.pop(old, None)
    return token


def _cache_get(token: str) -> List[str]:
    return list(_PATH_CACHE.get(token) or [])


# ---------------------------------------------------------------------
# JSON helpers (keep order!)
# ---------------------------------------------------------------------
def _read_json(path: Path, default):
    try:
        if not path.exists():
            return default
        return json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=OrderedDict)
    except Exception:
        return default


# ---------------------------------------------------------------------
# Load parsed_data + etalon
# ---------------------------------------------------------------------
_ETALON_CACHE: Optional[Dict[str, Any]] = None


def _load_etalon_tree() -> Dict[str, Any]:
    global _ETALON_CACHE
    if _ETALON_CACHE is not None:
        return _ETALON_CACHE

    root = _read_json(DATA_JSON, {})
    et = {}
    if isinstance(root, dict):
        et = root.get("etalon") or {}
    _ETALON_CACHE = et if isinstance(et, dict) else {}
    return _ETALON_CACHE


def _ensure_parsed_data() -> Dict[str, Any]:
    """
    UI НЕ пересобирает данные.
    parsed_data.json должен строиться пайплайном (results.py / main.py).
    """
    data = _read_json(PARSED_DATA_JSON, None)
    if not isinstance(data, dict) or not isinstance(data.get("catalog"), dict):
        return {"catalog": OrderedDict(), "timestamp": "", "stats": {}}
    return data


def _get_catalog_root(data: Dict[str, Any]) -> Dict[str, Any]:
    cat = data.get("catalog")
    return cat if isinstance(cat, dict) else OrderedDict()


# ---------------------------------------------------------------------
# Tree helpers
# ---------------------------------------------------------------------
def _dig(node: Dict[str, Any], path: List[str]) -> Any:
    cur: Any = node
    for p in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(p)
    return cur


def _is_variant_value(v: Any) -> bool:
    if not isinstance(v, dict):
        return False
    if not v:
        return True
    allowed = {"min_price", "best_channels"}
    return all(k in allowed for k in v.keys())


def _is_model_leaf(node: Any) -> bool:
    if not isinstance(node, dict):
        return False
    if not node:
        return True
    return all(_is_variant_value(v) for v in node.values())


def _fmt_price(p: Any) -> str:
    try:
        if p is None:
            return "—"
        if isinstance(p, (int, float)):
            f = float(p)
        else:
            f = float(str(p).strip().replace(" ", ""))
        if f.is_integer():
            return f"{int(f):,}".replace(",", " ")
        return f"{f:,.0f}".replace(",", " ")
    except Exception:
        return "—"


def _breadcrumb(path: List[str]) -> str:
    if not path:
        return "📚 Каталог"
    return " / ".join(["📚 Каталог"] + path)


def _norm_key(s: str) -> str:
    s = (s or "").strip().lower()
    s = s.replace("\u00A0", " ")
    while "  " in s:
        s = s.replace("  ", " ")
    return s


def _strip_ram(s: str) -> str:
    """
    "12/256gb" -> "256gb" (fallback для моделей без RAM в каталоге).
    """
    import re
    return re.sub(r"\b\d{1,2}\s*/\s*(\d{2,4}\s*(?:gb|tb))\b", r"\1", s)


def _variant_lookup_map(variants: Dict[str, Any]) -> Dict[str, str]:
    m: Dict[str, str] = {}
    stripped_bucket: Dict[str, List[str]] = {}
    for k in variants.keys():
        nk = _norm_key(str(k))
        m[nk] = str(k)
        sk = _strip_ram(nk)
        if sk != nk:
            stripped_bucket.setdefault(sk, []).append(str(k))
    for sk, keys in stripped_bucket.items():
        if len(keys) == 1 and sk not in m:
            m[sk] = keys[0]
    return m


# ---------------------------------------------------------------------
# Etalon order + separators for variants
# ---------------------------------------------------------------------
def _get_etalon_variant_list_for_model(path_to_model: List[str]) -> Optional[List[str]]:
    """
    path_to_model: ["Смартфоны","Apple","iPhone 16","iPhone 16 Pro Max"]
    В etalon здесь list, где "" — “разделитель” (показываем пустой строкой).
    """
    et = _load_etalon_tree()
    cur: Any = et
    for p in path_to_model:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(p)
    if isinstance(cur, list):
        out: List[str] = []
        for x in cur:
            out.append("" if x is None else str(x))
        return out
    return None


# ---------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------
def _render_variant_line(variant_title: str, info: Any) -> str:
    """
    Формат без буллетов:
      "<variant> — <price> ₽ (channels)"
    """
    vt = str(variant_title)

    if not isinstance(info, dict) or not info or info.get("min_price") is None:
        return f"{vt} — —"

    mp = _fmt_price(info.get("min_price"))
    ch = info.get("best_channels") or []
    ch_list: List[str] = []
    if isinstance(ch, list):
        for x in ch:
            s = str(x).strip()
            if s:
                ch_list.append(s)

    if ch_list:
        return f"{vt} — {mp} ₽ ({', '.join(ch_list)})"
    return f"{vt} — {mp} ₽"


def _collect_leaf_lines_for_model(path_to_model: List[str], variants: Dict[str, Any]) -> List[str]:
    """
    Варианты по эталону (если есть) с пустыми строками.
    Иначе — как в parsed_data.json (порядок ключей dict).
    """
    out: List[str] = []
    et_list = _get_etalon_variant_list_for_model(path_to_model)

    if et_list:
        seq = et_list
    else:
        seq = [str(k) for k in variants.keys()]

    vmap = _variant_lookup_map(variants)

    for raw_vt in seq:
        vt = str(raw_vt)

        # "" — разделитель: просто пустая строка
        if vt.strip() == "":
            out.append("")
            continue

        vt_norm = _norm_key(vt)
        real_key = vmap.get(vt_norm)
        if real_key is None:
            vt_stripped = _strip_ram(vt_norm)
            if vt_stripped != vt_norm:
                real_key = vmap.get(vt_stripped)
        info = variants.get(real_key) if real_key is not None else None
        out.append(_render_variant_line(vt, info))

    # уберем лишние пустые строки в конце
    while out and out[-1] == "":
        out.pop()

    return out


def _collect_models_in_subtree(subtree: Any, base_path: List[str]) -> List[Tuple[List[str], Dict[str, Any]]]:
    """
    Собираем все model-leaf в ветке, в порядке JSON (DFS).
    Возвращаем список: (path_to_model, variants_dict)
    """
    models: List[Tuple[List[str], Dict[str, Any]]] = []

    def walk(node: Any, path: List[str]):
        if isinstance(node, dict) and _is_model_leaf(node):
            models.append((path, node))
            return
        if isinstance(node, dict):
            for k, v in node.items():
                walk(v, path + [str(k)])

    walk(subtree, base_path)
    return models


def _render_model_message(path_to_model: List[str], variants: Dict[str, Any]) -> str:
    """
    1 сообщение = 1 модель
    """
    lines: List[str] = []
    lines.append(_breadcrumb(path_to_model))
    lines.append("")
    lines.extend(_collect_leaf_lines_for_model(path_to_model, variants))
    txt = "\n".join(lines).rstrip()
    # Telegram лимит — если вдруг модель огромная (крайний случай), режем аккуратно.
    if len(txt) > 3900:
        txt = txt[:3900].rstrip() + "\n…"
    return txt


# ---------------------------------------------------------------------
# Keyboards
# ---------------------------------------------------------------------
def _kb_home(children_keys: List[str]) -> InlineKeyboardMarkup:
    tok = _cache_put([])
    return _kb_branch(path=[], token=tok, children_keys=children_keys, page=0)


def _kb_branch(path: List[str], token: str, children_keys: List[str], page: int) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []

    total = len(children_keys)
    if page < 0:
        page = 0
    max_page = (total - 1) // PAGE_CHILDREN if total > 0 else 0
    if page > max_page:
        page = max_page

    start = page * PAGE_CHILDREN
    end = start + PAGE_CHILDREN
    chunk = children_keys[start:end]

    # children
    for name in chunk:
        child_tok = _cache_put(path + [str(name)])
        rows.append([InlineKeyboardButton(text=f"📁 {str(name)}", callback_data=f"vp:go:{child_tok}:0")])

    # pagination row for children
    if total > PAGE_CHILDREN:
        nav: List[InlineKeyboardButton] = []
        if page > 0:
            nav.append(InlineKeyboardButton(text="◀️", callback_data=f"vp:go:{token}:{page-1}"))
        nav.append(InlineKeyboardButton(text=f"{page+1}/{max_page+1}", callback_data="vp:noop"))
        if page < max_page:
            nav.append(InlineKeyboardButton(text="▶️", callback_data=f"vp:go:{token}:{page+1}"))
        rows.append(nav)

    # ✅ “Посмотреть цены” — ВНИЗУ, над “Назад”
    rows.append([InlineKeyboardButton(text="👀 Посмотреть цены", callback_data=f"vp:all:{token}:0")])

    # navigation bottom
    if path:
        back_tok = _cache_put(path[:-1])
        rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=f"vp:go:{back_tok}:0")])
        rows.append([InlineKeyboardButton(text="🔙 В начало", callback_data="vp:home")])
        rows.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")])
    else:
        rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")])
        rows.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")])

    return InlineKeyboardMarkup(inline_keyboard=rows)


def _kb_leaf(path: List[str], page: int, has_prev: bool, has_next: bool) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []

    nav: List[InlineKeyboardButton] = []
    if has_prev:
        nav.append(InlineKeyboardButton(text="◀️", callback_data=f"vp:leaf:{_cache_put(path)}:{page-1}"))
    nav.append(InlineKeyboardButton(text=f"{page+1}", callback_data="vp:noop"))
    if has_next:
        nav.append(InlineKeyboardButton(text="▶️", callback_data=f"vp:leaf:{_cache_put(path)}:{page+1}"))
    rows.append(nav)

    back_tok = _cache_put(path[:-1])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=f"vp:go:{back_tok}:0")])
    rows.append([InlineKeyboardButton(text="🔙 В начало", callback_data="vp:home")])
    rows.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _kb_all_prices(branch_path: List[str], page: int, total_pages: int) -> InlineKeyboardMarkup:
    """
    Пагинация по моделям:
      page = индекс модели внутри branch_path
    """
    rows: List[List[InlineKeyboardButton]] = []

    nav: List[InlineKeyboardButton] = []
    if total_pages > 1:
        if page > 0:
            nav.append(InlineKeyboardButton(text="◀️", callback_data=f"vp:all:{_cache_put(branch_path)}:{page-1}"))
        nav.append(InlineKeyboardButton(text=f"{page+1}/{total_pages}", callback_data="vp:noop"))
        if page < total_pages - 1:
            nav.append(InlineKeyboardButton(text="▶️", callback_data=f"vp:all:{_cache_put(branch_path)}:{page+1}"))
        rows.append(nav)

    # назад — в ветку
    tok = _cache_put(branch_path)
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=f"vp:go:{tok}:0")])
    rows.append([InlineKeyboardButton(text="🔙 В начало", callback_data="vp:home")])
    rows.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ---------------------------------------------------------------------
# Branch text (никаких “мин/разделы/буллеты/линии”)
# ---------------------------------------------------------------------
def _render_branch_text(path: List[str]) -> str:
    if not path:
        return "📚 Каталог\n\nВыбери раздел:"
    return f"{_breadcrumb(path)}\n\nВыбери дальше:"


# ---------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------
@router.message(Command("prices"))
async def cmd_prices(message: Message):
    data = _ensure_parsed_data()
    root = _get_catalog_root(data)
    children = list(root.keys())  # order as in JSON
    await message.answer(_render_branch_text([]), reply_markup=_kb_home(children))


@router.callback_query(F.data == "view_prices")
async def cb_open_prices(callback: CallbackQuery):
    data = _ensure_parsed_data()
    root = _get_catalog_root(data)
    children = list(root.keys())
    await callback.message.edit_text(_render_branch_text([]), reply_markup=_kb_home(children))
    await callback.answer()


@router.callback_query(F.data == "vp:home")
async def cb_home(callback: CallbackQuery):
    data = _ensure_parsed_data()
    root = _get_catalog_root(data)
    children = list(root.keys())
    await callback.message.edit_text(_render_branch_text([]), reply_markup=_kb_home(children))
    await callback.answer()


@router.callback_query(F.data == "vp:noop")
async def cb_noop(callback: CallbackQuery):
    await callback.answer()


@router.callback_query(F.data.startswith("vp:go:"))
async def cb_go(callback: CallbackQuery):
    # vp:go:{token}:{page}
    parts = (callback.data or "").split(":")
    token = parts[2] if len(parts) >= 3 else ""
    page = 0
    if len(parts) >= 4:
        try:
            page = int(parts[3])
        except Exception:
            page = 0

    path = _cache_get(token)

    data = _ensure_parsed_data()
    root = _get_catalog_root(data)

    node = _dig(root, path)
    if node is None:
        children = list(root.keys())
        await callback.message.edit_text("📚 Каталог\n\nПуть устарел. Выбери раздел:", reply_markup=_kb_home(children))
        await callback.answer()
        return

    # leaf -> варианты модели (как 1 модель)
    if _is_model_leaf(node):
        # leaf paging: обычно 1 страница, но оставим навигацию "vp:leaf" на всякий
        msg = _render_model_message(path, node)
        await callback.message.edit_text(msg, reply_markup=_kb_leaf(path, page=0, has_prev=False, has_next=False))
        await callback.answer()
        return

    # branch
    if isinstance(node, dict):
        children_keys = list(node.keys())  # ✅ order as in JSON
        cur_tok = _cache_put(path)
        await callback.message.edit_text(
            _render_branch_text(path),
            reply_markup=_kb_branch(path, cur_tok, children_keys, page=page),
        )
        await callback.answer()
        return

    await callback.message.edit_text(f"{_breadcrumb(path)}\n\n(нет данных)")
    await callback.answer()


@router.callback_query(F.data.startswith("vp:all:"))
async def cb_all_prices(callback: CallbackQuery):
    # vp:all:{token}:{page}  -> page = index of model inside branch
    parts = (callback.data or "").split(":")
    token = parts[2] if len(parts) >= 3 else ""
    page = 0
    if len(parts) >= 4:
        try:
            page = int(parts[3])
        except Exception:
            page = 0

    branch_path = _cache_get(token)

    data = _ensure_parsed_data()
    root = _get_catalog_root(data)
    subtree = _dig(root, branch_path)

    if subtree is None:
        tok = _cache_put(branch_path[:-1])
        await callback.message.edit_text(
            "Путь устарел.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"vp:go:{tok}:0")],
                    [InlineKeyboardButton(text="🔙 В начало", callback_data="vp:home")],
                ]
            ),
        )
        await callback.answer()
        return

    # Собираем модели (в порядке JSON)
    models = _collect_models_in_subtree(subtree, base_path=branch_path)

    if not models:
        tok = _cache_put(branch_path)
        await callback.message.edit_text(
            f"{_breadcrumb(branch_path)}\n\nНет данных по ценам в этой ветке.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"vp:go:{tok}:0")],
                    [InlineKeyboardButton(text="🔙 В начало", callback_data="vp:home")],
                ]
            ),
        )
        await callback.answer()
        return

    total_pages = len(models)
    if page < 0:
        page = 0
    if page >= total_pages:
        page = total_pages - 1

    model_path, variants = models[page]

    msg = _render_model_message(model_path, variants)
    await callback.message.edit_text(
        msg,
        reply_markup=_kb_all_prices(branch_path, page=page, total_pages=total_pages),
    )
    await callback.answer()
