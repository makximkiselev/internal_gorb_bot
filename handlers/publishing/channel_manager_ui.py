from __future__ import annotations

from typing import Optional

from aiogram import Router, F
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message  # noqa
from aiogram.exceptions import TelegramBadRequest
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext

import asyncio
from datetime import datetime, timezone, timedelta
import re
from pathlib import Path
import json
import time
import hashlib

from storage import load_data, save_data
from handlers.publishing.storage import (
    load_managed_channels,
    save_managed_channels,
    load_status_extra,
    save_status_extra,
    purge_channel_storage,
)
from handlers.publishing.channel_updater import sync_channel, hide_opt_models
from handlers.auth_utils import auth_get

router = Router()

# Московское время
MOSCOW_TZ = timezone(timedelta(hours=3))

# ---------- Telethon client storage ----------
_telethon_client = None


def attach_telethon_client(client):
    global _telethon_client
    _telethon_client = client


def _get_client():
    if _telethon_client is None:
        raise RuntimeError("❌ Telethon client не подключён. Вызови attach_telethon_client() в main.py")
    return _telethon_client


# =========================
# ✅ PATH TOKEN CACHE (fix BUTTON_DATA_INVALID)
# =========================
# Telegram ограничивает callback_data (примерно 64 байта), поэтому длинные пути "A|B|C|..."
# ломают клавиатуру. Делаем короткий токен и держим map token -> raw_path в памяти.
_PATH_CACHE: dict[str, tuple[str, str, str, float]] = {}
_PATH_CACHE_TTL_SECS = 6 * 60 * 60      # 6 часов
_PATH_CACHE_MAX = 50_000                # запас


def _prune_path_cache() -> None:
    if not _PATH_CACHE:
        return

    now = time.time()

    dead = [k for k, (_, __, ___, ts) in _PATH_CACHE.items() if (now - ts) > _PATH_CACHE_TTL_SECS]
    for k in dead:
        _PATH_CACHE.pop(k, None)

    if len(_PATH_CACHE) <= _PATH_CACHE_MAX:
        return

    items = sorted(_PATH_CACHE.items(), key=lambda kv: kv[1][3])
    for k, _v in items[: max(0, len(items) - _PATH_CACHE_MAX)]:
        _PATH_CACHE.pop(k, None)


def _make_path_token(kind: str, ch_id: str, raw_path: str) -> str:
    base = f"{kind}|{ch_id}|{raw_path}"
    return hashlib.blake2s(base.encode("utf-8"), digest_size=8).hexdigest()  # 16 hex


def _cache_path(kind: str, ch_id: str, raw_path: str) -> str:
    tok = _make_path_token(kind, ch_id, raw_path)
    _PATH_CACHE[tok] = (kind, ch_id, raw_path, time.time())
    _prune_path_cache()
    return tok


def _resolve_path_token(tok: str, *, kind: str, ch_id: str) -> Optional[str]:
    it = _PATH_CACHE.get(tok)
    if not it:
        return None
    k, c, raw, _ts = it
    if k != kind or c != ch_id:
        return None
    _PATH_CACHE[tok] = (k, c, raw, time.time())  # продлеваем жизнь
    return raw


async def _alert_stale(cb: CallbackQuery):
    await cb.answer("Сессия меню устарела. Откройте раздел заново.", show_alert=True)


# =========================
# ✅ helpers: порядок и “уровень модели”
# =========================
def _iter_node_keys_ordered(node: dict):
    """Идём в порядке, который задан в db['etalon'] (без sorted)."""
    if not isinstance(node, dict):
        return []
    return [k for k in node.keys()]


def _is_model_level_node(node: dict | list) -> bool:
    """
    True, если текущий узел = "модель", а ниже лежат SKU вида:
      "iPhone 17 Pro Max 256Gb Orange eSim": {}
    Т.е. ВСЕ дети (кроме служебных _*) — пустые dict.
    """
    if isinstance(node, list):
        return bool(node)
    if not isinstance(node, dict) or not node:
        return False
    kids = [(k, v) for k, v in node.items() if not str(k).startswith("_")]
    if not kids:
        return False
    return all(isinstance(v, dict) and len(v) == 0 for _k, v in kids)


# ---------- registry ----------
def _get_registry() -> dict:
    return load_managed_channels()


def _save_registry(reg: dict) -> None:
    save_managed_channels(reg)


def _is_owner(ch: dict, user_id: int | None) -> bool:
    try:
        return int(ch.get("user_id")) == int(user_id)
    except Exception:
        return False


def _filter_registry_for_user(reg: dict, user_id: int | None, is_admin: bool) -> dict:
    if is_admin:
        return reg
    return {k: v for k, v in reg.items() if isinstance(v, dict) and _is_owner(v, user_id)}


async def _require_cm_access(cb: CallbackQuery) -> Optional[dict]:
    u = await auth_get(cb.from_user.id)
    access = (u or {}).get("access") or {}
    if not u or not (u.get("role") == "admin" or access.get("settings.cm")):
        await cb.answer("⛔️ Нет доступа", show_alert=True)
        return None
    return u


async def _get_channel_for_cb(cb: CallbackQuery, ch_id: str) -> tuple[Optional[dict], Optional[dict], Optional[dict]]:
    u = await _require_cm_access(cb)
    if not u:
        return None, None, None
    reg = _get_registry()
    ch = reg.get(ch_id)
    if not ch:
        await cb.answer("Канал не найден", show_alert=True)
        return u, reg, None
    if u.get("role") != "admin" and not _is_owner(ch, cb.from_user.id):
        await cb.answer("⛔️ Нет доступа", show_alert=True)
        return u, reg, None
    return u, reg, ch


def _purge_channel_data(peer_id: str) -> None:
    reg = load_managed_channels()
    reg.pop(peer_id, None)
    save_managed_channels(reg)
    purge_channel_storage(peer_id)


# ---------- Файл настроек публикации для каналов ----------
PUBLISH_CONFIG_FILE = Path(__file__).resolve().parent / "channel_publish_paths.json"

# ---------- Файл привязки обложек для retail-каналов ----------
COVER_CONFIG_FILE = Path(__file__).resolve().parent / "channel_cover_images.json"
COVERS_DIR = Path(__file__).resolve().parent / "covers"
COVERS_DIR.mkdir(parents=True, exist_ok=True)


def _load_cover_config() -> dict:
    try:
        if not COVER_CONFIG_FILE.exists():
            return {}
        data = json.loads(COVER_CONFIG_FILE.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_cover_config(cfg: dict) -> None:
    try:
        COVER_CONFIG_FILE.write_text(
            json.dumps(cfg, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception:
        pass


def _cover_key(path: list[str]) -> str:
    return "|".join([p for p in (path or []) if p])


def _get_cover_for_path(ch_id: str, path: list[str]) -> Optional[str]:
    cfg = _load_cover_config()
    ch = cfg.get(ch_id) or {}
    by_path = ch.get("by_path") or {}
    if not isinstance(by_path, dict):
        return None
    return by_path.get(_cover_key(path))


def _has_cover_in_subtree(ch_id: str, path: list[str], *, cfg: Optional[dict] = None) -> bool:
    cfg = cfg or _load_cover_config()
    ch = cfg.get(ch_id) or {}
    by_path = ch.get("by_path") or {}
    if not isinstance(by_path, dict):
        return False

    prefix = _cover_key(path)

    if not prefix:
        return bool(by_path)

    if prefix in by_path:
        return True

    pref = prefix + "|"
    for k in by_path.keys():
        if isinstance(k, str) and k.startswith(pref):
            return True
    return False


def _set_cover_for_path(ch_id: str, path: list[str], rel_path: str) -> None:
    cfg = _load_cover_config()
    ch = cfg.setdefault(ch_id, {})
    by_path = ch.setdefault("by_path", {})
    if not isinstance(by_path, dict):
        by_path = {}
        ch["by_path"] = by_path
    by_path[_cover_key(path)] = rel_path
    _save_cover_config(cfg)


def _delete_cover_for_path(ch_id: str, path: list[str]) -> bool:
    cfg = _load_cover_config()
    ch = cfg.get(ch_id) or {}
    by_path = ch.get("by_path") or {}
    if not isinstance(by_path, dict):
        return False
    k = _cover_key(path)
    if k in by_path:
        by_path.pop(k, None)
        _save_cover_config(cfg)
        return True
    return False


def _safe_filename(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r'[\\/:*?"<>|]+', "_", s)
    s = re.sub(r"[^\w\s\-\.\(\)\[\]]+", "_", s, flags=re.UNICODE)
    return s.strip(" ._")[:120] or "cover"


def _load_publish_config() -> dict:
    try:
        if not PUBLISH_CONFIG_FILE.exists():
            return {}
        data = json.loads(PUBLISH_CONFIG_FILE.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            return data
    except Exception:
        pass
    return {}


def _save_publish_config(cfg: dict) -> None:
    try:
        PUBLISH_CONFIG_FILE.write_text(
            json.dumps(cfg, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception:
        pass


# ---------- keyboards ----------
def _kb_main(reg: dict):
    rows = []
    if not reg:
        rows.append([InlineKeyboardButton(text="➕ Добавить @канал", callback_data="cm:add_start")])
    else:
        for ch_id, ch in reg.items():
            t = "оптовый" if ch.get("type") == "opt" else "розничный"
            label = f"{ch.get('title') or ch.get('username') or ch_id} — {t}"
            rows.append([InlineKeyboardButton(text=label, callback_data=f"cm:view:{ch_id}")])
        rows.append([InlineKeyboardButton(text="➕ Добавить @канал", callback_data="cm:add_start")])
        rows.append([InlineKeyboardButton(text="🔄 Обновить все каналы", callback_data="cm:update_all")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")])
    return rows


def _kb_channel(ch: dict):
    t = ch.get("type", "opt")

    rows = [
        [InlineKeyboardButton(text="🔄 Обновить цены", callback_data=f"cm:update:{ch['id']}")],
        [InlineKeyboardButton(text="🙈 Скрытие цен", callback_data=f"cm:hide_menu:{ch['id']}")],
        [InlineKeyboardButton(text="📂 Что публиковать", callback_data=f"cm:publish:{ch['id']}")],
        [InlineKeyboardButton(text="✏️ Финальное сообщение", callback_data=f"cm:final:{ch['id']}")],
    ]

    if t != "opt":
        rows = [r for r in rows if "cm:hide_menu:" not in r[0].callback_data]
        rows.append([InlineKeyboardButton(text="🖼 Добавить картинки", callback_data=f"cm:images:{ch['id']}")])

    rows += [
        [InlineKeyboardButton(text="🗑 Удалить канал", callback_data=f"cm:del:{ch['id']}")],
        [
            InlineKeyboardButton(
                text=f"Тип канала: {'Оптовый' if t == 'opt' else 'Розничный'} (изменить)",
                callback_data=f"cm:toggle:{ch['id']}",
            )
        ],
        [
            InlineKeyboardButton(
                text=f"Ежедневное уведомление: {'вкл' if ch.get('daily_announce') else 'выкл'} (перекл.)",
                callback_data=f"cm:toggle_ann:{ch['id']}",
            )
        ],
        [InlineKeyboardButton(text="⬅️ К списку каналов", callback_data="cm:open")],
    ]
    return rows


def _kb_add_cancel():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data="cm:add_cancel")],
            [InlineKeyboardButton(text="⬅️ К списку каналов", callback_data="cm:open")],
        ]
    )


# ---------- Cover image (FSM) ----------
class CoverImageStates(StatesGroup):
    waiting_for_photo = State()


def _kb_cover_cancel(ch_id: str, parent_path: list[str]):
    raw_parent = _cover_key(parent_path)
    tok_parent = _cache_path("img", ch_id, raw_parent)
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отмена", callback_data=f"cm_img_cancel:{ch_id}:{tok_parent}")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"cm_img_back:{ch_id}:{tok_parent}")],
            [InlineKeyboardButton(text="⬅️ К каналу", callback_data=f"cm:view:{ch_id}")],
        ]
    )


# ---------- Add channel (FSM) ----------
class AddChannelStates(StatesGroup):
    waiting_for_input = State()


# ---------- Final message (FSM) ----------
class FinalMessageStates(StatesGroup):
    waiting_for_text = State()


class HideTimeStates(StatesGroup):
    waiting_for_time = State()


_USERNAME_RE = re.compile(r"(?i)^(?:@|https?://t\.me/)(?P<u>[a-z0-9_]{5,})$")


async def _resolve_channel_via_telethon(raw: str):
    raw = (raw or "").strip()
    m = _USERNAME_RE.match(raw)
    if m:
        username = m.group("u")
    else:
        if re.fullmatch(r"[a-z0-9_]{5,}", raw, flags=re.I):
            username = raw
        else:
            raise ValueError("Укажите @username или ссылку t.me/username")

    client = _get_client()
    entity = await client.get_entity(username)
    if getattr(entity, "bot", False):
        raise ValueError("Это бот, нужен канал/группа.")

    tg_id = getattr(entity, "id", None) or getattr(entity, "channel_id", None)
    if not tg_id:
        raise ValueError("Не удалось получить ID канала.")
    peer_id = str(int(tg_id))

    title = getattr(entity, "title", None) or getattr(entity, "username", None) or username
    uname = getattr(entity, "username", None) or username

    info = {
        "id": peer_id,
        "username": uname,
        "title": title,
        "type": "opt",
        "daily_announce": True,
    }
    return peer_id, info


@router.callback_query(F.data == "cm:add_start")
async def cm_add_start(cb: CallbackQuery, state: FSMContext):
    u = await _require_cm_access(cb)
    if not u:
        return
    await state.set_state(AddChannelStates.waiting_for_input)
    await cb.message.edit_text(
        "🎯 Добавление канала\n\n"
        "Отправьте @username канала или ссылку вида t.me/username\n\n"
        "Пример: <code>@my_channel</code> или <code>https://t.me/my_channel</code>",
        reply_markup=_kb_add_cancel(),
        parse_mode="HTML",
    )


@router.callback_query(F.data == "cm:add_cancel")
async def cm_add_cancel(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    u = await _require_cm_access(cb)
    if not u:
        return
    reg = _filter_registry_for_user(_get_registry(), cb.from_user.id, u.get("role") == "admin")
    await cb.message.edit_text(
        "Отменено.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=_kb_main(reg)),
    )


@router.message(AddChannelStates.waiting_for_input)
async def cm_add_handle_input(msg: Message, state: FSMContext):
    text = (msg.text or "").strip()
    try:
        peer_id, info = await _resolve_channel_via_telethon(text)
        reg = _get_registry()
        existed = reg.get(peer_id, {})
        u = await auth_get(msg.from_user.id)
        is_admin = (u or {}).get("role") == "admin"
        if not is_admin and existed and not _is_owner(existed, msg.from_user.id):
            await msg.answer("⛔️ Этот канал уже закреплён за другим пользователем.")
            return
        existed.update(info)
        if not is_admin:
            existed["user_id"] = msg.from_user.id
        reg[peer_id] = existed
        _save_registry(reg)

        await state.clear()

        kb = InlineKeyboardMarkup(inline_keyboard=_kb_channel(reg[peer_id]))
        await msg.answer(
            f"✅ Канал добавлен: <b>{reg[peer_id].get('title')}</b>\n"
            f"@{reg[peer_id].get('username') or '—'}",
            reply_markup=kb,
            parse_mode="HTML",
        )
    except Exception as e:
        await msg.answer(
            f"❌ {e}\n\nОтправьте @username или ссылку t.me/username",
            reply_markup=_kb_add_cancel(),
        )


# ---------- aiogram handlers ----------
@router.callback_query(F.data == "cm:open")
async def cm_open(cb: CallbackQuery):
    u = await _require_cm_access(cb)
    if not u:
        return
    reg = _get_registry()
    reg = _filter_registry_for_user(reg, cb.from_user.id, u.get("role") == "admin")
    await cb.message.edit_text(
        "📣 Управление каналами:\nВыберите канал для действий.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=_kb_main(reg)),
    )


@router.callback_query(F.data == "cm:back_root")
async def cm_close(cb: CallbackQuery):
    await cb.message.edit_text("Меню закрыто. Вызовите снова через главное меню.")


@router.callback_query(F.data.startswith("cm:view:"))
async def cm_view(cb: CallbackQuery):
    ch_id = cb.data.split(":")[-1]
    _u, _reg, ch = await _get_channel_for_cb(cb, ch_id)
    if not ch:
        return

    cfg = _load_publish_config()
    selected_paths = cfg.get(ch_id, [])
    selected_count = len(selected_paths)

    txt = (
        f"<b>{ch.get('title') or ch.get('username') or ch_id}</b>\n"
        f"Тип: {'Оптовый' if ch.get('type') == 'opt' else 'Розничный'}\n"
        f"Ежедневное уведомление: {'вкл' if ch.get('daily_announce') else 'выкл'}\n"
        f"Что публиковать: {selected_count} выбранных веток каталога"
    )

    await cb.message.edit_text(
        txt,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=_kb_channel(ch)),
        parse_mode="HTML",
    )


@router.callback_query(F.data.startswith("cm:toggle:"))
async def cm_toggle_type(cb: CallbackQuery):
    ch_id = cb.data.split(":")[-1]
    _u, reg, ch = await _get_channel_for_cb(cb, ch_id)
    if not ch:
        return
    ch["type"] = "retail" if ch.get("type") == "opt" else "opt"
    _save_registry(reg)
    await cm_view(cb)


@router.callback_query(F.data.startswith("cm:toggle_ann:"))
async def cm_toggle_ann(cb: CallbackQuery):
    ch_id = cb.data.split(":")[-1]
    _u, reg, ch = await _get_channel_for_cb(cb, ch_id)
    if not ch:
        return
    ch["daily_announce"] = not ch.get("daily_announce", True)
    _save_registry(reg)
    await cm_view(cb)


@router.callback_query(F.data.startswith("cm:del:"))
async def cm_delete(cb: CallbackQuery):
    ch_id = cb.data.split(":")[-1]
    _u, _reg, ch = await _get_channel_for_cb(cb, ch_id)
    if not ch:
        return

    _purge_channel_data(ch_id)

    cfg = _load_publish_config()
    if ch_id in cfg:
        cfg.pop(ch_id, None)
        _save_publish_config(cfg)

    ccfg = _load_cover_config()
    if ch_id in ccfg:
        ccfg.pop(ch_id, None)
        _save_cover_config(ccfg)

    try:
        ch_dir = COVERS_DIR / str(ch_id)
        if ch_dir.exists():
            for p in ch_dir.glob("*"):
                try:
                    p.unlink()
                except Exception:
                    pass
            try:
                ch_dir.rmdir()
            except Exception:
                pass
    except Exception:
        pass

    await cb.message.edit_text(
        "Канал удалён из управления.",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=_kb_main(
                _filter_registry_for_user(_get_registry(), cb.from_user.id, (_u or {}).get("role") == "admin")
            )
        ),
    )


# ---------- Публикация: выбор категорий / брендов / линеек / моделей ----------
def _get_catalog_tree_for_publish() -> dict:
    """
    Рисуем по db["etalon"] (там порядок и варианты).
    """
    db = load_data()
    etalon = db.get("etalon")
    if isinstance(etalon, dict) and etalon:
        return etalon
    return {}


def _load_publish_spec_for_channel(peer_id: str) -> list[list[str]]:
    cfg = _load_publish_config()
    raw = cfg.get(peer_id) or []
    if not isinstance(raw, list):
        return []

    out: list[list[str]] = []
    for item in raw:
        s = str(item).strip()
        if not s:
            continue
        parts = [p for p in s.split("|") if p]
        if parts:
            out.append(parts)
    return out


def _store_publish_spec_for_channel(peer_id: str, spec: list[list[str]]):
    cfg = _load_publish_config()
    lines = ["|".join(p) for p in spec if p]
    cfg[peer_id] = lines
    _save_publish_config(cfg)


def _path_has_any_selected(path: list[str], selected_spec: list[list[str]]) -> bool:
    if not selected_spec or not path:
        return False
    for sp in selected_spec:
        n = min(len(path), len(sp))
        if sp[:n] == path[:n]:
            return True
    return False


def _toggle_path_in_publish_spec(spec: list[list[str]], target: list[str]) -> list[list[str]]:
    if not target:
        return spec

    has_any = False
    new_spec: list[list[str]] = []
    for p in spec:
        if p[: len(target)] == target:
            has_any = True
            continue
        new_spec.append(p)

    if not has_any:
        new_spec.append(target)

    return new_spec


def _get_node_by_path_for_publish(tree: dict, path: list[str]):
    node = tree
    for key in path:
        if not isinstance(node, dict):
            return {}
        node = node.get(key) or {}
    return node if isinstance(node, (dict, list)) else {}


def _build_publish_keyboard_for_channel(
    tree: dict,
    current_path: list[str],
    selected_spec: list[list[str]],
    ch_id: str,
    ch_title: str,
) -> InlineKeyboardMarkup:
    node = _get_node_by_path_for_publish(tree, current_path)
    rows: list[list[InlineKeyboardButton]] = []

    if isinstance(node, (dict, list)) and node:
        # ✅ если это уровень модели (ниже SKU) — не показываем SKU
        if _is_model_level_node(node):
            rows.append([InlineKeyboardButton(text="(Это уровень модели — ниже SKU скрыты)", callback_data="noop")])
        else:
            for name in _iter_node_keys_ordered(node):  # ✅ порядок из etalon
                if str(name).startswith("_"):
                    continue

                child_path = current_path + [str(name)]
                checked = _path_has_any_selected(child_path, selected_spec)
                checkbox_text = "✅" if checked else "⬜️"

                raw_path = "|".join(child_path)
                tok = _cache_path("pub", ch_id, raw_path)

                cb_toggle = f"cm_pub_toggle:{ch_id}:{tok}"
                cb_open = f"cm_pub_open:{ch_id}:{tok}"

                rows.append(
                    [
                        InlineKeyboardButton(text=checkbox_text, callback_data=cb_toggle),
                        InlineKeyboardButton(text=f"📁 {name}", callback_data=cb_open),
                    ]
                )
    else:
        rows.append([InlineKeyboardButton(text="(Нет дочерних категорий)", callback_data="noop")])

    nav_row: list[InlineKeyboardButton] = []
    if current_path:
        parent_raw = "|".join(current_path[:-1])
        tok_parent = _cache_path("pub", ch_id, parent_raw)
        nav_row.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"cm_pub_back:{ch_id}:{tok_parent}"))
    else:
        nav_row.append(InlineKeyboardButton(text="⬅️ К каналу", callback_data=f"cm:view:{ch_id}"))

    rows.append(nav_row)
    rows.append([InlineKeyboardButton(text=f"📣 Канал: {ch_title}", callback_data=f"cm:view:{ch_id}")])

    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _render_publish_tree_for_channel(
    callback: CallbackQuery,
    ch_id: str,
    current_path: list[str],
    *,
    edit: bool = True,
):
    _u, _reg, ch = await _get_channel_for_cb(callback, ch_id)
    if not ch:
        return
    title = ch.get("title") or ch.get("username") or ch_id

    tree = _get_catalog_tree_for_publish()
    selected_spec = _load_publish_spec_for_channel(ch_id)

    if current_path:
        header = "📂 Что публиковать в канале\n" + " / ".join(current_path)
    else:
        header = f"📂 Что публиковать в канале:\n<b>{title}</b>"

    markup = _build_publish_keyboard_for_channel(tree, current_path, selected_spec, ch_id, title)

    try:
        if edit:
            await callback.message.edit_text(header, reply_markup=markup, parse_mode="HTML")
        else:
            await callback.message.answer(header, reply_markup=markup, parse_mode="HTML")
    except TelegramBadRequest as e:
        if "BUTTON_DATA_INVALID" in str(e):
            await callback.message.answer("❌ Ошибка кнопок. Откройте меню заново.")
        else:
            await callback.message.answer(header, reply_markup=markup, parse_mode="HTML")
    except Exception:
        await callback.message.answer(header, reply_markup=markup, parse_mode="HTML")


def _build_images_keyboard_for_channel(
    tree: dict,
    current_path: list[str],
    ch_id: str,
    ch_title: str,
) -> InlineKeyboardMarkup:
    node = _get_node_by_path_for_publish(tree, current_path)
    rows: list[list[InlineKeyboardButton]] = []

    cfg = _load_cover_config()
    is_model_level = _is_model_level_node(node)

    # ✅ дети показываем только если НЕ уровень модели
    if isinstance(node, dict) and node and not is_model_level:
        for name in _iter_node_keys_ordered(node):  # ✅ порядок из etalon
            if str(name).startswith("_"):
                continue
            child_path = current_path + [str(name)]

            has_any = _has_cover_in_subtree(ch_id, child_path, cfg=cfg)
            icon = "✅" if has_any else "❌"

            raw_path = _cover_key(child_path)
            tok = _cache_path("img", ch_id, raw_path)

            rows.append(
                [
                    InlineKeyboardButton(text=icon, callback_data=f"cm_img_set:{ch_id}:{tok}"),
                    InlineKeyboardButton(text=f"📁 {name}", callback_data=f"cm_img_open:{ch_id}:{tok}"),
                ]
            )
    else:
        if is_model_level:
            rows.append([InlineKeyboardButton(text="(Это уровень модели — ниже SKU скрыты)", callback_data="noop")])
        else:
            rows.append([InlineKeyboardButton(text="(Нет дочерних категорий)", callback_data="noop")])

    # действия для текущего узла
    if current_path:
        cur_has_exact = bool(_get_cover_for_path(ch_id, current_path))
        raw_cur = _cover_key(current_path)
        tok_cur = _cache_path("img", ch_id, raw_cur)

        rows.append(
            [
                InlineKeyboardButton(
                    text="🖼 Загрузить/заменить обложку для ЭТОГО узла",
                    callback_data=f"cm_img_set:{ch_id}:{tok_cur}",
                )
            ]
        )
        if cur_has_exact:
            rows.append(
                [
                    InlineKeyboardButton(
                        text="🗑 Удалить обложку этого узла",
                        callback_data=f"cm_img_del:{ch_id}:{tok_cur}",
                    )
                ]
            )

    # навигация
    nav_row: list[InlineKeyboardButton] = []
    if current_path:
        parent_raw = _cover_key(current_path[:-1])
        tok_parent = _cache_path("img", ch_id, parent_raw)
        nav_row.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"cm_img_back:{ch_id}:{tok_parent}"))
    else:
        nav_row.append(InlineKeyboardButton(text="⬅️ К каналу", callback_data=f"cm:view:{ch_id}"))
    rows.append(nav_row)
    rows.append([InlineKeyboardButton(text=f"📣 Канал: {ch_title}", callback_data=f"cm:view:{ch_id}")])

    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _render_images_tree_for_channel(
    callback: CallbackQuery,
    ch_id: str,
    current_path: list[str],
    *,
    edit: bool = True,
):
    _u, _reg, ch = await _get_channel_for_cb(callback, ch_id)
    if not ch:
        return
    title = ch.get("title") or ch.get("username") or ch_id

    if (ch.get("type") or "opt") == "opt":
        await callback.answer("Картинки доступны только для розничных каналов", show_alert=True)
        return

    tree = _get_catalog_tree_for_publish()

    if current_path:
        cur_node = _get_node_by_path_for_publish(tree, current_path)
        if _is_model_level_node(cur_node):
            header = (
                "🖼 Обложка для модели\n"
                + " / ".join(current_path)
                + "\n\n"
                "Ниже находятся SKU (варианты), но мы их не показываем.\n"
                "Нажмите «Загрузить/заменить обложку для ЭТОГО узла»."
            )
        else:
            header = "🖼 Обложки для каталога\n" + " / ".join(current_path)
    else:
        header = (
            f"🖼 Обложки для каталога канала:\n<b>{title}</b>\n\n"
            f"✅ — обложка задана (на узле или внутри)\n"
            f"❌ — нет обложки\n\n"
            f"Нажмите ✅/❌ чтобы загрузить/заменить картинку для узла."
        )

    markup = _build_images_keyboard_for_channel(tree, current_path, ch_id, title)

    try:
        if edit:
            await callback.message.edit_text(header, reply_markup=markup, parse_mode="HTML")
        else:
            await callback.message.answer(header, reply_markup=markup, parse_mode="HTML")
    except TelegramBadRequest as e:
        if "BUTTON_DATA_INVALID" in str(e):
            await callback.message.answer("❌ Ошибка кнопок. Откройте меню заново.")
        else:
            await callback.message.answer(header, reply_markup=markup, parse_mode="HTML")
    except Exception:
        await callback.message.answer(header, reply_markup=markup, parse_mode="HTML")


# ---------- Хендлеры: выбор категорий для публикации в канале ----------
@router.callback_query(F.data.startswith("cm:publish:"))
async def cm_publish_root(cb: CallbackQuery):
    ch_id = cb.data.split(":")[-1]
    await cb.answer()
    await _render_publish_tree_for_channel(cb, ch_id=ch_id, current_path=[], edit=False)


@router.callback_query(F.data.startswith("cm_pub_open:"))
async def cm_publish_open(cb: CallbackQuery):
    _, _, tail = (cb.data or "").partition("cm_pub_open:")
    try:
        ch_id, tok = tail.split(":", 1)
    except ValueError:
        await cb.answer("Ошибка пути", show_alert=True)
        return

    raw_path = _resolve_path_token(tok, kind="pub", ch_id=ch_id)
    if raw_path is None:
        await _alert_stale(cb)
        return

    path = [p for p in raw_path.split("|") if p]
    await cb.answer()
    await _render_publish_tree_for_channel(cb, ch_id=ch_id, current_path=path, edit=True)


@router.callback_query(F.data.startswith("cm_pub_toggle:"))
async def cm_publish_toggle(cb: CallbackQuery):
    _, _, tail = (cb.data or "").partition("cm_pub_toggle:")
    try:
        ch_id, tok = tail.split(":", 1)
    except ValueError:
        await cb.answer("Ошибка пути", show_alert=True)
        return

    _u, _reg, ch = await _get_channel_for_cb(cb, ch_id)
    if not ch:
        return

    raw_path = _resolve_path_token(tok, kind="pub", ch_id=ch_id)
    if raw_path is None:
        await _alert_stale(cb)
        return

    path = [p for p in raw_path.split("|") if p]

    spec = _load_publish_spec_for_channel(ch_id)

    parent_path = path[:-1]
    direct_parent_selected = bool(parent_path and any(p == parent_path for p in spec))

    if direct_parent_selected:
        tree = _get_catalog_tree_for_publish()

        filtered_spec: list[list[str]] = []
        for p in spec:
            if p[: len(parent_path)] == parent_path:
                continue
            filtered_spec.append(p)

        parent_node = _get_node_by_path_for_publish(tree, parent_path)
        if isinstance(parent_node, dict):
            for name in _iter_node_keys_ordered(parent_node):  # ✅ порядок из etalon
                if str(name).startswith("_"):
                    continue
                child = parent_path + [str(name)]
                if child == path:
                    continue
                filtered_spec.append(child)

        spec_new = filtered_spec
    else:
        spec_new = _toggle_path_in_publish_spec(spec, path)

    _store_publish_spec_for_channel(ch_id, spec_new)

    await cb.answer("Обновлено")
    await _render_publish_tree_for_channel(cb, ch_id=ch_id, current_path=parent_path, edit=True)


@router.callback_query(F.data.startswith("cm_pub_back:"))
async def cm_publish_back(cb: CallbackQuery):
    _, _, tail = (cb.data or "").partition("cm_pub_back:")
    try:
        ch_id, tok = tail.split(":", 1)
    except ValueError:
        ch_id = tail or ""
        await cb.answer()
        await cm_view(cb)
        return

    raw_path = _resolve_path_token(tok, kind="pub", ch_id=ch_id)
    if raw_path is None:
        await _alert_stale(cb)
        return

    path = [p for p in raw_path.split("|") if p]

    await cb.answer()
    await _render_publish_tree_for_channel(cb, ch_id=ch_id, current_path=path, edit=True)


# ---------- images tree handlers ----------
@router.callback_query(F.data.startswith("cm:images:"))
async def cm_images_root(cb: CallbackQuery):
    ch_id = cb.data.split(":")[-1]
    await cb.answer()
    await _render_images_tree_for_channel(cb, ch_id=ch_id, current_path=[], edit=False)


@router.callback_query(F.data.startswith("cm_img_open:"))
async def cm_img_open(cb: CallbackQuery):
    _, _, tail = (cb.data or "").partition("cm_img_open:")
    try:
        ch_id, tok = tail.split(":", 1)
    except ValueError:
        await cb.answer("Ошибка пути", show_alert=True)
        return

    raw_path = _resolve_path_token(tok, kind="img", ch_id=ch_id)
    if raw_path is None:
        await _alert_stale(cb)
        return

    path = [p for p in raw_path.split("|") if p]
    await cb.answer()
    await _render_images_tree_for_channel(cb, ch_id=ch_id, current_path=path, edit=True)


@router.callback_query(F.data.startswith("cm_img_back:"))
async def cm_img_back(cb: CallbackQuery):
    _, _, tail = (cb.data or "").partition("cm_img_back:")
    try:
        ch_id, tok = tail.split(":", 1)
    except ValueError:
        await cb.answer()
        return

    raw_path = _resolve_path_token(tok, kind="img", ch_id=ch_id)
    if raw_path is None:
        await _alert_stale(cb)
        return

    path = [p for p in raw_path.split("|") if p]
    await cb.answer()
    await _render_images_tree_for_channel(cb, ch_id=ch_id, current_path=path, edit=True)


@router.callback_query(F.data.startswith("cm_img_del:"))
async def cm_img_del(cb: CallbackQuery):
    _, _, tail = (cb.data or "").partition("cm_img_del:")
    try:
        ch_id, tok = tail.split(":", 1)
    except ValueError:
        await cb.answer("Ошибка", show_alert=True)
        return

    _u, _reg, ch = await _get_channel_for_cb(cb, ch_id)
    if not ch:
        return

    raw_path = _resolve_path_token(tok, kind="img", ch_id=ch_id)
    if raw_path is None:
        await _alert_stale(cb)
        return

    path = [p for p in raw_path.split("|") if p]

    ok = _delete_cover_for_path(ch_id, path)
    await cb.answer("Удалено" if ok else "Не найдено")
    await _render_images_tree_for_channel(cb, ch_id=ch_id, current_path=path[:-1], edit=True)


@router.callback_query(F.data.startswith("cm_img_set:"))
async def cm_img_set(cb: CallbackQuery, state: FSMContext):
    _, _, tail = (cb.data or "").partition("cm_img_set:")
    try:
        ch_id, tok = tail.split(":", 1)
    except ValueError:
        await cb.answer("Ошибка", show_alert=True)
        return

    _u, _reg, ch = await _get_channel_for_cb(cb, ch_id)
    if not ch:
        return

    raw_path = _resolve_path_token(tok, kind="img", ch_id=ch_id)
    if raw_path is None:
        await _alert_stale(cb)
        return

    path = [p for p in raw_path.split("|") if p]

    if (ch.get("type") or "opt") == "opt":
        await cb.answer("Только для розничных каналов", show_alert=True)
        return

    await state.set_state(CoverImageStates.waiting_for_photo)
    await state.update_data(ch_id=ch_id, target_path=path)

    title = ch.get("title") or ch.get("username") or ch_id
    path_txt = " / ".join(path) if path else "(корень)"
    await cb.answer()

    await cb.message.edit_text(
        f"🖼 Загрузка обложки\n"
        f"Канал: <b>{title}</b>\n"
        f"Узел: <b>{path_txt}</b>\n\n"
        f"Отправьте ОДНО фото (лучше PNG/JPG).\n"
        f"Я сохраню его и привяжу к этому узлу.",
        parse_mode="HTML",
        reply_markup=_kb_cover_cancel(ch_id, parent_path=path[:-1]),
    )


@router.callback_query(F.data.startswith("cm_img_cancel:"))
async def cm_img_cancel(cb: CallbackQuery, state: FSMContext):
    _, _, tail = (cb.data or "").partition("cm_img_cancel:")
    try:
        ch_id, tok = tail.split(":", 1)
    except ValueError:
        await cb.answer()
        await state.clear()
        return

    raw_parent = _resolve_path_token(tok, kind="img", ch_id=ch_id)
    if raw_parent is None:
        await state.clear()
        await _alert_stale(cb)
        return

    parent_path = [p for p in raw_parent.split("|") if p]

    await state.clear()
    await cb.answer("Отменено")
    await _render_images_tree_for_channel(cb, ch_id=ch_id, current_path=parent_path, edit=True)


@router.message(CoverImageStates.waiting_for_photo)
async def cm_img_receive_photo(msg: Message, state: FSMContext):
    data = await state.get_data()
    ch_id = data.get("ch_id")
    target_path = data.get("target_path") or []
    if not ch_id:
        await state.clear()
        await msg.answer("Ошибка: не найден канал. Откройте меню снова.")
        return
    u = await auth_get(msg.from_user.id)
    access = (u or {}).get("access") or {}
    if not u or not (u.get("role") == "admin" or access.get("settings.cm")):
        await state.clear()
        await msg.answer("⛔️ Нет доступа")
        return
    reg = _get_registry()
    ch = reg.get(str(ch_id)) or reg.get(ch_id)
    if not ch:
        await state.clear()
        await msg.answer("Канал не найден. Откройте меню снова.")
        return
    if u.get("role") != "admin" and not _is_owner(ch, msg.from_user.id):
        await state.clear()
        await msg.answer("⛔️ Нет доступа")
        return

    if not msg.photo:
        await msg.answer("Пришлите именно фото (не файл).")
        return

    photo = msg.photo[-1]

    ch_dir = COVERS_DIR / str(ch_id)
    ch_dir.mkdir(parents=True, exist_ok=True)

    base = "__".join(_safe_filename(p) for p in target_path) or "root"
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    out_path = ch_dir / f"{base}__{ts}.jpg"

    ok = False
    try:
        await msg.bot.download(photo, destination=out_path)
        ok = True
    except Exception:
        try:
            f = await msg.bot.get_file(photo.file_id)
            await msg.bot.download_file(f.file_path, destination=out_path)
            ok = True
        except Exception as e:
            await msg.answer(f"❌ Не удалось скачать фото: {e}")
            return

    if not ok or not out_path.exists():
        await msg.answer("❌ Фото не сохранилось (неизвестная ошибка).")
        return

    rel = out_path.relative_to(Path(__file__).resolve().parent).as_posix()
    _set_cover_for_path(str(ch_id), list(target_path), rel)

    await state.clear()

    path_txt = " / ".join(target_path) if target_path else "(корень)"
    await msg.answer(f"✅ Обложка сохранена для: <b>{path_txt}</b>", parse_mode="HTML")

    cb_like = type("Obj", (), {})()
    cb_like.message = msg
    cb_like.answer = (lambda *args, **kwargs: asyncio.sleep(0))
    cb_like.from_user = msg.from_user
    await _render_images_tree_for_channel(cb_like, ch_id=str(ch_id), current_path=target_path, edit=False)


# ---------- FIN: редактирование финального сообщения канала ----------
def _load_channel_final_message(ch_id: str) -> str:
    cfg = load_status_extra() or {}
    if not isinstance(cfg, dict):
        return ""
    full_peer_id = f"-100{ch_id}"
    return (cfg.get(full_peer_id) or "").strip()


def _store_channel_final_message(ch_id: str, text: str, username: Optional[str] = None) -> None:
    cfg = load_status_extra()
    if not isinstance(cfg, dict):
        cfg = {}

    full_peer_id = f"-100{ch_id}"

    cleaned = (text or "").strip()
    if not cleaned:
        cfg.pop(full_peer_id, None)
        if username:
            cfg.pop(username.lower(), None)
    else:
        cfg[full_peer_id] = cleaned

    save_status_extra(cfg)


@router.callback_query(F.data.startswith("cm:final:"))
async def cm_final_start(cb: CallbackQuery, state: FSMContext):
    ch_id = cb.data.split(":")[-1]

    _u, _reg, ch = await _get_channel_for_cb(cb, ch_id)
    if not ch:
        return
    title = ch.get("title") or ch.get("username") or ch_id

    current_text = _load_channel_final_message(ch_id)
    if current_text:
        current_block = f"Текущее финальное сообщение:\n\n<code>{current_text}</code>\n\n"
    else:
        current_block = "Финальное сообщение пока не задано.\n\n"

    await state.set_state(FinalMessageStates.waiting_for_text)
    await state.update_data(ch_id=ch_id)

    prompt = (
        f"✏️ Финальное сообщение для канала <b>{title}</b>\n\n"
        f"{current_block}"
        "Отправьте новый текст, который будет добавляться после строки "
        "<b>\"Цены и наличие обновлены.\"</b>\n\n"
        "Чтобы очистить финальное сообщение и ничего не добавлять — отправьте один дефис <code>-</code>."
    )

    await cb.message.edit_text(prompt, parse_mode="HTML")


@router.callback_query(F.data == "noop")
async def _noop(cb: CallbackQuery):
    await cb.answer()


@router.message(FinalMessageStates.waiting_for_text)
async def cm_final_save(msg: Message, state: FSMContext):
    data = await state.get_data()
    ch_id = data.get("ch_id")
    if not ch_id:
        await state.clear()
        await msg.answer("Канал не найден. Попробуйте ещё раз через меню каналов.")
        return
    u = await auth_get(msg.from_user.id)
    access = (u or {}).get("access") or {}
    if not u or not (u.get("role") == "admin" or access.get("settings.cm")):
        await state.clear()
        await msg.answer("⛔️ Нет доступа")
        return
    reg = _get_registry()
    ch = reg.get(str(ch_id)) or reg.get(ch_id)
    if not ch:
        await state.clear()
        await msg.answer("Канал не найден. Попробуйте ещё раз через меню каналов.")
        return
    if u.get("role") != "admin" and not _is_owner(ch, msg.from_user.id):
        await state.clear()
        await msg.answer("⛔️ Нет доступа")
        return

    new_text_raw = (msg.text or "").strip()
    new_text = "" if new_text_raw == "-" else new_text_raw

    title = ch.get("title") or ch.get("username") or ch_id
    username = (ch.get("username") or "").strip() or None

    _store_channel_final_message(ch_id, new_text, username=username)
    await state.clear()

    if new_text:
        text = (
            f"✅ Финальное сообщение обновлено для канала <b>{title}</b>.\n\n"
            f"Новый текст:\n<code>{new_text}</code>"
        )
    else:
        text = (
            f"✅ Финальное сообщение для канала <b>{title}</b> очищено.\n"
            f"Теперь после строки «Цены и наличие обновлены.» ничего добавляться не будет."
        )

    await msg.answer(
        text,
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=_kb_channel(ch)),
    )


# ---------- FIX: правильное формирование цели для Telethon.get_entity ----------
def _make_channel_ref(ch_id: str, ch: dict) -> str | int:
    username = (ch.get("username") or "").strip()
    if username:
        return username
    if ch_id.isdigit():
        return int(f"-100{ch_id}")
    return ch_id


# --- Обновление одного канала ---
@router.callback_query(F.data.startswith("cm:update:"))
async def cm_update_one(cb: CallbackQuery):
    ch_id = cb.data.split(":")[-1]
    _u, _reg, ch = await _get_channel_for_cb(cb, ch_id)
    if not ch:
        return

    mode = "opt" if ch.get("type") == "opt" else "retail"
    target = _make_channel_ref(ch_id, ch)

    try:
        result = await sync_channel(
            _get_client(),
            target,
            channel_mode=mode,
            aio_bot=cb.bot,
        )
    except Exception as e:
        await cb.answer(f"Ошибка обновления: {e}", show_alert=True)
        return

    msg = (
        f"✅ Обновление завершено.\n"
        f"Создано: {result['created']}\n"
        f"Отредактировано: {result['edited']}\n"
        f"Пропущено: {result['skipped']}\n"
        f"Удалено: {result['removed']}"
    )
    try:
        await cb.message.edit_text(msg, reply_markup=InlineKeyboardMarkup(inline_keyboard=_kb_channel(ch)))
    except TelegramBadRequest as e:
        if "message is not modified" in str(e).lower():
            pass
        else:
            raise


# --- Скрыть цены в одном opt-канале ---
@router.callback_query(F.data.startswith("cm:hide:"))
async def cm_hide_one(cb: CallbackQuery):
    ch_id = cb.data.split(":")[-1]
    _u, _reg, ch = await _get_channel_for_cb(cb, ch_id)
    if not ch:
        return
    await cb.answer("Скрываю…")

    if ch.get("type") != "opt":
        await cb.answer("Скрытие доступно только для оптовых каналов", show_alert=True)
        return

    target = _make_channel_ref(ch_id, ch)
    try:
        updated = await hide_opt_models(_get_client(), target, channel_mode="opt")
    except Exception as e:
        await cb.answer(f"Ошибка скрытия: {e}", show_alert=True)
        return

    msg = f"✅ Скрыто сообщений: {updated}"
    try:
        await cb.message.edit_text(msg, reply_markup=InlineKeyboardMarkup(inline_keyboard=_kb_channel(ch)))
    except TelegramBadRequest as e:
        if "message is not modified" in str(e).lower():
            pass
        else:
            await cb.message.answer(msg, reply_markup=InlineKeyboardMarkup(inline_keyboard=_kb_channel(ch)))
    except Exception:
        await cb.message.answer(msg, reply_markup=InlineKeyboardMarkup(inline_keyboard=_kb_channel(ch)))


@router.callback_query(F.data.startswith("cm:hide_menu:"))
async def cm_hide_menu(cb: CallbackQuery):
    ch_id = cb.data.split(":")[-1]
    _u, _reg, ch = await _get_channel_for_cb(cb, ch_id)
    if not ch:
        return
    if ch.get("type") != "opt":
        await cb.answer("Скрытие доступно только для оптовых каналов", show_alert=True)
        return
    ht = ch.get("hide_time") or "20:00"
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🙈 Скрыть сейчас", callback_data=f"cm:hide:{ch_id}")],
            [InlineKeyboardButton(text=f"⏰ Время скрытия: {ht}", callback_data=f"cm:hide_time:{ch_id}")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"cm:view:{ch_id}")],
        ]
    )
    await cb.message.edit_text("🙈 Скрытие цен", reply_markup=kb)


@router.callback_query(F.data.startswith("cm:hide_time:"))
async def cm_hide_time_start(cb: CallbackQuery, state: FSMContext):
    ch_id = cb.data.split(":")[-1]
    _u, _reg, ch = await _get_channel_for_cb(cb, ch_id)
    if not ch:
        return
    if ch.get("type") != "opt":
        await cb.answer("Скрытие доступно только для оптовых каналов", show_alert=True)
        return
    await state.set_state(HideTimeStates.waiting_for_time)
    await state.update_data(ch_id=ch_id)
    await cb.message.edit_text(
        "Введите время скрытия в формате HH:MM (МСК), например <code>20:00</code>.",
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data=f"cm:hide_menu:{ch_id}")]]
        ),
    )


@router.message(HideTimeStates.waiting_for_time)
async def cm_hide_time_save(msg: Message, state: FSMContext):
    data = await state.get_data()
    ch_id = data.get("ch_id")
    if not ch_id:
        await state.clear()
        await msg.answer("Канал не найден. Откройте меню снова.")
        return
    u = await auth_get(msg.from_user.id)
    access = (u or {}).get("access") or {}
    if not u or not (u.get("role") == "admin" or access.get("settings.cm")):
        await state.clear()
        await msg.answer("⛔️ Нет доступа")
        return
    reg = _get_registry()
    ch = reg.get(str(ch_id)) or reg.get(ch_id)
    if not ch:
        await state.clear()
        await msg.answer("Канал не найден. Откройте меню снова.")
        return
    if u.get("role") != "admin" and not _is_owner(ch, msg.from_user.id):
        await state.clear()
        await msg.answer("⛔️ Нет доступа")
        return

    text = (msg.text or "").strip()
    m = re.fullmatch(r"([01]?\d|2[0-3]):([0-5]\d)", text)
    if not m:
        await msg.answer("⚠️ Неверный формат. Введите время как HH:MM (например 20:00).")
        return
    hh, mm = m.group(1), m.group(2)
    ch["hide_time"] = f"{int(hh):02d}:{mm}"
    _save_registry(reg)
    await state.clear()
    await msg.answer("✅ Время скрытия сохранено.", reply_markup=InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data=f"cm:hide_menu:{ch_id}")]]
    ))


# --- Обновление всех каналов ---
@router.callback_query(F.data == "cm:update_all")
async def cm_update_all(cb: CallbackQuery):
    u = await _require_cm_access(cb)
    if not u:
        return
    reg = _get_registry()
    reg = _filter_registry_for_user(reg, cb.from_user.id, u.get("role") == "admin")
    total_created = total_edited = total_skipped = total_removed = 0
    total_channels = 0

    for ch_id, ch in list(reg.items()):
        mode = "opt" if ch.get("type") == "opt" else "retail"
        try:
            target = _make_channel_ref(ch_id, ch)
            result = await sync_channel(
                _get_client(),
                target,
                channel_mode=mode,
                aio_bot=cb.bot,
            )
            total_created += result["created"]
            total_edited += result["edited"]
            total_skipped += result["skipped"]
            total_removed += result["removed"]
            total_channels += 1
        except Exception:
            continue

    msg = (
        "📊 Сводка по всем каналам:\n"
        f"Каналов обновлено: {total_channels}\n"
        f"Создано: {total_created}\n"
        f"Отредактировано: {total_edited}\n"
        f"Пропущено: {total_skipped}\n"
        f"Удалено: {total_removed}"
    )
    try:
        updated_reg = _filter_registry_for_user(_get_registry(), cb.from_user.id, u.get("role") == "admin")
        await cb.message.edit_text(msg, reply_markup=InlineKeyboardMarkup(inline_keyboard=_kb_main(updated_reg)))
    except TelegramBadRequest as e:
        if "message is not modified" in str(e).lower():
            pass
        else:
            raise


# ---------- планировщик ежедневных анонсов ----------
async def schedule_daily_announcements(client):
    while True:
        now = datetime.now(timezone.utc)
        next_noon = now.replace(hour=12, minute=0, second=0, microsecond=0)
        if next_noon <= now:
            next_noon += timedelta(days=1)

        await asyncio.sleep((next_noon - now).total_seconds())

        reg = _get_registry()
        today = datetime.now(timezone.utc).date().isoformat()

        for ch_id, ch in list(reg.items()):
            if not ch.get("daily_announce", True):
                continue
            if ch.get("last_announce_date") == today:
                continue
            try:
                target = _make_channel_ref(ch_id, ch)
                await client.send_message(target, "Цены и наличие обновлены")
                ch["last_announce_date"] = today
                _save_registry(reg)
            except Exception:
                continue


async def schedule_daily_opt_hide(client):
    """
    В 20:00 МСК скрываем все модельные посты в opt-каналах (ставим ".").
    Тексты вернутся при следующем обновлении цен.
    """
    while True:
        now = datetime.now(MOSCOW_TZ)
        await asyncio.sleep(30)

        reg = _get_registry()
        today = now.date().isoformat()
        cur_hm = now.strftime("%H:%M")

        for ch_id, ch in list(reg.items()):
            if ch.get("type") != "opt":
                continue
            ht = (ch.get("hide_time") or "20:00").strip()
            if ht != cur_hm:
                continue
            last = ch.get("last_hide_at")
            if last == f"{today} {cur_hm}":
                continue
            try:
                target = _make_channel_ref(ch_id, ch)
                await hide_opt_models(_get_client(), target, channel_mode="opt")
                ch["last_hide_at"] = f"{today} {cur_hm}"
                _save_registry(reg)
            except Exception:
                continue
