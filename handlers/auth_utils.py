# handlers/auth_utils.py
from __future__ import annotations

import os
import json
import asyncio
from pathlib import Path
from typing import Dict, List, Optional, Any

# =====================================================
#              AUTH (JSON): роли и доступы
# =====================================================

ADMIN_USERNAME = os.getenv("ADMIN_USERNAME", "apple_optom2")  # без @

# project root = .../Under_price_final
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

AUTH_FILE = DATA_DIR / "auth_users.json"
AUTH_LOCK = asyncio.Lock()

# Черновики для редактирования роли в "Активные пользователи"
# key: admin_id -> {"target_id": int, "new_role": str}
AUTH_DRAFTS: dict[int, dict] = {}

PENDING_TEXT = (
    "Ваша заявка принята.\n"
    "Ожидайте подтверждения от администраторов.\n\n"
    "По вопросам пишите @apple_optom2.\n"
    "Чтобы обновить статус нажмите /start"
)


def _auth_default() -> dict:
    return {"version": 1, "users": {}}

_ACCESS_KEYS = [
    "main.view_prices",
    "main.send_request",
    "products.catalog",
    "products.collect",
    "sales.receipt",
    "external.update_gsheet",
    "external.competitors",
    "settings.auth",
    "settings.sources",
    "settings.auto_replies",
    "settings.accounts",
    "settings.cm",
]


def _default_access(role: str | None = None) -> dict:
    if role == "paid_user":
        access = {k: False for k in _ACCESS_KEYS}
        access["main.send_request"] = True
        access["main.view_prices"] = True
        access["products.collect"] = True
        access["settings.cm"] = True
        return access
    # default behavior (admin/user) — full access
    return {k: True for k in _ACCESS_KEYS}

def _default_sources_mode() -> str:
    return "default"

def _normalize_access(access: dict | None, role: str | None = None) -> dict:
    if not isinstance(access, dict):
        access = {}

    # миграция со старых ключей
    old_to_main = {
        "view_prices": "main.view_prices",
        "send_request": "main.send_request",
    }
    for old, new in old_to_main.items():
        if old in access and new not in access:
            access[new] = bool(access.get(old))

    if access.get("menu_products") or access.get("main.products"):
        access.setdefault("products.catalog", True)
        access.setdefault("products.collect", True)
    if access.get("menu_sales") or access.get("main.sales"):
        access.setdefault("sales.receipt", True)
    if access.get("menu_external") or access.get("main.external"):
        access.setdefault("external.update_gsheet", True)
        access.setdefault("external.competitors", True)
    if access.get("menu_settings") or access.get("main.settings"):
        access.setdefault("settings.auth", True)
        access.setdefault("settings.sources", True)
        access.setdefault("settings.auto_replies", True)
        access.setdefault("settings.accounts", True)
        access.setdefault("settings.cm", True)

    defaults = _default_access(role)
    for k, v in defaults.items():
        access.setdefault(k, v)
    return access


async def auth_load() -> dict:
    async with AUTH_LOCK:
        if not AUTH_FILE.exists():
            return _auth_default()
        try:
            with AUTH_FILE.open("r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"⚠️ Ошибка чтения {AUTH_FILE}: {e}")
            return _auth_default()


async def auth_save(doc: dict) -> None:
    async with AUTH_LOCK:
        tmp = str(AUTH_FILE) + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(doc, f, ensure_ascii=False, indent=2)
        os.replace(tmp, AUTH_FILE)


async def auth_upsert_user(tg_user: Any, role_if_new: str = "pending") -> dict:
    """
    tg_user: aiogram.types.User (или объект с полями id/username/first_name/last_name)
    """
    doc = await auth_load()
    uid = str(int(tg_user.id))
    users = doc.setdefault("users", {})

    if uid not in users:
        users[uid] = {
            "id": int(tg_user.id),
            "username": getattr(tg_user, "username", None),
            "first_name": getattr(tg_user, "first_name", None),
            "last_name": getattr(tg_user, "last_name", None),
            "role": role_if_new,  # pending/user/admin/rejected
            "paid_account": None,
        "access": _default_access(role_if_new),
            "sources_mode": _default_sources_mode(),
        }
        await auth_save(doc)
        return users[uid]

    u = users[uid]
    # обновляем профильные поля (роль не трогаем)
    u["username"] = getattr(tg_user, "username", None)
    u["first_name"] = getattr(tg_user, "first_name", None)
    u["last_name"] = getattr(tg_user, "last_name", None)
    u.setdefault("paid_account", None)
    u["access"] = _normalize_access(u.get("access"), u.get("role"))
    u.setdefault("sources_mode", _default_sources_mode())
    await auth_save(doc)
    return u


async def auth_set_paid_account(user_id: int, paid_account: dict | None) -> Optional[dict]:
    doc = await auth_load()
    uid = str(int(user_id))
    users = doc.setdefault("users", {})
    if uid not in users:
        return None
    users[uid]["paid_account"] = paid_account
    await auth_save(doc)
    return users[uid]


async def auth_set_access(user_id: int, access: dict) -> Optional[dict]:
    doc = await auth_load()
    uid = str(int(user_id))
    users = doc.setdefault("users", {})
    if uid not in users:
        return None
    users[uid]["access"] = access
    await auth_save(doc)
    return users[uid]


async def auth_toggle_access(user_id: int, key: str) -> Optional[dict]:
    doc = await auth_load()
    uid = str(int(user_id))
    users = doc.setdefault("users", {})
    if uid not in users:
        return None
    role = users[uid].get("role")
    access = users[uid].setdefault("access", _default_access(role))
    access[key] = not bool(access.get(key))
    await auth_save(doc)
    return users[uid]

async def auth_set_sources_mode(user_id: int, mode: str) -> Optional[dict]:
    doc = await auth_load()
    uid = str(int(user_id))
    users = doc.setdefault("users", {})
    if uid not in users:
        return None
    users[uid]["sources_mode"] = mode
    await auth_save(doc)
    return users[uid]


async def auth_get(user_id: int) -> Optional[dict]:
    doc = await auth_load()
    uid = str(int(user_id))
    u = doc.get("users", {}).get(uid)
    if not u:
        return None
    changed = False
    u["access"] = _normalize_access(u.get("access"), u.get("role"))
    if "sources_mode" not in u:
        if "use_default_sources" in u:
            u["sources_mode"] = "default" if u.get("use_default_sources") else "own"
        else:
            u["sources_mode"] = _default_sources_mode()
        changed = True
    if changed:
        await auth_save(doc)
    return u


async def auth_set_role(user_id: int, role: str) -> Optional[dict]:
    doc = await auth_load()
    uid = str(int(user_id))
    users = doc.setdefault("users", {})
    if uid not in users:
        return None
    users[uid]["role"] = role
    if role == "paid_user":
        users[uid]["access"] = _default_access("paid_user")
    await auth_save(doc)
    return users[uid]


async def auth_list_by_role(role: str) -> List[dict]:
    doc = await auth_load()
    arr = list(doc.get("users", {}).values())
    return [u for u in arr if u.get("role") == role]


def display_user(u: dict) -> str:
    uname = f"@{u['username']}" if u.get("username") else ""
    name = " ".join([x for x in [u.get("first_name"), u.get("last_name")] if x]).strip()
    if uname and name:
        return f"{uname} ({name})"
    return uname or name or str(u.get("id"))


async def is_admin(user_id: int) -> bool:
    u = await auth_get(user_id)
    return bool(u and u.get("role") == "admin")
