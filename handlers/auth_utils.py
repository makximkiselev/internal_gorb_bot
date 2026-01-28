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
        }
        await auth_save(doc)
        return users[uid]

    u = users[uid]
    # обновляем профильные поля (роль не трогаем)
    u["username"] = getattr(tg_user, "username", None)
    u["first_name"] = getattr(tg_user, "first_name", None)
    u["last_name"] = getattr(tg_user, "last_name", None)
    await auth_save(doc)
    return u


async def auth_get(user_id: int) -> Optional[dict]:
    doc = await auth_load()
    return doc.get("users", {}).get(str(int(user_id)))


async def auth_set_role(user_id: int, role: str) -> Optional[dict]:
    doc = await auth_load()
    uid = str(int(user_id))
    users = doc.setdefault("users", {})
    if uid not in users:
        return None
    users[uid]["role"] = role
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
