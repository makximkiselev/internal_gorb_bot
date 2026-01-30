import os
import signal
import multiprocessing
import uvicorn
import asyncio
import json
from pathlib import Path

from fastapi import FastAPI
from dotenv import load_dotenv
from contextlib import asynccontextmanager

from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
    User,
)
from aiogram.filters import CommandStart
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.fsm.context import FSMContext

# === Импорты проекта ===
from handlers.parsing import parser
from handlers.parsing import results  # ← роутер результатов (пагинация)
from telethon_manager import init_clients
from handlers.catalog import menu as catalog_menu
from handlers.catalog.crud import categories as cat_crud
from handlers.catalog.crud import brands as brand_crud
from handlers.catalog.crud import series as series_crud
from handlers.catalog.crud import models as model_crud
from handlers import accounts, sources, monitoring, view_prices, chat_request, paid_registration
from handlers.auto_replies import ui as auto_replies
from handlers.auto_replies.listener import register_auto_replies
from handlers.publishing import channel_manager_ui
from handlers.publishing.channel_manager_ui import schedule_daily_announcements, schedule_daily_opt_hide
from handlers.competitors.competitor_prices import (
    competitor_prices_daily_job,
    shutdown_playwright,
    shutdown_httpx,
)
from handlers.competitors import ui as competitors_ui

# 👇 новый импорт меню товарных чеков
from handlers.receipts import generator_ui as receipts_ui

# === Импорт обновления Google-таблицы ===
from gsheets_sync import update_prices_in_gsheet, schedule_gsheet_updates

# ✅ AUTH вынесли в handlers/auth_utils.py
from handlers.auth_utils import (
    ADMIN_USERNAME,
    PENDING_TEXT,
    AUTH_DRAFTS,
    auth_upsert_user,
    auth_get,
    auth_set_role,
    auth_list_by_role,
    auth_set_access,
    auth_toggle_access,
    auth_set_sources_mode,
    display_user,
    is_admin,
)

# === Конфиг ===
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN не задан в переменных окружения")

# =====================================================
#     Локальное хранилище юзеров бота (ID + ник/имя)
# =====================================================

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)
USERS_FILE = DATA_DIR / "bot_users.json"

# Вместо set[int] храним словарь: user_id -> данные
KNOWN_USERS: dict[int, dict] = {}


def load_known_users() -> dict[int, dict]:
    """
    Загрузить уже известных пользователей из bot_users.json.

    Поддерживаются два формата:
      1) Старый: [12345, 67890, ...]
      2) Новый: [{"id": 12345, "username": "...", "first_name": "...", ...}, ...]
    """
    global KNOWN_USERS

    if not USERS_FILE.exists():
        KNOWN_USERS = {}
        return KNOWN_USERS

    try:
        with USERS_FILE.open("r", encoding="utf-8") as f:
            data = json.load(f)

        users: dict[int, dict] = {}

        # Старый формат: просто список ID
        if isinstance(data, list) and all(isinstance(x, int) for x in data):
            for uid in data:
                users[int(uid)] = {
                    "id": int(uid),
                    "username": None,
                    "first_name": None,
                    "last_name": None,
                }

        # Новый формат: список словарей
        elif isinstance(data, list) and all(isinstance(x, dict) for x in data):
            for item in data:
                try:
                    uid = int(item.get("id"))
                except (TypeError, ValueError):
                    continue
                users[uid] = {
                    "id": uid,
                    "username": item.get("username"),
                    "first_name": item.get("first_name"),
                    "last_name": item.get("last_name"),
                }

        else:
            users = {}

        KNOWN_USERS = users
    except Exception as e:
        print(f"⚠️ Ошибка чтения {USERS_FILE}: {e}")
        KNOWN_USERS = {}

    return KNOWN_USERS


def remember_user(user: User) -> None:
    """
    Добавить/обновить пользователя в локальном хранилище.

    Сохраняем:
      - id
      - username
      - first_name
      - last_name
    """
    global KNOWN_USERS
    if not user:
        return

    uid = int(user.id)

    old_info = KNOWN_USERS.get(uid, {})
    new_info = {
        "id": uid,
        "username": user.username,
        "first_name": user.first_name,
        "last_name": user.last_name,
    }

    # Если данных не поменялись — ничего не делаем
    if old_info == new_info and uid in KNOWN_USERS:
        return

    KNOWN_USERS[uid] = new_info

    try:
        # Сохраняем как список словарей
        to_dump = sorted(KNOWN_USERS.values(), key=lambda x: x["id"])
        with USERS_FILE.open("w", encoding="utf-8") as f:
            json.dump(to_dump, f, ensure_ascii=False, indent=2)

        uname = f"@{user.username}" if user.username else ""
        name = (user.first_name or "") + ((" " + user.last_name) if user.last_name else "")
        name = name.strip()

        label_parts = [str(uid)]
        if uname:
            label_parts.append(uname)
        if name:
            label_parts.append(f"({name})")
        label = " ".join(label_parts)

        print(f"➕/🔄 Пользователь бота: {label}. Всего теперь: {len(KNOWN_USERS)}")
    except Exception as e:
        print(f"⚠️ Не удалось обновить файл с пользователями бота: {e}")


# =====================================================
#              ЗАПУСК AIOGRAM-БОТА
# =====================================================
def run_bot():
    async def _main():
        print("🚀 Запуск Aiogram polling...")

        # Загружаем ранее известных юзеров
        known = load_known_users()
        print(f"👥 Ранее ботом пользовались: {len(known)} человек(а)")

        bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        dp = Dispatcher()

        # === Главное меню (роль-зависимое) ===
        def _access_allowed(u: dict | None, key: str) -> bool:
            if not u:
                return False
            if u.get("role") == "admin":
                return True
            access = u.get("access") or {}
            return bool(access.get(key, False))

        def _any_access(u: dict, keys: list[str]) -> bool:
            return any(_access_allowed(u, k) for k in keys)

        def _main_menu_user(u: dict):
            role = u.get("role", "pending")
            # 👤 Обычный пользователь: только "Посмотреть цены"
            if role != "admin":
                rows = []
                if _access_allowed(u, "main.send_request"):
                    rows.append([InlineKeyboardButton(text="📨 Отправить запрос", callback_data="send_request")])
                if _any_access(u, ["products.catalog", "products.collect", "main.view_prices"]):
                    rows.append([InlineKeyboardButton(text="🧾 Товары и цены", callback_data="menu:products")])
                if _access_allowed(u, "sales.receipt"):
                    rows.append([InlineKeyboardButton(text="💰 Продажи", callback_data="menu:sales")])
                if _any_access(u, ["external.update_gsheet", "external.competitors"]):
                    rows.append([InlineKeyboardButton(text="📊 Внешние таблицы", callback_data="menu:external")])
                settings_keys = ["settings.auth", "settings.auto_replies", "settings.accounts", "settings.cm"]
                if u.get("role") == "admin" or u.get("sources_mode") in ("own", "custom"):
                    settings_keys.append("settings.sources")
                if _any_access(u, settings_keys):
                    rows.append([InlineKeyboardButton(text="⚙️ Настройки", callback_data="menu:settings")])
                if not rows:
                    rows = [[InlineKeyboardButton(text="🧾 Товары и цены", callback_data="menu:products")]]
                return InlineKeyboardMarkup(inline_keyboard=rows)

            # 🛡 Админ: новое главное меню (структурированное)
            kb = [
                [InlineKeyboardButton(text="📨 Отправить запрос", callback_data="send_request")],
                [InlineKeyboardButton(text="🧾 Товары и цены", callback_data="menu:products")],
                [InlineKeyboardButton(text="💰 Продажи", callback_data="menu:sales")],
                [InlineKeyboardButton(text="📊 Внешние таблицы", callback_data="menu:external")],
                [InlineKeyboardButton(text="⚙️ Настройки", callback_data="menu:settings")],
            ]
            return InlineKeyboardMarkup(inline_keyboard=kb)

        def role_label(role: str) -> str:
            if role == "admin":
                return "🛡 Администратор"
            if role == "paid_user":
                return "💼 Клиент"
            return "👤 Пользователь"

        ACCESS_GROUPS = [
            ("Главное меню", [
                ("main.send_request", "📨 Отправить запрос"),
            ]),
            ("Товары и цены", [
                ("products.catalog", "🛠 Каталог"),
                ("products.collect", "🏷 Собрать цены"),
                ("main.view_prices", "👁 Посмотреть цены"),
            ]),
            ("Продажи", [
                ("sales.receipt", "🧾 Товарный чек"),
            ]),
            ("Внешние таблицы", [
                ("external.update_gsheet", "🔄 Обновить Google таблицу"),
                ("external.competitors", "📊 Цены конкурентов"),
            ]),
            ("Настройки", [
                ("settings.auth", "🔐 Авторизация"),
                ("settings.sources", "📡 Источники"),
                ("settings.auto_replies", "🤖 Автоответы"),
                ("settings.accounts", "👤 Аккаунты"),
                ("settings.cm", "🗂 Управление каналами"),
            ]),
        ]

        def products_menu_kb(u: dict):
            rows = []
            if _access_allowed(u, "products.catalog"):
                rows.append([InlineKeyboardButton(text="🛠 Каталог", callback_data="catalog_menu")])
            if _access_allowed(u, "products.collect"):
                rows.append([InlineKeyboardButton(text="🏷 Собрать цены", callback_data="collect")])
            if _access_allowed(u, "main.view_prices"):
                rows.append([InlineKeyboardButton(text="👁 Посмотреть цены", callback_data="view_prices")])
            rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")])
            return InlineKeyboardMarkup(inline_keyboard=rows)

        def sales_menu_kb(u: dict):
            rows = []
            if _access_allowed(u, "sales.receipt"):
                rows.append([InlineKeyboardButton(text="🧾 Товарный чек", callback_data="receipt:menu")])
            rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")])
            return InlineKeyboardMarkup(inline_keyboard=rows)

        def external_tables_menu_kb(u: dict):
            rows = []
            if _access_allowed(u, "external.update_gsheet"):
                rows.append([InlineKeyboardButton(text="🔄 Обновить Google таблицу", callback_data="update_gsheet")])
            if _access_allowed(u, "external.competitors"):
                rows.append([InlineKeyboardButton(text="📊 Цены конкурентов", callback_data="competitors")])
            rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")])
            return InlineKeyboardMarkup(inline_keyboard=rows)

        def settings_menu_kb(u: dict):
            rows = []
            if _access_allowed(u, "settings.auth"):
                rows.append([InlineKeyboardButton(text="🔐 Авторизация", callback_data="auth:menu")])
            if _access_allowed(u, "settings.sources"):
                mode = u.get("sources_mode", "default")
                if u.get("role") == "admin" or mode in ("own", "custom"):
                    rows.append([InlineKeyboardButton(text="📡 Источники", callback_data="sources")])
            if _access_allowed(u, "settings.auto_replies"):
                rows.append([InlineKeyboardButton(text="🤖 Автоответы", callback_data="auto_replies")])
            if _access_allowed(u, "settings.accounts"):
                rows.append([InlineKeyboardButton(text="👤 Аккаунты", callback_data="accounts")])
            if _access_allowed(u, "settings.cm"):
                rows.append([InlineKeyboardButton(text="🗂 Управление каналами", callback_data="cm:open")])
            rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")])
            return InlineKeyboardMarkup(inline_keyboard=rows)

        # === /start (авторизация) ===
        @dp.message(CommandStart())
        async def start(msg: Message, state: FSMContext):
            if msg.from_user:
                remember_user(msg.from_user)

                u = await auth_upsert_user(msg.from_user, role_if_new="pending")

                # bootstrap админа по username
                if msg.from_user.username and msg.from_user.username.lower() == ADMIN_USERNAME.lower():
                    u = await auth_set_role(msg.from_user.id, "admin") or u

                role = u.get("role", "pending")

                if role in ("pending", "rejected"):
                    await msg.answer(PENDING_TEXT)
                    return

                if role == "paid_user":
                    paid = u.get("paid_account") or {}
                    if paid.get("status") != "ready":
                        await paid_registration.start_paid_registration(msg, state)
                        return

                await msg.answer("Привет 👋\nВыбери действие:", reply_markup=_main_menu_user(u))
                return

            await msg.answer(PENDING_TEXT)

        # === Назад в главное меню ===
        @dp.callback_query(F.data == "main_menu")
        async def back_to_main(callback: CallbackQuery):
            if callback.from_user:
                remember_user(callback.from_user)

            await callback.answer()

            u = await auth_get(callback.from_user.id)
            role = (u or {}).get("role", "pending")
            if role in ("pending", "rejected"):
                await callback.message.answer(PENDING_TEXT)
                return

            if not u:
                await callback.message.answer(PENDING_TEXT)
                return
            await callback.message.answer("Главное меню:", reply_markup=_main_menu_user(u))

        @dp.callback_query(F.data == "menu:products")
        async def open_products_menu(callback: CallbackQuery):
            if callback.from_user:
                remember_user(callback.from_user)
            await callback.answer()
            u = await auth_get(callback.from_user.id)
            role = (u or {}).get("role", "pending")
            if role in ("pending", "rejected"):
                await callback.message.answer(PENDING_TEXT)
                return
            if not _any_access(u, ["products.catalog", "products.collect", "main.view_prices"]):
                await callback.answer("⛔️ Нет доступа", show_alert=True)
                return
            await callback.message.answer("Товары и цены:", reply_markup=products_menu_kb(u))

        @dp.callback_query(F.data == "menu:sales")
        async def open_sales_menu(callback: CallbackQuery):
            if callback.from_user:
                remember_user(callback.from_user)
            await callback.answer()
            u = await auth_get(callback.from_user.id)
            role = (u or {}).get("role", "pending")
            if role in ("pending", "rejected"):
                await callback.message.answer(PENDING_TEXT)
                return
            if not _access_allowed(u, "sales.receipt"):
                await callback.answer("⛔️ Нет доступа", show_alert=True)
                return
            await callback.message.answer("Продажи:", reply_markup=sales_menu_kb(u))

        @dp.callback_query(F.data == "menu:external")
        async def open_external_menu(callback: CallbackQuery):
            if callback.from_user:
                remember_user(callback.from_user)
            await callback.answer()
            u = await auth_get(callback.from_user.id)
            role = (u or {}).get("role", "pending")
            if role in ("pending", "rejected"):
                await callback.message.answer(PENDING_TEXT)
                return
            if not _any_access(u, ["external.update_gsheet", "external.competitors"]):
                await callback.answer("⛔️ Нет доступа", show_alert=True)
                return
            await callback.message.answer("Внешние таблицы:", reply_markup=external_tables_menu_kb(u))

        @dp.callback_query(F.data == "menu:settings")
        async def open_settings_menu(callback: CallbackQuery):
            if callback.from_user:
                remember_user(callback.from_user)
            await callback.answer()
            u = await auth_get(callback.from_user.id)
            role = (u or {}).get("role", "pending")
            if role in ("pending", "rejected"):
                await callback.message.answer(PENDING_TEXT)
                return
            settings_keys = ["settings.auth", "settings.auto_replies", "settings.accounts", "settings.cm"]
            if u.get("role") == "admin" or u.get("sources_mode") in ("own", "custom"):
                settings_keys.append("settings.sources")
            if not _any_access(u, settings_keys):
                await callback.answer("⛔️ Нет доступа", show_alert=True)
                return
            await callback.message.answer("Настройки:", reply_markup=settings_menu_kb(u))

        # =====================================================
        # === Кнопка «Обновить Google таблицу» ===
        @dp.callback_query(F.data == "update_gsheet")
        async def on_update_gsheet(callback: CallbackQuery):
            if callback.from_user:
                remember_user(callback.from_user)

            if not await is_admin(callback.from_user.id):
                u = await auth_get(callback.from_user.id)
                if not _access_allowed(u, "external.update_gsheet"):
                    await callback.answer("⛔️ Нет доступа", show_alert=True)
                    return

            await callback.answer()
            msg = await callback.message.answer("⏳ Обновляю Google-таблицу…")

            async def _run():
                try:
                    rows_updated = await update_prices_in_gsheet()
                    await msg.edit_text(
                        f"✅ Готово.\n"
                        f"Обновлено строк: <b>{rows_updated}</b>"
                    )
                except Exception as e:
                    await msg.edit_text(
                        "❌ Ошибка обновления Google-таблицы:\n"
                        f"<code>{e}</code>"
                    )

            asyncio.create_task(_run())

        # =====================================================
        #               UI: Авторизация (admin-only)
        # =====================================================

        def kb_auth_root():
            return InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📥 Запросы", callback_data="auth:requests")],
                [InlineKeyboardButton(text="👥 Активные пользователи", callback_data="auth:active")],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")],
            ])

        @dp.callback_query(F.data == "auth:menu")
        async def auth_menu(callback: CallbackQuery):
            if not await is_admin(callback.from_user.id):
                await callback.answer("⛔️ Только для админов", show_alert=True)
                return
            await callback.answer()
            await callback.message.answer("🔐 Авторизация", reply_markup=kb_auth_root())

        # ---------- Запросы ----------

        def kb_requests_list(users: list[dict]):
            rows = []
            if not users:
                rows.append([InlineKeyboardButton(text="(пусто)", callback_data="noop")])
            else:
                for u in users:
                    rows.append([InlineKeyboardButton(
                        text=display_user(u),
                        callback_data=f"auth:req:{u['id']}"
                    )])
            rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="auth:menu")])
            return InlineKeyboardMarkup(inline_keyboard=rows)

        def kb_request_card(u: dict):
            role = u.get("role", "pending")
            role_txt = role_label(role)
            return InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=f"🔁 Роль: {role_txt}", callback_data=f"auth:toggle_req:{u['id']}")],
                [InlineKeyboardButton(text="✅ Одобрить", callback_data=f"auth:approve:{u['id']}")],
                [InlineKeyboardButton(text="❌ Отклонить", callback_data=f"auth:reject:{u['id']}")],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="auth:requests")],
            ])

        @dp.callback_query(F.data == "auth:requests")
        async def auth_requests(callback: CallbackQuery):
            if not await is_admin(callback.from_user.id):
                await callback.answer("⛔️ Только для админов", show_alert=True)
                return
            await callback.answer()
            pending = await auth_list_by_role("pending")
            await callback.message.answer("📥 Запросы на доступ:", reply_markup=kb_requests_list(pending))

        @dp.callback_query(F.data.startswith("auth:req:"))
        async def auth_req_open(callback: CallbackQuery):
            if not await is_admin(callback.from_user.id):
                await callback.answer("⛔️ Только для админов", show_alert=True)
                return
            await callback.answer()
            user_id = int(callback.data.split(":")[2])
            u = await auth_get(user_id)
            if not u:
                await callback.message.answer("❌ Пользователь не найден")
                return
            text = (
                f"👤 {display_user(u)}\n"
                f"ID: <code>{u['id']}</code>\n"
                f"Текущая роль: <b>{u.get('role')}</b>"
            )
            await callback.message.answer(text, reply_markup=kb_request_card(u))

        @dp.callback_query(F.data.startswith("auth:toggle_req:"))
        async def auth_toggle_req(callback: CallbackQuery):
            if not await is_admin(callback.from_user.id):
                await callback.answer("⛔️ Только для админов", show_alert=True)
                return
            user_id = int(callback.data.split(":")[2])
            u = await auth_get(user_id)
            if not u:
                await callback.answer("Не найден", show_alert=True)
                return

            cur = u.get("role", "pending")
            if cur == "pending":
                new = "user"
            elif cur == "user":
                new = "admin"
            elif cur == "admin":
                new = "paid_user"
            else:
                new = "user"
            await auth_set_role(user_id, new)

            u2 = await auth_get(user_id)
            await callback.answer("Роль переключена")
            text = (
                f"👤 {display_user(u2)}\n"
                f"ID: <code>{u2['id']}</code>\n"
                f"Текущая роль: <b>{u2.get('role')}</b>"
            )
            await callback.message.answer(text, reply_markup=kb_request_card(u2))

        @dp.callback_query(F.data.startswith("auth:approve:"))
        async def auth_approve(callback: CallbackQuery):
            if not await is_admin(callback.from_user.id):
                await callback.answer("⛔️ Только для админов", show_alert=True)
                return
            user_id = int(callback.data.split(":")[2])
            u = await auth_get(user_id)
            if not u:
                await callback.answer("Не найден", show_alert=True)
                return

            if u.get("role") == "pending":
                await auth_set_role(user_id, "user")

            await callback.answer("✅ Одобрено")
            u2 = await auth_get(user_id)
            if u2:
                role = u2.get("role", "user")
                if role == "paid_user":
                    text = (
                        "✅ Ваша заявка одобрена.\n"
                        "Пожалуйста, пройдите регистрацию: нажмите /start и следуйте инструкциям."
                    )
                else:
                    text = "✅ Ваша заявка одобрена. Нажмите /start."
                try:
                    await callback.message.bot.send_message(user_id, text)
                except Exception:
                    pass
            pending = await auth_list_by_role("pending")
            await callback.message.answer("📥 Запросы на доступ:", reply_markup=kb_requests_list(pending))

        @dp.callback_query(F.data.startswith("auth:reject:"))
        async def auth_reject(callback: CallbackQuery):
            if not await is_admin(callback.from_user.id):
                await callback.answer("⛔️ Только для админов", show_alert=True)
                return
            user_id = int(callback.data.split(":")[2])
            await auth_set_role(user_id, "rejected")
            await callback.answer("❌ Отклонено")
            try:
                await callback.message.bot.send_message(
                    user_id,
                    "❌ Ваша заявка отклонена. По вопросам пишите администратору.",
                )
            except Exception:
                pass
            pending = await auth_list_by_role("pending")
            await callback.message.answer("📥 Запросы на доступ:", reply_markup=kb_requests_list(pending))

        # ---------- Активные пользователи ----------

        def kb_active_root():
            return InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🛡 Администраторы", callback_data="auth:list:admin")],
                [InlineKeyboardButton(text="👤 Пользователи", callback_data="auth:list:user")],
                [InlineKeyboardButton(text="💼 Клиенты", callback_data="auth:list:paid_user")],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="auth:menu")],
            ])

        def kb_active_list(role: str, users: list[dict]):
            rows = []
            if not users:
                rows.append([InlineKeyboardButton(text="(пусто)", callback_data="noop")])
            else:
                for u in users:
                    rows.append([InlineKeyboardButton(
                        text=display_user(u),
                        callback_data=f"auth:edit:{u['id']}"
                    )])
            rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="auth:active")])
            return InlineKeyboardMarkup(inline_keyboard=rows)

        def kb_active_edit(admin_id: int, target: dict):
            draft = AUTH_DRAFTS.get(admin_id)
            new_role = (
                draft["new_role"]
                if draft and draft.get("target_id") == target["id"]
                else target.get("role", "user")
            )
            role_txt = role_label(new_role)
            back_role = target.get("role", "user")
            sources_mode = target.get("sources_mode", "default")
            sources_label = {
                "default": "✅ По умолчанию",
                "own": "👤 Только свои",
                "custom": "➕ Кастом",
            }.get(sources_mode, "✅ По умолчанию")
            return InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text=f"🔁 Роль: {role_txt}", callback_data=f"auth:toggle_edit:{target['id']}")],
                [InlineKeyboardButton(text="🔐 Доступы", callback_data=f"auth:access:{target['id']}")],
                [InlineKeyboardButton(text=f"🧩 Источники: {sources_label}", callback_data=f"auth:sources_cfg:{target['id']}")],
                [InlineKeyboardButton(text="🚫 Убрать доступ", callback_data=f"auth:remove_access:{target['id']}")],
                [InlineKeyboardButton(text="💾 Сохранить", callback_data=f"auth:save:{target['id']}")],
                [InlineKeyboardButton(text="❌ Отменить", callback_data=f"auth:cancel:{target['id']}")],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"auth:list:{back_role}")],
            ])

        @dp.callback_query(F.data == "auth:active")
        async def auth_active(callback: CallbackQuery):
            if not await is_admin(callback.from_user.id):
                await callback.answer("⛔️ Только для админов", show_alert=True)
                return
            await callback.answer()
            await callback.message.answer("👥 Активные пользователи:", reply_markup=kb_active_root())

        @dp.callback_query(F.data.startswith("auth:list:"))
        async def auth_list(callback: CallbackQuery):
            if not await is_admin(callback.from_user.id):
                await callback.answer("⛔️ Только для админов", show_alert=True)
                return
            role = callback.data.split(":")[2]
            await callback.answer()
            users = await auth_list_by_role(role)
            await callback.message.answer(f"Список: <b>{role}</b>", reply_markup=kb_active_list(role, users))

        @dp.callback_query(F.data.startswith("auth:edit:"))
        async def auth_edit_open(callback: CallbackQuery):
            if not await is_admin(callback.from_user.id):
                await callback.answer("⛔️ Только для админов", show_alert=True)
                return
            target_id = int(callback.data.split(":")[2])
            target = await auth_get(target_id)
            if not target:
                await callback.answer("Не найден", show_alert=True)
                return

            AUTH_DRAFTS[callback.from_user.id] = {
                "target_id": target_id,
                "new_role": target.get("role", "user"),
            }

            await callback.answer()
            text = (
                f"👤 {display_user(target)}\n"
                f"ID: <code>{target['id']}</code>\n"
                f"Текущая роль: <b>{target.get('role')}</b>"
            )
            await callback.message.answer(text, reply_markup=kb_active_edit(callback.from_user.id, target))

        @dp.callback_query(F.data.startswith("auth:toggle_edit:"))
        async def auth_toggle_edit(callback: CallbackQuery):
            if not await is_admin(callback.from_user.id):
                await callback.answer("⛔️ Только для админов", show_alert=True)
                return
            target_id = int(callback.data.split(":")[2])
            target = await auth_get(target_id)
            if not target:
                await callback.answer("Не найден", show_alert=True)
                return

            d = AUTH_DRAFTS.get(callback.from_user.id)
            if not d or d.get("target_id") != target_id:
                AUTH_DRAFTS[callback.from_user.id] = {
                    "target_id": target_id,
                    "new_role": target.get("role", "user"),
                }
                d = AUTH_DRAFTS[callback.from_user.id]

            if d["new_role"] == "user":
                d["new_role"] = "admin"
            elif d["new_role"] == "admin":
                d["new_role"] = "paid_user"
            else:
                d["new_role"] = "user"

            await callback.answer("Роль изменена (черновик)")
            await callback.message.edit_reply_markup(reply_markup=kb_active_edit(callback.from_user.id, target))

        def kb_access_edit(target: dict):
            access = target.get("access") or {}
            rows = []
            for group_name, items in ACCESS_GROUPS:
                rows.append([InlineKeyboardButton(text=f"— {group_name} —", callback_data="noop")])
                for key, label in items:
                    enabled = bool(access.get(key))
                    mark = "✅" if enabled else "❌"
                    rows.append([InlineKeyboardButton(
                        text=f"{mark} {label}",
                        callback_data=f"auth:access_toggle:{target['id']}:{key}",
                    )])
            rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=f"auth:edit:{target['id']}")])
            return InlineKeyboardMarkup(inline_keyboard=rows)

        @dp.callback_query(F.data.startswith("auth:access:"))
        async def auth_access_open(callback: CallbackQuery):
            if not await is_admin(callback.from_user.id):
                await callback.answer("⛔️ Только для админов", show_alert=True)
                return
            target_id = int(callback.data.split(":")[2])
            target = await auth_get(target_id)
            if not target:
                await callback.answer("Не найден", show_alert=True)
                return
            await callback.answer()
            await callback.message.answer(
                f"🔐 Доступы для {display_user(target)}",
                reply_markup=kb_access_edit(target),
            )

        @dp.callback_query(F.data.startswith("auth:access_toggle:"))
        async def auth_access_toggle(callback: CallbackQuery):
            if not await is_admin(callback.from_user.id):
                await callback.answer("⛔️ Только для админов", show_alert=True)
                return
            parts = callback.data.split(":")
            if len(parts) < 4:
                await callback.answer("Неверные данные", show_alert=True)
                return
            target_id = int(parts[2])
            key = parts[3]
            target = await auth_get(target_id)
            if not target:
                await callback.answer("Не найден", show_alert=True)
                return
            valid_keys = {k for _, items in ACCESS_GROUPS for k, _ in items}
            if key not in valid_keys:
                await callback.answer("Неизвестный доступ", show_alert=True)
                return
            await auth_toggle_access(target_id, key)
            target = await auth_get(target_id)
            await callback.answer("Обновлено")
            await callback.message.edit_reply_markup(reply_markup=kb_access_edit(target))

        def kb_sources_cfg(target: dict):
            mode = target.get("sources_mode", "default")
            rows = [
                [InlineKeyboardButton(
                    text=f"{'✅' if mode == 'default' else '❌'} По умолчанию",
                    callback_data=f"auth:sources_set:{target['id']}:default",
                )],
                [InlineKeyboardButton(
                    text=f"{'✅' if mode == 'own' else '❌'} Только свои",
                    callback_data=f"auth:sources_set:{target['id']}:own",
                )],
                [InlineKeyboardButton(
                    text=f"{'✅' if mode == 'custom' else '❌'} Кастом (наши + свои)",
                    callback_data=f"auth:sources_set:{target['id']}:custom",
                )],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"auth:edit:{target['id']}")],
            ]
            return InlineKeyboardMarkup(inline_keyboard=rows)

        @dp.callback_query(F.data.startswith("auth:sources_cfg:"))
        async def auth_sources_cfg(callback: CallbackQuery):
            if not await is_admin(callback.from_user.id):
                await callback.answer("⛔️ Только для админов", show_alert=True)
                return
            target_id = int(callback.data.split(":")[2])
            target = await auth_get(target_id)
            if not target:
                await callback.answer("Не найден", show_alert=True)
                return
            await callback.answer()
            await callback.message.answer(
                f"🧩 Источники для {display_user(target)}",
                reply_markup=kb_sources_cfg(target),
            )

        @dp.callback_query(F.data.startswith("auth:sources_set:"))
        async def auth_sources_set(callback: CallbackQuery):
            if not await is_admin(callback.from_user.id):
                await callback.answer("⛔️ Только для админов", show_alert=True)
                return
            parts = callback.data.split(":")
            if len(parts) < 4:
                await callback.answer("Неверные данные", show_alert=True)
                return
            target_id = int(parts[2])
            value = parts[3]
            target = await auth_get(target_id)
            if not target:
                await callback.answer("Не найден", show_alert=True)
                return
            if value not in ("default", "own", "custom"):
                await callback.answer("Неверный режим", show_alert=True)
                return
            await auth_set_sources_mode(target_id, value)
            target = await auth_get(target_id)
            await callback.answer("Обновлено")
            await callback.message.edit_reply_markup(reply_markup=kb_sources_cfg(target))

        @dp.callback_query(F.data.startswith("auth:remove_access:"))
        async def auth_remove_access(callback: CallbackQuery):
            if not await is_admin(callback.from_user.id):
                await callback.answer("⛔️ Только для админов", show_alert=True)
                return
            target_id = int(callback.data.split(":")[2])
            target = await auth_get(target_id)
            if not target:
                await callback.answer("Не найден", show_alert=True)
                return
            if target.get("role") == "admin":
                await callback.answer("Нельзя убрать доступ у администратора", show_alert=True)
                return
            await auth_set_role(target_id, "pending")
            await callback.answer("Доступ убран")
            try:
                await callback.message.bot.send_message(
                    target_id,
                    "⛔️ Ваш доступ отозван. Статус изменён на ожидание подтверждения.",
                )
            except Exception:
                pass
            await callback.message.answer("✅ Доступ убран, статус: pending", reply_markup=kb_active_root())

        @dp.callback_query(F.data.startswith("auth:save:"))
        async def auth_save_edit(callback: CallbackQuery):
            if not await is_admin(callback.from_user.id):
                await callback.answer("⛔️ Только для админов", show_alert=True)
                return
            target_id = int(callback.data.split(":")[2])
            d = AUTH_DRAFTS.get(callback.from_user.id)
            if not d or d.get("target_id") != target_id:
                await callback.answer("Нет изменений", show_alert=True)
                return

            new_role = d["new_role"]

            # защита: не даём снять админку с самого себя
            if target_id == callback.from_user.id and new_role != "admin":
                await callback.answer("Нельзя снять админку с самого себя", show_alert=True)
                return

            await auth_set_role(target_id, new_role)
            AUTH_DRAFTS.pop(callback.from_user.id, None)

            await callback.answer("✅ Сохранено")
            target = await auth_get(target_id)
            text = (
                f"✅ Роль сохранена\n\n"
                f"👤 {display_user(target)}\n"
                f"ID: <code>{target['id']}</code>\n"
                f"Новая роль: <b>{target.get('role')}</b>"
            )
            await callback.message.answer(text, reply_markup=kb_active_root())

        @dp.callback_query(F.data.startswith("auth:cancel:"))
        async def auth_cancel_edit(callback: CallbackQuery):
            if not await is_admin(callback.from_user.id):
                await callback.answer("⛔️ Только для админов", show_alert=True)
                return
            AUTH_DRAFTS.pop(callback.from_user.id, None)
            await callback.answer("Отменено")
            await callback.message.answer("👥 Активные пользователи:", reply_markup=kb_active_root())

        @dp.callback_query(F.data == "noop")
        async def noop(callback: CallbackQuery):
            await callback.answer()

        # === Подключаем роутеры ===
        dp.include_router(results.router)
        dp.include_router(catalog_menu.router)
        dp.include_router(accounts.router)
        dp.include_router(paid_registration.router)
        dp.include_router(sources.router)
        dp.include_router(monitoring.router)
        dp.include_router(parser.router)
        dp.include_router(view_prices.router)
        dp.include_router(cat_crud.router)
        dp.include_router(brand_crud.router)
        dp.include_router(series_crud.router)
        dp.include_router(model_crud.router)
        dp.include_router(chat_request.router)
        dp.include_router(auto_replies.router)
        dp.include_router(channel_manager_ui.router)
        dp.include_router(competitors_ui.router)
        dp.include_router(receipts_ui.router)

        # --- Telethon ---
        clients = await init_clients()

        connected_count = len(clients)
        if connected_count == 0:
            print("⚠️ Ни одного Telethon-аккаунта не подключено.")
        else:
            acc_list = ", ".join(clients.keys())
            print(f"🔌 Подключено аккаунтов к боту: {connected_count} (используются: {acc_list})")

        first_client = next(iter(clients.values()), None)
        if first_client:
            channel_manager_ui.attach_telethon_client(first_client)
            asyncio.create_task(schedule_daily_announcements(first_client))
            asyncio.create_task(schedule_daily_opt_hide(first_client))

        for acc_name, client in clients.items():
            register_auto_replies(client, acc_name)

        # Мониторинг цен
        asyncio.create_task(monitoring.monitoring_loop())

        # Google-таблица (11–19 МСК каждый час)
        asyncio.create_task(schedule_gsheet_updates())

        # ✅ Конкуренты: цены (каждый день в 12:00 МСК)
        asyncio.create_task(competitor_prices_daily_job())

        try:
            await dp.start_polling(bot)
        except asyncio.CancelledError:
            pass
        finally:
            # ✅ закрываем aiohttp-сессию aiogram
            await bot.session.close()

            # ✅ закрываем shared-клиент httpx и Chromium (Playwright cache)
            try:
                await shutdown_playwright()
            except Exception:
                pass
            try:
                await shutdown_httpx()
            except Exception:
                pass

            print("🛑 Polling остановлен")


    asyncio.run(_main())


# =====================================================
#        Lifespan FastAPI (современная замена on_event)
# =====================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    print("🧩 FastAPI запускается — бот стартует отдельно.")
    yield
    print("🧩 FastAPI завершает работу.")


app = FastAPI(title="UnderPrice Platform", lifespan=lifespan)


def start_server():
    """Главный цикл FastAPI + управление процессом бота."""
    bot_process = multiprocessing.Process(target=run_bot)
    bot_process.start()
    print(f"🤖 Процесс бота запущен (PID {bot_process.pid})")

    try:
        uvicorn.run(
            "main:app",
            host="0.0.0.0",
            port=8000,
            reload=True,
            reload_dirs=["handlers", "."],
            log_level="info",
        )
    except KeyboardInterrupt:
        print("🛑 Остановка по Ctrl+C")
    finally:
        print("🛑 Остановка подпроцесса бота...")
        if bot_process.is_alive():
            os.kill(bot_process.pid, signal.SIGTERM)
            bot_process.join(timeout=3)
        print("✅ Сервер и бот завершены.")


if __name__ == "__main__":
    start_server()
