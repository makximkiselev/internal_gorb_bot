# handlers/accounts.py
from aiogram import Router, F
from aiogram.types import CallbackQuery, Message, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from telethon import TelegramClient
from pathlib import Path
import json

router = Router()

# === Основной файл ===
SOURCES_FILE = Path("sources.json")
SESSIONS_DIR = Path("sessions")
SESSIONS_DIR.mkdir(exist_ok=True)

def ensure_sources_file():
    """Создаёт sources.json, если нет"""
    if not SOURCES_FILE.exists():
        data = {"accounts": [], "channels": [], "chats": [], "bots": []}
        SOURCES_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

def load_sources() -> dict:
    ensure_sources_file()
    try:
        data = json.loads(SOURCES_FILE.read_text(encoding="utf-8"))
    except Exception:
        data = {"accounts": [], "channels": [], "chats": [], "bots": []}
    for key in ("accounts", "channels", "chats", "bots"):
        data.setdefault(key, [])
    return data

def save_sources(data: dict):
    SOURCES_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

# === FSM ===
class AccountStates(StatesGroup):
    waiting_for_api_id = State()
    waiting_for_api_hash = State()
    waiting_for_name = State()
    waiting_for_phone = State()
    waiting_for_code = State()
    waiting_for_password = State()

# === Кнопки ===
def cancel_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_account")]
    ])

# === Главное меню ===
@router.callback_query(F.data == "accounts")
async def accounts_menu(callback: CallbackQuery):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить аккаунт", callback_data="add_account")],
        [InlineKeyboardButton(text="📋 Список аккаунтов", callback_data="list_accounts")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")]
    ])
    await callback.message.answer("👤 Управление аккаунтами:", reply_markup=kb)

# === Добавление ===
@router.callback_query(F.data == "add_account")
async def add_account(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AccountStates.waiting_for_api_id)
    await callback.message.answer("🔑 Введи API_ID:", reply_markup=cancel_kb())

@router.message(AccountStates.waiting_for_api_id)
async def process_api_id(msg: Message, state: FSMContext):
    await state.update_data(api_id=msg.text.strip())
    await state.set_state(AccountStates.waiting_for_api_hash)
    await msg.answer("🔑 Введи API_HASH:", reply_markup=cancel_kb())

@router.message(AccountStates.waiting_for_api_hash)
async def process_api_hash(msg: Message, state: FSMContext):
    await state.update_data(api_hash=msg.text.strip())
    await state.set_state(AccountStates.waiting_for_name)
    await msg.answer("✏️ Введи название аккаунта (например, apple_optom):", reply_markup=cancel_kb())

@router.message(AccountStates.waiting_for_name)
async def process_account_name(msg: Message, state: FSMContext):
    data = await state.get_data()
    api_id, api_hash = data["api_id"], data["api_hash"]
    name = msg.text.strip()

    db = load_sources()
    if any(acc["name"] == name for acc in db["accounts"]):
        await msg.answer(f"⚠️ Аккаунт <b>{name}</b> уже существует.", reply_markup=cancel_kb())
        await state.clear()
        return

    session_path = SESSIONS_DIR / f"{name}.session"
    client = TelegramClient(session_path, api_id, api_hash)

    await state.update_data(name=name, client=client)
    await state.set_state(AccountStates.waiting_for_phone)
    await msg.answer("📱 Введи номер телефона (в формате +79998887766):", reply_markup=cancel_kb())

@router.message(AccountStates.waiting_for_phone)
async def process_phone(msg: Message, state: FSMContext):
    phone = msg.text.strip()
    data = await state.get_data()
    client: TelegramClient = data["client"]

    await client.connect()
    try:
        await client.send_code_request(phone)
        await state.update_data(phone=phone)
        await state.set_state(AccountStates.waiting_for_code)
        await msg.answer("📩 Введи код подтверждения (например, 12345):", reply_markup=cancel_kb())
    except Exception as e:
        await msg.answer(f"❌ Ошибка при отправке кода: {e}")
        await state.clear()

@router.message(AccountStates.waiting_for_code)
async def process_code(msg: Message, state: FSMContext):
    code = msg.text.strip()
    data = await state.get_data()
    client: TelegramClient = data["client"]
    phone = data["phone"]

    try:
        await client.sign_in(phone=phone, code=code)
    except Exception as e:
        if "PASSWORD" in str(e).upper():
            await state.set_state(AccountStates.waiting_for_password)
            await msg.answer("🔒 Аккаунт защищён паролем. Введи пароль:", reply_markup=cancel_kb())
            return
        else:
            await msg.answer(f"❌ Ошибка авторизации: {e}")
            await state.clear()
            return

    await finish_auth(msg, state)

@router.message(AccountStates.waiting_for_password)
async def process_password(msg: Message, state: FSMContext):
    password = msg.text.strip()
    data = await state.get_data()
    client: TelegramClient = data["client"]

    try:
        await client.sign_in(password=password)
        await finish_auth(msg, state)
    except Exception as e:
        await msg.answer(f"❌ Ошибка авторизации с паролем: {e}")
        await state.clear()

async def finish_auth(msg: Message, state: FSMContext):
    data = await state.get_data()
    name, api_id, api_hash = data["name"], data["api_id"], data["api_hash"]
    session_path = f"sessions/{name}.session"

    db = load_sources()
    db["accounts"].append({
        "name": name,
        "api_id": api_id,
        "api_hash": api_hash,
        "session": session_path
    })
    save_sources(db)

    await msg.answer(f"✅ Аккаунт <b>{name}</b> успешно авторизован и сохранён.",
                     reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                         [InlineKeyboardButton(text="⬅️ Назад", callback_data="accounts")]
                     ]))
    await state.clear()

# === Список ===
@router.callback_query(F.data == "list_accounts")
async def list_accounts(callback: CallbackQuery):
    db = load_sources()
    accounts = db.get("accounts", [])

    if not accounts:
        await callback.message.answer("⚠️ Список аккаунтов пуст.",
                                      reply_markup=InlineKeyboardMarkup(
                                          inline_keyboard=[[InlineKeyboardButton(text="⬅️ Назад", callback_data="accounts")]]
                                      ))
        return

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"❌ {acc['name']}", callback_data=f"del_account:{acc['name']}")]
        for acc in accounts
    ] + [[InlineKeyboardButton(text="⬅️ Назад", callback_data="accounts")]])

    text = "👤 Список аккаунтов:\n" + "\n".join([f"• {acc['name']}" for acc in accounts])
    await callback.message.answer(text, reply_markup=kb)

# === Удаление ===
@router.callback_query(F.data.startswith("del_account:"))
async def delete_account(callback: CallbackQuery):
    _, name = callback.data.split(":")
    db = load_sources()
    db["accounts"] = [acc for acc in db["accounts"] if acc["name"] != name]
    save_sources(db)

    session_path = SESSIONS_DIR / f"{name}.session"
    if session_path.exists():
        session_path.unlink()

    await callback.answer(f"🗑 Аккаунт {name} удалён", show_alert=True)
    await list_accounts(callback)

# === Отмена ===
@router.callback_query(F.data == "cancel_account")
async def cancel_account(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.answer("❌ Добавление аккаунта отменено.",
                                  reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                      [InlineKeyboardButton(text="⬅️ Назад", callback_data="accounts")]
                                  ]))
