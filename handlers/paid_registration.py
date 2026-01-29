# handlers/paid_registration.py
from __future__ import annotations

from aiogram import Router, F
from aiogram.types import CallbackQuery, Message, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from telethon import TelegramClient
from pathlib import Path
from urllib.parse import quote
import asyncio

from handlers.auth_utils import auth_get, auth_set_paid_account

router = Router()

PAID_SESSIONS_DIR = Path("sessions") / "paid"
PAID_SESSIONS_DIR.mkdir(parents=True, exist_ok=True)


class PaidRegistrationStates(StatesGroup):
    waiting_for_api_id = State()
    waiting_for_api_hash = State()
    waiting_for_method = State()
    waiting_for_phone = State()
    waiting_for_code = State()
    waiting_for_password = State()
    waiting_for_qr_confirm = State()


def _cancel_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="paid_reg:cancel")]]
    )

def _method_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📩 Ввести код", callback_data="paid_reg:method:code")],
            [InlineKeyboardButton(text="🔳 QR-код", callback_data="paid_reg:method:qr")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="paid_reg:cancel")],
        ]
    )

def _qr_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Я отсканировал", callback_data="paid_reg:qr_check")],
            [InlineKeyboardButton(text="🔁 Новый QR", callback_data="paid_reg:qr_new")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="paid_reg:cancel")],
        ]
    )

def _qr_image_url(url: str) -> str:
    encoded = quote(url, safe="")
    return f"https://api.qrserver.com/v1/create-qr-code/?size=320x320&data={encoded}"


async def _ensure_paid_user(msg: Message, state: FSMContext) -> bool:
    u = await auth_get(msg.from_user.id)
    if not u or u.get("role") != "paid_user":
        await state.clear()
        await msg.answer("⛔️ Доступ запрещён.")
        return False
    return True


async def _ensure_paid_user_cb(cb: CallbackQuery, state: FSMContext) -> bool:
    u = await auth_get(cb.from_user.id)
    if not u or u.get("role") != "paid_user":
        await state.clear()
        await cb.message.answer("⛔️ Доступ запрещён.")
        return False
    return True


async def start_paid_registration(msg: Message, state: FSMContext) -> None:
    await state.clear()
    await state.set_state(PaidRegistrationStates.waiting_for_api_id)
    await msg.answer(
        "🔑 Введи API_ID (my.telegram.org):",
        reply_markup=_cancel_kb(),
    )


@router.callback_query(F.data == "paid_reg:cancel")
async def paid_reg_cancel(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.answer("❌ Регистрация отменена. Нажми /start чтобы начать заново.")


@router.message(PaidRegistrationStates.waiting_for_api_id)
async def paid_reg_api_id(msg: Message, state: FSMContext):
    if not await _ensure_paid_user(msg, state):
        return
    api_id = (msg.text or "").strip()
    if not api_id.isdigit():
        await msg.answer("⚠️ API_ID должен быть числом. Введи ещё раз:", reply_markup=_cancel_kb())
        return
    await state.update_data(api_id=api_id)
    await state.set_state(PaidRegistrationStates.waiting_for_api_hash)
    await msg.answer("🔑 Введи API_HASH:", reply_markup=_cancel_kb())


@router.message(PaidRegistrationStates.waiting_for_api_hash)
async def paid_reg_api_hash(msg: Message, state: FSMContext):
    if not await _ensure_paid_user(msg, state):
        return
    api_hash = (msg.text or "").strip()
    if not api_hash:
        await msg.answer("⚠️ API_HASH не может быть пустым. Введи ещё раз:", reply_markup=_cancel_kb())
        return
    await state.update_data(api_hash=api_hash)
    await state.set_state(PaidRegistrationStates.waiting_for_method)
    await msg.answer(
        "Выбери способ входа:",
        reply_markup=_method_kb(),
    )


@router.callback_query(F.data.startswith("paid_reg:method:"))
async def paid_reg_method(callback: CallbackQuery, state: FSMContext):
    if not await _ensure_paid_user_cb(callback, state):
        return
    data = await state.get_data()
    if not data.get("api_id") or not data.get("api_hash"):
        await callback.answer("Сначала укажи API_ID и API_HASH", show_alert=True)
        return
    method = callback.data.split(":")[2]
    await callback.answer()
    if method == "code":
        await state.set_state(PaidRegistrationStates.waiting_for_phone)
        await callback.message.answer("📱 Введи номер телефона (в формате +79998887766):", reply_markup=_cancel_kb())
        return
    if method == "qr":
        api_id = data["api_id"]
        api_hash = data["api_hash"]
        session_path = PAID_SESSIONS_DIR / f"{callback.from_user.id}.session"
        client = TelegramClient(session_path, api_id, api_hash)
        await client.connect()
        try:
            qr = await client.qr_login()
        except Exception as e:
            await callback.message.answer(f"❌ Не удалось создать QR: {e}")
            await state.clear()
            return
        await state.update_data(client=client, qr=qr)
        await state.set_state(PaidRegistrationStates.waiting_for_qr_confirm)
        img_url = _qr_image_url(qr.url)
        await callback.message.answer_photo(
            img_url,
            caption=(
                "🔳 Отсканируй QR-код в Telegram:\n"
                "Настройки → Устройства → Сканировать QR.\n\n"
                "После сканирования нажми «Я отсканировал»."
            ),
            reply_markup=_qr_kb(),
        )
        return
    await callback.message.answer("Неизвестный способ. Повтори /start.")


@router.message(PaidRegistrationStates.waiting_for_phone)
async def paid_reg_phone(msg: Message, state: FSMContext):
    if not await _ensure_paid_user(msg, state):
        return
    phone = (msg.text or "").strip()
    data = await state.get_data()
    api_id = data["api_id"]
    api_hash = data["api_hash"]

    session_path = PAID_SESSIONS_DIR / f"{msg.from_user.id}.session"
    client = TelegramClient(session_path, api_id, api_hash)

    await client.connect()
    try:
        await client.send_code_request(phone)
        await state.update_data(phone=phone, client=client)
        await state.set_state(PaidRegistrationStates.waiting_for_code)
        await msg.answer(
            "📩 Введи код подтверждения (например, 12345).\n"
            "Важно: не нажимай «Это не я» в уведомлении Telegram.",
            reply_markup=_cancel_kb(),
        )
    except Exception as e:
        await msg.answer(f"❌ Ошибка при отправке кода: {e}")
        await state.clear()


@router.message(PaidRegistrationStates.waiting_for_code)
async def paid_reg_code(msg: Message, state: FSMContext):
    if not await _ensure_paid_user(msg, state):
        return
    code = (msg.text or "").strip()
    data = await state.get_data()
    client: TelegramClient = data["client"]
    phone = data["phone"]

    try:
        await client.sign_in(phone=phone, code=code)
    except Exception as e:
        if "PASSWORD" in str(e).upper():
            await state.set_state(PaidRegistrationStates.waiting_for_password)
            await msg.answer("🔒 Аккаунт защищён паролем. Введи пароль:", reply_markup=_cancel_kb())
            return
        await msg.answer(f"❌ Ошибка авторизации: {e}")
        await state.clear()
        return

    await _finish_paid_auth(msg, state)


@router.message(PaidRegistrationStates.waiting_for_password)
async def paid_reg_password(msg: Message, state: FSMContext):
    if not await _ensure_paid_user(msg, state):
        return
    password = (msg.text or "").strip()
    data = await state.get_data()
    client: TelegramClient = data["client"]

    try:
        await client.sign_in(password=password)
        await _finish_paid_auth(msg, state)
    except Exception as e:
        await msg.answer(f"❌ Ошибка авторизации с паролем: {e}")
        await state.clear()


@router.callback_query(F.data == "paid_reg:qr_check")
async def paid_reg_qr_check(callback: CallbackQuery, state: FSMContext):
    if not await _ensure_paid_user_cb(callback, state):
        return
    data = await state.get_data()
    qr = data.get("qr")
    if not qr:
        await callback.answer("QR не найден, создай новый", show_alert=True)
        return
    await callback.answer("Проверяю вход...")
    try:
        await asyncio.wait_for(qr.wait(), timeout=60)
    except asyncio.TimeoutError:
        await callback.message.answer("⏳ Вход ещё не подтверждён. Отсканируй QR и нажми ещё раз.")
        return
    except Exception as e:
        await callback.message.answer(f"❌ Ошибка QR-входа: {e}")
        await state.clear()
        return
    await _finish_paid_auth(callback.message, state)


@router.callback_query(F.data == "paid_reg:qr_new")
async def paid_reg_qr_new(callback: CallbackQuery, state: FSMContext):
    if not await _ensure_paid_user_cb(callback, state):
        return
    data = await state.get_data()
    client: TelegramClient | None = data.get("client")
    if not client:
        await callback.answer("Сессия не найдена", show_alert=True)
        return
    await callback.answer()
    try:
        qr = await client.qr_login()
    except Exception as e:
        await callback.message.answer(f"❌ Не удалось создать QR: {e}")
        await state.clear()
        return
    await state.update_data(qr=qr)
    img_url = _qr_image_url(qr.url)
    await callback.message.answer_photo(
        img_url,
        caption=(
            "🔳 Отсканируй QR-код в Telegram:\n"
            "Настройки → Устройства → Сканировать QR.\n\n"
            "После сканирования нажми «Я отсканировал»."
        ),
        reply_markup=_qr_kb(),
    )


async def _finish_paid_auth(msg: Message, state: FSMContext):
    data = await state.get_data()
    api_id = data["api_id"]
    api_hash = data["api_hash"]
    phone = data.get("phone")
    account_name = f"paid_{msg.from_user.id}"
    session_path = f"sessions/paid/{msg.from_user.id}.session"

    paid_account = {
        "name": account_name,
        "api_id": api_id,
        "api_hash": api_hash,
        "phone": phone,
        "session": session_path,
        "status": "ready",
    }
    await auth_set_paid_account(msg.from_user.id, paid_account)

    await msg.answer("✅ Регистрация завершена. Нажми /start для входа в меню.")
    await state.clear()
