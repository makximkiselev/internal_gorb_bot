# handlers/chat_request.py

from aiogram import Router, F
from aiogram.types import CallbackQuery, Message, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from telethon_manager import get_all_clients
from handlers.auth_utils import auth_get
from pathlib import Path
import json

router = Router()

# === Путь к sources.json ===
SOURCES_FILE = Path("sources.json")


def load_sources() -> dict:
    """Загружаем источники из sources.json"""
    if not SOURCES_FILE.exists():
        SOURCES_FILE.write_text(
            json.dumps({"accounts": [], "channels": [], "chats": [], "bots": []}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    try:
        return json.loads(SOURCES_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {"accounts": [], "channels": [], "chats": [], "bots": []}


# --- helpers ---
def _norm(s: str | None) -> str:
    return (s or "").strip().lower()

def _strip_at(s: str | None) -> str:
    s = _norm(s)
    return s[1:] if s.startswith("@") else s


# === FSM ===
class ChatRequestStates(StatesGroup):
    waiting_for_text = State()


# === Главное меню ===
def request_menu_kb():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📨 Новый запрос", callback_data="send_request")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")],
        ]
    )


# === Старт ввода текста ===
@router.callback_query(F.data == "send_request")
async def send_request_start(callback: CallbackQuery, state: FSMContext):
    u = await auth_get(callback.from_user.id)
    if not u or not (u.get("role") == "admin" or (u.get("access") or {}).get("send_request")):
        await callback.answer("⛔️ Нет доступа", show_alert=True)
        return
    await state.set_state(ChatRequestStates.waiting_for_text)
    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="⬅️ Отмена", callback_data="cancel_request")]]
    )
    await callback.message.answer(
        "✍️ Введи текст запроса, который разослать по чатам (не каналам и не ботам):",
        reply_markup=kb,
    )


# === Отмена ===
@router.callback_query(F.data == "cancel_request")
async def cancel_request(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.answer("❌ Отмена. Меню запросов:", reply_markup=request_menu_kb())


# === Обработка текста и рассылка ===
@router.message(ChatRequestStates.waiting_for_text)
async def process_request_text(msg: Message, state: FSMContext):
    text = (msg.text or "").strip()
    if not text:
        await msg.answer("⚠️ Пустой запрос, попробуй ещё раз:")
        return

    # --- Источники ---
    sources = load_sources()
    chat_sources = sources.get("chats", []) or []
    if not chat_sources:
        await msg.answer("⚠️ Нет доступных чатов в списке источников.", reply_markup=request_menu_kb())
        await state.clear()
        return

    # --- Клиенты (из telethon_manager; там ключи и алиасы уже нормализованы) ---
    clients = get_all_clients()

    total_sent = 0
    total_failed = 0
    total_skipped = 0

    # Группируем чаты по аккаунтам
    chats_by_acc: dict[str, list[dict]] = {}
    for chat in chat_sources:
        acc_raw = chat.get("account") or ""
        acc_key = _strip_at(acc_raw)  # нормализуем (lower, без @)
        if not acc_key:
            continue
        chats_by_acc.setdefault(acc_key, []).append(chat)

    # Основной цикл
    for acc_key, chats in chats_by_acc.items():
        # берём клиента: допускаем обращение и по @username (в менеджере есть алиасы)
        client = clients.get(acc_key) or clients.get(f"@{acc_key}")
        if not client:
            # скрыто считаем пропуски, но не шумим в интерфейсе
            total_skipped += len(chats)
            continue

        # Подключаем при необходимости
        try:
            if not client.is_connected():
                await client.connect()
        except Exception:
            total_skipped += len(chats)
            continue

        # Проверяем авторизацию (в Telethon метод бывает sync/async)
        try:
            try:
                authorized = await client.is_user_authorized()  # async вариант
            except TypeError:
                authorized = client.is_user_authorized()        # sync вариант
        except Exception:
            authorized = False

        if not authorized:
            total_skipped += len(chats)
            continue

        # Отправка
        for chat in chats:
            chat_id = chat.get("chat_id") or chat.get("channel_id") or chat.get("id")
            if not chat_id:
                total_failed += 1
                continue
            try:
                await client.send_message(chat_id, text)
                total_sent += 1
            except Exception:
                total_failed += 1

    # Итог — КРАТКИЙ
    await msg.answer(
        "✅ Рассылка завершена!\n\n"
        f"💬 Отправлено: {total_sent}\n"
        f"⚠️ Ошибок: {total_failed}\n"
        f"⏸️ Пропущено: {total_skipped}\n\n"
        f"<b>Текст рассылки:</b>\n{text}",
        reply_markup=request_menu_kb(),
    )
    await state.clear()
