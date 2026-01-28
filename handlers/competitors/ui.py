from aiogram import Router, F
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
import asyncio

from handlers.competitors.competitor_prices import competitor_prices_run_once
from handlers.auth_utils import is_admin  # ✅ вынести из main, иначе циклический импорт

router = Router(name="competitors_ui")


# ===================== КЛАВИАТУРЫ =====================

def kb_competitors_root():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="▶️ Запустить парсер", callback_data="competitors:run")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")],
    ])


def kb_after_run():
    """
    Клава, которую показываем после успеха/ошибки,
    чтобы пользователь не "зависал" и мог выйти.
    """
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="▶️ Запустить ещё раз", callback_data="competitors:run")],
        [InlineKeyboardButton(text="⬅️ Главное меню", callback_data="main_menu")],
    ])


# ===================== ХЕНДЛЕРЫ =====================

@router.callback_query(F.data == "competitors")
async def competitors_menu(callback: CallbackQuery):
    user = callback.from_user
    if not user or not await is_admin(user.id):
        await callback.answer("⛔️ Только для админов", show_alert=True)
        return

    await callback.answer()

    text = (
        "📊 <b>Цены конкурентов</b>\n\n"
        "• Store77\n"
        "• CordStore\n"
        "• BigGeek\n"
        "• Upstore24\n"
        "• Appmistore\n"
        "• Alikson\n\n"
        "Можно запустить сбор вручную."
    )

    # чтобы не плодить сообщения — пробуем отредактировать текущее
    try:
        await callback.message.edit_text(text, reply_markup=kb_competitors_root())
    except Exception:
        await callback.message.answer(text, reply_markup=kb_competitors_root())


@router.callback_query(F.data == "competitors:run")
async def competitors_run(callback: CallbackQuery):
    user = callback.from_user
    if not user or not await is_admin(user.id):
        await callback.answer("⛔️ Только для админов", show_alert=True)
        return

    await callback.answer()
    msg = await callback.message.answer("⏳ Запускаю парсер конкурентов…")

    async def _run():
        try:
            updated = await competitor_prices_run_once()
            await msg.edit_text(
                f"✅ Готово. Обновлено цен: <b>{updated}</b>",
                reply_markup=kb_after_run(),
            )
        except Exception as e:
            await msg.edit_text(
                "❌ Ошибка при сборе цен конкурентов:\n"
                f"<code>{e}</code>",
                reply_markup=kb_after_run(),
            )

    asyncio.create_task(_run())
