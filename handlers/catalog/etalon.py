from aiogram import Router, F
from aiogram.types import CallbackQuery, Message, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from storage import load_data, save_data
from handlers.catalog.crud.models import build_model_index, model_index, sid_sr  # ✅ добавили sid_sr

router = Router()

# === FSM ===
class EtalonStates(StatesGroup):
    waiting_for_etalon = State()


# === Меню эталона у модели ===
async def render_etalon_menu(event, cat: str, br: str, sr: str, m: str, state: FSMContext):
    db = load_data()
    await state.update_data(category=cat, brand=br, series=sr, model=m)

    s_id = sid_sr(cat, br, sr)  # ✅ используем хэш-ид линейки

    current = db.get("etalon", {}).get(cat, {}).get(br, {}).get(sr, {}).get(m)
    if current:
        text = "📄 Текущий эталон:\n\n<pre>{}</pre>".format("\n".join(current))
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✏️ Редактировать эталон", callback_data="edit_etalon_text")],
            [InlineKeyboardButton(text="❌ Удалить эталон", callback_data="delete_etalon")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"nav_series:{s_id}")],  # ✅ фикс
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")]
        ])
    else:
        text = "ℹ️ Эталон пока не задан."
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="➕ Создать эталон", callback_data="edit_etalon_text")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"nav_series:{s_id}")],  # ✅ фикс
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")]
        ])

    target = getattr(event, "message", event)
    await target.answer(f"{text}\n\n📱 {cat} / {br} / {sr} / {m}", reply_markup=kb)


# === Вход в эталон при выборе модели ===
@router.callback_query(F.data.startswith("nav_model:"))
async def etalon_model(callback: CallbackQuery, state: FSMContext):
    _, m_id = callback.data.split(":", maxsplit=1)

    db = load_data()
    build_model_index(db)

    if m_id not in model_index:
        await callback.answer("⚠️ Модель не найдена", show_alert=True)
        return

    cat, br, sr, m = model_index[m_id]
    await render_etalon_menu(callback, cat, br, sr, m, state)


# === Редактирование эталона ===
@router.callback_query(F.data == "edit_etalon_text")
async def edit_etalon_text(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    data = await state.get_data()

    if not all(k in data for k in ("category", "brand", "series", "model")):
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📂 Каталог", callback_data="catalog_menu")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")]
        ])
        await callback.message.answer(
            "⚠️ Контекст потерян. Выберите модель заново через каталог.",
            reply_markup=kb
        )
        return

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_edit_etalon")]
    ])
    await state.set_state(EtalonStates.waiting_for_etalon)
    await callback.message.answer("✏️ Введи новый эталонный текст (каждая строка = вариант):", reply_markup=kb)


@router.callback_query(F.data == "cancel_edit_etalon")
async def cancel_edit_etalon(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    if not all(k in data for k in ("category", "brand", "series", "model")):
        await callback.answer("⚠️ Контекст потерян", show_alert=True)
        return

    cat, br, sr, m = data["category"], data["brand"], data["series"], data["model"]
    await state.clear()
    await render_etalon_menu(callback, cat, br, sr, m, state)



@router.message(EtalonStates.waiting_for_etalon)
async def save_etalon_text(msg: Message, state: FSMContext):
    data = await state.get_data()
    if not all(k in data for k in ("category", "brand", "series", "model")):
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📂 Каталог", callback_data="catalog_menu")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")]
        ])
        await msg.answer(
            "⚠️ Контекст потерян. Выберите модель заново через каталог.",
            reply_markup=kb
        )
        await state.clear()
        return

    cat, br, sr, m = data["category"], data["brand"], data["series"], data["model"]

    # сохраняем как список строк
    lines = msg.text.splitlines()
    db = load_data()
    db.setdefault("etalon", {}).setdefault(cat, {}).setdefault(br, {}).setdefault(sr, {})[m] = lines
    save_data(db)
    
    await state.clear()
    await msg.answer(f"✅ Эталон сохранён для <b>{m}</b>")
    await render_etalon_menu(msg, cat, br, sr, m, state)


# === Удаление эталона ===
@router.callback_query(F.data == "delete_etalon")
async def delete_etalon(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()

    if not all(k in data for k in ("category", "brand", "series", "model")):
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📂 Каталог", callback_data="catalog_menu")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")]
        ])
        await callback.message.answer(
            "⚠️ Контекст потерян. Выберите модель заново через каталог.",
            reply_markup=kb
        )
        return

    cat, br, sr, m = data["category"], data["brand"], data["series"], data["model"]

    db = load_data()
    db.setdefault("etalon", {}).setdefault(cat, {}).setdefault(br, {}).setdefault(sr, {}).pop(m, None)
    save_data(db)

    await callback.message.answer("🗑 Эталон удалён.")
    await render_etalon_menu(callback, cat, br, sr, m, state)
