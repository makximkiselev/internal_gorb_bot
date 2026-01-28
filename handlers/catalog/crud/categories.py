from aiogram import Router, F
from aiogram.types import CallbackQuery, Message, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from storage import load_data, save_data

router = Router()

# === FSM ===
class CatStates(StatesGroup):
    waiting_for_name = State()
    choosing_item = State()
    confirming_action = State()

# ================== Навигация ==================
@router.callback_query(F.data == "nav_catalog")
async def nav_catalog(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await show_categories_menu(callback, "📂 Категории")

# ================== Меню категорий ==================
async def show_categories_menu(msg_or_cb, title: str = "📂 Категории"):
    db = load_data()
    cats = list(db.get("etalon", {}).keys())

    kb = [
        [InlineKeyboardButton(text=c, callback_data=f"nav_cat:{c}")]
        for c in cats
    ]
    kb += [
        [InlineKeyboardButton(text="➕ Добавить категорию", callback_data="cat_add")],
        [InlineKeyboardButton(text="✏️ Переименовать категорию", callback_data="cat_rename")],
        [InlineKeyboardButton(text="❌ Удалить категорию", callback_data="cat_delete")],
        [InlineKeyboardButton(text="🔀 Сортировка", callback_data="cat_sort")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="catalog_menu")]
    ]

    target = msg_or_cb.message if isinstance(msg_or_cb, CallbackQuery) else msg_or_cb
    await target.answer(title, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))


# ================== Добавление ==================
@router.callback_query(F.data == "cat_add")
async def cat_add(callback: CallbackQuery, state: FSMContext):
    await state.set_state(CatStates.waiting_for_name)
    await state.set_data({"action": "add_cat"})

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_edit")]
    ])
    await callback.message.answer("✏️ Введи название новой категории:", reply_markup=kb)


# ================== Переименование ==================
@router.callback_query(F.data == "cat_rename")
async def cat_rename(callback: CallbackQuery, state: FSMContext):
    db = load_data()
    cats = list(db.get("etalon", {}).keys())

    if not cats:
        await callback.message.answer("⚠️ Категорий пока нет.")
        return

    kb = [
        [InlineKeyboardButton(text=c, callback_data=f"choose_cat_rename:{c}")]
        for c in cats
    ]
    kb.append([InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_edit")])

    await state.set_state(CatStates.choosing_item)
    await callback.message.answer("Выбери категорию для переименования:", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))


@router.callback_query(F.data.startswith("choose_cat_rename:"))
async def choose_cat_for_rename(callback: CallbackQuery, state: FSMContext):
    _, cat = callback.data.split(":")
    await state.set_state(CatStates.waiting_for_name)
    await state.set_data({"action": "rename_cat", "selected": cat})
    await callback.message.answer(f"✏️ Введи новое имя для категории <b>{cat}</b>:")


# ================== Удаление ==================
@router.callback_query(F.data == "cat_delete")
async def cat_delete(callback: CallbackQuery, state: FSMContext):
    db = load_data()
    cats = list(db.get("etalon", {}).keys())

    if not cats:
        await callback.message.answer("⚠️ Категорий пока нет.")
        return

    kb = [
        [InlineKeyboardButton(text=c, callback_data=f"confirm_cat_delete:{c}")]
        for c in cats
    ]
    kb.append([InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_edit")])

    await callback.message.answer("Выбери категорию для удаления:", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))


@router.callback_query(F.data.startswith("confirm_cat_delete:"))
async def confirm_cat_delete(callback: CallbackQuery, state: FSMContext):
    _, cat = callback.data.split(":")
    db = load_data()

    has_nested = bool(db.get("etalon", {}).get(cat))

    if has_nested:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"cat_confirm_delete_final:{cat}")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_edit")]
        ])
        await callback.message.answer(
            f"⚠️ В категории <b>{cat}</b> есть вложенные бренды, линейки, модели или эталоны.\n"
            f"Удаление приведёт к потере всех связанных данных.\n\nПродолжить?",
            reply_markup=kb
        )
    else:
        db.get("etalon", {}).pop(cat, None)
        save_data(db)
        await callback.message.answer(f"🗑 Категория <b>{cat}</b> удалена.")
        await show_categories_menu(callback, "📂 Обновлённые категории:")


@router.callback_query(F.data.startswith("cat_confirm_delete_final:"))
async def cat_confirm_delete_final(callback: CallbackQuery, state: FSMContext):
    _, cat = callback.data.split(":")
    db = load_data()

    if "etalon" in db and isinstance(db["etalon"], dict):
        db["etalon"].pop(cat, None)

    save_data(db)
    await callback.message.answer(
        f"🗑 Категория <b>{cat}</b> и все связанные бренды, линейки, модели и эталоны удалены."
    )
    await show_categories_menu(callback, "📂 Обновлённые категории:")


# ================== Обработка названий ==================
@router.message(CatStates.waiting_for_name)
async def process_name(msg: Message, state: FSMContext):
    data = await state.get_data()
    action = data["action"]
    name = msg.text.strip()
    db = load_data()

    if action == "add_cat":
        db.setdefault("etalon", {})[name] = {}
        save_data(db)

        await state.clear()
        await msg.answer(f"✅ Категория <b>{name}</b> добавлена")
        await show_categories_menu(msg, "📂 Обновлённые категории:")

    elif action == "rename_cat":
        old_cat = data.get("selected")
        if old_cat not in db.get("etalon", {}):
            await msg.answer("⚠️ Категория не найдена.")
            await state.clear()
            return

        etalon_root = db.setdefault("etalon", {})
        etalon_root[name] = etalon_root.pop(old_cat)

        save_data(db)

        await state.clear()
        await msg.answer(f"✅ Категория <b>{old_cat}</b> переименована в <b>{name}</b>")
        await show_categories_menu(msg, "📂 Обновлённые категории:")


# ================== Отмена ==================
@router.callback_query(F.data == "cancel_edit")
async def cancel_edit(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.answer("❌ Отменено", show_alert=True)
    await show_categories_menu(callback, "📂 Категории")

# ================== Сортировка категорий ==================
@router.callback_query(F.data == "cat_sort")
async def cat_sort(callback: CallbackQuery, state: FSMContext | None = None):
    await _show_cat_sort(callback)


async def _show_cat_sort(callback: CallbackQuery):
    db = load_data()
    cats = list(db.get("etalon", {}).keys())

    if not cats:
        await callback.message.edit_text("⚠️ Категорий пока нет.")
        return

    kb = []
    for i, c in enumerate(cats):
        row = [InlineKeyboardButton(text=c, callback_data="noop")]
        if i > 0:
            row.append(InlineKeyboardButton(text="⬆️", callback_data=f"cat_move_up:{c}"))
        if i < len(cats) - 1:
            row.append(InlineKeyboardButton(text="⬇️", callback_data=f"cat_move_down:{c}"))
        kb.append(row)

    kb.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="nav_catalog")])

    await callback.message.edit_text(
        "🔀 Сортировка категорий:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)
    )


def move_item_in_dict(d: dict, key: str, direction: str):
    """Перемещает элемент key вверх/вниз внутри словаря"""
    keys = list(d.keys())
    if key not in keys:
        return d
    idx = keys.index(key)
    if direction == "up" and idx > 0:
        keys[idx], keys[idx - 1] = keys[idx - 1], keys[idx]
    elif direction == "down" and idx < len(keys) - 1:
        keys[idx], keys[idx + 1] = keys[idx + 1], keys[idx]
    return {k: d[k] for k in keys}


@router.callback_query(F.data.startswith("cat_move_up:"))
async def cat_move_up(callback: CallbackQuery):
    _, cat = callback.data.split(":", maxsplit=1)
    db = load_data()
    db["etalon"] = move_item_in_dict(db.get("etalon", {}), cat, "up")
    save_data(db)
    await _show_cat_sort(callback)


@router.callback_query(F.data.startswith("cat_move_down:"))
async def cat_move_down(callback: CallbackQuery):
    _, cat = callback.data.split(":", maxsplit=1)
    db = load_data()
    db["etalon"] = move_item_in_dict(db.get("etalon", {}), cat, "down")
    save_data(db)
    await _show_cat_sort(callback)

