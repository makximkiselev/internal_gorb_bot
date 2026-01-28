from aiogram import Router, F
from aiogram.types import CallbackQuery, Message, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from storage import load_data, save_data
import hashlib

router = Router()

# === FSM ===
class BrandStates(StatesGroup):
    waiting_for_name = State()
    choosing_item = State()
    confirming_action = State()

# === Индексы ===
brand_index = {}  # b_id -> (cat, br)

def sid_br(cat: str, br: str) -> str:
    raw = f"{cat}:{br}"
    return hashlib.md5(raw.encode()).hexdigest()[:8]

def build_brand_index(db: dict):
    brand_index.clear()
    for cat, brands in db.get("etalon", {}).items():
        for br in brands.keys():
            b_id = sid_br(cat, br)
            brand_index[b_id] = (cat, br)

# ================== Меню брендов внутри категории ==================
async def show_brands_menu(msg_or_cb, cat: str, title: str = None):
    db = load_data()
    build_brand_index(db)
    brands = list(db.get("etalon", {}).get(cat, {}).keys())

    kb = [
        [InlineKeyboardButton(text=br, callback_data=f"nav_brand:{sid_br(cat, br)}")]
        for br in brands
    ]
    kb += [
        [InlineKeyboardButton(text="➕ Добавить бренд", callback_data=f"brand_add:{cat}")],
        [InlineKeyboardButton(text="✏️ Переименовать бренд", callback_data=f"brand_rename:{cat}")],
        [InlineKeyboardButton(text="❌ Удалить бренд", callback_data=f"brand_delete:{cat}")],
        [InlineKeyboardButton(text="🔀 Сортировка", callback_data=f"brand_sort:{cat}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="nav_catalog")]
    ]

    title = title or f"📂 Категория: <b>{cat}</b>"
    target = msg_or_cb.message if isinstance(msg_or_cb, CallbackQuery) else msg_or_cb
    await target.answer(title, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

# ================== Навигация ==================
@router.callback_query(F.data.startswith("nav_cat:"))
async def nav_cat(callback: CallbackQuery, state: FSMContext):
    _, cat = callback.data.split(":")
    await show_brands_menu(callback, cat)

# ================== Добавление ==================
@router.callback_query(F.data.startswith("brand_add:"))
async def brand_add(callback: CallbackQuery, state: FSMContext):
    _, cat = callback.data.split(":")
    await state.set_state(BrandStates.waiting_for_name)
    await state.set_data({"action": "add_brand", "cat": cat})

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Отмена", callback_data="cancel_edit_brand")]
    ])
    await callback.message.answer(f"✏️ Введи название нового бренда в категории <b>{cat}</b>:", reply_markup=kb)

# ================== Переименование ==================
@router.callback_query(F.data.startswith("brand_rename:"))
async def brand_rename(callback: CallbackQuery, state: FSMContext):
    _, cat = callback.data.split(":")
    db = load_data()
    build_brand_index(db)
    brands = list(db.get("etalon", {}).get(cat, {}).keys())

    if not brands:
        await callback.message.answer("⚠️ Брендов пока нет.")
        return

    kb = [
        [InlineKeyboardButton(text=br, callback_data=f"choose_brand_rename:{sid_br(cat, br)}")]
        for br in brands
    ]
    kb.append([InlineKeyboardButton(text="⬅️ Отмена", callback_data="cancel_edit_brand")])

    await state.set_state(BrandStates.choosing_item)
    await state.set_data({"cat": cat})
    await callback.message.answer(f"Выбери бренд для переименования (категория {cat}):", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@router.callback_query(F.data.startswith("choose_brand_rename:"))
async def choose_brand_for_rename(callback: CallbackQuery, state: FSMContext):
    _, b_id = callback.data.split(":")
    db = load_data()
    build_brand_index(db)
    if b_id not in brand_index:
        await callback.answer("⚠️ Бренд не найден", show_alert=True)
        return
    cat, br = brand_index[b_id]

    await state.set_state(BrandStates.waiting_for_name)
    await state.set_data({"action": "rename_brand", "cat": cat, "selected": br})

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Отмена", callback_data="cancel_edit_brand")]
    ])
    await callback.message.answer(f"✏️ Введи новое имя для бренда <b>{br}</b>:", reply_markup=kb)

# ================== Удаление ==================
@router.callback_query(F.data.startswith("brand_delete:"))
async def brand_delete(callback: CallbackQuery, state: FSMContext):
    _, cat = callback.data.split(":")
    db = load_data()
    build_brand_index(db)
    brands = list(db.get("etalon", {}).get(cat, {}).keys())

    if not brands:
        await callback.message.answer("⚠️ Брендов пока нет.")
        return

    kb = [
        [InlineKeyboardButton(text=br, callback_data=f"confirm_brand_delete:{sid_br(cat, br)}")]
        for br in brands
    ]
    kb.append([InlineKeyboardButton(text="⬅️ Отмена", callback_data="cancel_edit_brand")])

    await state.set_data({"cat": cat})
    await callback.message.answer(f"Выбери бренд для удаления (категория {cat}):", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@router.callback_query(F.data.startswith("confirm_brand_delete:"))
async def confirm_brand_delete(callback: CallbackQuery, state: FSMContext):
    _, b_id = callback.data.split(":")
    db = load_data()
    build_brand_index(db)
    if b_id not in brand_index:
        await callback.answer("⚠️ Бренд не найден", show_alert=True)
        return
    cat, br = brand_index[b_id]

    has_nested = bool(db.get("etalon", {}).get(cat, {}).get(br))
    if has_nested:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"brand_confirm_delete_final:{b_id}")],
            [InlineKeyboardButton(text="⬅️ Отмена", callback_data="cancel_edit_brand")]
        ])
        await state.set_data({"cat": cat})
        await callback.message.answer(
            f"⚠️ У бренда <b>{br}</b> есть вложенные линейки, модели или эталоны.\nУдаление приведёт к потере всех связанных данных.\n\nПродолжить?",
            reply_markup=kb
        )
    else:
        db.get("etalon", {}).get(cat, {}).pop(br, None)
        save_data(db)
        await callback.message.answer(f"🗑 Бренд <b>{br}</b> удалён.")
        await show_brands_menu(callback, cat, "📂 Обновлённые бренды:")

@router.callback_query(F.data.startswith("brand_confirm_delete_final:"))
async def brand_confirm_delete_final(callback: CallbackQuery, state: FSMContext):
    _, b_id = callback.data.split(":")
    db = load_data()
    build_brand_index(db)
    if b_id not in brand_index:
        await callback.answer("⚠️ Бренд не найден", show_alert=True)
        return
    cat, br = brand_index[b_id]

    etalon_root = db.get("etalon", {}).get(cat, {})
    if isinstance(etalon_root, dict) and br in etalon_root:
        etalon_root.pop(br, None)

    save_data(db)
    await callback.message.answer(f"🗑 Бренд <b>{br}</b> и все связанные линейки, модели и эталоны удалены.")
    await show_brands_menu(callback, cat, "📂 Обновлённые бренды:")

# ================== Обработка названий ==================
@router.message(BrandStates.waiting_for_name)
async def process_brand_name(msg: Message, state: FSMContext):
    data = await state.get_data()
    action = data["action"]
    name = msg.text.strip()
    cat = data["cat"]

    db = load_data()
    build_brand_index(db)

    if action == "add_brand":
        db.setdefault("etalon", {}).setdefault(cat, {})[name] = {}
        save_data(db)
        await state.clear()
        await msg.answer(f"✅ Бренд <b>{name}</b> добавлен в категорию {cat}")
        await show_brands_menu(msg, cat, "📂 Обновлённые бренды:")

    elif action == "rename_brand":
        old_br = data.get("selected")
        if old_br not in db.get("etalon", {}).get(cat, {}):
            await msg.answer("⚠️ Бренд не найден.")
            await state.clear()
            return

        etalon_root = db.setdefault("etalon", {}).setdefault(cat, {})
        etalon_root[name] = etalon_root.pop(old_br)

        save_data(db)
        await state.clear()
        await msg.answer(f"✅ Бренд <b>{old_br}</b> переименован в <b>{name}</b>")
        await show_brands_menu(msg, cat, "📂 Обновлённые бренды:")

# ================== Отмена ==================
@router.callback_query(F.data == "cancel_edit_brand")
async def cancel_edit_brand(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    cat = data.get("cat") if data else None
    await state.clear()
    await callback.answer("❌ Отменено", show_alert=True)
    if cat:
        await show_brands_menu(callback, cat, "📂 Бренды")
    else:
        await callback.message.answer("📂 Действие отменено")

# ================== Сортировка брендов ==================
@router.callback_query(F.data.startswith("brand_sort:"))
async def brand_sort(callback: CallbackQuery):
    _, cat = callback.data.split(":", maxsplit=1)
    await _show_brand_sort(callback, cat)

async def _show_brand_sort(callback: CallbackQuery, cat: str):
    db = load_data()
    build_brand_index(db)
    brands = list(db.get("etalon", {}).get(cat, {}).keys())
    if not brands:
        await callback.message.edit_text("⚠️ Брендов пока нет.")
        return

    kb = []
    for i, br in enumerate(brands):
        row = [InlineKeyboardButton(text=br, callback_data="noop")]
        if i > 0:
            row.append(InlineKeyboardButton(text="⬆️", callback_data=f"brand_move_up:{cat}:{br}"))
        if i < len(brands) - 1:
            row.append(InlineKeyboardButton(text="⬇️", callback_data=f"brand_move_down:{cat}:{br}"))
        kb.append(row)

    kb.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=f"nav_cat:{cat}")])
    await callback.message.edit_text(f"🔀 Сортировка брендов в {cat}:", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

def move_item_in_dict(d: dict, key: str, direction: str):
    keys = list(d.keys())
    if key not in keys:
        return d
    idx = keys.index(key)
    if direction == "up" and idx > 0:
        keys[idx], keys[idx - 1] = keys[idx - 1], keys[idx]
    elif direction == "down" and idx < len(keys) - 1:
        keys[idx], keys[idx + 1] = keys[idx + 1], keys[idx]
    return {k: d[k] for k in keys}

@router.callback_query(F.data.startswith("brand_move_up:"))
async def brand_move_up(callback: CallbackQuery):
    _, cat, br = callback.data.split(":", maxsplit=2)
    db = load_data()
    db["etalon"][cat] = move_item_in_dict(db.get("etalon", {}).get(cat, {}), br, "up")
    save_data(db)
    await _show_brand_sort(callback, cat)

@router.callback_query(F.data.startswith("brand_move_down:"))
async def brand_move_down(callback: CallbackQuery):
    _, cat, br = callback.data.split(":", maxsplit=2)
    db = load_data()
    db["etalon"][cat] = move_item_in_dict(db.get("etalon", {}).get(cat, {}), br, "down")
    save_data(db)
    await _show_brand_sort(callback, cat)
