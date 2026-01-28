from aiogram import Router, F
from aiogram.types import CallbackQuery, Message, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from storage import load_data, save_data
import hashlib

router = Router()

# === FSM ===
class SeriesStates(StatesGroup):
    waiting_for_name = State()
    choosing_item = State()
    confirming_action = State()

# === Глобальные индексы ===
brand_index = {}   # sid_br -> (cat, br)
series_index = {}  # sid_sr -> (cat, br, sr)

# === Утилиты ===
def sid_br(cat: str, br: str) -> str:
    raw = f"{cat}:{br}"
    return hashlib.md5(raw.encode()).hexdigest()[:8]

def sid_sr(cat: str, br: str, sr: str) -> str:
    raw = f"{cat}:{br}:{sr}"
    return hashlib.md5(raw.encode()).hexdigest()[:8]

def build_indexes(db: dict):
    """Перестраивает глобальные индексы для брендов и линеек"""
    brand_index.clear()
    series_index.clear()
    for cat, brands in db.get("etalon", {}).items():
        for br, series in brands.items():
            b_id = sid_br(cat, br)
            brand_index[b_id] = (cat, br)
            for sr in series.keys():
                s_id = sid_sr(cat, br, sr)
                series_index[s_id] = (cat, br, sr)

# ================== Меню линеек внутри бренда ==================
async def show_series_menu(msg_or_cb, b_id: str, title: str = None):
    db = load_data()
    build_indexes(db)
    cat, br = brand_index[b_id]

    series = list(db.get("etalon", {}).get(cat, {}).get(br, {}).keys())
    kb = [
        [InlineKeyboardButton(text=sr, callback_data=f"nav_series:{sid_sr(cat, br, sr)}")]
        for sr in series
    ]
    kb += [
        [InlineKeyboardButton(text="➕ Добавить линейку", callback_data=f"series_add:{b_id}")],
        [InlineKeyboardButton(text="✏️ Переименовать линейку", callback_data=f"series_rename:{b_id}")],
        [InlineKeyboardButton(text="❌ Удалить линейку", callback_data=f"series_delete:{b_id}")],
        [InlineKeyboardButton(text="🔀 Сортировка", callback_data=f"series_sort:{b_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"nav_cat:{cat}")]
    ]

    title = title or f"📂 {cat} / {br}"
    target = msg_or_cb.message if isinstance(msg_or_cb, CallbackQuery) else msg_or_cb
    await target.answer(title, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

# ================== Навигация ==================
@router.callback_query(F.data.startswith("nav_brand:"))
async def nav_brand(callback: CallbackQuery, state: FSMContext):
    _, b_id = callback.data.split(":", 1)
    await show_series_menu(callback, b_id)

@router.callback_query(F.data.startswith("nav_series:"))
async def nav_series(callback: CallbackQuery, state: FSMContext):
    _, s_id = callback.data.split(":", 1)
    db = load_data()
    build_indexes(db)
    if s_id not in series_index:
        await callback.answer("⚠️ Линейка не найдена", show_alert=True)
        return
    cat, br, sr = series_index[s_id]
    from handlers.catalog.crud.models import show_models_menu
    await show_models_menu(callback, cat, br, sr)

# ================== Добавление ==================
@router.callback_query(F.data.startswith("series_add:"))
async def series_add(callback: CallbackQuery, state: FSMContext):
    _, b_id = callback.data.split(":", 1)
    db = load_data()
    build_indexes(db)
    cat, br = brand_index[b_id]

    await state.set_state(SeriesStates.waiting_for_name)
    await state.set_data({"action": "add_series", "b_id": b_id})

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Отмена", callback_data="cancel_edit_series")]
    ])
    await callback.message.answer(f"✏️ Введи название новой линейки для {br} ({cat}):", reply_markup=kb)

# ================== Переименование ==================
@router.callback_query(F.data.startswith("series_rename:"))
async def series_rename(callback: CallbackQuery, state: FSMContext):
    _, b_id = callback.data.split(":", 1)
    db = load_data()
    build_indexes(db)
    cat, br = brand_index[b_id]
    series = list(db.get("etalon", {}).get(cat, {}).get(br, {}).keys())
    if not series:
        await callback.message.answer("⚠️ Линеек пока нет.")
        return

    kb = [
        [InlineKeyboardButton(text=sr, callback_data=f"choose_series_rename:{sid_sr(cat, br, sr)}")]
        for sr in series
    ]
    kb.append([InlineKeyboardButton(text="⬅️ Отмена", callback_data="cancel_edit_series")])
    await state.set_state(SeriesStates.choosing_item)
    await state.set_data({"b_id": b_id})
    await callback.message.answer(f"Выбери линейку для переименования ({cat}/{br}):", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@router.callback_query(F.data.startswith("choose_series_rename:"))
async def choose_series_for_rename(callback: CallbackQuery, state: FSMContext):
    _, s_id = callback.data.split(":", 1)
    db = load_data()
    build_indexes(db)
    if s_id not in series_index:
        await callback.answer("⚠️ Линейка не найдена", show_alert=True)
        return
    cat, br, sr = series_index[s_id]
    b_id = sid_br(cat, br)

    await state.set_state(SeriesStates.waiting_for_name)
    await state.set_data({"action": "rename_series", "b_id": b_id, "selected": sr})

    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ Отмена", callback_data="cancel_edit_series")]])
    await callback.message.answer(f"✏️ Введи новое имя для линейки <b>{sr}</b>:", reply_markup=kb)

# ================== Удаление ==================
@router.callback_query(F.data.startswith("series_delete:"))
async def series_delete(callback: CallbackQuery, state: FSMContext):
    _, b_id = callback.data.split(":", 1)
    db = load_data()
    build_indexes(db)
    cat, br = brand_index[b_id]
    series = list(db.get("etalon", {}).get(cat, {}).get(br, {}).keys())
    if not series:
        await callback.message.answer("⚠️ Линеек пока нет.")
        return

    kb = [
        [InlineKeyboardButton(text=sr, callback_data=f"confirm_series_delete:{sid_sr(cat, br, sr)}")]
        for sr in series
    ]
    kb.append([InlineKeyboardButton(text="⬅️ Отмена", callback_data="cancel_edit_series")])
    await state.set_data({"b_id": b_id})
    await callback.message.answer(f"Выбери линейку для удаления ({cat}/{br}):", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@router.callback_query(F.data.startswith("confirm_series_delete:"))
async def confirm_series_delete(callback: CallbackQuery, state: FSMContext):
    _, s_id = callback.data.split(":", 1)
    db = load_data()
    build_indexes(db)
    if s_id not in series_index:
        await callback.answer("⚠️ Линейка не найдена", show_alert=True)
        return
    cat, br, sr = series_index[s_id]

    has_nested = bool(db.get("etalon", {}).get(cat, {}).get(br, {}).get(sr))
    if has_nested:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"series_confirm_delete_final:{s_id}")],
            [InlineKeyboardButton(text="⬅️ Отмена", callback_data="cancel_edit_series")]
        ])
        await state.set_data({"b_id": sid_br(cat, br)})
        await callback.message.answer(f"⚠️ У линейки <b>{sr}</b> есть вложенные модели или эталоны.\nУдаление приведёт к потере всех связанных данных.\n\nПродолжить?", reply_markup=kb)
    else:
        db.get("etalon", {}).get(cat, {}).get(br, {}).pop(sr, None)
        save_data(db)
        await callback.message.answer(f"🗑 Линейка <b>{sr}</b> удалена.")
        await show_series_menu(callback, sid_br(cat, br), "📂 Обновлённые линейки:")

@router.callback_query(F.data.startswith("series_confirm_delete_final:"))
async def series_confirm_delete_final(callback: CallbackQuery, state: FSMContext):
    _, s_id = callback.data.split(":", 1)
    db = load_data()
    build_indexes(db)
    if s_id not in series_index:
        await callback.answer("⚠️ Линейка не найдена", show_alert=True)
        return
    cat, br, sr = series_index[s_id]
    etalon_root = db.get("etalon", {}).get(cat, {}).get(br, {})
    if sr in etalon_root:
        etalon_root.pop(sr, None)
    save_data(db)
    await callback.message.answer(f"🗑 Линейка <b>{sr}</b>, все её модели и эталоны удалены.")
    await show_series_menu(callback, sid_br(cat, br), "📂 Обновлённые линейки:")

# ================== Обработка названий ==================
@router.message(SeriesStates.waiting_for_name)
async def process_series_name(msg: Message, state: FSMContext):
    data = await state.get_data()
    action = data["action"]
    name = msg.text.strip()
    b_id = data["b_id"]

    db = load_data()
    build_indexes(db)
    cat, br = brand_index[b_id]

    if action == "add_series":
        db.setdefault("etalon", {}).setdefault(cat, {}).setdefault(br, {})[name] = {}
        save_data(db)
        await state.clear()
        await msg.answer(f"✅ Линейка <b>{name}</b> добавлена в {cat}/{br}")
        await show_series_menu(msg, b_id, "📂 Обновлённые линейки:")
    elif action == "rename_series":
        old_sr = data.get("selected")
        if old_sr not in db.get("etalon", {}).get(cat, {}).get(br, {}):
            await msg.answer("⚠️ Линейка не найдена.")
            await state.clear()
            return
        etalon_branch = db.setdefault("etalon", {}).setdefault(cat, {}).setdefault(br, {})
        etalon_branch[name] = etalon_branch.pop(old_sr)
        save_data(db)
        await state.clear()
        await msg.answer(f"✅ Линейка <b>{old_sr}</b> переименована в <b>{name}</b>")
        await show_series_menu(msg, b_id, "📂 Обновлённые линейки:")

# ================== Отмена ==================
@router.callback_query(F.data == "cancel_edit_series")
async def cancel_edit_series(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    b_id = data.get("b_id") if data else None
    await state.clear()
    await callback.answer("❌ Отменено", show_alert=True)
    if b_id:
        await show_series_menu(callback, b_id, "📂 Линейки")
    else:
        await callback.message.answer("📂 Действие отменено")

# ================== Сортировка линеек ==================
@router.callback_query(F.data.startswith("series_sort:"))
async def series_sort(callback: CallbackQuery):
    _, b_id = callback.data.split(":", 1)
    await _show_series_sort(callback, b_id)

async def _show_series_sort(callback: CallbackQuery, b_id: str):
    db = load_data()
    build_indexes(db)
    cat, br = brand_index[b_id]
    series = list(db.get("etalon", {}).get(cat, {}).get(br, {}).keys())
    if not series:
        await callback.message.edit_text("⚠️ Линеек пока нет.")
        return

    kb = []
    for i, sr in enumerate(series):
        s_id = sid_sr(cat, br, sr)
        row = [InlineKeyboardButton(text=sr, callback_data="noop")]
        if i > 0:
            row.append(InlineKeyboardButton(text="⬆️", callback_data=f"series_move_up:{s_id}"))
        if i < len(series) - 1:
            row.append(InlineKeyboardButton(text="⬇️", callback_data=f"series_move_down:{s_id}"))
        kb.append(row)

    kb.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=f"nav_brand:{b_id}")])
    await callback.message.edit_text(f"🔀 Сортировка линеек ({cat}/{br}):", reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

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

@router.callback_query(F.data.startswith("series_move_up:"))
async def series_move_up(callback: CallbackQuery):
    _, s_id = callback.data.split(":", 1)
    db = load_data()
    build_indexes(db)
    if s_id not in series_index:
        await callback.answer("⚠️ Линейка не найдена", show_alert=True)
        return
    cat, br, sr = series_index[s_id]
    db["etalon"][cat][br] = move_item_in_dict(db.get("etalon", {}).get(cat, {}).get(br, {}), sr, "up")
    save_data(db)
    await _show_series_sort(callback, sid_br(cat, br))

@router.callback_query(F.data.startswith("series_move_down:"))
async def series_move_down(callback: CallbackQuery):
    _, s_id = callback.data.split(":", 1)
    db = load_data()
    build_indexes(db)
    if s_id not in series_index:
        await callback.answer("⚠️ Линейка не найдена", show_alert=True)
        return
    cat, br, sr = series_index[s_id]
    db["etalon"][cat][br] = move_item_in_dict(db.get("etalon", {}).get(cat, {}).get(br, {}), sr, "down")
    save_data(db)
    await _show_series_sort(callback, sid_br(cat, br))
