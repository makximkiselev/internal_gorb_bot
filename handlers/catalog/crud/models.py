# handlers/catalog/crud/models.py
from aiogram import Router, F
from aiogram.types import CallbackQuery, Message, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from storage import load_data, save_data
from handlers.catalog import etalon
import hashlib

router = Router()

# === FSM ===
class ModelStates(StatesGroup):
    waiting_for_name = State()
    choosing_item = State()
    confirming_action = State()

# === Индексы ===
model_index = {}   # m_id -> (cat, br, sr, m)

def sid_br(cat: str, br: str) -> str:
    raw = f"{cat}:{br}"
    return hashlib.md5(raw.encode()).hexdigest()[:8]

def sid_sr(cat: str, br: str, sr: str) -> str:
    raw = f"{cat}:{br}:{sr}"
    return hashlib.md5(raw.encode()).hexdigest()[:8]

def sid_m(cat: str, br: str, sr: str, m: str) -> str:
    raw = f"{cat}:{br}:{sr}:{m}"
    return hashlib.md5(raw.encode()).hexdigest()[:8]

def build_model_index(db: dict):
    """Перестраиваем индекс моделей."""
    model_index.clear()
    for cat, brands in db.get("etalon", {}).items():
        for br, series in brands.items():
            for sr, models in series.items():
                for m in models.keys():
                    m_id = sid_m(cat, br, sr, m)
                    model_index[m_id] = (cat, br, sr, m)

# ================== Меню моделей внутри линейки ==================
async def show_models_menu(msg_or_cb, cat: str, br: str, sr: str, title: str = None):
    db = load_data()
    build_model_index(db)

    models = list(db.get("etalon", {}).get(cat, {}).get(br, {}).get(sr, {}).keys())
    b_id = sid_br(cat, br)
    s_id = sid_sr(cat, br, sr)

    kb = [
        [InlineKeyboardButton(text=m, callback_data=f"nav_model:{sid_m(cat, br, sr, m)}")]
        for m in models
    ]
    kb += [
        [InlineKeyboardButton(text="➕ Добавить модель", callback_data=f"model_add:{s_id}")],
        [InlineKeyboardButton(text="✏️ Переименовать модель", callback_data=f"model_rename:{s_id}")],
        [InlineKeyboardButton(text="❌ Удалить модель", callback_data=f"model_delete:{s_id}")],
        [InlineKeyboardButton(text="🔀 Сортировка", callback_data=f"model_sort:{s_id}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"nav_brand:{b_id}")]
    ]

    title = title or f"📂 {cat} / {br} / {sr}"
    target = msg_or_cb.message if isinstance(msg_or_cb, CallbackQuery) else msg_or_cb
    await target.answer(title, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

# ================== Навигация на модель ==================
@router.callback_query(F.data.startswith("nav_model:"))
async def nav_model(callback: CallbackQuery, state: FSMContext):
    _, m_id = callback.data.split(":", maxsplit=1)
    db = load_data()
    build_model_index(db)

    if m_id not in model_index:
        await callback.answer("⚠️ Модель не найдена", show_alert=True)
        return

    cat, br, sr, m = model_index[m_id]
    await etalon.render_etalon_menu(callback, cat, br, sr, m, state)

# ================== Добавление ==================
@router.callback_query(F.data.startswith("model_add:"))
async def model_add(callback: CallbackQuery, state: FSMContext):
    _, s_id = callback.data.split(":", maxsplit=1)

    # s_id -> (cat, br, sr) через перебор (без отдельного series_index)
    db = load_data()
    cat = br = sr = None
    for c, brands in db.get("etalon", {}).items():
        for b, series in brands.items():
            for s in series.keys():
                if sid_sr(c, b, s) == s_id:
                    cat, br, sr = c, b, s
                    break
            if sr: break
        if sr: break

    if not sr:
        await callback.answer("⚠️ Линейка не найдена", show_alert=True)
        return

    await state.set_state(ModelStates.waiting_for_name)
    await state.set_data({"action": "add_model", "cat": cat, "br": br, "sr": sr})

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Отмена", callback_data="cancel_edit_model")]
    ])
    await callback.message.answer(f"✏️ Введи название новой модели для {sr} ({cat}/{br}):", reply_markup=kb)

# ================== Переименование ==================
@router.callback_query(F.data.startswith("model_rename:"))
async def model_rename(callback: CallbackQuery, state: FSMContext):
    _, s_id = callback.data.split(":", maxsplit=1)

    db = load_data()
    # найдём (cat, br, sr)
    cat = br = sr = None
    series_branch = None
    for c, brands in db.get("etalon", {}).items():
        for b, series in brands.items():
            for s, models in series.items():
                if sid_sr(c, b, s) == s_id:
                    cat, br, sr = c, b, s
                    series_branch = models
                    break
            if sr: break
        if sr: break

    if not series_branch:
        await callback.answer("⚠️ Линейка не найдена", show_alert=True)
        return

    build_model_index(db)
    models = list(series_branch.keys())
    if not models:
        await callback.message.answer("⚠️ Моделей пока нет.")
        return

    kb = [
        [InlineKeyboardButton(text=m, callback_data=f"choose_model_rename:{sid_m(cat, br, sr, m)}")]
        for m in models
    ]
    kb.append([InlineKeyboardButton(text="⬅️ Отмена", callback_data="cancel_edit_model")])

    await state.set_state(ModelStates.choosing_item)
    await state.set_data({"cat": cat, "br": br, "sr": sr})
    await callback.message.answer(
        f"Выбери модель для переименования ({cat}/{br}/{sr}):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)
    )

@router.callback_query(F.data.startswith("choose_model_rename:"))
async def choose_model_for_rename(callback: CallbackQuery, state: FSMContext):
    _, m_id = callback.data.split(":", maxsplit=1)
    db = load_data()
    build_model_index(db)
    if m_id not in model_index:
        await callback.answer("⚠️ Модель не найдена", show_alert=True)
        return
    cat, br, sr, m = model_index[m_id]

    await state.set_state(ModelStates.waiting_for_name)
    await state.set_data({"action": "rename_model", "cat": cat, "br": br, "sr": sr, "selected": m})

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Отмена", callback_data="cancel_edit_model")]
    ])
    await callback.message.answer(f"✏️ Введи новое имя для модели <b>{m}</b>:", reply_markup=kb)

# ================== Удаление ==================
@router.callback_query(F.data.startswith("model_delete:"))
async def model_delete(callback: CallbackQuery, state: FSMContext):
    _, s_id = callback.data.split(":", maxsplit=1)
    db = load_data()

    # найдём (cat, br, sr)
    cat = br = sr = None
    series_branch = None
    for c, brands in db.get("etalon", {}).items():
        for b, series in brands.items():
            for s, models in series.items():
                if sid_sr(c, b, s) == s_id:
                    cat, br, sr = c, b, s
                    series_branch = models
                    break
            if sr: break
        if sr: break

    if not series_branch:
        await callback.answer("⚠️ Линейка не найдена", show_alert=True)
        return

    build_model_index(db)
    models = list(series_branch.keys())
    if not models:
        await callback.message.answer("⚠️ Моделей пока нет.")
        return

    kb = [
        [InlineKeyboardButton(text=m, callback_data=f"confirm_model_delete:{sid_m(cat, br, sr, m)}")]
        for m in models
    ]
    kb.append([InlineKeyboardButton(text="⬅️ Отмена", callback_data="cancel_edit_model")])

    await state.set_data({"cat": cat, "br": br, "sr": sr})
    await callback.message.answer(
        f"Выбери модель для удаления ({cat}/{br}/{sr}):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=kb)
    )

@router.callback_query(F.data.startswith("confirm_model_delete:"))
async def confirm_model_delete(callback: CallbackQuery, state: FSMContext):
    _, m_id = callback.data.split(":", maxsplit=1)
    db = load_data()
    build_model_index(db)
    if m_id not in model_index:
        await callback.answer("⚠️ Модель не найдена", show_alert=True)
        return
    cat, br, sr, m = model_index[m_id]

    has_etalon = bool(db.get("etalon", {}).get(cat, {}).get(br, {}).get(sr, {}).get(m))
    if has_etalon:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"model_confirm_delete_final:{m_id}")],
            [InlineKeyboardButton(text="⬅️ Отмена", callback_data="cancel_edit_model")]
        ])
        await callback.message.answer(
            f"⚠️ У модели <b>{m}</b> есть эталоны.\n"
            f"Удаление приведёт к потере связанных данных.\n\nПродолжить?",
            reply_markup=kb
        )
    else:
        db.get("etalon", {}).get(cat, {}).get(br, {}).get(sr, {}).pop(m, None)
        save_data(db)
        await callback.message.answer(f"🗑 Модель <b>{m}</b> удалена.")
        await show_models_menu(callback, cat, br, sr, "📂 Обновлённые модели:")

@router.callback_query(F.data.startswith("model_confirm_delete_final:"))
async def model_confirm_delete_final(callback: CallbackQuery, state: FSMContext):
    _, m_id = callback.data.split(":", maxsplit=1)
    db = load_data()
    build_model_index(db)
    if m_id not in model_index:
        await callback.answer("⚠️ Модель не найдена", show_alert=True)
        return
    cat, br, sr, m = model_index[m_id]

    etalon_branch = db.get("etalon", {}).get(cat, {}).get(br, {}).get(sr, {})
    if m in etalon_branch:
        etalon_branch.pop(m, None)

    save_data(db)
    await callback.message.answer(f"🗑 Модель <b>{m}</b> и все её эталоны удалены.")
    await show_models_menu(callback, cat, br, sr, "📂 Обновлённые модели:")

# ================== Сортировка моделей ==================
@router.callback_query(F.data.startswith("model_sort:"))
async def model_sort(callback: CallbackQuery):
    _, s_id = callback.data.split(":", maxsplit=1)
    await _show_model_sort(callback, s_id)

async def _show_model_sort(callback: CallbackQuery, s_id: str):
    db = load_data()

    # извлекаем (cat, br, sr)
    cat = br = sr = None
    for c, brands in db.get("etalon", {}).items():
        for b, series in brands.items():
            for s, models in series.items():
                if sid_sr(c, b, s) == s_id:
                    cat, br, sr = c, b, s
                    break
            if sr: break
        if sr: break

    if not sr:
        await callback.message.edit_text("⚠️ Линейка не найдена.")
        return

    models = list(db.get("etalon", {}).get(cat, {}).get(br, {}).get(sr, {}).keys())
    if not models:
        await callback.message.edit_text("⚠️ Моделей пока нет.")
        return

    kb = []
    for i, m in enumerate(models):
        m_id = sid_m(cat, br, sr, m)
        row = [InlineKeyboardButton(text=m, callback_data="noop")]
        if i > 0:
            row.append(InlineKeyboardButton(text="⬆️", callback_data=f"model_move_up:{m_id}"))
        if i < len(models) - 1:
            row.append(InlineKeyboardButton(text="⬇️", callback_data=f"model_move_down:{m_id}"))
        kb.append(row)

    kb.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=f"nav_series:{s_id}")])

    await callback.message.edit_text(
        f"🔀 Сортировка моделей ({cat}/{br}/{sr}):",
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

@router.callback_query(F.data.startswith("model_move_up:"))
async def model_move_up(callback: CallbackQuery):
    _, m_id = callback.data.split(":", maxsplit=1)
    db = load_data()
    build_model_index(db)
    if m_id not in model_index:
        await callback.answer("⚠️ Модель не найдена", show_alert=True)
        return
    cat, br, sr, m = model_index[m_id]
    db["etalon"][cat][br][sr] = move_item_in_dict(db.get("etalon", {}).get(cat, {}).get(br, {}).get(sr, {}), m, "up")
    save_data(db)
    await _show_model_sort(callback, sid_sr(cat, br, sr))

@router.callback_query(F.data.startswith("model_move_down:"))
async def model_move_down(callback: CallbackQuery):
    _, m_id = callback.data.split(":", maxsplit=1)
    db = load_data()
    build_model_index(db)
    if m_id not in model_index:
        await callback.answer("⚠️ Модель не найдена", show_alert=True)
        return
    cat, br, sr, m = model_index[m_id]
    db["etalon"][cat][br][sr] = move_item_in_dict(db.get("etalon", {}).get(cat, {}).get(br, {}).get(sr, {}), m, "down")
    save_data(db)
    await _show_model_sort(callback, sid_sr(cat, br, sr))

# ================== Обработка названий ==================
@router.message(ModelStates.waiting_for_name)
async def process_model_name(msg: Message, state: FSMContext):
    data = await state.get_data()
    action = data["action"]
    name = msg.text.strip()
    cat, br, sr = data["cat"], data["br"], data["sr"]

    db = load_data()

    if action == "add_model":
        db.setdefault("etalon", {}).setdefault(cat, {}).setdefault(br, {}).setdefault(sr, {})[name] = []
        save_data(db)
        await state.clear()
        await msg.answer(f"✅ Модель <b>{name}</b> добавлена в {cat}/{br}/{sr}")
        await show_models_menu(msg, cat, br, sr, "📂 Обновлённые модели:")

    elif action == "rename_model":
        old_m = data.get("selected")
        if old_m not in db.get("etalon", {}).get(cat, {}).get(br, {}).get(sr, {}):
            await msg.answer("⚠️ Модель не найдена.")
            await state.clear()
            return

        etalon_branch = (
            db.setdefault("etalon", {})
              .setdefault(cat, {})
              .setdefault(br, {})
              .setdefault(sr, {})
        )
        etalon_branch[name] = etalon_branch.pop(old_m)

        save_data(db)
        await state.clear()
        await msg.answer(f"✅ Модель <b>{old_m}</b> переименована в <b>{name}</b>")
        await show_models_menu(msg, cat, br, sr, "📂 Обновлённые модели:")

# ================== Отмена ==================
@router.callback_query(F.data == "cancel_edit_model")
async def cancel_edit_model(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    cat, br, sr = (data.get("cat"), data.get("br"), data.get("sr")) if data else (None, None, None)
    await state.clear()
    await callback.answer("❌ Отменено", show_alert=True)
    if cat and br and sr:
        await show_models_menu(callback, cat, br, sr, "📂 Модели")
    else:
        await callback.message.answer("📂 Действие отменено")
