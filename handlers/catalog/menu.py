from aiogram import Router, F
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from handlers.auth_utils import auth_get
from storage import load_data
from . import etalon   # для вызова render_etalon_menu
from aiogram.fsm.context import FSMContext

router = Router()
router.include_router(etalon.router)   # подключаем хендлеры эталона

# Главное меню каталога
def catalog_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📂 Каталог (редактировать)", callback_data="nav_catalog")],
        [InlineKeyboardButton(text="🗂 Весь каталог", callback_data="show_full_catalog")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")]
    ])


@router.callback_query(F.data == "catalog_menu")
async def show_catalog_menu(callback: CallbackQuery):
    u = await auth_get(callback.from_user.id)
    access = (u or {}).get("access") or {}
    if not u or not (u.get("role") == "admin" or access.get("products.catalog")):
        await callback.answer("⛔️ Нет доступа", show_alert=True)
        return
    await callback.answer()
    await callback.message.answer("📂 Меню каталога", reply_markup=catalog_menu())


# === Показ всего каталога с кнопками по сериям ===
@router.callback_query(F.data == "show_full_catalog")
async def show_full_catalog(callback: CallbackQuery):
    u = await auth_get(callback.from_user.id)
    access = (u or {}).get("access") or {}
    if not u or not (u.get("role") == "admin" or access.get("products.catalog")):
        await callback.answer("⛔️ Нет доступа", show_alert=True)
        return
    db = load_data()
    catalog = db.get("etalon", {})
    etalons = catalog

    if not catalog:
        await callback.message.answer("📂 Каталог пуст.", reply_markup=catalog_menu())
        return

    text_lines = []
    blocks = []  # массив блоков с кнопками

    for cat, brands in catalog.items():
        text_lines.append(f"📂 <b>{cat}</b>")
        for br, series in brands.items():
            text_lines.append(f"  └─ 🏷 {br}")
            for sr, models in series.items():
                text_lines.append(f"      └─ 🔖 {sr}")

                # собираем кнопки для одной серии
                series_buttons = []
                for m in models.keys():
                    etalon_exists = etalons.get(cat, {}).get(br, {}).get(sr, {}).get(m)
                    mark = "✅" if etalon_exists else "❌"

                    # текст дерева
                    text_lines.append(f"          └─ 📱 {m} — Эталон {mark}")

                    # кнопка для модели
                    series_buttons.append(InlineKeyboardButton(
                        text=f"{m} {mark}",
                        callback_data=f"full_etalon:{cat}:{br}:{sr}:{m}"
                    ))

                # если есть кнопки для серии → добавляем их как блок
                if series_buttons:
                    # делаем их по 2 в ряд для компактности
                    row = []
                    for btn in series_buttons:
                        row.append(btn)
                        if len(row) == 2:
                            blocks.append(row)
                            row = []
                    if row:
                        blocks.append(row)

                # добавляем разделитель между сериями
                blocks.append([InlineKeyboardButton(text="⏸ " + sr, callback_data="ignore")])

    # финальное сообщение
    text = "\n".join(text_lines)
    blocks.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="catalog_menu")])

    await callback.message.answer(
        f"🗂 Весь каталог:\n\n{text}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=blocks)
    )


# === Клик по модели из полного каталога ===
@router.callback_query(F.data.startswith("full_etalon:"))
async def full_etalon(callback: CallbackQuery, state: FSMContext):
    _, cat, br, sr, m = callback.data.split(":")
    await etalon.render_etalon_menu(callback, cat, br, sr, m, state)


# === Игнор-кнопка (для разделителей) ===
@router.callback_query(F.data == "ignore")
async def ignore_callback(callback: CallbackQuery):
    await callback.answer()
