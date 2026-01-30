from __future__ import annotations

import os
import re
import tempfile
from datetime import datetime, date
from pathlib import Path
from typing import List, Dict, Any

from aiogram import Router, F
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    FSInputFile,
)
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from handlers.auth_utils import auth_get

from .generator import generate_receipt_pdf, get_last_receipts, RECEIPTS_DIR

router = Router(name="receipt_ui")


# ================== FSM ==================

class ReceiptForm(StatesGroup):
    waiting_for_name = State()
    waiting_for_serial = State()
    waiting_for_price = State()
    waiting_for_quantity = State()
    waiting_for_add_more = State()
    waiting_for_date = State()


# ================== Клавиатуры ==================

def receipt_root_kb() -> InlineKeyboardMarkup:
    """Главное меню блока 'Товарный чек'."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🧾 Создать товарный чек",
                    callback_data="receipt:create",
                )
            ],
            [
                InlineKeyboardButton(
                    text="📂 Посмотреть чеки",
                    callback_data="receipt:list",
                )
            ],
            [
                InlineKeyboardButton(
                    text="🏠 Главное меню",
                    callback_data="main_menu",
                )
            ],
        ]
    )


def fsm_kb(back_cb: str | None = None) -> InlineKeyboardMarkup:
    """
    Универсальная клавиатура для шагов FSM:
    - опционально 'Назад'
    - 'Отмена'
    """
    rows: List[List[InlineKeyboardButton]] = []
    if back_cb:
        rows.append(
            [InlineKeyboardButton(text="⬅️ Назад", callback_data=back_cb)]
        )
    rows.append(
        [InlineKeyboardButton(text="❌ Отмена", callback_data="receipt:cancel")]
    )
    return InlineKeyboardMarkup(inline_keyboard=rows)


def add_more_kb() -> InlineKeyboardMarkup:
    """Клавиатура после добавления позиции: ещё / сформировать чек / отмена."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="➕ Добавить ещё товар", callback_data="receipt:more"
                )
            ],
            [
                InlineKeyboardButton(
                    text="✅ Сформировать чек", callback_data="receipt:done_items"
                )
            ],
            [
                InlineKeyboardButton(
                    text="❌ Отмена", callback_data="receipt:cancel"
                )
            ],
        ]
    )


def done_kb() -> InlineKeyboardMarkup:
    """Клавиатура после генерации чека."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🏠 Главное меню",
                    callback_data="main_menu",
                )
            ],
            [
                InlineKeyboardButton(
                    text="🔙 Товарные чеки",
                    callback_data="receipt:menu",
                )
            ],
        ]
    )


# ================== Точка входа ==================

@router.message(F.text.casefold() == "товарный чек")
async def open_receipt_menu_message(message: Message):
    """Пользователь нажал кнопку 'товарный чек' (ReplyKeyboard)."""
    u = await auth_get(message.from_user.id)
    access = (u or {}).get("access") or {}
    if not u or not (u.get("role") == "admin" or access.get("sales.receipt")):
        await message.answer("⛔️ Нет доступа")
        return
    await message.answer("Меню товарных чеков:", reply_markup=receipt_root_kb())


@router.callback_query(F.data == "receipt:menu")
async def open_receipt_menu_callback(callback: CallbackQuery):
    """Открыть меню товарных чеков по callback."""
    u = await auth_get(callback.from_user.id)
    access = (u or {}).get("access") or {}
    if not u or not (u.get("role") == "admin" or access.get("sales.receipt")):
        await callback.answer("⛔️ Нет доступа", show_alert=True)
        return
    if callback.message:
        await callback.message.edit_text(
            "Меню товарных чеков:",
            reply_markup=receipt_root_kb(),
        )
    await callback.answer()


# ================== Отмена ==================

@router.callback_query(F.data == "receipt:cancel")
async def cancel_receipt(callback: CallbackQuery, state: FSMContext):
    """Отмена процесса создания чека."""
    await state.clear()
    if callback.message:
        await callback.message.answer(
            "Ок, создание товарного чека отменено.",
            reply_markup=receipt_root_kb(),
        )
    await callback.answer()


# ================== OCR-помощник ==================

def _extract_serial_from_text(raw_text: str) -> str:
    """
    Пытаемся вытащить в ПЕРВУЮ ОЧЕРЕДЬ Serial / S/N, и только если не нашли —
    берём IMEI. В конце — эвристики.
    """
    lines = [l.strip() for l in raw_text.splitlines() if l.strip()]
    upper_lines = [l.upper() for l in lines]

    # === 1) Serial / S/N (приоритет) ===
    for idx, uline in enumerate(upper_lines):
        if "SERIAL" in uline or "S/N" in uline or "S N" in uline:
            # Иногда значение на следующей строке
            candidate_sources = [uline]
            if idx + 1 < len(upper_lines):
                candidate_sources.append(upper_lines[idx + 1])

            for src in candidate_sources:
                # Берём последнюю группу A-Z0-9 длиной 8–20 символов — обычно это серийник
                matches = re.findall(r"[A-Z0-9]{8,20}", src)
                if matches:
                    return matches[-1]

    # === 2) IMEI / IMEI2 / IMEI/MEID по ключевым словам ===
    for uline in upper_lines:
        if "IMEI" in uline:
            digits = re.sub(r"[^0-9]", "", uline)
            if 14 <= len(digits) <= 17:
                return digits[:17]

    # === 3) fallback: длинные числовые последовательности (похожи на IMEI) ===
    for uline in upper_lines:
        digits = re.sub(r"[^0-9]", "", uline)
        if len(digits) >= 14:
            return digits[:17]

    # === 4) fallback: буквы+цифры длиной >= 8 (может быть серийник) ===
    for uline in upper_lines:
        if any(ch.isdigit() for ch in uline) and any(ch.isalpha() for ch in uline) and len(uline) >= 8:
            candidate = uline.replace(" ", "")
            candidate = candidate.replace("O", "0").replace("o", "0")
            candidate = re.sub(r"[^A-Z0-9]", "", candidate)
            return candidate[:32]

    return ""


async def _run_ocr_from_photo(message: Message) -> str:
    """
    Скачиваем фото, запускаем Tesseract и вытаскиваем кандидата для серийника/IMEI.
    """
    try:
        from PIL import Image
        import pytesseract
    except Exception:
        await message.answer(
            "⚠️ Ошибка OCR: не найдены библиотеки Pillow или pytesseract.\n"
            "Пожалуйста, введите серийный номер или IMEI вручную."
        )
        return ""

    if not message.photo:
        return ""

    photo = message.photo[-1]  # максимальное качество

    tmp_path = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=".jpg") as tmp:
            tmp_path = tmp.name

        await message.bot.download(photo, destination=tmp_path)

        img = Image.open(tmp_path)
        # На коробках всё английское → eng даёт чуть лучше качество
        raw_text = pytesseract.image_to_string(img, lang="eng")

    except Exception as e:
        await message.answer(
            f"⚠️ Не удалось распознать серийный номер по фото:\n<code>{e}</code>\n"
            f"Введи, пожалуйста, серийный номер или IMEI вручную."
        )
        return ""
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass

    candidate = _extract_serial_from_text(raw_text)
    return candidate


# ================== Создание чека (FSM) ==================

@router.callback_query(F.data == "receipt:create")
async def start_create_receipt(callback: CallbackQuery, state: FSMContext):
    """Старт процесса создания товарного чека."""
    await state.clear()
    await state.update_data(items=[], ocr_suggested_serial=None)
    await state.set_state(ReceiptForm.waiting_for_name)

    if callback.message:
        await callback.message.answer(
            "📝 Введите наименование товара (позиция 1):",
            reply_markup=fsm_kb(back_cb="receipt:menu"),
        )
    await callback.answer()


@router.message(ReceiptForm.waiting_for_name)
async def receipt_get_name(message: Message, state: FSMContext):
    name = (message.text or "").strip()
    if not name:
        await message.answer("Наименование не может быть пустым, введите ещё раз.")
        return

    await state.update_data(current_name=name, ocr_suggested_serial=None)
    await state.set_state(ReceiptForm.waiting_for_serial)
    await message.answer(
        "Введите серийный номер / IMEI:\n\n"
        "➕ Можно прислать фото коробки, я попробую распознать серийный номер.\n"
        "Если распознаю — покажу результат, и ты сможешь его отредактировать.",
        reply_markup=fsm_kb(back_cb="receipt:back_to_name"),
    )


@router.message(ReceiptForm.waiting_for_serial)
async def receipt_get_serial(message: Message, state: FSMContext):
    """
    Логика:
    1) Если в стейте есть ocr_suggested_serial и пришёл текст —
       считаем это подтверждением/правкой и идём дальше.
    2) Если пришёл текст (впервые) — берём как серийник.
    3) Если пришло фото — пытаемся OCR, кладём результат в ocr_suggested_serial
       и ждём следующего текста-подтверждения.
    """
    data = await state.get_data()
    ocr_suggested = data.get("ocr_suggested_serial")

    # --- Подтверждение/правка распознанного OCR серийника ---
    if message.text and ocr_suggested:
        txt = message.text.strip()

        confirm_tokens = {"+", "++", "ok", "ок", "да", "Да", "OK", "ОК"}
        if txt in confirm_tokens:
            serial = ocr_suggested
        else:
            serial = txt

        await state.update_data(current_serial=serial, ocr_suggested_serial=None)
        await state.set_state(ReceiptForm.waiting_for_price)
        await message.answer(
            f"Принял серийный номер / IMEI: <code>{serial}</code>\n\n"
            "Теперь введите цену за единицу (в рублях, только число):",
            reply_markup=fsm_kb(back_cb="receipt:back_to_serial"),
        )
        return

    # --- Ручной ввод серийника (без OCR) ---
    if message.text and not message.photo and not ocr_suggested:
        serial = message.text.strip()
        if not serial:
            await message.answer("Серийный номер не может быть пустым, введите ещё раз.")
            return

        await state.update_data(current_serial=serial, ocr_suggested_serial=None)
        await state.set_state(ReceiptForm.waiting_for_price)
        await message.answer(
            "Введите цену за единицу (в рублях, только число):",
            reply_markup=fsm_kb(back_cb="receipt:back_to_serial"),
        )
        return

    # --- Фото: запускаем OCR и ждём подтверждения ---
    if message.photo:
        serial_candidate = await _run_ocr_from_photo(message)
        if not serial_candidate:
            await message.answer(
                "Не удалось распознать серийный номер по фото.\n"
                "Пожалуйста, отправьте серийный номер или IMEI текстом.",
                reply_markup=fsm_kb(back_cb="receipt:back_to_name"),
            )
            return

        await state.update_data(ocr_suggested_serial=serial_candidate)
        await message.answer(
            "Я распознал серийный номер / IMEI по фото:\n"
            f"<code>{serial_candidate}</code>\n\n"
            "Если всё верно — отправь «+».\n"
            "Если нужно поправить — просто отправь правильный серийный номер текстом.",
            reply_markup=fsm_kb(back_cb="receipt:back_to_name"),
        )
        return

    await message.answer(
        "Не понял формат. Отправь серийный номер / IMEI текстом или фото коробки.",
        reply_markup=fsm_kb(back_cb="receipt:back_to_name"),
    )


@router.message(ReceiptForm.waiting_for_price)
async def receipt_get_price(message: Message, state: FSMContext):
    raw = (message.text or "").strip()
    text = raw.replace(" ", "").replace(",", ".")

    try:
        price = int(float(text))
        if price <= 0:
            raise ValueError
    except ValueError:
        await message.answer(
            "Не получилось распознать цену.\n"
            "Введите, пожалуйста, только число, без лишних символов."
        )
        return

    await state.update_data(current_price=price)
    await state.set_state(ReceiptForm.waiting_for_quantity)
    await message.answer(
        "Введите количество (шт):",
        reply_markup=fsm_kb(back_cb="receipt:back_to_price"),
    )


@router.message(ReceiptForm.waiting_for_quantity)
async def receipt_get_quantity(message: Message, state: FSMContext):
    raw = (message.text or "").strip()
    text = raw.replace(" ", "")

    try:
        qty = int(text)
        if qty <= 0:
            raise ValueError
    except ValueError:
        await message.answer(
            "Количество должно быть положительным целым числом. Попробуйте ещё раз."
        )
        return

    data = await state.get_data()
    name = data["current_name"]
    serial = data.get("current_serial", "")
    price = int(data["current_price"])

    items: List[Dict[str, Any]] = data.get("items", [])
    items.append(
        {
            "name": name,
            "serial": serial,
            "price": price,
            "quantity": qty,
        }
    )
    await state.update_data(items=items)

    # чистим временные поля
    await state.update_data(
        current_name=None,
        current_serial=None,
        current_price=None,
        ocr_suggested_serial=None,
    )

    await state.set_state(ReceiptForm.waiting_for_add_more)
    await message.answer(
        f"Добавлена позиция:\n"
        f"• {name}\n"
        f"• S/N / IMEI: {serial or '—'}\n"
        f"• Цена: {price} ₽\n"
        f"• Кол-во: {qty} шт.\n\n"
        f"Добавить ещё товар или сформировать чек?",
        reply_markup=add_more_kb(),
    )


# ====== Кнопки 'Назад' по шагам ======

@router.callback_query(F.data == "receipt:back_to_name")
async def receipt_back_to_name(callback: CallbackQuery, state: FSMContext):
    """
    Назад к вводу наименования товара.
    """
    data = await state.get_data()
    items: List[Dict[str, Any]] = data.get("items", [])
    idx = len(items) + 1

    await state.set_state(ReceiptForm.waiting_for_name)
    await state.update_data(ocr_suggested_serial=None)
    if callback.message:
        await callback.message.answer(
            f"📝 Введите наименование товара (позиция {idx}):",
            reply_markup=fsm_kb(back_cb="receipt:menu"),
        )
    await callback.answer()


@router.callback_query(F.data == "receipt:back_to_serial")
async def receipt_back_to_serial(callback: CallbackQuery, state: FSMContext):
    """
    Назад к вводу серийного номера.
    """
    await state.set_state(ReceiptForm.waiting_for_serial)
    await state.update_data(ocr_suggested_serial=None)
    if callback.message:
        await callback.message.answer(
            "Введите серийный номер / IMEI:\n\n"
            "Можно прислать фото коробки — попробую распознать.\n"
            "Или введи серийный номер/IMEI текстом.",
            reply_markup=fsm_kb(back_cb="receipt:back_to_name"),
        )
    await callback.answer()


@router.callback_query(F.data == "receipt:back_to_price")
async def receipt_back_to_price(callback: CallbackQuery, state: FSMContext):
    """
    Назад к вводу цены.
    """
    await state.set_state(ReceiptForm.waiting_for_price)
    if callback.message:
        await callback.message.answer(
            "Введите цену за единицу (в рублях, только число):",
            reply_markup=fsm_kb(back_cb="receipt:back_to_serial"),
        )
    await callback.answer()


@router.callback_query(F.data == "receipt:back_to_items")
async def receipt_back_to_items(callback: CallbackQuery, state: FSMContext):
    """
    Назад от ввода даты к выбору: добавить ещё / сформировать чек.
    """
    data = await state.get_data()
    items: List[Dict[str, Any]] = data.get("items", [])
    if not items:
        if callback.message:
            await callback.message.answer(
                "Позиции не найдены, начните с начала.",
                reply_markup=receipt_root_kb(),
            )
        await state.clear()
        await callback.answer()
        return

    await state.set_state(ReceiptForm.waiting_for_add_more)
    if callback.message:
        await callback.message.answer(
            f"У вас уже добавлено позиций: {len(items)}.\n"
            f"Добавить ещё товар или сформировать чек?",
            reply_markup=add_more_kb(),
        )
    await callback.answer()


# ====== Добавление ещё товаров / окончание списка ======

@router.callback_query(F.data == "receipt:more")
async def receipt_add_more(callback: CallbackQuery, state: FSMContext):
    """Пользователь хочет добавить ещё товар."""
    data = await state.get_data()
    items: List[Dict[str, Any]] = data.get("items", [])
    idx = len(items) + 1

    await state.set_state(ReceiptForm.waiting_for_name)
    await state.update_data(ocr_suggested_serial=None)
    if callback.message:
        await callback.message.answer(
            f"📝 Введите наименование товара (позиция {idx}):",
            reply_markup=fsm_kb(back_cb="receipt:menu"),
        )
    await callback.answer()


@router.callback_query(F.data == "receipt:done_items")
async def receipt_done_items(callback: CallbackQuery, state: FSMContext):
    """Пользователь закончил добавление позиций, переходим к дате."""
    data = await state.get_data()
    items: List[Dict[str, Any]] = data.get("items", [])
    if not items:
        if callback.message:
            await callback.message.answer("Вы ещё не добавили ни одной позиции.")
        await callback.answer()
        return

    await state.set_state(ReceiptForm.waiting_for_date)
    today_str = date.today().strftime("%d.%m.%Y")

    if callback.message:
        await callback.message.answer(
            f"Введите дату чека в формате ДД.ММ.ГГГГ.\n"
            f"Или отправьте «-», чтобы использовать сегодняшнюю дату ({today_str}).",
            reply_markup=fsm_kb(back_cb="receipt:back_to_items"),
        )
    await callback.answer()


@router.message(ReceiptForm.waiting_for_date)
async def receipt_get_date(message: Message, state: FSMContext):
    raw = (message.text or "").strip()

    if raw in ("-", "—"):
        receipt_date = date.today()
    else:
        try:
            receipt_date = datetime.strptime(raw, "%d.%m.%Y").date()
        except ValueError:
            await message.answer(
                "Не удалось распознать дату. Введите в формате ДД.ММ.ГГГГ "
                "или '-' для сегодняшней."
            )
            return

    data = await state.get_data()
    items: List[Dict[str, Any]] = data.get("items", [])
    await state.clear()

    if not items:
        await message.answer("Не нашёл ни одной позиции для чека, начните заново.")
        return

    pdf_path = generate_receipt_pdf(
        items=items,
        receipt_date=receipt_date,
    )

    pdf_file = FSInputFile(str(pdf_path))
    await message.answer_document(
        document=pdf_file,
        caption=f"Товарный чек\nДата: {receipt_date.strftime('%d.%m.%Y')}",
    )

    await message.answer(
        "Готово ✅\nЧек сохранён в папке бота по датам.",
        reply_markup=done_kb(),
    )


# ================== Просмотр чеков ==================

def _collect_months() -> List[tuple[int, int]]:
    """
    Собирает список (год, месяц), для которых есть какие-то чеки.
    """
    if not RECEIPTS_DIR.exists():
        return []

    months: set[tuple[int, int]] = set()

    for year_dir in RECEIPTS_DIR.iterdir():
        if not year_dir.is_dir():
            continue
        if not year_dir.name.isdigit():
            continue
        y = int(year_dir.name)

        for month_dir in year_dir.iterdir():
            if not month_dir.is_dir():
                continue
            if not month_dir.name.isdigit():
                continue
            m = int(month_dir.name)
            if list(month_dir.rglob("receipt_*.pdf")):
                months.add((y, m))

    return sorted(months, key=lambda ym: (ym[0], ym[1]), reverse=True)


def _collect_days(year: int, month: int) -> List[int]:
    """
    Возвращает список дней (int), где есть чеки за указанный год/месяц.
    """
    month_path = RECEIPTS_DIR / str(year) / f"{month:02d}"
    if not month_path.exists():
        return []

    days: set[int] = set()

    for day_dir in month_path.iterdir():
        if not day_dir.is_dir():
            continue
        if not day_dir.name.isdigit():
            continue
        d = int(day_dir.name)
        if list(day_dir.glob("receipt_*.pdf")):
            days.add(d)

    return sorted(days, reverse=True)


@router.callback_query(F.data == "receipt:list")
async def list_receipts(callback: CallbackQuery):
    """
    Первый шаг просмотра чеков — выбор месяца.
    """
    months = _collect_months()

    if not months:
        if callback.message:
            await callback.message.answer("Пока нет сохранённых товарных чеков.")
        await callback.answer()
        return

    kb_rows: List[List[InlineKeyboardButton]] = []
    for y, m in months:
        label = f"{m:02d}.{y}"
        cb = f"receipt:month:{y}:{m:02d}"
        kb_rows.append([InlineKeyboardButton(text=label, callback_data=cb)])

    kb_rows.append(
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="receipt:menu")]
    )
    kb_rows.append(
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")]
    )

    kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)

    if callback.message:
        await callback.message.answer("Выберите месяц:", reply_markup=kb)
    await callback.answer()


@router.callback_query(F.data.startswith("receipt:month:"))
async def list_receipts_days(callback: CallbackQuery):
    """
    Второй шаг — выбор конкретной даты внутри месяца.
    """
    try:
        _, _, year_str, month_str = callback.data.split(":")
        year = int(year_str)
        month = int(month_str)
    except Exception:
        await callback.answer("Ошибка формата месяца.")
        return

    days = _collect_days(year, month)
    if not days:
        if callback.message:
            await callback.message.answer("В этом месяце пока нет чеков.")
        await callback.answer()
        return

    kb_rows: List[List[InlineKeyboardButton]] = []
    for d in days:
        label = f"{d:02d}.{month:02d}.{year}"
        cb = f"receipt:day:{year}:{month:02d}:{d:02d}"
        kb_rows.append([InlineKeyboardButton(text=label, callback_data=cb)])

    kb_rows.append(
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="receipt:list")]
    )
    kb_rows.append(
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")]
    )

    kb = InlineKeyboardMarkup(inline_keyboard=kb_rows)

    if callback.message:
        await callback.message.answer(
            f"Выберите дату ({month:02d}.{year}):",
            reply_markup=kb,
        )
    await callback.answer()


@router.callback_query(F.data.startswith("receipt:day:"))
async def list_receipts_for_day(callback: CallbackQuery):
    """
    Третий шаг — отправка чеков за конкретную дату.
    """
    try:
        _, _, year_str, month_str, day_str = callback.data.split(":")
        year = int(year_str)
        month = int(month_str)
        day = int(day_str)
    except Exception:
        await callback.answer("Ошибка формата даты.")
        return

    day_path = RECEIPTS_DIR / str(year) / f"{month:02d}" / f"{day:02d}"
    if not day_path.exists():
        if callback.message:
            await callback.message.answer("За этот день чеков не найдено.")
        await callback.answer()
        return

    files = sorted(day_path.glob("receipt_*.pdf"), key=lambda p: p.name)
    if not files:
        if callback.message:
            await callback.message.answer("За этот день чеков не найдено.")
        await callback.answer()
        return

    if callback.message:
        await callback.message.answer(
            f"Товарные чеки за {day:02d}.{month:02d}.{year}:"
        )

    for path in files:
        filename = path.name
        try:
            number = filename.split("_")[1].split(".")[0]
        except Exception:
            number = "?"

        pdf_file = FSInputFile(str(path))
        if callback.message:
            await callback.message.answer_document(
                document=pdf_file,
                caption=f"Чек №{number}",
            )

    await callback.answer()
