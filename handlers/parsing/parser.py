# handlers/parsing/parser.py
from __future__ import annotations

import asyncio
import json
import re
import sys
import os
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from aiogram import Router, F
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup

# твои импорты (как было)
from telethon_manager import get_all_clients, resolve_entity, get_clients_for_user  # noqa
from handlers.auth_utils import auth_get

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from handlers.normalizers.entry import run_build_parsed_goods, run_build_parsed_etalon
from handlers.parsing.context import set_parsing_data_dir, user_data_dir, DEFAULT_BASE_DIR

router = Router()

# =========================
# FILES
# =========================
MODULE_DIR = Path(__file__).parent.resolve()
DATA_DIR = (MODULE_DIR / "data").resolve()
DATA_DIR.mkdir(parents=True, exist_ok=True)
MESSAGES_FILE = DATA_DIR / "parsed_messages.json"


# =========================
# UI (collect menu + запуск)
# =========================
def collect_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📊 Собрать все цены", callback_data="collect_all")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")],
        ]
    )


@router.callback_query(F.data == "collect")
async def collect_menu(callback: CallbackQuery):
    u = await auth_get(callback.from_user.id)
    access = (u or {}).get("access") or {}
    if not u or not (u.get("role") == "admin" or access.get("products.collect")):
        await callback.answer("⛔️ Нет доступа", show_alert=True)
        return
    if callback.message:
        await callback.message.answer("🏷 Выбери режим парсинга:", reply_markup=collect_menu_keyboard())
    try:
        await callback.answer()
    except Exception:
        pass
    if (u or {}).get("role") != "admin":
        set_parsing_data_dir(DEFAULT_BASE_DIR)


@router.callback_query(F.data == "show_unmatched")
async def show_unmatched(callback: CallbackQuery):
    if callback.message:
        await callback.message.answer(
            "ℹ️ Unmatched формируется позже (matcher/results). Здесь только parsed_messages.json."
        )
    try:
        await callback.answer()
    except Exception:
        pass


def _reset_outputs() -> None:
    _reset_data_dir_files()



@router.callback_query(F.data == "clear_prices")
async def clear_prices(callback: CallbackQuery):
    _reset_outputs()
    if callback.message:
        await callback.message.answer("🗑 parsed_messages.json очищен.", reply_markup=collect_menu_keyboard())
    try:
        await callback.answer()
    except Exception:
        pass


@router.callback_query(F.data == "collect_all")
async def collect_all(callback: CallbackQuery):
    u = await auth_get(callback.from_user.id)
    access = (u or {}).get("access") or {}
    if not u or not (u.get("role") == "admin" or access.get("products.collect")):
        await callback.answer("⛔️ Нет доступа", show_alert=True)
        return
    if u.get("role") != "admin" and u.get("sources_mode") == "default":
        await callback.message.answer("⛔️ В режиме 'По умолчанию' сбор своих цен недоступен.")
        return
    if u.get("role") == "admin":
        set_parsing_data_dir(DEFAULT_BASE_DIR)
    else:
        set_parsing_data_dir(user_data_dir(callback.from_user.id))
    _reset_outputs()

    # ✅ дефолты, чтобы не словить UnboundLocalError даже если что-то упадёт раньше
    stats_sources: Dict[str, Any] = {}
    messages: List[Dict[str, Any]] = []
    errors_block = ""
    zeros_block = ""

    if callback.message:
        await callback.message.answer("🚀 Запускаю сбор сообщений…")

    # --- collect stage ---
    if (u or {}).get("role") == "admin":
        messages, stats_sources = await collect_messages()
    else:
        sources_mode = (u or {}).get("sources_mode", "default")
        messages, stats_sources = await collect_messages(user_id=callback.from_user.id, sources_mode=sources_mode)

    # --- errors per source (collect stage) ---
    per = (stats_sources or {}).get("per_source") or []
    err_items = [x for x in per if isinstance(x, dict) and not x.get("ok")]
    zero_items = [
        x for x in per
        if isinstance(x, dict) and x.get("ok") and int(x.get("messages") or 0) == 0
    ]

    def _fmt_err(x: Dict[str, Any]) -> str:
        src = (x.get("source") or "Unknown").strip()
        err = (x.get("error") or "unknown error").strip()
        err = re.sub(r"\s+", " ", err)
        return f"• <b>{src}</b> — {err}"

    def _fmt_zero(x: Dict[str, Any]) -> str:
        src = (x.get("source") or "Unknown").strip()
        note = (x.get("skipped") or "no messages").strip()
        note = re.sub(r"\s+", " ", note)
        return f"• <b>{src}</b> — {note}"

    if err_items:
        MAX_ERR = 12
        shown = err_items[:MAX_ERR]
        tail = len(err_items) - len(shown)
        lines = "\n".join(_fmt_err(x) for x in shown)
        if tail > 0:
            lines += f"\n…и ещё <b>{tail}</b> источн."
        errors_block = f"\n\n<b>Ошибки по источникам:</b>\n{lines}"

    if zero_items:
        MAX_ZERO = 10
        shown = zero_items[:MAX_ZERO]
        tail = len(zero_items) - len(shown)
        lines = "\n".join(_fmt_zero(x) for x in shown)
        if tail > 0:
            lines += f"\n…и ещё <b>{tail}</b> источн."
        zeros_block = f"\n\n<b>Без сообщений:</b>\n{lines}"

    # ✅ ДО парсинга: если шапка одинаковая — оставляем только самое новое сообщение
    messages = dedupe_messages_by_header_keep_latest(messages)

    parsed_messages = parse_messages(messages)
    _write_json(MESSAGES_FILE, parsed_messages)

    # ✅ полный пайплайн в одном thread, чтобы не плодить ошибки/гонки
    def _run_pipeline() -> None:
        run_build_parsed_etalon()
        run_build_parsed_goods()

        try:
            from handlers.parsing import results as results_mod
            run_results = getattr(results_mod, "run_results", None)
        except Exception:
            run_results = None

        if callable(run_results):
            run_results()

    await asyncio.to_thread(_run_pipeline)

    # кастом: дополняем пользовательский matched нашими данными и пересобираем parsed_data.json
    if (u or {}).get("role") != "admin" and (u or {}).get("sources_mode") == "custom":
        try:
            from handlers.parsing import results as results_mod
            base_matched = (DEFAULT_BASE_DIR / "parsed_matched.json")
            user_dir = user_data_dir(callback.from_user.id)
            user_matched = user_dir / "parsed_matched.json"

            def _load_items(p: Path) -> list[dict]:
                if not p.exists():
                    return []
                try:
                    raw = json.loads(p.read_text(encoding="utf-8"))
                except Exception:
                    return []
                if isinstance(raw, dict) and isinstance(raw.get("items"), list):
                    return [x for x in raw["items"] if isinstance(x, dict)]
                if isinstance(raw, list):
                    return [x for x in raw if isinstance(x, dict)]
                return []

            merged_items = _load_items(base_matched) + _load_items(user_matched)
            user_matched.write_text(
                json.dumps(
                    {"items": merged_items, "items_count": len(merged_items)},
                    ensure_ascii=False,
                    indent=2,
                ),
                encoding="utf-8",
            )
            set_parsing_data_dir(user_dir)
            results_mod.rebuild_parsed_data_all()
        except Exception:
            pass

    total_msgs = len(parsed_messages)
    total_lines = sum(int(m.get("lines_count") or 0) for m in parsed_messages)

    if callback.message:
        await callback.message.answer(
            f"✅ Готово.\n"
            f"Сообщений: <b>{total_msgs}</b>\n"
            f"Строк: <b>{total_lines}</b>\n"
            f"Источники: <b>{int((stats_sources or {}).get('total', 0) or 0)}</b>\n"
            f"Обработано: <b>{int((stats_sources or {}).get('processed', 0) or 0)}</b>\n"
            f"Ошибок: <b>{int((stats_sources or {}).get('errors', 0) or 0)}</b>\n"
            f"{errors_block}"
            f"{zeros_block}",
            reply_markup=collect_menu_keyboard(),
        )

    try:
        await callback.answer()
    except Exception:
        pass



# =========================
# IO helpers
# =========================
def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

def _reset_data_dir_files() -> None:
    """
    Очищаем ВСЕ файлы в DATA_DIR, но НЕ удаляем их:
      - *.json -> записываем []
      - остальные -> записываем пустую строку ""
    Папки внутри DATA_DIR не трогаем.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    for p in DATA_DIR.iterdir():
        if not p.is_file():
            continue

        try:
            if p.suffix.lower() == ".json":
                p.write_text("[]", encoding="utf-8")
            else:
                p.write_text("", encoding="utf-8")
        except Exception:
            # не валим парсер, если один файл не удалось очистить
            pass



# =========================
# Stage 2: message-level drops (глушим целиком)
# =========================

# ✅ Дефектные блоки "обмен/брак", "уценка" и т.п. — если это заголовок (первые строки)
RE_DROP_DEFECT_HEADER = re.compile(
    r"(?is)^\s*(?:✨\s*)?(?:[\W_]{0,6}\s*)?"
    r"(обмен\s*/\s*брак|обменка|брак|уценк\w*|витрин\w*|"
    r"ремонт\w*|сц\b|service\s*center|refurb\w*|ref\b|used|б/у|б\\у)\b"
)

# ✅ Технические/сервисные анонсы (не прайс)
RE_DROP_ANNOUNCE = re.compile(
    r"(?is)\b("
    r"обновил[аи]\s+наличие|наличие\s+товар(ов)?\s+на|"
    r"сделать\s+заказ|задать\s+вопрос|для\s+заказа\s+пишите|"
    r"прайс\s+(?:закрыт|обновл[её]н|обновили)|работаем\s+по\s+запросу|"
    r"заказ\s+в\s+директ|в\s+личку|в\s+лс|"
    r"@[\w\d_]{3,}"
    r")\b"
)

# ✅ Инструкции / формат заказа (глушим пост целиком) — РАСШИРЕНО (ловим "в следующем формате")
RE_DROP_INSTRUCTION = re.compile(
    r"(?is)\b("
    r"в\s+(?:следующ\w*|так\w*|эт\w*|данн\w*|таком|этом|данном)\s+формат\w*|"
    r"в\s*формате|формат\s+(?:заказа|запроса)|"
    r"пример\s+(?:заказа|запроса)|образец\s+(?:заказа|запроса)|шаблон\s+(?:заказа|запроса)|"
    r"как\s+(?:оформить|написать|сделать)\s+(?:заказ|запрос)|"
    r"заказы\s+принимаются|"
    r"запрос\s+по\s+товару\s+делать|делать\s+запрос|"
    r"не\s+отписываем|не\s+успеваем\s+отписывать|уважайте\s+время|"
    r"для\s+заказа\s+пишите|заказ\s+в\s+(?:директ|личку|лс)|"
    r"гарантийный\s+срок|до\s+активации|"
    r"выдача\s*/\s*прием|проверка\s+устройства"
    r")\b"
)

# Розыгрыши / билеты / промо (глушим пост целиком)
RE_DROP_GIVEAWAY = re.compile(
    r"(?is)\b("
    r"розыгрыш|конкурс|"
    r"разыгр\w*|"              # ✅ разыгрывать/разыграем/разыгрываем/разыграли
    r"give\s*away|giveaway|"
    r"бесплатн\w*\s+билет|билет(ы)?\b|приз(ы)?\b|призов(ых)?\s+мест|"
    r"участв(уй|уйте|овать)\w*|участвуют|"
    r"успей(те)?\s+(?:купить|приобрести)|"
    r"вылож(у|им)\s+\d+\s+билет|"
    r"раздел[еа]\s*\"?билет(ы)?\"?|@[\w\d_]{3,}|_bot\b|"
    r"подар(ок|ки)\b|🎁|🏆"
    r")\b"
)

# “Шапки” и баннеры — не причина глушить, но будем удалять как строки
RE_LINE_BANNER = re.compile(r"^(?:[\W_]{6,}|[=]{6,}|_{3,})$")


# =========================
# Stage 3: emoji cleanup (keep flags)
# =========================
# Оставляем 🇺🇸🇯🇵 и т.п., удаляем остальное emoji/pictographs
RE_FLAGS = re.compile(r"[\U0001F1E6-\U0001F1FF]{2}")
RE_REMOVE_EMOJI_EXCEPT_FLAGS = re.compile(
    r"(?![\U0001F1E6-\U0001F1FF]{2})"
    r"[\U0001F000-\U0001FAFF\u2600-\u27BF]"
)


def strip_emoji_except_flags(text: str) -> str:
    if not text:
        return ""
    flags: List[str] = RE_FLAGS.findall(text)
    tmp = RE_FLAGS.sub("<<FLAG>>", text)

    tmp = RE_REMOVE_EMOJI_EXCEPT_FLAGS.sub("", tmp)

    for f in flags:
        tmp = tmp.replace("<<FLAG>>", f, 1)

    tmp = re.sub(r"[\u200d\uFE0F]", "", tmp)
    return tmp


# =========================
# Stage 4: line logic (YouTake-style join)
# =========================

# ✅ Даты/время — вырезаем перед поиском цены, чтобы "2026" не считалась ценой
RE_DATE = re.compile(r"(?i)\b\d{1,2}[./-]\d{1,2}[./-]\d{2,4}\b")
RE_TIME = re.compile(r"(?i)\b\d{1,2}:\d{2}\b")
RE_YEAR = re.compile(r"(?i)\b20\d{2}\b")  # 2000-2099


def _strip_dates_times_for_price(s: str) -> str:
    s = RE_DATE.sub(" ", s or "")
    s = RE_TIME.sub(" ", s)
    # если остался голый год — тоже убираем (часто в анонсах)
    s = RE_YEAR.sub(" ", s)
    return _clean_spaces(s)


# =========================
# Price detection (supports "k-format": " - 19", " - 14,5", " - 2,3")
# =========================

# 1) классика: 45 900 / 45900 / 125000
_RE_PRICE_CLASSIC = r"(?:\d{1,3}(?:[ .]\d{3})+|\d{4,6})"

# 2) "тысячный" формат: строго как разделитель товара:
#    ДОЛЖНЫ быть пробелы вокруг тире: " ... - 19", " ... — 14,5"
#    (так мы НЕ ловим "10-20 минут" и НЕ ловим ": 10")
_RE_PRICE_K_AFTER_DASH = r"(?:\s[-—]\s*\d{1,3}(?:[.,]\d{1,2})?)"

RE_PRICE = re.compile(rf"(?i)(?:{_RE_PRICE_CLASSIC}|{_RE_PRICE_K_AFTER_DASH})")

RE_PRICE_ONLY = re.compile(
    rf"(?i)^\s*(?:от\s*\d+\s*шт\s*[-—]?\s*)?"
    rf"(?:{_RE_PRICE_CLASSIC}|{_RE_PRICE_K_AFTER_DASH})"
    rf"\s*(?:₽|руб|р\.|р)?\s*$"
)

# ✅ YouTakeBot: "От N шт - PRICE" (с флагом/без)
RE_YOUTAKE_TIER = re.compile(
    rf"(?i)^\s*(?:{RE_FLAGS.pattern}\s*)?"   # optional flag
    r"от\s*(\d+)\s*(?:шт\.?|штук)\s*[-—]\s*"
    rf"({_RE_PRICE_CLASSIC})"
    r"\s*(?:₽|руб|р\.|р)?\s*$"
)


RE_YOUTAKE_TIER_ANY = re.compile(
    rf"(?i)(?:от\s*)?(\d+)\s*шт\s*[-—]\s*({_RE_PRICE_CLASSIC})"
)


# “нет в наличии”
RE_OOS = re.compile(r"(?i)\b(нет\s*в\s*наличии|out\s*of\s*stock|sold\s*out|❌)\b")

# ✅ дефектка/уценка/актив/обмен/б/у — РАСШИРИЛИ
RE_DEFECT_LINE = re.compile(
    r"(?i)\b("
    r"уценк\w*|дефект\w*|скол\w*|царап\w*|помят\w*|подмят\w*|примят\w*|вмят\w*|трещин\w*|"
    r"мят\w*|"
    r"бит(ый)?|разбит\w*|битый\s*пиксел\w*|"
    r"ремонт\w*|сц\b|service\s*center|"
    r"гаранти\w*\s+(?:вышл\w*|нет)|"
    r"замен\w*\s+(?:плат\w*|диспле\w*|экран\w*|микрофон\w*|камер\w*|аккум\w*|"
    r"динамик\w*|корпус\w*|шлейф\w*|разъ[её]м\w*|usb)|"
    r"обмен\w*|обменка|swap|refurb\w*|ref\b|"
    r"витрин\w*|демо|ex[- ]?demo|used|б/у|б\\у|"
    r"не\s*актив|актив\w*|active|откр\w*|open|пломб\w*|"
    r"размотан\w*|комплект\s*непол\w*"
    r")\b"
)

RE_WHOLESALE_QTY = re.compile(r"(?i)\bот\s*(?:10|20|30|50|100)\s*шт\b")

# ✅ количество штук в строках прайса/примера: "- 2шт", "2 шт", "1pcs"
RE_QTY_IN_LINE = re.compile(r"(?i)(?:^|[\s])[-—]?\s*\d+\s*(?:шт|штук|pcs)\b")

# ✅ блок "Пример/например"
# ЛОВИМ "например" ВНУТРИ СТРОКИ (инструкции) — чтобы не пропускать пример-товар ниже
RE_EXAMPLE_START = re.compile(r"(?i)\b(пример|например)\b")

# ✅ признаки "это уже не пример", закрываем example_mode
RE_EXAMPLE_END_HINT = re.compile(
    r"(?i)^\s*(?:"
    r"выдача|прием|кэш|гарантия|возврат|доставка|самовывоз|"
    r"оформление|важная\s+информация|цены\s+и\s+количество|"
    r"заказ\s+считается|для\s+связи|контакт|адрес|время|"
    r"работаем|прайс\b|по\s+прайсу|наличие|оплата"
    r")\b"
)

# ✅ "10-20 минут" и подобное — не цена
RE_NOT_PRICE_TAIL = re.compile(r"(?i)\b(минут|мин|час|часов|дн(?:я|ей)?|%|процент)\b")
RE_RANGE_PREFIX = re.compile(r"^\s*\d+\s*[-—]\s*\d+\b")  # "10-20", "5-30"

# ✅ информационные строки (логистика/оплата/гарантия/инструкции)
RE_INFO_GUARD = re.compile(
    r"(?i)\b("
    r"самовывоз|доставк\w*|курьер\w*|склад\w*|загрузк\w*|"
    r"оплат\w*|наличн\w*|купюр\w*|номинал\w*|сдач\w*|процент|%|"
    r"гаранти\w*|возврат\w*|обмен\w*|диагност\w*|сервисн\w*\s*цент|"
    r"как\s+оформить|как\s+заказать|пример|образец|формат\s+заказа|шаблон\s+заказа|"
    r"заказ\s+не\s+считается|не\s+считается\s+принят|"
    r"пишите\s+за\s*\d+\s*минут|пожалуйста\s+пишите|"
    r"график\s+работы|режим\s+работы|"
    r"работаем|по\s+прайсу|прайс\b"
    r")\b"
)

# ✅ уточняющий хинт: "например" как вводная инструкция (а не товарная строка)
RE_EXAMPLE_INSTRUCTION_HINT = re.compile(
    r"(?i)\b("
    r"в\s+следующ\w*\s+формат\w*|в\s+таком\s+формат\w*|"
    r"формат\w*\s+(?:заказа|запроса)|"
    r"как\s+(?:оформить|сделать)\s+(?:заказ|запрос)|"
    r"запрос\s+по\s+товару|делать\s+в\s+следующ\w*\s+формат\w*"
    r")\b"
)

RE_ENUM_PREFIX = re.compile(r"(?i)^\s*\d{1,2}[.)]\s+")
RE_SPEC_LINE_START = re.compile(r"(?i)^\s*(\d{1,4}\s*(gb|tb)\b|\d{1,3}\s*/\s*\d{2,4}\s*(gb|tb)?\b)")
RE_SPEC_LINE_NUMBERED = re.compile(r"(?i)^\s*\d{1,2}\s+\d{1,2}\s*/\s*\d{2,4}\s*(gb|tb)?\b")
RE_PRODUCT_TOKENS = re.compile(
    r"(?i)\b(iphone|ipad|macbook|imac|airpods|watch|apple|galaxy|samsung|pixel|xiaomi|"
    r"poco|redmi|realme|honor|huawei|oneplus|oppo|vivo|tecno|infinix|ps5|playstation)\b"
)
RE_VARIANT_HEADER = re.compile(r"(?i)\b(air|pro|max|plus|mini|ultra|m\d)\b|\d{1,2}")


# =========================
# Parsing result structs
# =========================
@dataclass
class DeletedItem:
    text: str
    reason: str


def _clean_spaces(s: str) -> str:
    s = (s or "").replace("\xa0", " ")
    s = re.sub(r"[ \t]{2,}", " ", s)
    return s.strip()


def _basic_lines_from_message(text: str) -> List[str]:
    raw_lines = (text or "").splitlines()
    out: List[str] = []
    for ln in raw_lines:
        ln = strip_emoji_except_flags(ln)
        ln = _clean_spaces(ln)
        if not ln:
            continue
        out.append(ln)
    return out


def _is_header_or_separator_line(line: str) -> bool:
    if not line:
        return True
    if RE_LINE_BANNER.match(line):
        return True

    alnum = sum(ch.isalnum() for ch in line)
    if alnum == 0 and len(line) >= 4:
        return True

    if len(line) >= 8 and len(set(line.replace(" ", ""))) <= 3:
        return True

    return False


def _is_example_start_line(ln: str) -> bool:
    """
    Старт "пример/например" блока:
    - есть "пример/например"
    - и строка похожа на инструкцию/вводную (а не товар)
    """
    if not ln:
        return False

    if not RE_EXAMPLE_START.search(ln):
        return False

    # если в строке уже есть цена — это почти точно НЕ "старт примера"
    probe = _strip_dates_times_for_price(ln)
    if RE_PRICE.search(probe):
        return False

    # явные инструкции — почти гарантированно "пример"
    if RE_EXAMPLE_INSTRUCTION_HINT.search(ln):
        return True

    # fallback: "например," / "например:" / "например" как короткая вводная
    if re.search(r"(?i)\bнапример\b\s*[:,-]?\s*$", ln.strip()):
        return True

    # иначе тоже считаем стартом, но только если строка не выглядит как товар (без цены)
    return True


def _strip_second_price(line: str) -> str:
    """
    Если в строке два ценовых токена (например, "X - 17900 - 17850"),
    оставляем только первую цену, флаги на конце сохраняем.
    """
    if not line:
        return line
    prices = list(RE_PRICE.finditer(line))
    if len(prices) < 2:
        return line

    # Ignore year-like tokens (e.g., 2024) and price-like fragments inside codes (e.g., MXN63).
    price_matches = []
    for m in prices:
        raw_token = m.group(0)
        token = raw_token.replace(" ", "").replace(".", "")
        if len(token) == 4 and token.isdigit() and 2000 <= int(token) <= 2099:
            continue
        # Skip matches that start inside an alphanumeric token or continue a digit sequence.
        if m.start() > 0 and line[m.start() - 1].isalnum():
            continue
        if m.end() < len(line) and line[m.end()].isdigit():
            continue
        # Skip memory-like pairs like "17 256" or "16 128".
        if " " in raw_token:
            parts = [p for p in raw_token.split() if p.isdigit()]
            if len(parts) == 2:
                a, b = parts
                if b in {"64", "128", "256", "512", "1024", "2048"}:
                    try:
                        if int(a) <= 30:
                            continue
                    except Exception:
                        pass
        price_matches.append(m)

    if len(price_matches) < 2:
        return line

    # Require a clear separator between prices (e.g., "- 17900 - 17850").
    between = line[price_matches[0].end():price_matches[1].start()]
    if "-" not in between and "—" not in between:
        return line

    flags = RE_FLAGS.findall(line)
    cut_pos = price_matches[1].start()
    head = line[:cut_pos].rstrip(" -–—")
    if flags:
        head = f"{head} {' '.join(flags)}"
    return _clean_spaces(head)


def _apply_header_context(lines: List[str]) -> List[str]:
    """
    Префиксуем спецификационные строки (например, "256GB Blue - 35500")
    последним заголовком ("iPad Air 11 M3 Wi-Fi"), чтобы entry.py смог смэтчить.
    """
    out: List[str] = []
    current_header = ""
    base_header = ""

    for ln in lines:
        ln_clean = strip_emoji_except_flags(ln)
        ln_clean = _clean_spaces(ln_clean)
        if not ln_clean:
            continue

        ln_clean = _strip_second_price(ln_clean)

        probe = _strip_dates_times_for_price(ln_clean)
        has_price = bool(RE_PRICE.search(probe))

        if not has_price:
            if re.search(r"[A-Za-zА-Яа-я]", ln_clean):
                header = re.sub(r"[:\s-]+$", "", ln_clean).strip()
                if RE_PRODUCT_TOKENS.search(header):
                    base_header = header
                    current_header = header
                elif RE_VARIANT_HEADER.search(header) and base_header:
                    current_header = _clean_spaces(f"{base_header} {header}")
                else:
                    # не меняем заголовок на материал/тип
                    pass
            out.append(ln_clean)
            continue

        ln_check = _clean_spaces(RE_FLAGS.sub(" ", ln_clean))
        if current_header and (RE_SPEC_LINE_START.search(ln_check) or RE_SPEC_LINE_NUMBERED.search(ln_check)) and not RE_PRODUCT_TOKENS.search(ln_check):
            out.append(_clean_spaces(f"{current_header} {ln_clean}"))
            continue

        out.append(ln_clean)

    return out


# =========================
# Stage 1.5: channel-level dedupe by header (keep only latest)
# =========================

RE_MENTIONS = re.compile(r"(?i)@\w{3,}")
RE_NON_WORD_PUNCT = re.compile(r"[^\w\s\u0400-\u04FF]+", re.UNICODE)  # пунктуация (оставляем буквы/цифры/_)
RE_CONTACT_LINE = re.compile(r"(?i)\b(для\s+связи|контакт|контакты)\b")


def _normalize_header_line(s: str) -> str:
    s = (s or "").strip().lower()
    if not s:
        return ""

    # выкидываем @mentions
    s = RE_MENTIONS.sub("", s)

    s = re.sub(r"\s+", " ", s)

    # если строка контактная — делаем пустой, чтобы не влияла на fingerprint
    if RE_CONTACT_LINE.search(s):
        return ""

    # убираем одиночные латинские токены (I, l, x и т.п.)
    for _ in range(2):
        s = re.sub(r"(?i)\b[a-z]\b", " ", s)
        s = re.sub(r"\s+", " ", s).strip()

    # убираем пунктуацию/декор (оставляем слова/цифры)
    s = RE_NON_WORD_PUNCT.sub(" ", s)
    s = re.sub(r"\s+", " ", s).strip()

    return s


def _extract_header_fingerprint(raw_text: str) -> str:
    """
    "Шапка" = первые непустые строки ДО первого реального товара с ценой.
    Нормализуем агрессивно, чтобы одинаковые шапки совпадали
    даже при артефактах (например ' I '), @юзерах и т.п.
    """
    lines = _basic_lines_from_message(raw_text or "")
    if not lines:
        return ""

    head_parts: List[str] = []
    max_lines = 14  # чтобы захватить гарантию/условия/важно

    for ln in lines:
        if _is_header_or_separator_line(ln):
            continue

        probe = _strip_dates_times_for_price(ln)
        has_text_signal = any(ch.isalpha() for ch in probe) or bool(
            re.search(r"(?i)\b(usb|type-?c|iphone|ipad|airpods|dyson|whoop|starlink|dji|pro|max|ultra|m\d)\b", probe)
        )

        # если это уже строка товара с ценой — шапку заканчиваем
        if has_text_signal and RE_PRICE.search(probe) and not RE_OOS.search(ln):
            break

        ln2 = _normalize_header_line(ln)
        if not ln2:
            continue

        head_parts.append(ln2)
        if len(head_parts) >= max_lines:
            break

    fp = " | ".join(head_parts)
    fp = re.sub(r"\s+", " ", fp).strip()
    return fp


def _message_sort_key(m: Dict[str, Any]) -> Tuple[int, int]:
    dt = m.get("date")
    ts = 0
    if isinstance(dt, str) and dt:
        try:
            ts = int(datetime.fromisoformat(dt.replace("Z", "+00:00")).timestamp())
        except Exception:
            ts = 0
    mid = int(m.get("message_id") or 0)
    return (ts, mid)


def dedupe_messages_by_header_keep_latest(messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Для каждого channel:
      - группируем сообщения по fingerprint шапки
      - оставляем только самое новое (по date/message_id)
    """
    by_channel: Dict[str, List[Dict[str, Any]]] = {}
    for m in messages:
        ch = (m.get("channel") or "").strip() or "Unknown"
        by_channel.setdefault(ch, []).append(m)

    out: List[Dict[str, Any]] = []
    for ch, items in by_channel.items():
        items_sorted = sorted(items, key=_message_sort_key)  # старые -> новые

        best_by_fp: Dict[str, Dict[str, Any]] = {}
        for mm in items_sorted:
            fp = _extract_header_fingerprint(mm.get("message") or "")
            if not fp:
                # если шапка пустая — не сливаем в одну группу
                fp = f"__empty__:{mm.get('message_id')}:{mm.get('date')}"
            best_by_fp[fp] = mm  # перезапишется => останется самое новое

        out.extend(best_by_fp.values())

    return sorted(out, key=_message_sort_key)


def _looks_like_price_list(lines: List[str]) -> bool:
    """
    Эвристика: если в сообщении есть несколько строк, похожих на прайс,
    мы НЕ глушим сообщение целиком даже если есть "гарантия/обмен/до активации" и т.п.
    """
    if not lines:
        return False

    price_like = 0
    for ln in lines:
        if _is_header_or_separator_line(ln):
            continue
        if RE_OOS.search(ln):
            continue

        probe = _strip_dates_times_for_price(ln)

        # строка должна быть "похожа на товар": либо есть буквы, либо типовые токены
        has_text_signal = any(ch.isalpha() for ch in probe) or bool(
            re.search(
                r"(?i)\b(usb|type-?c|iphone|ipad|airpods|dyson|whoop|starlink|dji|pro|max|ultra|m\d)\b",
                probe,
            )
        )

        if has_text_signal and RE_PRICE.search(probe):
            price_like += 1
            if price_like >= 3:  # ✅ порог: 3 товарных строки с ценой
                return True

    return False


# Розыгрыши / билеты / промо (глушим пост целиком)
RE_DROP_GIVEAWAY = re.compile(
    r"(?is)\b("
    r"розыгрыш|конкурс|"
    r"разыгр\w*|"              # ✅ разыгрывать/разыграем/разыгрываем/разыграли
    r"give\s*away|giveaway|"
    r"бесплатн\w*\s+билет|билет(ы)?\b|приз(ы)?\b|призов(ых)?\s+мест|"
    r"участв(уй|уйте|овать)\w*|участвуют|"
    r"успей(те)?\s+(?:купить|приобрести)|"
    r"вылож(у|им)\s+\d+\s+билет|"
    r"раздел[еа]\s*\"?билет(ы)?\"?|@[\w\d_]{3,}|_bot\b|"
    r"подар(ок|ки)\b|🎁|🏆"
    r")\b"
)

def _should_drop_message_entirely(message_text: str) -> Optional[str]:
    t = message_text or ""
    raw_lines = _basic_lines_from_message(t)
    first_lines = raw_lines[:4]
    head = "\n".join(first_lines)

    defect_head = re.search(
        r"(?is)^\s*(?:[\W_]{0,10}\s*)?"
        r"(обмен\s*(?:/|\\|\s+)?\s*брак|обменка|брак|уценк\w*|витрин\w*|"
        r"ремонт\w*|refurb\w*|ref\b|used|б/у|б\\у)\b",
        head,
    )
    if defect_head:
        return "defect_header_message"

    # ✅ РОЗЫГРЫШИ/ПРОМО — ВСЕГДА глушим целиком
    if RE_DROP_GIVEAWAY.search(t):
        return "giveaway_message"

    looks_like_price = _looks_like_price_list(raw_lines)

    # ✅ инструкции + много строк с количеством "шт" => почти всегда пример оформления заказа, глушим ВСЕГДА
    if RE_DROP_INSTRUCTION.search(t):
        qty_lines = 0
        for ln in raw_lines:
            if _is_header_or_separator_line(ln):
                continue
            probe = _strip_dates_times_for_price(ln)
            if RE_PRICE.search(probe) and RE_QTY_IN_LINE.search(ln):
                qty_lines += 1
        if qty_lines >= 2:
            return "instruction_message"

    if RE_DROP_INSTRUCTION.search(t) and not looks_like_price:
        return "instruction_message"
    if RE_DROP_ANNOUNCE.search(t) and not looks_like_price:
        return "announce_message"

    if "\n" in t:
        defect_lines = 0
        price_lines = 0
        for ln in raw_lines:
            if _is_header_or_separator_line(ln):
                continue
            probe = _strip_dates_times_for_price(ln)
            if RE_DEFECT_LINE.search(ln):
                defect_lines += 1
            if RE_PRICE.search(probe) and (any(ch.isalpha() for ch in probe) or "-" in probe or "—" in probe):
                price_lines += 1

        if defect_lines >= 3 and price_lines >= 3 and defect_lines >= int(price_lines * 0.6):
            return "defect_multiline_message"

    return None

def _join_youtake_pairs(lines: List[str]) -> List[str]:
    """
    Склейка YouTakeBot:
      A: строка товара (обычно начинается с "• ")
      B: "<flag> От N шт - PRICE"
        - N == 1: склеиваем в "A - PRICE <flags>"
        - N  > 1: цену игнорируем, строку B выкидываем (wholesale tier)
    Важно: сохраняем формат с тире, чтобы downstream парсеры не ломались.
    """
    out: List[str] = []
    i = 0

    def _dedup_flags(s: str) -> str:
        fl = RE_FLAGS.findall(s or "")
        if not fl:
            return ""
        # дедуп с сохранением порядка
        uniq: List[str] = []
        for f in fl:
            if f not in uniq:
                uniq.append(f)
        return "".join(uniq)

    while i < len(lines):
        a = lines[i]
        b = lines[i + 1] if i + 1 < len(lines) else None

        a_clean = _clean_spaces(a)

        if b:
            b_clean = _clean_spaces(b)

            # ✅ YouTake tier line
            m = RE_YOUTAKE_TIER.match(b_clean)
            if m:
                qty = int(m.group(1) or 0)
                price = (m.group(2) or "").strip()

                # флаги из A и B, без дублей
                flags = _dedup_flags(a_clean + " " + b_clean)

                if qty == 1:
                    # ✅ КЛЮЧЕВО: добавляем " - " разделитель
                    merged = _clean_spaces(f"{a_clean} - {price} {flags}".strip())
                    out.append(merged)
                # qty > 1: выкидываем tier line целиком (не добавляем никуда)

                i += 2
                continue

            # ✅ старый кейс: B = чистая цена (на всякий, если вдруг встречается)
            if RE_PRICE_ONLY.match(b_clean):
                # цена из B
                pm = RE_PRICE.search(_strip_dates_times_for_price(b_clean))
                price = pm.group(0) if pm else ""
                if price:
                    flags = _dedup_flags(a_clean + " " + b_clean)
                    merged = _clean_spaces(f"{a_clean} - {price} {flags}".strip())
                    out.append(merged)
                    i += 2
                    continue

        out.append(a_clean)
        i += 1

    return out


def _filter_lines(lines: List[str]) -> Tuple[List[str], List[DeletedItem], List[str]]:
    kept: List[str] = []
    deleted: List[DeletedItem] = []
    deleted_rows_legacy: List[str] = []

    example_mode = False

    prev_price_line = False
    for ln in lines:
        # ✅ старт "пример/например" (ловим и "например," в середине строки-инструкции)
        if _is_example_start_line(ln):
            example_mode = True
            deleted.append(DeletedItem(text=ln, reason="example_block"))
            deleted_rows_legacy.append(ln)
            continue

        # ✅ внутри примера дропаем всё, пока не увидим явный конец блока
        if example_mode:
            if _is_header_or_separator_line(ln) or RE_EXAMPLE_END_HINT.search(ln):
                example_mode = False  # текущую строку дальше обработаем обычными правилами
            else:
                deleted.append(DeletedItem(text=ln, reason="example_block"))
                deleted_rows_legacy.append(ln)
                continue

        if _is_header_or_separator_line(ln):
            deleted.append(DeletedItem(text=ln, reason="header_or_separator"))
            deleted_rows_legacy.append(ln)
            continue

        if RE_OOS.search(ln):
            deleted.append(DeletedItem(text=ln, reason="out_of_stock"))
            deleted_rows_legacy.append(ln)
            continue

        if RE_DEFECT_LINE.search(ln):
            deleted.append(DeletedItem(text=ln, reason="defective_or_used"))
            deleted_rows_legacy.append(ln)
            continue

        # ✅ логистика/оплата/гарантия/инструкции — дропаем как инфо, даже если там есть цифры
        if RE_INFO_GUARD.search(ln):
            deleted.append(DeletedItem(text=ln, reason="info_line"))
            deleted_rows_legacy.append(ln)
            continue

        # ✅ дополнительный страховочный кейс: "1. ... 10-20 минут" / "2. ... 10-30 минут"
        if RE_ENUM_PREFIX.search(ln) and (RE_RANGE_PREFIX.search(ln) or RE_NOT_PRICE_TAIL.search(ln)):
            deleted.append(DeletedItem(text=ln, reason="not_a_price_range"))
            deleted_rows_legacy.append(ln)
            continue

        # ✅ отсекаем "10-20 минут", "5-30 минут" и подобное
        if RE_RANGE_PREFIX.search(ln) and RE_NOT_PRICE_TAIL.search(ln):
            deleted.append(DeletedItem(text=ln, reason="not_a_price_range"))
            deleted_rows_legacy.append(ln)
            continue

        # ✅ wholesale tiers ("от N шт ...") — дропаем
        if re.match(r"(?i)^\s*от\s+\d+\s*шт\b", ln) and RE_PRICE.search(_strip_dates_times_for_price(ln)):
            deleted.append(DeletedItem(text=ln, reason="wholesale_tier"))
            deleted_rows_legacy.append(ln)
            continue

        # ✅ Цена ищется по строке БЕЗ дат/времени/года, чтобы анонсы не пролезали
        ln_price_probe = _strip_dates_times_for_price(ln)
        if not RE_PRICE.search(ln_price_probe):
            deleted.append(DeletedItem(text=ln, reason="no_price"))
            deleted_rows_legacy.append(ln)
            continue

        kept.append(ln)
        prev_price_line = True

    return kept, deleted, deleted_rows_legacy


# =========================
# Main parse (pipeline)
# =========================
def parse_messages(messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    messages: список сырья от collect_messages(), где есть хотя бы:
      {channel, message_id, date, message}
    """
    parsed: List[Dict[str, Any]] = []

    for m in messages:
        raw_text = (m.get("message") or "").strip()
        raw_lines = _basic_lines_from_message(raw_text)

        deleted: List[DeletedItem] = []
        deleted_rows_legacy: List[str] = []

        # шаг 2: глушим сообщение целиком
        drop_reason = _should_drop_message_entirely(raw_text)
        if drop_reason:
            for ln in raw_lines:
                deleted.append(DeletedItem(text=ln, reason=drop_reason))
                deleted_rows_legacy.append(ln)

            parsed.append(
                {
                    "channel": m.get("channel"),
                    "message_id": m.get("message_id"),
                    "date": m.get("date"),
                    "message": raw_text,
                    "lines": [],
                    "deleted_rows": deleted_rows_legacy,
                    "deleted": [d.__dict__ for d in deleted],
                    "lines_count": 0,
                    "parsed_at": _utcnow_iso(),
                }
            )
            continue

        # шаг 4: склейка (YouTake-стиль)
        joined = _join_youtake_pairs(raw_lines)
        # шаг 4.1: восстановление контекста заголовков (например, iPad ...)
        joined = _apply_header_context(joined)

        # шаг 5: фильтрация строк
        kept, del_items, del_rows = _filter_lines(joined)
        deleted.extend(del_items)
        deleted_rows_legacy.extend(del_rows)

        # ✅ RULE: один товар + рядом дефектные строки ("не актив", "царапина" и т.п.) => снести всё сообщение
        if len(kept) == 1 and any(d.reason == "defective_or_used" for d in deleted):
            all_lines = _basic_lines_from_message(raw_text)
            deleted = [DeletedItem(text=ln, reason="defect_context_message") for ln in all_lines]
            deleted_rows_legacy = all_lines[:]
            kept = []

        parsed.append(
            {
                "channel": m.get("channel"),
                "message_id": m.get("message_id"),
                "date": m.get("date"),
                "message": raw_text,
                "lines": kept,
                "deleted_rows": deleted_rows_legacy,
                "deleted": [d.__dict__ for d in deleted],
                "lines_count": len(kept),
                "parsed_at": _utcnow_iso(),
            }
        )

    return parsed


# =========================
# Collect messages (FIXED: как в рабочем коде)
# =========================

def _find_sources_json() -> Optional[Path]:
    """
    ✅ КАК БЫЛО В РАБОЧЕМ КОДЕ:
    Ищем sources.json в CWD (откуда запущен процесс).
    Именно это и было ключевым, почему у тебя всё работало.
    """
    p = Path("sources.json")
    return p if p.exists() and p.is_file() else None


def _load_sources_from_file() -> Tuple[Dict[str, List[Dict[str, Any]]], Optional[Path]]:
    """
    Поддерживаем 2 формата:
    1) { "channels": [...], "bots": [...]  }  ✅ твой старый рабочий
    2) { "items": [...] } или просто [ ... ]  (универсальный)
    """
    path = _find_sources_json()
    if not path:
        return {"channels": [], "bots": []}, None

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"channels": [], "bots": []}, path

    # 1) старый формат
    if isinstance(data, dict) and ("channels" in data or "bots" in data):
        ch = data.get("channels", []) or []
        bt = data.get("bots", []) or []
        return {
            "channels": [x for x in ch if isinstance(x, dict)],
            "bots": [x for x in bt if isinstance(x, dict)],
        }, path

    # 2) новый формат items/list
    items: List[Dict[str, Any]] = []
    if isinstance(data, dict) and isinstance(data.get("items"), list):
        items = [x for x in data["items"] if isinstance(x, dict)]
    elif isinstance(data, list):
        items = [x for x in data if isinstance(x, dict)]
    else:
        return {"channels": [], "bots": []}, path

    channels: List[Dict[str, Any]] = []
    bots: List[Dict[str, Any]] = []
    for s in items:
        t = (s.get("type") or s.get("source_type") or "channel").strip().lower()
        if t == "bot":
            bots.append(s)
        else:
            channels.append(s)

    return {"channels": channels, "bots": bots}, path


def _filter_sources_for_user(
    sources_pack: Dict[str, List[Dict[str, Any]]],
    user_id: int | None,
    sources_mode: str,
) -> Dict[str, List[Dict[str, Any]]]:
    if user_id is None:
        return sources_pack

    def _is_own(item: dict) -> bool:
        return item.get("user_id") == user_id

    def _is_default(item: dict) -> bool:
        return item.get("user_id") is None

    channels = sources_pack.get("channels", []) or []
    bots = sources_pack.get("bots", []) or []

    if sources_mode == "default":
        return {
            "channels": [s for s in channels if _is_default(s)],
            "bots": [s for s in bots if _is_default(s)],
        }
    if sources_mode == "custom":
        return {
            "channels": [s for s in channels if _is_own(s)],
            "bots": [s for s in bots if _is_own(s)],
        }
    # "own"
    return {
        "channels": [s for s in channels if _is_own(s)],
        "bots": [s for s in bots if _is_own(s)],
    }


async def _clients_map(user_id: int | None = None, include_default: bool = True) -> Dict[str, Any]:
    """
    ✅ В рабочем коде get_all_clients() возвращал dict.
    Но на всякий случай поддержим и list.
    """
    if user_id is None:
        cl = get_all_clients()
    else:
        cl = await get_clients_for_user(user_id, include_default=include_default)
    if isinstance(cl, dict):
        return cl

    out: Dict[str, Any] = {}
    if isinstance(cl, (list, tuple)):
        for i, c in enumerate(cl):
            out[str(i)] = c
    return out


def _pick_client_for_source(clients: Dict[str, Any], src: Dict[str, Any]) -> Optional[Any]:
    """
    ✅ КАК БЫЛО:
    src["account"] матчится по ключу в clients (и "@account" тоже)
    иначе берём первый доступный клиент.
    """
    if not clients:
        return None

    acc = (src.get("account") or "").strip()
    if acc:
        c = clients.get(acc) or clients.get(f"@{acc}")
        if c:
            return c

    return next(iter(clients.values()), None)


def _source_display_name(src: Dict[str, Any]) -> str:
    return (src.get("name") or src.get("title") or src.get("channel") or "").strip() or "Unknown"


def _source_entity_ref(src: Dict[str, Any]) -> Any:
    """
    ✅ КАК БЫЛО В ТВОЁМ РАБОЧЕМ:
    resolve_entity(client, channel_id)
    """
    for k in ("channel_id", "peer_id", "chat_id", "id", "username", "entity"):
        v = src.get(k)
        if v is None:
            continue
        if isinstance(v, int):
            return v
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


async def _run_bot_scenario(client: Any, entity: Any, src: Dict[str, Any]) -> None:
    """
    ✅ КАК БЫЛО:
    Если scenario нет — бот НЕ парсим (иначе тянем старую историю)
    """
    scenario = src.get("scenario") or []
    if not isinstance(scenario, list) or not scenario:
        trigger_text = (src.get("trigger_text") or src.get("command") or src.get("text") or "").strip()
        if trigger_text:
            await client.send_message(entity, trigger_text)
            await asyncio.sleep(float(src.get("delay_sec", 1.5) or 1.5))
        return

    step_delay = float(src.get("scenario_delay_sec", 1.2) or 1.2)
    for step in scenario:
        if not isinstance(step, dict):
            continue
        value = (step.get("value") or "").strip()
        if not value:
            continue
        await client.send_message(entity, value)
        await asyncio.sleep(step_delay)


async def _get_last_message_id(client: Any, entity: Any) -> int:
    try:
        from telethon import functions  # локальный импорт

        history = await client(
            functions.messages.GetHistoryRequest(
                peer=entity,
                limit=1,
                offset_id=0,
                offset_date=None,
                add_offset=0,
                max_id=0,
                min_id=0,
                hash=0,
            )
        )
        msgs = getattr(history, "messages", None) or []
        if not msgs:
            return 0
        return int(getattr(msgs[0], "id", 0) or 0)
    except Exception:
        return 0


async def _collect_new_messages_after_id(
    client: Any,
    entity: Any,
    min_id: int,
    attempts: int = 6,
    sleep_sec: float = 1.2,
) -> List[Any]:
    collected: List[Any] = []
    best_max = int(min_id or 0)

    from telethon import functions  # локальный импорт

    for _ in range(max(1, int(attempts))):
        try:
            history = await client(
                functions.messages.GetHistoryRequest(
                    peer=entity,
                    limit=200,
                    offset_id=0,
                    offset_date=None,
                    add_offset=0,
                    max_id=0,
                    min_id=best_max,
                    hash=0,
                )
            )
            msgs = getattr(history, "messages", None) or []
        except Exception:
            msgs = []

        new_msgs: List[Any] = []
        for mm in msgs:
            mid = int(getattr(mm, "id", 0) or 0)
            if mid > best_max:
                new_msgs.append(mm)

        if new_msgs:
            collected.extend(new_msgs)
            best_max = max(best_max, max(int(getattr(mm, "id", 0) or 0) for mm in new_msgs))
            break

        await asyncio.sleep(float(sleep_sec))

    return collected


async def collect_messages(
    user_id: int | None = None,
    sources_mode: str = "default",
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    ✅ ИТОГ: поведение как в твоём рабочем файле:
    - sources.json читаем из CWD
    - формат channels/bots поддерживаем
    - get_all_clients dict поддерживаем
    - для bot: парсим только сообщения ПОСЛЕ scenario (baseline_id)
    """
    sources_pack, sources_path = _load_sources_from_file()
    sources_pack = _filter_sources_for_user(sources_pack, user_id, sources_mode)
    channels = sources_pack.get("channels", []) or []
    bots = sources_pack.get("bots", []) or []

    if not channels and not bots:
        return [], {
            "total": 0,
            "processed": 0,
            "messages": 0,
            "errors": 0,
            "reason": "sources.json not found or empty",
            "sources_path": str(sources_path) if sources_path else None,
        }

    clients = await _clients_map(user_id=user_id, include_default=(sources_mode == "default"))
    if not clients:
        return [], {
            "total": 0,
            "processed": 0,
            "messages": 0,
            "errors": 0,
            "reason": "no telethon clients",
            "sources_path": str(sources_path) if sources_path else None,
        }

    all_messages: List[Dict[str, Any]] = []

    stats: Dict[str, Any] = {
        "total": int(len(channels) + len(bots)),
        "processed": 0,
        "messages": 0,
        "errors": 0,
        "sources_path": str(sources_path) if sources_path else None,
        "per_source": [],
    }

    async def _collect_one(src: Dict[str, Any], source_type: str) -> None:
        nonlocal all_messages, stats

        title = _source_display_name(src)
        entity_ref = _source_entity_ref(src)
        if entity_ref is None:
            stats["errors"] += 1
            stats["per_source"].append({"source": title, "ok": False, "error": "no_entity_ref"})
            return

        client = _pick_client_for_source(clients, src)
        if not client:
            stats["errors"] += 1
            stats["per_source"].append({"source": title, "ok": False, "error": "no_client"})
            return

        try:
            entity = await resolve_entity(client, entity_ref)
        except Exception as e:
            stats["errors"] += 1
            stats["per_source"].append({"source": title, "ok": False, "error": f"resolve_entity: {e}"})
            return

        got = 0

        try:
            if source_type == "bot":
                scenario = src.get("scenario") or []
                if not isinstance(scenario, list) or not scenario:
                    stats["per_source"].append({"source": title, "ok": True, "messages": 0, "skipped": "no_scenario"})
                    stats["processed"] += 1
                    return

                baseline_id = await _get_last_message_id(client, entity)
                await _run_bot_scenario(client, entity, src)
                await asyncio.sleep(float(src.get("post_scenario_delay_sec", 1.6) or 1.6))

                msgs = await _collect_new_messages_after_id(
                    client,
                    entity,
                    min_id=baseline_id,
                    attempts=int(src.get("wait_attempts", 6) or 6),
                    sleep_sec=float(src.get("wait_sleep_sec", 1.2) or 1.2),
                )
            else:
                from telethon import functions  # локальный импорт

                history = await client(
                    functions.messages.GetHistoryRequest(
                        peer=entity,
                        limit=int(src.get("limit", 200) or 200),
                        offset_id=0,
                        offset_date=None,
                        add_offset=0,
                        max_id=0,
                        min_id=0,
                        hash=0,
                    )
                )
                msgs = getattr(history, "messages", None) or []

            for msg in msgs:
                text = getattr(msg, "message", None) or getattr(msg, "caption", None) or ""
                if not str(text).strip():
                    continue

                all_messages.append(
                    {
                        "channel": title,
                        "source_type": source_type,
                        "message_id": int(getattr(msg, "id", 0) or 0),
                        "date": msg.date.isoformat() if getattr(msg, "date", None) else None,
                        "message": str(text).strip(),
                    }
                )
                got += 1

            stats["processed"] += 1
            stats["messages"] += got
            stats["per_source"].append({"source": title, "ok": True, "messages": got})

        except Exception as e:
            stats["errors"] += 1
            stats["per_source"].append({"source": title, "ok": False, "error": str(e)})

    # ✅ строго последовательно, без gather (и без параллели)
    for src in channels:
        await _collect_one(src, "channel")
    for src in bots:
        await _collect_one(src, "bot")

    return all_messages, stats
