from __future__ import annotations

from pathlib import Path
from datetime import date
from typing import List, Dict, Optional

from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.lib.units import mm
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont

# ===== БАЗОВЫЕ ПУТИ =====

BASE_DIR = Path(__file__).resolve().parent

RECEIPTS_DIR = BASE_DIR / "files"
RECEIPTS_DIR.mkdir(parents=True, exist_ok=True)

SIGNATURE_FILE = BASE_DIR / "signature.png"
LOGO_FILE = BASE_DIR / "logo.jpg"

# Пытаемся найти шрифты: сначала TimesNewRoman, потом DejaVuSans
_PREFERRED_FONTS = ["TimesNewRoman.ttf", "Times New Roman.ttf", "DejaVuSans.ttf"]
_PREFERRED_BOLD_FONTS = [
    "TimesNewRomanBold.ttf",
    "Times New Roman Bold.ttf",
    "DejaVuSans-Bold.ttf",
]

_FONT_FILE: Optional[Path] = None
_FONT_NAME: str = "Helvetica"

for fname in _PREFERRED_FONTS:
    candidate = BASE_DIR / fname
    if candidate.exists():
        _FONT_FILE = candidate
        _FONT_NAME = Path(fname).stem
        break

_BOLD_FONT_FILE: Optional[Path] = None
_BOLD_FONT_NAME: Optional[str] = None

for fname in _PREFERRED_BOLD_FONTS:
    candidate = BASE_DIR / fname
    if candidate.exists():
        _BOLD_FONT_FILE = candidate
        _BOLD_FONT_NAME = Path(fname).stem
        break

FONT_FILE = _FONT_FILE
FONT_NAME = _FONT_NAME
BOLD_FONT_FILE = _BOLD_FONT_FILE
BOLD_FONT_NAME = _BOLD_FONT_NAME


# ===== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ =====

def _register_font():
    """Регистрируем шрифты для кириллицы, если нашли .ttf рядом с генератором."""
    if FONT_FILE is None:
        print("⚠️ Файл основного шрифта не найден, используется Helvetica (без кириллицы).")
    else:
        try:
            pdfmetrics.registerFont(TTFont(FONT_NAME, str(FONT_FILE)))
            print(f"✅ Зарегистрирован шрифт {FONT_NAME} из {FONT_FILE}")
        except Exception as e:
            print(f"⚠️ Не удалось зарегистрировать шрифт {FONT_FILE}: {e}")

    if BOLD_FONT_FILE is not None:
        try:
            pdfmetrics.registerFont(TTFont(BOLD_FONT_NAME, str(BOLD_FONT_FILE)))
            print(f"✅ Зарегистрирован жирный шрифт {BOLD_FONT_NAME} из {BOLD_FONT_FILE}")
        except Exception as e:
            print(f"⚠️ Не удалось зарегистрировать жирный шрифт {BOLD_FONT_FILE}: {e}")


def _format_date_ru(d: date) -> str:
    months = {
        1: "января",
        2: "февраля",
        3: "марта",
        4: "апреля",
        5: "мая",
        6: "июня",
        7: "июля",
        8: "августа",
        9: "сентября",
        10: "октября",
        11: "ноября",
        12: "декабря",
    }
    return f"{d.day} {months[d.month]} {d.year}"


def get_next_receipt_number(start_from: int = 100) -> int:
    """
    Находит максимальный номер среди существующих чеков receipt_<num>.pdf
    и возвращает следующий. Если чеков нет — вернет start_from.
    """
    max_num = 0
    if RECEIPTS_DIR.exists():
        for path in RECEIPTS_DIR.rglob("receipt_*.pdf"):
            try:
                num_part = path.stem.split("_", 1)[1]  # "receipt_123" -> "123"
                num = int(num_part)
                if num > max_num:
                    max_num = num
            except Exception:
                continue

    # если чеков нет (max_num == 0), стартуем с 100
    return max(max_num + 1, start_from)

def _split_text_to_lines(text: str, max_width: float, font_size: int) -> List[str]:
    """Делим текст на строки по словам так, чтобы каждая строка не превышала max_width."""
    words = text.split()
    lines: List[str] = []
    line = ""
    for word in words:
        test_line = (line + " " + word).strip()
        w = pdfmetrics.stringWidth(test_line, FONT_NAME, font_size)
        if w <= max_width:
            line = test_line
        else:
            if line:
                lines.append(line)
            line = word
    if line:
        lines.append(line)
    if not lines:
        lines = [""]
    return lines


def _draw_wrapped_text(
    c: canvas.Canvas,
    text: str,
    x: float,
    y: float,
    max_width: float,
    line_height: float,
    font_size: int = 9,
) -> float:
    """
    Перенос длинного текста по словам под max_width.
    Пишем сверху вниз (уменьшая y).
    """
    words = text.split()
    line = ""
    for word in words:
        test_line = (line + " " + word).strip()
        w = pdfmetrics.stringWidth(test_line, FONT_NAME, font_size)
        if w <= max_width:
            line = test_line
        else:
            c.drawString(x, y, line)
            y -= line_height
            line = word
    if line:
        c.drawString(x, y, line)
        y -= line_height
    return y


def _draw_heading(
    c: canvas.Canvas,
    text: str,
    x: float,
    y: float,
    font_size: int,
) -> float:
    """
    Рисуем заголовок гарантий жирным (если есть bold-шрифт),
    иначе — имитируем жирный двойной отрисовкой.
    Возвращает новый y чуть ниже.
    """
    if BOLD_FONT_NAME:
        c.setFont(BOLD_FONT_NAME, font_size)
        c.drawString(x, y, text)
    else:
        c.setFont(FONT_NAME, font_size)
        c.drawString(x, y, text)
        c.drawString(x + 0.3, y, text)
    return y - (font_size + 4)


def generate_receipt_pdf(
    items: List[Dict],
    receipt_date: Optional[date] = None,
    number: Optional[int] = None,
) -> Path:
    """
    Генерация PDF чека.

    items: список словарей вида:
      {"name": str, "serial": str, "price": int, "quantity": int}
    """
    if receipt_date is None:
        receipt_date = date.today()
    if number is None:
        number = get_next_receipt_number()

    # Папка по дате: files/YYYY/MM/DD
    date_dir = (
        RECEIPTS_DIR
        / str(receipt_date.year)
        / f"{receipt_date.month:02d}"
        / f"{receipt_date.day:02d}"
    )
    date_dir.mkdir(parents=True, exist_ok=True)

    output_path = date_dir / f"receipt_{number}.pdf"

    # Нормализуем items
    norm_items: List[Dict] = []
    for raw in items:
        name = str(raw.get("name", "")).strip()
        serial = str(raw.get("serial", "")).strip()
        price = int(raw.get("price", 0) or 0)
        quantity = int(raw.get("quantity", 1) or 1)
        norm_items.append(
            {
                "name": name,
                "serial": serial,
                "price": price,
                "quantity": quantity,
            }
        )

    _register_font()

    c = canvas.Canvas(str(output_path), pagesize=A4)
    width, height = A4

    # Отступы
    left_margin = 20 * mm
    right_margin = 20 * mm
    top_margin = height - 20 * mm
    bottom_margin = 20 * mm

    # ===== ШАПКА: реквизиты слева, логотип справа =====

    header_font_size = 9
    c.setFont(FONT_NAME, header_font_size)
    c.drawString(left_margin, top_margin, "UnderPrice Store")

    header_text = (
        "Багратионовский проезд, д.7,к3, B2-124\n"
        "Россия, Москва, 121087\n"
        "8 (966) 923-29-40"
    )
    y = top_margin - (header_font_size + 2)
    for line in header_text.split("\n"):
        c.drawString(left_margin, y, line)
        y -= (header_font_size + 1)

    # Логотип справа
    if LOGO_FILE.exists():
        try:
            logo_w = 35 * mm
            logo_h = 20 * mm
            logo_x = width - right_margin - logo_w
            logo_y = top_margin - logo_h + 2 * mm
            c.drawImage(
                str(LOGO_FILE),
                logo_x,
                logo_y,
                width=logo_w,
                height=logo_h,
                preserveAspectRatio=True,
                mask="auto",
            )
        except Exception as e:
            print(f"⚠️ Не удалось нарисовать логотип: {e}")

    # ===== Заголовок "Товарный чек №..." =====

    title = f"Товарный чек №{number} от {_format_date_ru(receipt_date)} г."
    title_font_size = 11  # чуть меньше
    title_font = BOLD_FONT_NAME or FONT_NAME

    c.setFont(title_font, title_font_size)
    title_width = pdfmetrics.stringWidth(title, title_font, title_font_size)

    title_y = y - 26
    c.drawString((width - title_width) / 2, title_y, title)

    # Отступ перед таблицей
    y = title_y - 24

    # ===== Таблица товаров =====

    table_font_size = 8
    c.setFont(FONT_NAME, table_font_size)

    # Колонки: №, Наименование, Серийный, Цена, Кол-во, Сумма
    col_widths_mm = [8, 70, 45, 20, 12, 15]  # суммарно 170 мм = 210 - 20 - 20
    col_widths = [w * mm for w in col_widths_mm]
    col_x = [left_margin]
    for w in col_widths:
        col_x.append(col_x[-1] + w)

    base_line_height = 10
    table_y = y

    headers = [
        "№",
        "Наименование",
        "Серийный номер / IMEI",
        "Цена, руб",
        "Кол-во",
        "Сумма, руб",
    ]

    # --- Заголовки: считаем max-кол-во строк и рисуем одну общую высоту ---
    header_lines_per_col: List[List[str]] = []
    max_header_lines = 1
    for i, h in enumerate(headers):
        lines = _split_text_to_lines(h, col_widths[i] - 4, table_font_size)
        if not lines:
            lines = [""]
        header_lines_per_col.append(lines)
        max_header_lines = max(max_header_lines, len(lines))

    header_row_height = base_line_height * max_header_lines + 6

    for i, lines in enumerate(header_lines_per_col):
        c.rect(
            col_x[i],
            table_y - header_row_height,
            col_widths[i],
            header_row_height,
            stroke=1,
            fill=0,
        )

        total_text_height = base_line_height * len(lines)
        text_y = table_y - (header_row_height - total_text_height) / 2 - table_font_size

        for line in lines:
            text_width = pdfmetrics.stringWidth(line, FONT_NAME, table_font_size)

            # Заголовки всех колонок — по центру
            start_x = col_x[i] + (col_widths[i] - text_width) / 2
            c.drawString(start_x, text_y, line)
            text_y -= base_line_height

    table_y -= header_row_height

    total_sum = 0

    # --- Строки товаров ---
    for idx, item in enumerate(norm_items, start=1):
        line_sum = item["price"] * item["quantity"]
        total_sum += line_sum

        name_text = item["name"]
        name_max_width = col_widths[1] - 4
        name_lines = _split_text_to_lines(
            name_text, name_max_width, font_size=table_font_size
        )
        lines_count = max(1, len(name_lines))

        row_height = base_line_height * lines_count + 6

        values = [
            str(idx),
            name_lines,
            item["serial"],
            f"{item['price']:,}".replace(",", " "),
            str(item["quantity"]),
            f"{line_sum:,}".replace(",", " "),
        ]

        # рамки строк
        for i in range(len(col_widths)):
            c.rect(
                col_x[i],
                table_y - row_height,
                col_widths[i],
                row_height,
                stroke=1,
                fill=0,
            )

        # Колонка 0: № — по центру
        c.setFont(FONT_NAME, table_font_size)
        text = values[0]
        text_width = pdfmetrics.stringWidth(text, FONT_NAME, table_font_size)
        center_y = table_y - row_height / 2 - table_font_size / 2 + 2
        start_x = col_x[0] + (col_widths[0] - text_width) / 2
        c.drawString(start_x, center_y, text)

        # Колонка 1: Наименование — слева, с переносами
        name_start_y = table_y - 4 - table_font_size
        for line in name_lines:
            c.drawString(col_x[1] + 2, name_start_y, line)
            name_start_y -= base_line_height

        # Колонка 2: Серийный номер / IMEI — по центру
        serial = values[2]
        if serial:
            text_width = pdfmetrics.stringWidth(serial, FONT_NAME, table_font_size)
            start_x = col_x[2] + (col_widths[2] - text_width) / 2
            c.drawString(start_x, center_y, serial)

        # Колонка 3: Цена
        price_str = values[3]
        text_width = pdfmetrics.stringWidth(price_str, FONT_NAME, table_font_size)
        start_x = col_x[3] + (col_widths[3] - text_width) / 2
        c.drawString(start_x, center_y, price_str)

        # Колонка 4: Кол-во
        qty_str = values[4]
        text_width = pdfmetrics.stringWidth(qty_str, FONT_NAME, table_font_size)
        start_x = col_x[4] + (col_widths[4] - text_width) / 2
        c.drawString(start_x, center_y, qty_str)

        # Колонка 5: Сумма
        sum_str = values[5]
        text_width = pdfmetrics.stringWidth(sum_str, FONT_NAME, table_font_size)
        start_x = col_x[5] + (col_widths[5] - text_width) / 2
        c.drawString(start_x, center_y, sum_str)

        table_y -= row_height

    y = table_y - 18

    # ===== Итого =====

    c.setFont(FONT_NAME, 8)
    total_str = f"Итого: {total_sum:,} руб.".replace(",", " ")
    c.drawString(left_margin, y, total_str)
    y -= 22

    # ===== Блок гарантий =====

    guarantees_font_size = 8
    line_height = 10
    c.setFont(FONT_NAME, guarantees_font_size)
    text_max_width = width - left_margin - right_margin

    paragraphs = [
        ("h", "Правила гарантийного обслуживания."),
        (
            "p",
            "Гарантия распространяется только на товары, серийные номера которых соответствуют номерам, "
            "указанным в соответствующем заказу Товарном чеке, гарантия не распространяется на товары, "
            "имеющие нечитаемый штрих-код или серийный номер."
        ),
        (
            "p",
            "Доставка цифровой техники, подлежащей обмену, возврату, ремонту или диагностике, а также вывоз "
            "такой техники осуществляется клиентом самостоятельно и за свой счет."
        ),
        (
            "p",
            "При передаче техники, подлежащей обмену, возврату, ремонту или диагностике устройство должно быть "
            "передано без паролей входа и отвязано от учетной записи."
        ),
        ("h", "Условия предоставления гарантии:"),
        ("p", "Гарантийный срок – 12 месяцев с момента покупки."),
        (
            "p",
            "Гарантия не распространяется на товары, которые вышли из строя либо получили дефекты по причине:"
        ),
        ("p", "- изделие имеет механические, термические, электрические повреждения (в т.ч. скрытые)"),
        ("p", "- изделие имеет повреждения, вызванные небрежным обращением"),
        ("p", "- изделие имеет следы попадания внутрь посторонних веществ, предметов, жидкостей"),
        ("p", "- изделие имеет повреждения, вызванные стихией, пожаром, бытовыми факторами"),
        (
            "p",
            "- повреждены гарантийные пломбы производителя или поставщика, имеются следы постороннего вмешательства "
            "или была попытка несанкционированного ремонта"
        ),
        ("p", "- заводская маркировка или серийный номер повреждены, неразборчивы или имеют следы переклеивания"),
        (
            "p",
            "- изделие повреждено при транспортировке, хранении или нарушены правила эксплуатации. В частности, "
            "если изделие содержит элементы со следами перегрева, сгоревшие контакты или дорожки платы."
        ),
        (
            "p",
            "- выход из строя изделия вызван использованием нестандартных или несовместимых запчастей, комплектующих, "
            "программного обеспечения, расходных материалов, чистящих материалов"
        ),
        ("h", "Обмен и / или возврат товара ненадлежащего качества:"),
        (
            "p",
            "В случае обнаружения потребителем недостатков приобретенного товара заводского характера до окончания "
            "гарантийного срока, продавец обязуется заменить такой товар в течение десяти дней с момента обращения "
            "покупателя, а при необходимости дополнительной проверки качества такого товара - в течение двадцати дней "
            "с момента обращения."
        ),
        ("h", "Обмен и / или возврат товара надлежащего качества:"),
        (
            "p",
            "Согласно п. 4 ст. 26.1 Закона N 2300-1; п. 22 Правил N 2463, потребитель имеет право обменять или вернуть "
            "товар надлежащего качества в течение семи дней с момента покупки. Обмен или возврат товара надлежащего "
            "качества производится в случае, если:"
        ),
        ("p", "- товар не был в употреблении"),
        ("p", "- сохранен его товарный вид"),
        ("p", "- сохранены его потребительские свойства"),
        ("p", "- сохранены фабричные ярлыки и пломбы"),
        (
            "p",
            "Покупатель подтверждает, что приобретенный им, согласно настоящему Товарному чеку, товар осмотрен, "
            "его работоспособность и комплектность при нем проверена, претензий по качеству работы, комплектации, "
            "внешнему виду (наличие каких-либо повреждений) и цены на товар не имеется. Покупатель уведомлен, "
            "что на данном устройстве приложение RuStore не предустановлено и установка данного приложения невозможна. "
            "Покупатель соглашается и принимает данные обстоятельства, претензий к Продавцу не имеет."
        ),
    ]

    for p_type, text in paragraphs:
        # При желании легко добавить разбиение на вторую страницу:
        # if y < bottom_margin + 40: c.showPage(); y = height - 20*mm; c.setFont(...)
        if p_type == "h":
            y = _draw_heading(c, text, left_margin, y, font_size=guarantees_font_size)
            y -= 2
        else:
            c.setFont(FONT_NAME, guarantees_font_size)
            y = _draw_wrapped_text(
                c,
                text,
                left_margin,
                y,
                text_max_width,
                line_height=line_height,
                font_size=guarantees_font_size,
            )
            y -= 4

    # 🔹 раньше было y -= 12 — делаем больше воздуха перед подписями
    y -= 28

    # ===== Подписи =====

    c.setFont(FONT_NAME, 9)

    label_y = y
    c.drawString(left_margin, label_y, "Продавец")
    buyer_x = width / 2 + 10 * mm
    c.drawString(buyer_x, label_y, "Покупатель")

    line_y = label_y - 14
    line_width = 50 * mm
    c.line(left_margin, line_y, left_margin + line_width, line_y)
    c.line(buyer_x, line_y, buyer_x + line_width, line_y)

    # Подпись-картинка: поменьше и чуть выше линии
    if SIGNATURE_FILE.exists():
        try:
            sig_height = 14 * mm
            sig_width = 30 * mm
            img_y = line_y + 1 * mm
            img_x = left_margin + (line_width - sig_width) / 2
            c.drawImage(
                str(SIGNATURE_FILE),
                img_x,
                img_y,
                width=sig_width,
                height=sig_height,
                preserveAspectRatio=True,
                mask="auto",
            )
        except Exception as e:
            print(f"⚠️ Не удалось нарисовать подпись: {e}")

    c.showPage()
    c.save()

    return output_path


def get_last_receipts(limit: int = 10) -> List[Path]:
    """Возвращает список путей к последним (по времени изменения) чекам."""
    if not RECEIPTS_DIR.exists():
        return []

    files = list(RECEIPTS_DIR.rglob("receipt_*.pdf"))
    if not files:
        return []

    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return files[:limit]


__all__ = [
    "generate_receipt_pdf",
    "get_last_receipts",
    "RECEIPTS_DIR",
    "SIGNATURE_FILE",
    "LOGO_FILE",
]
