# handlers/normalizers/entry_regex.py
from __future__ import annotations

import re
from typing import List, Tuple

# =========================================================
# REGIONS (compiled) — вынесли из entry_dicts.py
# =========================================================

REGION_WORDS_STRICT: List[Tuple[re.Pattern, str]] = [
    (re.compile(r"(?i)(?:^|[\s\(\[\{,;:/\-])(?:eu|europe|европа)(?:$|[\s\)\]\},;:/\-])"), "eu"),

    (re.compile(r"(?i)(?:^|[\s\(\[\{,;:/\-])uk(?:$|[\s\)\]\},;:/\-])"), "uk"),
    (re.compile(r"(?i)\b(?:britain|англия|британи\w*|великобрит\w*)\b"), "uk"),

    (re.compile(r"(?i)(?:^|[\s\(\[\{,;:/\-])(?:ru|рф)(?:$|[\s\)\]\},;:/\-])"), "ru"),
    (re.compile(r"(?i)\b(?:russia|росси\w*)\b"), "ru"),

    (re.compile(r"(?i)(?:^|[\s\(\[\{,;:/\-])by(?:$|[\s\)\]\},;:/\-])"), "by"),
    (re.compile(r"(?i)\b(?:belarus|беларус\w*)\b"), "by"),

    (re.compile(r"(?i)(?:^|[\s\(\[\{,;:/\-])ua(?:$|[\s\)\]\},;:/\-])"), "ua"),
    (re.compile(r"(?i)\b(?:ukraine|украин\w*)\b"), "ua"),

    (re.compile(r"(?i)(?:^|[\s\(\[\{,;:/\-])kz(?:$|[\s\)\]\},;:/\-])"), "kz"),
    (re.compile(r"(?i)\b(?:kazakhstan|казах\w*)\b"), "kz"),

    (re.compile(r"(?i)(?:^|[\s\(\[\{,;:/\-])(?:us|usa)(?:$|[\s\)\]\},;:/\-])"), "us"),
    (re.compile(r"(?i)\b(?:сша|штат\w*|америк\w*)\b"), "us"),

    (re.compile(r"(?i)(?:^|[\s\(\[\{,;:/\-])ca(?:$|[\s\)\]\},;:/\-])"), "ca"),
    (re.compile(r"(?i)\b(?:canada|канада)\b"), "ca"),

    (re.compile(r"(?i)(?:^|[\s\(\[\{,;:/\-])mx(?:$|[\s\)\]\},;:/\-])"), "mx"),
    (re.compile(r"(?i)\b(?:mexico|мексик\w*)\b"), "mx"),

    (re.compile(r"(?i)(?:^|[\s\(\[\{,;:/\-])br(?:$|[\s\)\]\},;:/\-])"), "br"),
    (re.compile(r"(?i)\b(?:brazil|бразил\w*)\b"), "br"),

    (re.compile(r"(?i)\b(?:hk|hong\s*kong|гонконг)\b"), "hk"),
    (re.compile(r"(?i)\b(?:cn|china|китай|кит\.)\b"), "cn"),
    (re.compile(r"(?i)\b(?:ch|china\s*version|китайск\w*|кит\s*версия)\b"), "ch"),
]

# =========================================================
# COMMON (memory / price / code)
# =========================================================

RX_MEM_CONFIG_SLASH_NO_UNIT = re.compile(
    r"""(?ix)
    (?<!\d)
    (?P<a>\d{1,3})
    \s*/\s*
    (?P<b>\d{2,4})
    (?!\d)
    """
)

RX_MEM_EXPLICIT_ALL = re.compile(
    r"""(?ix)
    (?<!\d)
    (?P<num>\d{1,4})
    \s*
    (?P<unit>tb|тб|gb|гб|g)\b
    """
)

RX_MEM_CONFIG_SLASH = re.compile(
    r"""(?ix)
    (?<!\d)
    (?P<a>\d{1,4})
    \s*/\s*
    (?P<b>\d{1,4})
    \s*
    (?P<unit>tb|тб|gb|гб|g)\b
    """
)

RX_MEM_BARE_ALL = re.compile(r"(?<!\d)(64|128|256|512|1024|2048)(?!\d)")
RX_PRICE_HINT = re.compile(r"(?i)(₽|руб|р\.|\$|usd|eur|€)")

UNIT_AFTER_RE = re.compile(r"^\s*(gb|гб|tb|тб|mm)\b", re.I)

# Универсальный “ценовой токен” (используй вместе с валютой/контекстом)
RX_MONEY_TOKEN = re.compile(r"(?<!\d)(\d{1,3}(?:[ .,_]\d{3})+|\d{4,7})(?!\d)")

PRICE_TOKEN_RE = re.compile(
    r"""
    (?:
        (?<!\d)
        (?:\d{1,3}(?:[ \u00A0\.\,]\d{3})+)
        (?!\d)
    )
    |
    (?:
        (?<!\d)
        \d{5,}
        (?!\d)
    )
    |
    (?:
        (?<!\d)
        \d{2,3}[.,]\d
        (?!\d)
    )
    """,
    re.VERBOSE,
)

CURR_AFTER_RE = re.compile(r"""(?ix)
    (?<!\d)
    (?:\d{1,3}(?:[ \.,]?\d{3})+|\d{2,})
    \s*
    (?:₽|rub|р\.?|руб\.?|usd|\$|eur|€)\b
""")

CURR_BEFORE_RE = re.compile(r"""(?ix)
    (?:₽|rub|р\.?|руб\.?|usd|\$|eur|€)
    \s*
    (?:\d{1,3}(?:[ \.,]?\d{3})+|\d{2,})
    \b
""")

RX_CODE_TOKEN = re.compile(r"(?<![A-Z0-9])([A-Z0-9]{4,8})(?![A-Z0-9])")

# Год: “любой 20xx”
RX_YEAR_ANY_20XX = re.compile(r"(?<!\d)(20\d{2})(?!\d)")
# Год: “безопасный диапазон” для нормализации (чтобы не ловить мусор)
RX_YEAR_SAFE_RANGE = re.compile(r"\b(20(?:1[8-9]|2\d|3[0-5]))\b", re.I)
# Строгий токен “ровно год”
RX_YEAR_TOKEN = re.compile(r"^20\d{2}$")
RX_YEAR_HINT = re.compile(r"(?i)\b(year|год|model\s*year|202\d)\b")

RX_TITANIUM_COLOR = re.compile(
    r"(?i)\b(titanium)\s+(black|gray|grey|violet|yellow|blue|white|green|orange|red)\b"
)


# =========================================================
# ACCESSORIES
# =========================================================

RX_CASE = re.compile(
    r"""(?ix)
    \b(
        case|
        charging\s*case|
        wireless\s*case|
        magsafe\s*case|
        usb[-\s]*c\s*case|
        футляр|
        кейс|
        бокс|
        зарядн\w*\s*футляр|
        зарядн\w*\s*кейс
    )\b
"""
)

RX_LEFT_EARBUD = re.compile(
    r"""(?ix)
    \b(left|лев\w*|l)\b
    .*?
    \b(earbud|bud|earpiece|наушник\w*)\b
    |
    \b(лев\w*\s*наушник\w*|лев\w*\s*вкладыш\w*)\b
"""
)

RX_RIGHT_EARBUD = re.compile(
    r"""(?ix)
    \b(right|прав\w*|r)\b
    .*?
    \b(earbud|bud|earpiece|наушник\w*)\b
    |
    \b(прав\w*\s*наушник\w*|прав\w*\s*вкладыш\w*)\b
"""
)

RX_SINGLE_EARBUD = re.compile(
    r"""(?ix)
    \b(one|single|1\s*шт|штучн\w*|один)\b
    .*?
    \b(earbud|bud|earpiece|наушник\w*)\b
"""
)

# =========================================================
# SMARTPHONES / SIM
# =========================================================

RX_SIM_NANO_ESIM = re.compile(r"(?i)\bnano\s*[- ]?\s*sim\s*\+\s*e\s*[- ]?\s*sim\b")
RX_SIM_SIM_ESIM = re.compile(r"(?i)\bsim\s*\+\s*e\s*[- ]?\s*sim\b")
RX_SIM_ESIM = re.compile(r"(?i)\be\s*[- ]?\s*sim\b|\besim\b")
RX_SIM_2SIM = re.compile(r"(?i)\b(2\s*sim|2\s*сим|dual\s*(?:nano[\s\-]*)?sim)\b")

# iPhone gen hints
RX_IPHONE_GEN = re.compile(r"(?i)\biphone\s*(1[0-9]|[7-9])\s*e?\b")
RX_IPHONE_GEN_BARE = re.compile(r"(?i)\b(1[0-9]|[7-9])e?\b")

# iPhone gen in text: "iphone 16" OR "16 pro/max/plus/mini/e"
RX_IPHONE_GEN_NUM = re.compile(
    r"\b(?:iphone\s*(?P<gen>\d{1,2})|(?P<gen2>\d{1,2})\s*(?:pro|max|plus|mini|e))\b",
    re.I,
)

# iPhone pricelist implied (line begins with gen + variants)
RX_IPHONE_PRICESTYLE = re.compile(
    r"^\s*"
    r"(?P<gen>1[0-9]|20)"
    r"(?:\s*(?P<suffix>e)\b)?"
    r"(?:\s+(?P<variant>pro|max|plus|mini))?"
    r"(?:\s+(?P<variant2>max))?"
    r"\b",
    re.I,
)

# =========================================================
# WATCHES
# =========================================================

RX_FLAGS = re.compile(r"[\U0001F1E6-\U0001F1FF]{2}")

RX_WATCH_MM_ANY = re.compile(r"(?i)(?<!\d)(?P<mm>3[0-9]|4[0-9]|5[0-9])\s*mm\b")

# Один канонический band size (широкий)
RX_BAND_SIZE = re.compile(r"(?i)\b(xs\/s|s\/m|m\/l|l\/xl)\b|\b(xs|s|m|l|xl)\b")
# Компактные SM/ML (отдельно, потому что часто в хвостах)
RX_BAND_SIZE_COMPACT = re.compile(r"(?i)\b(sm|ml)\b")

RX_WATCH_NOISE = re.compile(
    r"(?ix)\b("
    r"watch|apple\s*watch|series\s*\d+|se|ultra|ultra\s*\d+|edition|gps|cellular|"
    r"mm|strap|band|loop|bracelet|"
    r"xs\/s|s\/m|m\/l|l\/xl|xs|s|m|l|xl|sm|ml"
    r")\b"
)

# AW prefixes + shorthand
RX_AW_SHORT = re.compile(r"(?i)^\s*(?:aw|a\.?w\.?)\b")
RX_APPLE_WATCH_CTX = re.compile(r"(?i)\b(?:apple\s*watch|iwatch|\s*watch|watch\s*ultra|watch\s*s\d{1,2}|watch\s*se)\b")
RX_AW_BAND_SIZE = re.compile(r"(?i)\b(?:m/l|s/m)\b")
RX_AW_SIZE_BARE = re.compile(r"(?i)(?<!\d)(40|41|42|44|45|46|49)\b")

RX_AW_SERIES_SHORT = re.compile(r"(?i)\bS\s*(\d{1,2})\b")
RX_AW_ULTRA_SHORT = re.compile(r"(?i)\bU\s*([23])\b")
RX_AW_SE_YEAR = re.compile(r"(?i)\bSE\s*(20\d{2})\b")
RX_AW_SE_GEN = re.compile(r"(?i)\bSE\s*([23])\b")
RX_AW_SE_BARE = re.compile(r"(?i)\bSE\b")

# Watch pricelist implied (если строка начинается с series/ultra/se)
RX_WATCH_PRICESTYLE = re.compile(
    r"^\s*"
    r"(?P<s>(?:s|series)\s*\d{1,2}|ultra\s*\d{0,2}|se\s*\d{0,2})"
    r"(?:\s+(?P<mm>\d{2}))?"
    r"\b",
    re.I,
)

RX_WATCH_AW_PRICESTYLE = re.compile(
    r"^\s*aw\s+"
    r"(?P<family>ultra|se|\d{1,2})"
    r"(?:\s+(?P<num>\d{1,2}))?"
    r"(?:\s+(?P<mm>3[8-9]|4[0-9]))?"
    r"\b",
    re.I,
)

RX_WATCH_S_SERIES = re.compile(r"\bS(?P<num>\d{1,2})\b", re.I)
RX_AW_TOKEN = re.compile(r"^aw$", re.I)

# =========================================================
# PROCESSOR / DIAGONAL / CONTEXT
# =========================================================

RX_APPLE_M = re.compile(r"(?i)\b(m\d)\b")
RX_APPLE_A_PRO = re.compile(r"(?i)\b(a\d{2})\s*(pro|max|ultra)?\b")

# Универсальный Apple chip (Axx / Mx Pro/Max/Ultra) — удобно для entry.py
RX_CHIP_APPLE = re.compile(
    r"\b("
    r"A\s*(?P<a>\d{1,2})(?:\s*(?P<a_suffix>pro|bionic))?"
    r"|"
    r"M\s*(?P<m>\d{1,2})(?:\s*(?P<m_suffix>pro|max|ultra))?"
    r")\b",
    re.I,
)

RX_SNAPDRAGON = re.compile(
    r"""(?ix)
    \b(snapdragon)\b
    (?:\s+(?P<fam>8|7|6|4))?
    (?:\s*(?:(?P<plus>\+)|gen\s*(?P<gen>\d+)))?
    (?:\s*(?P<gengen>gen\s*\d+))?
    """
)
RX_EXYNOS = re.compile(r"(?i)\b(exynos)\s*(\d{3,5})\b")
RX_TENSOR = re.compile(r"(?i)\b(tensor)\s*(g?\d{1,2})\b")
RX_DIMENSITY = re.compile(r"(?i)\b(dimensity)\s*(\d{3,5})\b")
RX_KIRIN = re.compile(r"(?i)\b(kirin)\s*(\d{3,5})\b")

RX_IPAD_CTX = re.compile(r"(?i)\bipad\b")
RX_MACBOOK_CTX = re.compile(r"(?i)\bmac\s*book\b|\bmacbook\b")

RX_DIAGONAL = re.compile(
    r"""(?ix)
    (?<!\d)
    (?P<d>
        (?:[7-9]|1[0-8])(?:[.,]\d)?
    )
    (?:\s*(?:["″]|in(?:ch)?\b|дюйм\w*\b))?
    """
)

# =========================================================
# CONNECTIVITY (tablet/laptop common)
# =========================================================

RX_WIFI_CELL = re.compile(r"(?i)\b(wi[- ]?fi)\s*\+\s*(cellular|cell|lte|4g|5g)\b")
RX_WIFI = re.compile(r"(?i)\b(wi[\s-]?fi|wifi|wlan)\b")
RX_CELLULAR = re.compile(r"(?i)\b(cellular|cell)\b")
RX_CELL = re.compile(r"(?i)\b(cellular|lte|4g|5g|nr|umts|gsm|sim\s*free)\b")
RX_LTE = re.compile(r"(?i)\b(lte|4g)\b")
RX_5G = re.compile(r"(?i)\b(5g|nr)\b")
RX_4G = re.compile(r"(?i)\b(4g)\b")

RX_CONN_COMBO = re.compile(
    r"(?i)\b(wi[\s-]?fi)\b.*\b(cellular|lte|4g|5g)\b|\b(cellular|lte|4g|5g)\b.*\b(wi[\s-]?fi)\b"
)

# =========================================================
# FEATURES / PORTS
# =========================================================

RX_ANC = re.compile(
    r"\b("
    r"anc|"
    r"active\s*noise\s*cancell?ation|"
    r"noise\s*cancell?ation|"
    r"шумоподавлен\w*|"
    r"активн\w*\s*шумоподавлен\w*"
    r")\b",
    re.I,
)

RX_MAGSAFE = re.compile(r"(?i)\b(mag\s*safe|magsafe)\b")
RX_LIGHTNING = re.compile(r"(?i)\b(lightning|lighting)\b")
RX_USBC = re.compile(r"(?i)\b(usb[\s\-]*c|type[\s\-]*c)\b")

# =========================================================
# TABLETS signals
# =========================================================

RX_TABLET_SIGNALS = re.compile(
    r"\b("
    r"tab|tablet|galaxy\s*tab|"
    r"sm[-\s]?x\d{3,4}\w*|"
    r"x9\d{2}"
    r")\b",
    re.I,
)

# =========================================================
# CONTEXT HEADERS
# =========================================================

RX_HAS_PRICE = re.compile(r"(?i)(?:\b\d{3,}\b|\b\d{1,4}\s*(?:₽|руб|р\.?|rur)\b|\$\s*\d+)")
RX_HAS_MODELISH = re.compile(r"(?i)\b(?:\d{1,2}\s*/\s*\d{2,4}|\d{2}\s*(?:pro|max|plus|ultra|mini|e)\b)\b")
RX_HEADER_CANDIDATE = re.compile(r"(?i)^(?:[☎️📱⌚️🕒🔹🔸💠⭐️🔥]+)?\s*[a-zа-яё][a-zа-яё0-9 .+\-]{0,40}\s*$")

# =========================================================
# BRAND / CATEGORY DETECTION
# =========================================================

RX_AIRPODS = re.compile(r"(?i)\b(?:air\s*pods?|airpods?)\b")
RX_WATCH = re.compile(r"(?i)\b(?:apple\s*watch\b|iwatch\b|watch\b)\b")
RX_IPHONE_CTX2 = re.compile(r"(?i)\b(?:iphone\b|\s*i?phone\s*)\b")
RX_IPAD_CTX2 = re.compile(r"(?i)\bipad\b")
RX_MAC = re.compile(r"(?i)\b(?:macbook\b|mac\s*book\b)\b")

RX_APPLE_TOKENS = re.compile(r"(?i)\b(apple|macbook|ipad|iphone|airpods|watch|pencil|imac|mac)\b|")

# =========================================================
# MODEL ALIASES / noise
# =========================================================

RX_AIRPODS_MAX = re.compile(r"(?i)\bair\s*pods?\s*max\b|\bairpods?\s*max\b")
RX_APM_V2_TOKENS = re.compile(r"(?i)\b(?:2|ii|v2|2nd|second|gen\s*2|2\s*gen|2(?:-|\s*)?generation|2\s*поколен\w*)\b")
RX_YEAR_2024 = re.compile(r"(?i)\b2024\b")
RX_USB_C = re.compile(r"(?i)\b(?:usb[-\s]?c|type[-\s]?c|usbc)\b")

RX_IPHONE_AIR_NUM = re.compile(r"(?i)^\s*(?:iphone\s*)?(\d{2})\s+air\b")
RX_USB_C_NOISE = re.compile(r"(?i)\b(?:usb[-\s]?c|type[-\s]?c|usbc)\b")

# =========================================================
# ITEM NAME relaxed key
# =========================================================

RX_CODE_TOKEN_ITEM = re.compile(r"(?i)\b[A-Z0-9]{4,7}(?:[A-Z0-9]{1,3})?(?:/[A])?\b")
RX_PRICE_TOKEN_ITEM = re.compile(r"(?i)\b\d{3,}(?:[ .\u00A0]?\d{3})*(?:[.,]\d+)?\b")


# =========================================================
# ENTRY.PY COMPAT (do not duplicate patterns)
# Оставляем ИМЕНА как в актуальном entry.py, чтобы правки были минимальные:
# entry.py: _RX_FOO  ->  R._RX_FOO
# =========================================================

# --- money / year ---
_RX_MONEY = RX_MONEY_TOKEN
_RX_YEAR_20XX = RX_YEAR_TOKEN
_RX_YEAR = RX_YEAR_SAFE_RANGE

# --- iPhone generation / styles ---
_RX_IPHONE_GEN_NUM = RX_IPHONE_GEN_NUM
_RX_IPHONE_PRICESTYLE = RX_IPHONE_PRICESTYLE

# --- watch / bands / sizes ---
_RX_BAND_SIZE_COMPACT = RX_BAND_SIZE_COMPACT
_RX_BAND_SIZE = RX_BAND_SIZE

# generic mm + inches + bare watch mm (используются в entry.py)
_RX_SCREEN_MM = re.compile(r"(?i)\b(?P<mm>\d{2,3})\s*mm\b")
_RX_INCH = re.compile(r"(?i)\b(?P<inch>1[0-7]|[7-9])(?:\s*(?:\"|inch|in|-inch|-\s*inch|дюйм\w*))\b")
_RX_WATCH_MM_BARE = re.compile(r"(?i)\b(?P<mm>3[8-9]|4[0-9])\b")

_RX_WATCH_PRICESTYLE = RX_WATCH_PRICESTYLE
_RX_WATCH_AW_PRICESTYLE = RX_WATCH_AW_PRICESTYLE
_RX_WATCH_S_SERIES = RX_WATCH_S_SERIES
_RX_AW_TOKEN = RX_AW_TOKEN

# --- connectivity ---
_RX_WIFI_CELL = RX_WIFI_CELL
_RX_WIFI = RX_WIFI
_RX_CELLULAR = RX_CELLULAR
_RX_LTE = RX_LTE
_RX_5G = RX_5G
_RX_4G = RX_4G

# --- chips / features / ports ---
_RX_CHIP_APPLE = RX_CHIP_APPLE
_RX_ANC = RX_ANC
_RX_MAGSAFE = RX_MAGSAFE
_RX_LIGHTNING = RX_LIGHTNING
_RX_USBC = RX_USBC

# --- tablets ---
_RX_TABLET_SIGNALS = RX_TABLET_SIGNALS
