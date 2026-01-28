# handlers/normalizers/entry_dicts.py
# Все словари/константы, вынесенные из common.py (и немного из entry.py),
# чтобы entry.py был “функциональный”, но без мегасловарей.

from __future__ import annotations

from typing import Dict, List, Set, Tuple

# =========================================================
# FLAGS / REGIONS
# =========================================================

REGION_FLAG_MAP: Dict[str, str] = {
    "🇪🇺": "eu", "🇬🇧": "uk", "🇷🇺": "ru", "🇧🇾": "by", "🇺🇦": "ua", "🇰🇿": "kz",
    "🇦🇲": "am", "🇦🇿": "az", "🇬🇪": "ge", "🇰🇬": "kg", "🇺🇿": "uz",
    "🇺🇸": "us", "🇨🇦": "ca", "🇲🇽": "mx",
    "🇧🇷": "br", "🇦🇷": "ar", "🇨🇱": "cl", "🇵🇪": "pe", "🇨🇴": "co", "🇪🇨": "ec",
    "🇯🇵": "jp", "🇨🇳": "cn", "🇭🇰": "hk", "🇲🇴": "mo", "🇹🇼": "tw",
    "🇮🇳": "in", "🇸🇬": "sg", "🇰🇷": "kr", "🇻🇳": "vn", "🇹🇭": "th",
    "🇲🇾": "my", "🇮🇩": "id", "🇵🇭": "ph", "🇵🇰": "pk", "🇧🇩": "bd",
    "🇳🇵": "np", "🇱🇰": "lk", "🇹🇷": "tr",
    "🇦🇪": "ae", "🇶🇦": "qa", "🇰🇼": "kw", "🇧🇭": "bh", "🇴🇲": "om",
    "🇸🇦": "sa", "🇮🇱": "il", "🇯🇴": "jo", "🇱🇧": "lb", "🇮🇶": "iq",
    "🇪🇬": "eg", "🇿🇦": "za", "🇳🇬": "ng", "🇰🇪": "ke", "🇲🇦": "ma",
    "🇩🇿": "dz", "🇹🇳": "tn",
    "🇦🇺": "au", "🇳🇿": "nz",
    "🇨🇭": "ch",
}

# =========================================================
# COLORS
# =========================================================

BASE_COLORS = [
    "Black", "White", "Blue", "Green", "Red", "Pink", "Purple", "Yellow", "Gold", "Silver", "Gray", "Grey", "Graphite", "Orange",
    "Midnight", "Starlight", "Titanium", "Space Black", "Space Gray", "Space Grey",
    "Natural", "Natural Titanium", "Blue Titanium", "White Titanium", "Black Titanium",
    "Desert", "Desert Titanium", "Ultramarine", "Lavender", "Cream", "Violet", "Coral", "Mint", "Lime", "Olive", "Navy", "Burgundy",
    "Sky Blue", "Light Gray", "Light Grey", "Icy Blue", "Silver Blue", "Silver Shadow", "Jade Green", "Pink Gold", "Jet Black",
    "Rose Gold", "Charcoal", "Black/Charcoal", "Dark Green", "Denim", "Sage", "Teal", "Moonstone", "Indigo","Lemongrass","Frost","Obsidian","Peony","Porcelain", "Hazel","Astral Trail",
    "Nebula Noir", "Coralred", "Lightgray", "SilverBlue", "PinkGold", "JadeGreen", "IcyBlue", "BlueBlack", "Rose Quartz", "Wintergreen", "Iris", "Bay", "Rose", "Aloe", "Brown",
    "Terra Cotta",
    "Ocean Cyan", "Dry Ice", "Marble Sands", "Marble Mist", "Earth", "Dune", "Moon", "Sandstone", "Deep Brown", "Transparent", "Clear", "Ivory", "Skyline", "Beige", "Fog", "Lunar Radiance",
    "Caramel", "Slate", "Fuchisa", "Nickel", "Strawberry Bronze", "Blackberry", "Moss", "Chrome Pearl", "Camouflage", "Light Blush", "Terra Cotta",
    "Alpine Green", "Chrome Indigo", "Chrome Teal", "Starlight Blue", "Sterling Silver", "Volcanic Red", "Cobalt Blue", "Cosmic Red",
    "Ceramic Patina", "Ceramic Pink", "Vinca Blue", "Vinca Blue/Topaz", "Ceramic Patina/Topaz",
    "Amber Silk", "Jasper Plum", "Kanzan Pink", "Prussian Blue", "Red Velvet",
    "Nickel Copper", "Nickel/Copper", "Nickel/Gold", "Gold/Nickel",
    "White/Gold", "Silver/Yellow", "Silver/Nickel", "Yellow/Nickel", "Nickel/Purple",
    "Ceramic Pink/Rose Gold", "Strawberry Bronze/Blush Pink", "Prussian Blue/Copper", "Red Velvet/Gold",
    "Prussian Blue/Rich Copper", "Onyx Black/Gold", "Blue/Black", "Black/Copper",
    "Blue/Bright Blue", "Black/Charcoal", "Anthracite", "Cobalt", "Copper", "Emerald", "Raspberry", "Turquoise", "Lilac",
    "Mist Blue"
]

COLOR_SYNONYMS: Dict[str, str] = {
    # ru -> en base
    "черный": "Black", "чёрный": "Black",
    "черная": "Black", "чёрная": "Black",
    "белый": "White", "белая": "White",

    "синий": "Blue", "синяя": "Blue", "голубой": "Blue", "голубая": "Blue",
    "зеленый": "Green", "зелёный": "Green", "зеленая": "Green", "зелёная": "Green",
    "красный": "Red", "красная": "Red",
    "розовый": "Pink", "розовая": "Pink",
    "фиолетовый": "Purple", "фиолетовая": "Purple",
    "лавандовый": "Lavender", "лавандовая": "Lavender",
    "желтый": "Yellow", "жёлтый": "Yellow", "желтая": "Yellow", "жёлтая": "Yellow",
    "оранжевый": "Orange", "оранжевая": "Orange", "оранж": "Orange",
    "золото": "Gold", "золотой": "Gold", "золотая": "Gold",
    "серебро": "Silver", "серебристый": "Silver", "серебристая": "Silver",
    "серый": "Gray", "серая": "Gray",
    "антрацит": "Anthracite", "антрацитовый": "Anthracite",
    "анцтрацит": "Anthracite",
    "кобальт": "Cobalt", "кобальтовый": "Cobalt",
    "медный": "Copper", "медная": "Copper",
    "изумруд": "Emerald", "изумрудный": "Emerald", "изумрудная": "Emerald",
    "малиновый": "Raspberry", "малиновая": "Raspberry",
    "коралловый": "Coral", "коралловая": "Coral",
    "бежевый": "Beige", "бежевая": "Beige",
    "лиловый": "Lilac", "лиловая": "Lilac",
    "бирюзовый": "Turquoise", "бирюзовая": "Turquoise",
    "графит": "Graphite",
    "кремовый": "Cream", "кремов": "Cream",
    "фиолет": "Violet",
    "коралл": "Coral",
    "мята": "Mint", "мятный": "Mint", "мятная": "Mint",
    "лайм": "Lime", "лаймовый": "Lime", "лаймовая": "Lime",
    "олив": "Olive", "оливковый": "Olive", "оливковая": "Olive",
    "бордовый": "Burgundy", "бордовая": "Burgundy",
    "небесн": "Sky Blue",
    "темно-синий": "Navy", "тёмно-синий": "Navy",
    "титан": "Titanium", "титановый": "Titanium",
    "jade green": "Jade Green",
    "jadegreen": "JadeGreen",
    "bright blue": "Blue",
    "blue/bright blue": "Blue",
    "blue bright blue": "Blue",

    # slang
    "блэк": "Black", "блек": "Black",
    "вайт": "White", "уайт": "White",
    "блю": "Blue", "блу": "Blue",
    "грин": "Green",
    "ред": "Red",
    "пинк": "Pink",
    "пурпл": "Purple", "перпл": "Purple",
    "йеллоу": "Yellow",
    "голд": "Gold", "голден": "Gold",
    "сильвер": "Silver", "силвер": "Silver",

    # common typos
    "lavander": "Lavender",
    "Lavander": "Lavender",

    # ultramarine family
    "ультрамарин": "Ultramarine",
    "ultramarin": "Ultramarine",
    "ultra marine": "Ultramarine",
    "ultra blue": "Ultramarine",

    # natural / titanium
    "натурал": "Natural",
    "натуральный": "Natural",
    "натурал титаниум": "Natural Titanium",
    "natural titanium": "Natural Titanium",

    # desert
    "дезерт": "Desert",
    "дезерт титаниум": "Desert Titanium",
    "desert titanium": "Desert Titanium",

    # space*
    "spaceblack": "Space Black",
    "space black": "Space Black",
    "spacegray": "Space Gray",
    "space grey": "Space Gray",
    "spacegray": "Space Gray",
    "space": "Space Gray",

    # misc
    "jetblack": "Jet Black",
    "jet black": "Jet Black",
    "jatblack": "Jet Black",
    "terra cotta": "Terra Cotta",
    "terracotta": "Terra Cotta",
    "black/charcoal": "Black/Charcoal",
    "black charcoal": "Black/Charcoal",
    "black black charcoal": "Black/Charcoal",
    "spark orange": "Orange",
    "power pink": "Purple",
    "gravel gray": "Gravel Gray",
    "sand": "Gravel Gray",
    "rosegold": "Rose Gold",
    "rose gold": "Rose Gold",
    "(product)red": "Red",
    "product red": "Red",

    "denim": "Denim",
    "джинс": "Denim",
    "джинсов": "Denim",

    "sage": "Sage",
    "сейдж": "Sage",
    "шалфей": "Sage",
    "шалфейн": "Sage",

    "light blush": "Light Blush",
    "blush": "Blush",
    "plum": "Light Blush",
    "jasper plum": "Jasper Plum",
    "kanzan pink": "Kanzan Pink",
    "prussian blue": "Prussian Blue",
    "red velvet": "Red Velvet",
    "amber silk": "Amber Silk",
    "strawberry bronze": "Strawberry Bronze",
    "ceramic patina": "Ceramic Patina",
    "ceramica patina": "Ceramic Patina",
    "ceramic pink": "Ceramic Pink",
    "ceramica pink": "Ceramic Pink",
    "vinca blue": "Vinca Blue",
    "vinca blue/topaz": "Vinca Blue/Topaz",
    "vinca blue topaz": "Vinca Blue/Topaz",
    "vinca blue/topaz orange": "Vinca Blue/Topaz",
    "vinca blue topaz orange": "Vinca Blue/Topaz",
    "ceramic patina/topaz": "Ceramic Patina/Topaz",
    "ceramic patina topaz": "Ceramic Patina/Topaz",
    "white/gold": "White/Gold",
    "silver/yellow": "Silver/Yellow",
    "silver/nickel": "Silver/Nickel",
    "yellow/nickel": "Yellow/Nickel",
    "nickel/purple": "Nickel/Purple",
    "nickel copper": "Nickel Copper",
    "nickel cooper": "Nickel Copper",
    "nickel/copper": "Nickel/Copper",
    "nickel/gold": "Nickel/Gold",
    "gold/nickel": "Gold/Nickel",
    "ceramic pink/rose gold": "Ceramic Pink/Rose Gold",
    "ceramic pink rose gold": "Ceramic Pink/Rose Gold",
    "strawberry bronze/blush pink": "Strawberry Bronze/Blush Pink",
    "strawberry bronze blush pink": "Strawberry Bronze/Blush Pink",
    "prussian blue/copper": "Prussian Blue/Copper",
    "prussian blue copper": "Prussian Blue/Copper",
    "prussian blue/rich copper": "Prussian Blue/Rich Copper",
    "prussian blue rich copper": "Prussian Blue/Rich Copper",
    "red velvet/gold": "Red Velvet/Gold",
    "red velvet gold": "Red Velvet/Gold",
    "onyx black/gold": "Onyx Black/Gold",
    "onyx black gold": "Onyx Black/Gold",
    "blue/black": "Blue/Black",
    "black/blue": "Blue/Black",
    "black/copper": "Black/Copper",
    "black/cooper": "Black/Copper",
    "copper": "Copper",
    "cooper": "Copper",
    "mist": "Mist Blue",
    "mist blue": "Mist Blue",
    "alpine green": "Alpine Green",
    "chrome indigo": "Chrome Indigo",
    "chrome pearl": "Chrome Pearl",
    "chrome teal": "Chrome Teal",
    "starlight blue": "Starlight Blue",
    "sterling silver": "Sterling Silver",
    "volcanic red": "Volcanic Red",
    "cobalt blue": "Cobalt Blue",
    "cosmic red": "Cosmic Red",
    "camouflage": "Camouflage",
    "camo": "Camouflage",

    # brown synonyms
    "brown": "Brown",

    # Beats / neutral sand family
    "sandstone": "Sandstone",
    "sand stone": "Sandstone",
    "sand gray": "Sandstone",
    "sand grey": "Sandstone",
    "sandgray": "Sandstone",
    "sandgrey": "Sandstone",

    # ru sand
    "песочный": "Sandstone",
    "песок": "Sandstone",
    "сэнд": "Sandstone",

    # common misspells
    "iceblue": "Icy Blue",
    "icyblue": "Icy Blue",
    "strarlight": "Starlight",

    # titanium + base color combos
    "titanium black": "Black Titanium",
    "titanium white": "White Titanium",
    "titanium silver": "Silver",
    "titanium whitesilver": "Silver",
    "titanium white silver": "Silver",
    "white silver": "White",
    "whitesilver": "White",
    "titanium gray": "Gray",
    "titanium grey": "Gray",
    "titanium jetblack": "Black",
    "titanium silverblue": "SilverBlue",
    "titanium silver blue": "SilverBlue",
    "black titanium": "Black Titanium",
    "black ti": "Black Titanium",
    "ti black": "Black Titanium",
}

# =========================================================
# GAMES / BUNDLES
# =========================================================

GAME_TOKENS = {
    "mario",
    "zelda",
    "horizon",
    "forbidden",
    "war",
    "god",
    "spider",
    "last",
    "gta",
    "cyberpunk",
    "hogwarts",
    "star",
    "jedi",
    "outlaws",
    "mortal",
    "kombat",
    "mk",
    "ufc",
    "f1",
    "gran",
    "turismo",
    "fortnite",
    "dragon",
    "ball",
    "assassin",
    "shadows",
    "hades",
    "survival",
    "kids",
    "nightmares",
}

# ⚠️ Важно: keys должны совпадать по регистру с тем,
# что реально возвращает нормализатор цвета (обычно Title Case).
COLOR_CANON_MAP: Dict[str, str] = {
    # black family
    "Space Black": "Black",
    "Jet Black": "Black",
    "Charcoal": "Black",
    "Graphite": "Black",
    "Midnight": "Black",

    # blue family
    "Sky Blue": "Blue",
    "Mist Blue": "Blue",
    "Icy Blue": "Blue",
    "IcyBlue": "Blue",
    "Navy": "Blue",
    "Silver Blue": "Blue",
    "Ultramarine": "Blue",
    "SilverBlue": "Blue",

    # gray family
    "Space Gray": "Gray",
    "Space Grey": "Gray",
    "Light Gray": "Gray",
    "Light Grey": "Gray",
    "Silver Shadow": "Gray",
    "Grey": "Gray",

    # ✅ ключевой кейс:
    # Silver считается совместимым/канонизируемым к White для мэтчера
    "Silver": "White",
    "Starlight": "White",

    # green family
    "Dark Green": "Green",
    "Jade Green": "Green",
    "JadeGreen": "Green",
    "Olive": "Green",
    "Mint": "Green",
    "Emerald": "Green",

    # purple family
    "Lavender": "Purple",
    "Violet": "Purple",
    "Lilac": "Purple",

    # pink family
    "Rose Gold": "Pink",
    "Pink Gold": "Pink",
    "PinkGold": "Pink",
    "Raspberry": "Pink",

    # orange/yellow family
    "Coral": "Orange",
    "Cream": "Yellow",
    "Copper": "Brown",
    "Beige": "Sandstone",

    # blue family additions
    "Cobalt": "Blue",
    "Turquoise": "Mint",

    # gray/black family additions
    "Anthracite": "Black",

    # titanium
    "Natural Titanium": "Natural",
    "Blue Titanium": "Blue",
    "White Titanium": "White",
    "Black Titanium": "Black",
    "Titanium Black": "Black",
    "Titanium White": "White",

    # desert titanium
    "Desert Titanium": "Desert",
}

# =========================================================
# WATCH BANDS
# =========================================================

BAND_TYPE_SYNONYMS: Dict[str, str] = {
    "braided solo loop": "Braided Solo Loop",
    "milanese loop": "Milanese Loop",
    "link bracelet": "Link Bracelet",
    "trail loop": "Trail Loop",
    "alpine loop": "Alpine Loop",
    "ocean band": "Ocean Band",
    "solo loop": "Solo Loop",
    "sport loop": "Sport Loop",
    "sports band": "Sport Band",
    "sport band": "Sport Band",
    "milanese": "Milanese Loop",
    "charcoal loop": "Trail Loop",
    "спорт ремешок": "Sport Band",
    "modern buckle": "Modern Buckle",
}

# =========================================================
# MEMORY / RAM plausibility
# =========================================================

RAM_PLAUSIBLE: Set[int] = {2, 3, 4, 6, 8, 10, 12, 16, 18, 20, 24, 32, 36, 48, 64, 96, 128}

# =========================================================
# CONTRACT PARAM KEYS (unify_item_contract)
# =========================================================

PARAM_KEYS_DEFAULT = {
    "sim",
    "connectivity",
    "lte",
    "wifi",
    "cellular",
    "radio",
    "processor",
    "chip",
    "screen",
    "diagonal",
    "size",
    "material",
    "year",
    "watch_size_mm",
    "band_size",
    "band_type",
    "band_color",
    "code",
    "color",
    "storage",
    "ram",
    "region",
}

# =========================================================
# COLOR COMPATIBILITY GROUPS (matcher)
# =========================================================
# ⚠️ Здесь — lower-case, matcher сам приведёт к lower.
# Эти группы НЕ «инференс», а справочник эквивалентности,
# чтобы не ломать матчи между эталоном/прайсом из-за разных слов.
COLOR_COMPAT_GROUPS = [
    # black family
    {"black", "space black", "jet black", "graphite", "midnight", "charcoal"},
    # ✅ white family
    {"white", "starlight", "silver", "light silver"},
    # gray family
    {"gray", "grey", "space gray", "space grey", "light gray", "light grey", "silver shadow"},
    # blue family
    {"blue", "deep blue", "navy", "sky blue", "icy blue", "ultramarine", "blue titanium", "silver blue"},
    # green family
    {"green", "mint", "jade green", "dark green", "olive"},
    # pink family
    {"pink", "rose", "rose gold", "pink gold"},
    # purple family
    {"purple", "violet", "lavender"},
    # red family
    {"red", "product red", "(product)red"},
    {"gold"},
    # titanium family
    {"natural", "natural titanium", "titanium"},
    {"black titanium"},
    {"white titanium"},
    {"desert", "desert titanium"},
    {"coral"},
    {"cream"},
    {"yellow"},
    {"orange"},
    {"denim"},
    {"sage"},
]
