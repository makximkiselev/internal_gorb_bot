# handlers/auto_replies/listener.py
from __future__ import annotations

import json
import os
import re
import hashlib
import unicodedata
import asyncio
import inspect
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Tuple, Optional

from telethon import events
from telethon.tl.types import User
from telethon.errors import RPCError
from telethon.errors.rpcerrorlist import (
    UserPrivacyRestrictedError,
    ChatWriteForbiddenError,
    PeerIdInvalidError,
    InputUserDeactivatedError,
)

from storage import load_data
from handlers.normalizers.entry import run_build_parsed_goods
from handlers.normalizers import entry as entry_mod  # ✅ extract_* / match_model_from_text / indexes
from handlers.parsing.matcher import match_product  # (ok, reason)
from handlers.parsing import PARSED_FILE  # parsed_data.json

# ====================== Файлы/пути ======================
BASE_DIR = Path(__file__).resolve().parent
LOG_FILE = BASE_DIR / "auto_replies.log"
SPAM_FILE = BASE_DIR / "spam_messages.json"
UNMATCHED_FILE = BASE_DIR / "unmatched_queries.json"
MATCHED_FILE = BASE_DIR / "matched_queries.json"
SOURCES_FILE = BASE_DIR.parent.parent / "sources.json"


def _ensure_parent(p: Path):
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass


def _ensure_json_file(file: Path, default):
    _ensure_parent(file)
    if not file.exists():
        try:
            file.write_text(json.dumps(default, ensure_ascii=False, indent=2), encoding="utf-8")
            return
        except Exception:
            return
    try:
        _ = json.loads(file.read_text(encoding="utf-8"))
    except Exception:
        try:
            file.write_text(json.dumps(default, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass


_ensure_json_file(SPAM_FILE, [])
_ensure_json_file(UNMATCHED_FILE, [])
_ensure_json_file(MATCHED_FILE, [])
_ensure_parent(LOG_FILE)

LOG_TO_STDOUT = os.getenv("AR_VERBOSE", "0") == "1"

# 👉 основной аккаунт-отвечатель (по умолчанию el_opt, можно переопределить через env)
PRIMARY_REPLY_ACCOUNT = os.getenv("AR_PRIMARY_ACCOUNT", "el_opt")

# если хочешь отвечать и не на buy-intent — поставь 0
REQUIRE_BUY_INTENT = os.getenv("AR_REQUIRE_BUY_INTENT", "0") == "1"

# TTL для кешей
ETALON_TTL_SEC = int(os.getenv("AR_ETALON_TTL_SEC", "120"))  # ✅ было 20 — слишком часто (и дорого)
ALLOWED_PATHS_TTL_SEC = int(os.getenv("AR_ALLOWED_PATHS_TTL_SEC", "10"))

# ✅ НЕ БЛОЧИМ СООБЩЕНИЯ "через бота" автоматически
ALLOW_BOT_SENDER_FOR_PRODUCTS = os.getenv("AR_ALLOW_BOT_SENDER_FOR_PRODUCTS", "1") == "1"
IGNORE_BOT_SENDERS_HARD = os.getenv("AR_IGNORE_BOT_SENDERS_HARD", "0") == "1"

# ====================== Наценка (только для текста ответа) ======================
AR_MARKUP_ENABLED = os.getenv("AR_MARKUP_ENABLED", "1") == "1"

# градация:
# <20k        +300
# 20–150k     +500
# 150–250k    +1000
# >=250k      +2000
AR_MARKUP_T1 = int(os.getenv("AR_MARKUP_T1", "20000"))
AR_MARKUP_T2 = int(os.getenv("AR_MARKUP_T2", "150000"))
AR_MARKUP_T3 = int(os.getenv("AR_MARKUP_T3", "250000"))

AR_MARKUP_A0 = int(os.getenv("AR_MARKUP_A0", "300"))
AR_MARKUP_A1 = int(os.getenv("AR_MARKUP_A1", "500"))
AR_MARKUP_A2 = int(os.getenv("AR_MARKUP_A2", "1000"))
AR_MARKUP_A3 = int(os.getenv("AR_MARKUP_A3", "2000"))

_PRUNE_TASK_STARTED = False

MSK_TZ = timezone(timedelta(hours=3))


def _msk_now() -> datetime:
    return datetime.now(MSK_TZ)


def _clear_runtime_state():
    global _cache_per_user, _cache_global, _cache_replied_recently
    _cache_per_user = {}
    _cache_global = {}
    _cache_replied_recently = {}


def _clear_files():
    _ensure_json_file(SPAM_FILE, [])
    _ensure_json_file(UNMATCHED_FILE, [])
    _ensure_json_file(MATCHED_FILE, [])
    _ensure_parent(LOG_FILE)

    try:
        LOG_FILE.write_text("", encoding="utf-8")
    except Exception:
        pass
    try:
        SPAM_FILE.write_text("[]", encoding="utf-8")
    except Exception:
        pass
    try:
        UNMATCHED_FILE.write_text("[]", encoding="utf-8")
    except Exception:
        pass
    try:
        MATCHED_FILE.write_text("[]", encoding="utf-8")
    except Exception:
        pass


def _clear_all_logs_and_state():
    _clear_runtime_state()
    _clear_files()


def _seconds_until_2359_msk() -> int:
    now = _msk_now()
    target = now.replace(hour=23, minute=59, second=0, microsecond=0)
    if now >= target:
        target = target + timedelta(days=1)
    return max(1, int((target - now).total_seconds()))


async def _daily_prune_job(acc_name: str):
    while True:
        try:
            await asyncio.sleep(_seconds_until_2359_msk())
            _clear_all_logs_and_state()
        except Exception:
            await asyncio.sleep(60)


# ====================== SAFE wrapper for run_build_parsed_goods ======================
_RBPG_SIG = None


def _rbpg(text: str, *, channel: str | None = None, message_id=None, date: str | None = None, path=None) -> list[dict]:
    """
    Safe wrapper around run_build_parsed_goods with signature drift tolerance.
    Your entry.py may change args (channel/message_id/date/path). This wrapper adapts.
    """
    global _RBPG_SIG
    if date is None:
        date = datetime.now(timezone.utc).isoformat()

    # Fast-path: single-line normalization for incoming queries
    try:
        if hasattr(entry_mod, "normalize_text_as_goods_item"):
            item = entry_mod.normalize_text_as_goods_item(
                text,
                channel=channel,
                message_id=message_id,
                date=date,
                path=path,
            )
            if isinstance(item, dict) and not item.get("empty"):
                return [item]
            return []
    except Exception:
        pass

    if _RBPG_SIG is None:
        try:
            _RBPG_SIG = inspect.signature(run_build_parsed_goods)
        except Exception:
            _RBPG_SIG = None

    if _RBPG_SIG is not None:
        params = _RBPG_SIG.parameters
        kwargs = {}

        if "channel" in params:
            kwargs["channel"] = channel
        elif "account" in params and channel is not None:
            kwargs["account"] = channel

        if "message_id" in params:
            kwargs["message_id"] = message_id
        elif "msg_id" in params:
            kwargs["msg_id"] = message_id

        if "date" in params:
            kwargs["date"] = date
        elif "dt" in params:
            kwargs["dt"] = date

        if "path" in params:
            kwargs["path"] = path

        try:
            res = run_build_parsed_goods(text, **kwargs)
            return res if isinstance(res, list) else (res or [])
        except TypeError:
            pass
        except Exception:
            return []

    # fallbacks
    try:
        res = run_build_parsed_goods(text, date=date, path=path)
        return res if isinstance(res, list) else (res or [])
    except TypeError:
        pass
    except Exception:
        return []

    try:
        res = run_build_parsed_goods(text, path=path)
        return res if isinstance(res, list) else (res or [])
    except TypeError:
        pass
    except Exception:
        return []

    try:
        res = run_build_parsed_goods(text, date=date)
        return res if isinstance(res, list) else (res or [])
    except TypeError:
        pass
    except Exception:
        return []

    try:
        res = run_build_parsed_goods(text)
        return res if isinstance(res, list) else (res or [])
    except Exception:
        return []


# ====================== Анти-дубликаты ======================
WINDOW_PER_USER = timedelta(minutes=2)
WINDOW_GLOBAL = timedelta(seconds=60)

_cache_per_user: dict[str, datetime] = {}
_cache_global: dict[str, datetime] = {}


def _norm_text(text: str) -> str:
    return " ".join((text or "").lower().split())


def _h_user_text(uid: int, text: str) -> str:
    return hashlib.md5(f"{uid}:{_norm_text(text)}".encode()).hexdigest()


def _h_text(text: str) -> str:
    return hashlib.md5(_norm_text(text).encode()).hexdigest()


def _was_replied_user(uid: int, text: str) -> bool:
    global _cache_per_user
    now = datetime.utcnow()
    _cache_per_user = {k: ts for k, ts in _cache_per_user.items() if now - ts < WINDOW_PER_USER}
    key = _h_user_text(uid, text)
    if key in _cache_per_user:
        return True
    _cache_per_user[key] = now
    return False


def _blocked_globally(text: str) -> bool:
    global _cache_global
    now = datetime.utcnow()
    _cache_global = {k: ts for k, ts in _cache_global.items() if now - ts < WINDOW_GLOBAL}
    key = _h_text(text)
    if key in _cache_global:
        return True
    _cache_global[key] = now
    return False


# ====================== Контекст ответов (анти-дубликат по реплаям) ======================
WINDOW_CONTEXT = timedelta(minutes=30)
_cache_replied_recently: dict[str, tuple[str, datetime]] = {}


def _already_sent_recently(user_id: int, reply: str) -> bool:
    global _cache_replied_recently
    now = datetime.utcnow()

    _cache_replied_recently = {
        uid: (h, ts)
        for uid, (h, ts) in _cache_replied_recently.items()
        if now - ts < WINDOW_CONTEXT
    }

    key = str(user_id)
    reply_hash = hashlib.md5(_norm_text(reply).encode()).hexdigest()

    if key in _cache_replied_recently:
        prev_hash, _ = _cache_replied_recently[key]
        if prev_hash == reply_hash:
            return True

    _cache_replied_recently[key] = (reply_hash, now)
    return False


# ====================== Логгер ======================
def _log(msg: str):
    ts = datetime.now(timezone.utc).isoformat()
    line = f"{ts} {msg}"
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass
    if LOG_TO_STDOUT:
        print(line)


def _format_origin(event) -> str:
    try:
        if getattr(event, "is_private", False):
            return f"DM:{event.sender_id}"
        chat = getattr(event, "chat", None)
        if chat:
            return f"CHAT:{chat.id} ({getattr(chat, 'title', 'no_title')})"
        return f"CHAT:{event.chat_id}"
    except Exception:
        return f"CHAT:{getattr(event, 'chat_id', 'unknown')}"


# ====================== Sources allowlist (sources.json) ======================
_SOURCES_CACHE: dict[str, dict] = {}


def _normalize_chat_id_for_match(cid: int) -> int:
    """
    Telethon chat_id for channels/supergroups can be -100XXXXXXXXX.
    Normalize to the base channel_id to match sources.json.
    """
    try:
        cid_abs = abs(int(cid))
    except Exception:
        return 0
    s = str(cid_abs)
    if s.startswith("100") and len(s) > 10:
        try:
            return int(s[3:])
        except Exception:
            return cid_abs
    return cid_abs


def _load_sources_allowed_ids(acc_name: str) -> set[int]:
    try:
        if not SOURCES_FILE.exists():
            return set()
        raw = json.loads(SOURCES_FILE.read_text(encoding="utf-8"))
    except Exception:
        return set()

    if not isinstance(raw, dict):
        return set()

    out: set[int] = set()
    for key in ("chats",):
        arr = raw.get(key)
        if not isinstance(arr, list):
            continue
        for it in arr:
            if not isinstance(it, dict):
                continue
            acc = (it.get("account") or "").strip()
            if acc and acc != acc_name:
                continue
            cid = it.get("channel_id")
            try:
                if cid is not None:
                    out.add(_normalize_chat_id_for_match(int(cid)))
            except Exception:
                continue
    return out


def _sources_allowed_ids_live(acc_name: str, ttl_sec: int = 30) -> set[int]:
    now = datetime.utcnow()
    bucket = _SOURCES_CACHE.get(acc_name)
    if bucket:
        try:
            if (now - bucket["ts"]).total_seconds() < ttl_sec:
                return bucket["ids"]
        except Exception:
            pass

    ids = _load_sources_allowed_ids(acc_name)
    _SOURCES_CACHE[acc_name] = {"ids": ids, "ts": now}
    return ids


# ====================== Guard / Классификация ======================
WINDOW_KEEP_LOGS = timedelta(hours=1)
WINDOW_SPAM_KEEP = WINDOW_KEEP_LOGS

DASH_CH = r"[—–-]"

PRICE_TOKEN_RE = re.compile(
    r"""(?x)
    (?:\$?\s*\d{1,3}(?:[ \u00A0]?\d{3})+(?:[.,]\d+)?\s*(?:₽|руб|р\.?|rub|usd|\$)\b)|
    (?:\$\s*\d+(?:[.,]\d+)?\b)
    """,
    re.I,
)

ARROW_PRICE_RE = re.compile(
    r"(?:->|:)\s*\d{3,}(?:[ .\u00A0]?\d{3})*(?:[.,]\d+)?(?:[^\n\d]{0,20})?$",
    re.I,
)

BARE_TAIL_PRICE_RE = re.compile(r"\b\d{4,}(?:[.,]\d+)?\s*$")

MIDLINE_PRICE_RE = re.compile(
    rf"(?:^|\s)(?:{DASH_CH}|:)\s*\d{{3,}}(?:[ .\u00A0]?\d{{3}})*(?:[.,]\d+)?\b",
    re.I,
)

PRICE_LIST_LINE_RE = re.compile(
    rf"(?:{DASH_CH}\s*\d{{3,}}(?:[ .\u00A0]?\d{{3}})*(?:[.,]\d+)?(?:[^\n\d]{{0,20}})?$)|"
    r"(?:(?:->|:)\s*\d{3,}(?:[ .\u00A0]?\d{3})*(?:[.,]\d+)?(?:[^\n\d]{0,20})?$)",
    re.I,
)

SIM_TOKENS_RE = re.compile(r"\b(?:esim|sim\+esim|2\s*sim|dual\s*sim|1\s*sim)\b", re.I)

PRODAM_RE = re.compile(r"\b(продам|продаю|продажа|sell|selling)\b", re.I)
BUY_INTENT_RE = re.compile(
    r"\b(куплю|ищу|нужен|нужна|нужно|надо|возьму|подскажите|предложите|рассматриваю|рассмотрю|buy|looking\s*for|need|lf|wtb)\b",
    re.I,
)
RESERVE_RE = re.compile(r"\b(бронь|заберу|беру|взял)\b", re.I)

JOB_RE = re.compile(r"\b(подработк|ваканси|работ[аыу]|заработа|доход)\b", re.I)
LINK_RE = re.compile(r"(https?://|t\.me/|@\w+)", re.I)
CRYPTO_RE = re.compile(r"\b(u[.\s_-]*s[.\s_-]*d[.\s_-]*t|btc|bit\s*coin|биткоин|crypto|крипт)\b", re.I)
BROADCAST_RE = re.compile(r"\b(ак+а?унт\s*рассыл|рассылк[аи]|broadcast)\b", re.I)

PRODUCT_HINTS = [
    "iphone",
    "samsung",
    "xiaomi",
    "redmi",
    "realme",
    "oneplus",
    "huawei",
    "honor",
    "google",
    "pixel",
    "oppo",
    "vivo",
    "tecno",
    "infinix",
    "airpods",
    "watch",
    "watch ultra",
    "ipad",
    "ipad pro",
    "ipad air",
    "ipad mini",
    "whoop",
    "macbook",
    "macbook pro",
    "macbook air",
    "imac",
    "mac mini",
    "mac studio",
    "apple pencil",
    "pencil",
    "pencil usb c",
    "magic mouse",
    "magic keyboard",
    "playstation",
    "ps5",
    "dualsense",
    "xbox",
    "switch",
    "beats",
    "beats studio",
    "beats studio pro",
    "plaud",
    "plaud note",
    "yandex",
    "yandex station",
    "станция алиса",
    "алиса",
]


def _normalize_query_text(t: str) -> str:
    t = unicodedata.normalize("NFKC", t or "")
    t = re.sub(r"\s+", " ", t).strip()
    return t


# ====================== Apple Watch canon ======================
AW_SERIES_RE = re.compile(r"(?i)\bapple\s*watch\s*(?:series\s*)?(?P<num>\d{1,2})\b")
AW_SERIES_ALT_RE = re.compile(r"(?i)\bwatch\s*(?:series\s*)?(?P<num>\d{1,2})\b")


def _canon_aw_model(model: str, brand: str | None = None, series: str | None = None) -> Tuple[str, str]:
    m = AW_SERIES_RE.search(model or "") or AW_SERIES_ALT_RE.search(model or "")
    if m or ((brand or "").lower() == "apple" and (series or "").lower() == "watch"):
        mm = re.search(r"\d{1,2}", model or "")
        num = (m.group("num") if m else (mm.group(0) if mm else "")) or ""
        if num:
            canon = f"Apple Watch Series {int(num)}"
            key = f"aw-{int(num)}"
            return canon, key
    return model or "", ""


def _canonize_parsed_watch(p: dict) -> dict:
    if not isinstance(p, dict):
        return p
    brand = (p.get("brand") or "").strip()
    series = (p.get("series") or "").strip()
    model = (p.get("model") or "").strip()
    canon, key = _canon_aw_model(model, brand, series)
    if key:
        p = dict(p)
        p["model"] = canon
        p["_model_key"] = key
    ds = p.get("dial_size")
    if isinstance(ds, str):
        p["dial_size"] = ds.replace(" ", "").lower()
    return p


def _canonize_etalon_watch(e: dict) -> dict:
    if not isinstance(e, dict):
        return e
    model = (e.get("model") or "").strip()
    path = e.get("path") or e.get("_path") or []
    brand = "Apple" if any(isinstance(x, str) and x.lower() == "apple" for x in path) else ""
    series = "Watch" if any(isinstance(x, str) and x.lower() == "watch" for x in path) else ""
    canon, key = _canon_aw_model(model, brand, series)
    if key:
        e = dict(e)
        e["model"] = canon
        e["_model_key"] = key
    return e


def _has_many_prices(text: str) -> bool:
    t = text or ""
    lines = t.splitlines()
    tokens_total = len(PRICE_TOKEN_RE.findall(t))

    price_like_lines = 0
    for ln in lines:
        s = ln.strip()
        if not s:
            continue
        if PRICE_LIST_LINE_RE.search(s) or ARROW_PRICE_RE.search(s) or BARE_TAIL_PRICE_RE.search(s) or MIDLINE_PRICE_RE.search(s):
            price_like_lines += 1

    if tokens_total >= 2:
        return True
    if price_like_lines >= 3:
        return True

    sim_price_lines = 0
    for ln in lines:
        if MIDLINE_PRICE_RE.search(ln) and SIM_TOKENS_RE.search(ln):
            sim_price_lines += 1
    if sim_price_lines >= 2:
        return True

    price_lines = sum(1 for ln in lines if PRICE_TOKEN_RE.search(ln))
    return price_lines >= 2


def _looks_like_price_list(text: str) -> bool:
    hits = 0
    for ln in (text or "").splitlines():
        s = ln.strip()
        if not s:
            continue
        if PRICE_LIST_LINE_RE.search(s) or ARROW_PRICE_RE.search(s) or BARE_TAIL_PRICE_RE.search(s) or MIDLINE_PRICE_RE.search(s):
            hits += 1
            if hits >= 3:
                return True
    return False


def _looks_like_product(text: str) -> bool:
    t = _normalize_query_text(text).lower()
    return any(h in t for h in PRODUCT_HINTS)


def classify_message(text: str) -> str:
    low = (_normalize_query_text(text) or "").lower()

    if PRODAM_RE.search(low):
        return "spam"

    if BUY_INTENT_RE.search(low):
        return "product"

    if RESERVE_RE.search(low):
        return "silent"

    if _looks_like_price_list(text):
        return "spam"
    if BROADCAST_RE.search(low):
        return "spam"
    if JOB_RE.search(low):
        return "spam"
    if CRYPTO_RE.search(low):
        return "spam"

    if LINK_RE.search(text or ""):
        lines = (text or "").splitlines()
        first = lines[0] if lines else ""
        if "@" in first and len(first.split()) <= 5 and not re.search(r"https?://", first):
            pass
        else:
            return "spam"

    if _has_many_prices(text):
        return "spam"

    if re.search(r"\b(id|паспорт|photo\s*id|документ|оплат[ау]|usd|\$|доллар)\b", low):
        return "spam"

    if len(re.findall(r"[🔥💥💎⭐️✨🎯🚀🎁💰❤️‍🔥]", text or "")) > 5:
        return "spam"

    if _looks_like_product(text):
        return "product"

    parsed_probe = _rbpg(
        text,
        channel="__probe__",
        message_id=None,
        date=datetime.now(timezone.utc).isoformat(),
        path=None,
    ) or []
    return "product" if parsed_probe else "spam"


# ====================== Очистка входящего текста перед парсингом ======================
INTENT_RE = re.compile(
    r"""(?ix)
    \b(куплю|ищу|нужен|нужна|нужно|надо|возьму|подскажите|предложите|рассматриваю|рассмотрю|
       buy|looking\s*for|need|lf|wtb)\b
    """
)
POLITENESS_RE = re.compile(r"(?i)\b(пожалуйста|pls|please|пжл|пж)\b")
TRAIL_PUNCT_RE = re.compile(r"[?!.…]+$")
EMOJI_RE = re.compile(r"[\U0001F300-\U0001FAFF]+")


def clean_for_matching(text: str) -> str:
    t = unicodedata.normalize("NFKC", text or "")
    lines = []
    for ln in t.splitlines():
        ln = ARROW_PRICE_RE.sub("", ln)
        lines.append(ln)
    t = "\n".join(lines)

    t = INTENT_RE.sub(" ", t)
    t = POLITENESS_RE.sub(" ", t)
    t = EMOJI_RE.sub(" ", t)
    t = TRAIL_PUNCT_RE.sub("", t)

    t = re.sub(r"[ \t]+", " ", t)
    t = re.sub(r"\n{2,}", "\n", t)
    return t.strip()


def _split_candidate_lines(text: str) -> list[str]:
    out: list[str] = []
    for ln in (text or "").splitlines():
        ln = re.sub(r"^[\s*•\-–—]+", "", ln.strip())
        if not ln:
            continue
        ln = clean_for_matching(ln)
        if ln:
            out.append(ln)
    if not out:
        whole = clean_for_matching(text or "")
        if whole:
            out.append(whole)
    return out


# ====================== Логи prune ======================
def _parse_iso(dt_str: str) -> datetime | None:
    try:
        ts = datetime.fromisoformat(dt_str)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts
    except Exception:
        return None


def _prune_records(rows: List[dict], keep_window: timedelta) -> List[dict]:
    now = datetime.now(timezone.utc)
    fresh: List[dict] = []
    for r in rows or []:
        ts = _parse_iso(r.get("date", ""))
        if ts is None:
            continue
        if now - ts <= keep_window:
            fresh.append(r)
    return fresh


# ====================== SPAM helpers ======================
def _load_spam() -> List[dict]:
    _ensure_json_file(SPAM_FILE, [])
    try:
        data = json.loads(SPAM_FILE.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _save_spam_list(data: List[dict]):
    try:
        SPAM_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


def save_spam(user_id: int, text: str, account_name: str, origin: str, reason: str = ""):
    data = _load_spam()
    data = _prune_records(data, WINDOW_SPAM_KEEP)

    rec = {
        "user_id": user_id,
        "text": (text or "").strip(),
        "account": account_name,
        "origin": origin,
        "reason": reason,
        "date": datetime.now(timezone.utc).isoformat(),
    }

    key = (str(rec["user_id"]), rec["text"].lower())
    for r in data:
        if (str(r.get("user_id")), str(r.get("text", "")).lower()) == key:
            r["date"] = rec["date"]
            r["reason"] = reason or r.get("reason", "")
            _save_spam_list(data)
            return

    data.append(rec)
    _save_spam_list(data)


# ====================== parsed_data.json loaders ======================
def _resolve_parsed_path() -> Path | None:
    env_path = os.getenv("AR_PARSED_FILE")
    if env_path:
        p = Path(env_path).expanduser().resolve()
        if p.exists():
            _log(f"🔎 AR_PARSED_FILE → {p}")
            return p
        _log(f"⚠️ AR_PARSED_FILE задан, но файла нет: {p}")

    try:
        if PARSED_FILE and Path(PARSED_FILE).exists():
            return Path(PARSED_FILE)
    except Exception:
        pass

    candidates = [
        BASE_DIR.parent / "parsing" / "parsed_data.json",
        BASE_DIR.parent / "parsing" / "data" / "parsed_data.json",
        BASE_DIR.parent.parent / "parsing" / "data" / "parsed_data.json",
        BASE_DIR.parent.parent / "handlers" / "parsing" / "data" / "parsed_data.json",
        BASE_DIR.parent.parent / "handlers" / "parsing" / "parsed_data.json",
    ]
    for p in candidates:
        try:
            if p.exists():
                _log(f"🔎 Fallback parsed_data.json → {p}")
                return p
        except Exception:
            pass
    return None


def _extract_min_price(item: dict) -> int | None:
    if item is None:
        return None
    if "min_price" in item and item["min_price"] is not None:
        try:
            return int(float(item["min_price"]))
        except Exception:
            pass
    for k in ("price_min", "min", "minPrice", "min_price_rub", "min_rub"):
        if k in item and item[k] is not None:
            try:
                return int(float(item[k]))
            except Exception:
                continue
    return None


# ✅ NEW: обход дерева parsed_data["catalog"] и сбор листьев с min_price
def _is_leaf_with_price(node: object) -> bool:
    return isinstance(node, dict) and ("min_price" in node) and (node.get("min_price") is not None)


def _walk_catalog_leaves(catalog: dict) -> list[tuple[list[str], str, dict]]:
    """
    Возвращает список листов:
      (path_parts, leaf_title, leaf_payload)

    path_parts: ["Смартфоны", "Apple", "iPhone 17", "iPhone 17 Pro Max"]
    leaf_title: "iPhone 17 Pro Max 256Gb Orange eSim"
    leaf_payload: {"min_price": 102800.0, "best_channels": [...]}
    """
    out: list[tuple[list[str], str, dict]] = []

    def rec(node: object, path: list[str]):
        if not isinstance(node, dict):
            return
        for k, v in node.items():
            key = str(k)
            if _is_leaf_with_price(v):
                out.append((path, key, v))
                continue
            if isinstance(v, dict):
                rec(v, path + [key])

    rec(catalog, [])
    return out


# === SIM нормализация: 1sim == sim+esim (склейка), 2sim и esim — отдельные ===
def _norm_sim(sim_val: str | None) -> str | None:
    s = (sim_val or "").strip().lower().replace(" ", "").replace("-", "")
    if not s:
        return None
    if s in ("1sim", "single", "one", "1", "sim+esim", "1sim+esim", "1plusesim", "1+esim", "1esim"):
        return "sim+esim"
    if s in ("2sim", "dualsim", "dual", "dual_sim", "2"):
        return "2sim"
    if s in ("esim", "e-sim"):
        return "esim"
    return None


def _format_config(sim: str | None, region: str | None) -> str:
    s = _norm_sim(sim) or ""
    r = (region or "").strip().upper()
    if r in ("DEFAULT", ""):
        r = ""
    if s and r:
        return f"({s}; {r})"
    if s:
        return f"({s})"
    if r:
        return f"({r})"
    return ""


# ---- кеш эталонов
_ETALON_CACHE: dict = {"ts": datetime.min, "items": [], "src_mtime": 0, "src_path": ""}

# ---- кеш модельного индекса (для быстрых offers из листьев)
_MODEL_INDEX_CACHE: dict = {"ts": datetime.min, "idx": {}}


def _model_index_live(ttl_sec: int = 300) -> dict:
    now = datetime.utcnow()
    try:
        if (now - _MODEL_INDEX_CACHE["ts"]).total_seconds() < ttl_sec and isinstance(_MODEL_INDEX_CACHE["idx"], dict):
            return _MODEL_INDEX_CACHE["idx"]
    except Exception:
        pass
    try:
        _MODEL_INDEX_CACHE["idx"] = entry_mod._load_model_index() or {}
    except Exception:
        _MODEL_INDEX_CACHE["idx"] = {}
    _MODEL_INDEX_CACHE["ts"] = now
    return _MODEL_INDEX_CACHE["idx"]


def _offer_from_leaf_fast(leaf_title: str, path_parts: list[str], leaf_payload: dict, price: int) -> dict | None:
    """
    ✅ ВАЖНО: больше НЕ парсим каждый leaf через run_build_parsed_goods (дорого).
    Вместо этого:
      - match_model_from_text по model_index
      - extract_storage/colors/region/sim + apply_default_sim
      - path берём из match_model (если получилось), иначе из дерева
    """
    raw = (leaf_title or "").strip()
    if not raw or price <= 0:
        return None

    model_index = _model_index_live()
    meta = None
    try:
        meta = entry_mod.match_model_from_text(raw, model_index)
    except Exception:
        meta = None

    path = []
    model = ""
    brand = ""
    series = ""
    if isinstance(meta, dict):
        pth = meta.get("path") or []
        if isinstance(pth, list) and pth:
            path = [str(x) for x in pth]
            brand = str(path[1]) if len(path) > 1 else ""
            series = str(path[2]) if len(path) > 2 else ""
            model = str(path[3]) if len(path) > 3 else ""

    # fallback path from catalog tree
    if not path and isinstance(path_parts, list) and path_parts:
        path = [str(x) for x in path_parts]
        # модель берём из последнего узла дерева (обычно это модельный уровень)
        model = str(path[-1]) if path else ""

    # params
    try:
        storage, ram = entry_mod.extract_storage(raw)
    except Exception:
        storage, ram = "", ""
    try:
        colors = entry_mod.extract_colors_all(raw, limit=3) or []
        color = colors[0] if colors else ""
    except Exception:
        color = ""
    try:
        region = (entry_mod.extract_region(raw) or "").strip().lower() or None
    except Exception:
        region = None
    try:
        sim0 = entry_mod.extract_sim(raw) or ""
    except Exception:
        sim0 = ""
    try:
        sim = entry_mod.apply_default_sim(brand=brand, series=series, model=model, region=region or "", sim=sim0, cat=cat)
    except Exception:
        sim = sim0

    e = {
        "model": model or "",
        "storage": storage or "",
        "ram": ram or "",
        "color": color or "",
        "sim": _norm_sim(sim),
        "region": (region or None),
        "price": int(price),
        "currency": "₽",
        "path": path or [],
        "_path": path or [],
        "_raw": raw,
        "_best_channels": (leaf_payload or {}).get("best_channels") or [],
    }
    e = _canonize_etalon_watch(e)
    if not (e.get("model") or "").strip():
        return None
    return e


def _load_etalons_from_parsed() -> List[dict]:
    """
    Загружает офферы для автоответчика из parsed_data.json.

    Поддерживаем 2 формата:
    1) legacy: parsed_data["etalon_with_prices"] (list)
    2) новый:  parsed_data["catalog"] (tree) где листья содержат {"min_price": ...}
    """
    path = _resolve_parsed_path()
    if not path:
        _log("❌ parsed_data.json не найден (PARSED_FILE/ENV/fallbacks)")
        return []

    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        _log(f"❌ Ошибка чтения {path}: {e}")
        return []

    if not isinstance(parsed, dict):
        _log("❌ parsed_data.json имеет неожиданный формат (не dict)")
        return []

    out: List[dict] = []

    # 1) legacy формат: etalon_with_prices
    etalon_list = parsed.get("etalon_with_prices")
    if isinstance(etalon_list, list):
        ok_cnt, drop_cnt = 0, 0
        for it in etalon_list:
            try:
                raw = (it.get("raw_etalon") or "").strip()
                pth = it.get("path") or []
                price = _extract_min_price(it)
                if not raw or price is None:
                    drop_cnt += 1
                    continue

                leaf_payload = {"best_channels": it.get("best_channels") or it.get("_best_channels") or []}
                e = _offer_from_leaf_fast(raw, [str(x) for x in (pth or [])], leaf_payload, int(price))
                if not e:
                    drop_cnt += 1
                    continue

                out.append(e)
                ok_cnt += 1
            except Exception:
                drop_cnt += 1
                continue

        _log(f"✅ etalon_with_prices: загружено {ok_cnt}, пропущено {drop_cnt}")
        return out

    # 2) новый формат: tree catalog
    catalog = parsed.get("catalog")
    if not isinstance(catalog, dict):
        _log("⚠️ parsed_data.json: ключ 'catalog' не найден или не dict — офферы пустые.")
        return []

    leaves = _walk_catalog_leaves(catalog)
    ok_cnt, drop_cnt = 0, 0

    for path_parts, leaf_title, leaf_payload in leaves:
        try:
            price = _extract_min_price(leaf_payload)
            if price is None:
                drop_cnt += 1
                continue

            e = _offer_from_leaf_fast(leaf_title, path_parts, leaf_payload, int(price))
            if not e:
                drop_cnt += 1
                continue

            out.append(e)
            ok_cnt += 1
        except Exception:
            drop_cnt += 1
            continue

    _log(f"✅ catalog: листов={len(leaves)}; офферов загружено {ok_cnt}, пропущено {drop_cnt}")
    return out


def _etalons_live(ttl_sec: int = ETALON_TTL_SEC) -> List[dict]:
    """
    ✅ умный кеш:
      - TTL
      - + пересборка если изменился mtime файла parsed_data.json
    """
    now = datetime.utcnow()
    p = _resolve_parsed_path()
    mtime = 0
    try:
        if p and p.exists():
            mtime = int(p.stat().st_mtime)
    except Exception:
        mtime = 0

    try:
        cached_ok = (now - _ETALON_CACHE["ts"]).total_seconds() < ttl_sec
        same_src = (str(p or "") == str(_ETALON_CACHE.get("src_path") or "")) and (
            mtime == int(_ETALON_CACHE.get("src_mtime") or 0)
        )
        if cached_ok and same_src:
            return _ETALON_CACHE["items"]
    except Exception:
        pass

    items = _load_etalons_from_parsed()
    _ETALON_CACHE["items"] = items
    _ETALON_CACHE["ts"] = now
    _ETALON_CACHE["src_mtime"] = mtime
    _ETALON_CACHE["src_path"] = str(p or "")
    return items


# ====================== JSON helpers для matched/unmatched ======================
def _append_json(file: Path, record: dict, unique_keys: tuple[str, ...] = ()):
    _ensure_json_file(file, [])
    try:
        data = json.loads(file.read_text(encoding="utf-8"))
        if not isinstance(data, list):
            data = []
    except Exception:
        data = []

    data = _prune_records(data, WINDOW_KEEP_LOGS)

    if unique_keys:
        key_val = tuple(str(record.get(k, "")).strip().lower() for k in unique_keys)
        for r in data:
            other = tuple(str(r.get(k, "")).strip().lower() for k in unique_keys)
            if other == key_val:
                r.update(record)
                try:
                    file.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
                except Exception:
                    pass
                return

    data.append(record)
    try:
        file.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


def _save_unmatched(
    user_id: int,
    text: str,
    account_name: str,
    origin: str,
    parsed_attempts: list[dict] | None = None,
    kind: str = "product",
    reason: str = "",
):
    record = {
        "user_id": user_id,
        "text": text.strip(),
        "type": kind,
        "reason": reason,
        "parsed": parsed_attempts or [],
        "account": account_name,
        "origin": origin,
        "date": datetime.now(timezone.utc).isoformat(),
    }
    _append_json(UNMATCHED_FILE, record)


def _save_matched(
    user_id: int,
    text: str,
    account_name: str,
    origin: str,
    reply: str,
    parsed_used: dict,
    matched_entry: dict,
    kind: str = "product",
):
    record = {
        "user_id": user_id,
        "text": text.strip(),
        "type": kind,
        "reply": reply,
        "parsed": parsed_used,
        "matched_entry": matched_entry,
        "account": account_name,
        "origin": origin,
        "date": datetime.now(timezone.utc).isoformat(),
    }
    _append_json(MATCHED_FILE, record, unique_keys=("user_id", "text"))


# ====================== Вспомогательные: сбор whitelist ======================
def _to_int(val) -> int | None:
    try:
        return int(val)
    except Exception:
        return None


def _collect_allowed_chat_ids(db: dict, acc_name: str) -> set[int]:
    allowed: set[int] = set()
    for s in (db.get("sources") or []):
        t = (s.get("type") or "").strip().lower()
        if t != "chat":
            continue
        if s.get("account") and s["account"] != acc_name:
            continue
        cid = _to_int(s.get("channel_id"))
        if cid:
            allowed.add(cid)

    for s in (db.get("chats") or []):
        if s.get("account") and s["account"] != acc_name:
            continue
        cid = _to_int(s.get("channel_id"))
        if cid:
            allowed.add(cid)

    for s in (db.get("channels") or []):
        if s.get("account") and s["account"] != acc_name:
            continue
        cid = _to_int(s.get("channel_id"))
        if cid:
            allowed.add(cid)

    return allowed


# ====================== Ограничение по категориям/моделям ======================
def _load_allowed_paths_spec(db: dict) -> list[list[str]]:
    raw = db.get("auto_replies_allowed_paths") or []
    if not isinstance(raw, list):
        return []
    out: list[list[str]] = []
    for item in raw:
        s = str(item).strip()
        if not s:
            continue
        parts = [p for p in s.split("|") if p]
        if parts:
            out.append(parts)
    return out


def _path_matches_allowed(path: list[str], allowed_spec: list[list[str]]) -> bool:
    if not allowed_spec:
        return True
    if not path:
        return False
    for ap in allowed_spec:
        n = min(len(path), len(ap))
        if path[:n] == ap[:n]:
            return True
    return False


def _filter_parsed_by_allowed(parsed_list: list[dict], allowed_spec: list[list[str]]) -> list[dict]:
    if not allowed_spec:
        return parsed_list
    out: list[dict] = []
    for p in parsed_list:
        p_path = p.get("path") or []
        if not isinstance(p_path, list):
            continue
        path_norm = [str(x) for x in p_path]
        if _path_matches_allowed(path_norm, allowed_spec):
            out.append(p)
    return out


_ALLOWED_PATHS_CACHE: dict = {"ts": datetime.min, "spec": []}


def _allowed_paths_live(ttl_sec: int = ALLOWED_PATHS_TTL_SEC) -> list[list[str]]:
    now = datetime.utcnow()
    try:
        if (now - _ALLOWED_PATHS_CACHE["ts"]).total_seconds() < ttl_sec:
            return _ALLOWED_PATHS_CACHE["spec"]
    except Exception:
        pass

    try:
        db = load_data()
        spec = _load_allowed_paths_spec(db)
    except Exception:
        spec = []

    _ALLOWED_PATHS_CACHE["spec"] = spec
    _ALLOWED_PATHS_CACHE["ts"] = now
    return spec


# ====================== Жёсткие пост-гейты соответствия ======================
def _eq_norm(a: str | None, b: str | None) -> bool:
    return (a or "").strip().lower() == (b or "").strip().lower()


def _hard_attribute_guards(parsed: dict, offer: dict) -> tuple[bool, str]:
    try:
        price = int(offer.get("price", 0))
    except Exception:
        price = 0
    if price <= 0:
        return False, "no_price"

    p_color = (parsed.get("color") or "").strip()
    if p_color and not _eq_norm(p_color, offer.get("color")):
        return False, "color_mismatch"

    p_storage = (parsed.get("storage") or "").strip()
    if p_storage and not _eq_norm(p_storage, offer.get("storage")):
        return False, "storage_mismatch"

    p_sim = _norm_sim(parsed.get("sim"))
    o_sim = _norm_sim(offer.get("sim"))
    if p_sim and p_sim != o_sim:
        return False, "sim_mismatch"

    p_region = (parsed.get("region") or "").strip().lower()
    o_region = (offer.get("region") or "").strip().lower()
    if p_region and p_region != o_region:
        return False, "region_mismatch"

    return True, ""


# ====================== Live флаги + whitelist ======================
_ENABLED_CACHE = {"val": False, "ts": datetime.min}
_ALLOWED_CACHE: dict[str, dict] = {}


def _enabled_live(ttl_sec: int = 2) -> bool:
    now = datetime.utcnow()
    try:
        if (now - _ENABLED_CACHE["ts"]).total_seconds() < ttl_sec:
            return bool(_ENABLED_CACHE["val"])
    except Exception:
        pass

    try:
        _ENABLED_CACHE["val"] = bool(load_data().get("auto_replies_enabled", False))
    except Exception:
        _ENABLED_CACHE["val"] = False
    _ENABLED_CACHE["ts"] = now
    return bool(_ENABLED_CACHE["val"])


def _allowed_ids_live(acc_name: str, ttl_sec: int = 15) -> set[int]:
    now = datetime.utcnow()
    bucket = _ALLOWED_CACHE.get(acc_name)
    if bucket:
        try:
            if (now - bucket["ts"]).total_seconds() < ttl_sec:
                return bucket["ids"]
        except Exception:
            pass

    try:
        db = load_data()
        ids = _collect_allowed_chat_ids(db, acc_name)
    except Exception:
        ids = set()

    _ALLOWED_CACHE[acc_name] = {"ids": ids, "ts": now}
    return ids


_ME_CACHE: dict[str, int] = {}


async def _get_my_id(client, acc_name: str) -> int | None:
    if acc_name in _ME_CACHE:
        return _ME_CACHE[acc_name]
    try:
        me = await client.get_me()
        if me and getattr(me, "id", None):
            _ME_CACHE[acc_name] = int(me.id)
            return _ME_CACHE[acc_name]
    except Exception:
        return None
    return None


# ====================== Формирование ответа ======================
def _fmt_price(p: int) -> str:
    try:
        return f"{int(p):,}".replace(",", " ")
    except Exception:
        return str(p)


def _apply_markup(price: int) -> int:
    if not AR_MARKUP_ENABLED:
        return int(price or 0)
    p = int(price or 0)
    if p <= 0:
        return p
    if p < AR_MARKUP_T1:
        return p + AR_MARKUP_A0
    if p < AR_MARKUP_T2:
        return p + AR_MARKUP_A1
    if p < AR_MARKUP_T3:
        return p + AR_MARKUP_A2
    return p + AR_MARKUP_A3


def _compose_reply(parsed_used: dict, offer: dict) -> str:
    model = (parsed_used.get("model") or offer.get("model") or "").strip()
    storage = (parsed_used.get("storage") or offer.get("storage") or "").strip()
    color = (parsed_used.get("color") or offer.get("color") or "").strip()
    region = (parsed_used.get("region") or offer.get("region") or "").strip()

    base_price = int(offer.get("price") or 0)
    price = _apply_markup(base_price)  # ✅ наценка только в ответе

    cfg = _format_config(offer.get("sim"), region or offer.get("region"))

    left = model
    if storage:
        left = f"{left} {storage}"
    if color:
        left = f"{left} {color}"
    if region:
        left = f"{left} {region.upper()}"

    return f"{left} - {_fmt_price(price)} ₽ {cfg}".strip()


# ====================== Матчинг ======================
def _match_best_offer(parsed_item: dict, offers: list[dict]) -> tuple[Optional[dict], str]:
    best = None
    best_price = None
    reasons: dict[str, int] = {}

    for e in offers:
        ok, reason = match_product(e, parsed_item)
        if not ok:
            reasons[reason or "no_match"] = reasons.get(reason or "no_match", 0) + 1
            continue

        ok2, r2 = _hard_attribute_guards(parsed_item, e)
        if not ok2:
            reasons[r2] = reasons.get(r2, 0) + 1
            continue

        try:
            price = int(e.get("price", 0))
        except Exception:
            price = 0
        if price <= 0:
            continue

        if best is None or (best_price is None) or price < best_price:
            best = e
            best_price = price

    if best:
        return best, ""

    if reasons:
        top = sorted(reasons.items(), key=lambda kv: kv[1], reverse=True)[0][0]
        return None, top
    return None, "no_match"


# ====================== DM send helper ======================
_DM_PRIVACY_ERRORS = (
    UserPrivacyRestrictedError,
    ChatWriteForbiddenError,
    PeerIdInvalidError,
    InputUserDeactivatedError,
)


async def _try_send_dm(client, user_id: int, text: str) -> tuple[bool, str]:
    """
    Возвращает (ok, reason).
    Если личка закрыта/нельзя писать — ok=False и reason='dm_closed'.
    """
    if not user_id:
        return False, "no_user_id"
    try:
        await client.send_message(user_id, text)
        return True, ""
    except _DM_PRIVACY_ERRORS:
        return False, "dm_closed"
    except RPCError as e:
        return False, f"dm_error:{type(e).__name__}"
    except Exception as e:
        return False, f"dm_error:{type(e).__name__}"


# ====================== ВАЖНО: корректная идентификация DM-получателя ======================
def _get_sender_user_id(sender, event) -> int:
    """
    ✅ Фикс: в группах/каналах sender может быть Channel/Chat/anon admin.
    Автоответчик шлёт ТОЛЬКО в личку пользователю => если sender не User, игнорируем.
    """
    try:
        if isinstance(sender, User) and getattr(sender, "id", None):
            return int(sender.id)
    except Exception:
        pass
    # fallback: берем sender_id из события, но только если это user-id (positive)
    try:
        sid = int(getattr(event, "sender_id", 0) or 0)
        return sid if sid > 0 else 0
    except Exception:
        return 0


# ====================== Регистрация ======================
def register_auto_replies(client, acc_name: str):
    global _PRUNE_TASK_STARTED

    is_primary = (acc_name == PRIMARY_REPLY_ACCOUNT)

    if is_primary and not _PRUNE_TASK_STARTED:
        try:
            asyncio.create_task(_daily_prune_job(acc_name))
            _PRUNE_TASK_STARTED = True
        except Exception:
            pass

    allowed_chat_ids_boot = _sources_allowed_ids_live(acc_name)
    _log(
        f"[{acc_name}] ✅ Разрешено чатов (sources.json): {len(allowed_chat_ids_boot)}; "
        f"{'PRIMARY' if is_primary else 'SECONDARY'} listener активен "
        f"(PRIMARY_REPLY_ACCOUNT={PRIMARY_REPLY_ACCOUNT})"
    )

    @client.on(events.NewMessage)
    async def handler(event):
        try:
            enabled = _enabled_live()

            if event.out:
                return

            msg_obj = getattr(event, "message", None)
            if not msg_obj:
                return

            orig_text = getattr(msg_obj, "message", "").strip()
            if not orig_text:
                return

            origin = _format_origin(event)

            is_private = bool(getattr(event, "is_private", False))
            is_group = bool(getattr(event, "is_group", False))
            peer_id = int(getattr(event, "chat_id", 0) or 0)
            peer_id_norm = _normalize_chat_id_for_match(peer_id)

            # ✅ читаем только разрешенные чаты из sources.json, остальное игнорим без логов
            if not is_private:
                allowed_source_ids = _sources_allowed_ids_live(acc_name)
                if allowed_source_ids and peer_id_norm not in allowed_source_ids:
                    return

            if is_private:
                return

            sender = await event.get_sender()

            # ✅ FIX: если sender НЕ User (канал/анон/чат) — нечего DM'ить, игнорим тихо
            sender_id = _get_sender_user_id(sender, event)
            if not sender_id:
                if not is_private and peer_id_norm in _sources_allowed_ids_live(acc_name):
                    _log(f"[{acc_name}] ⏭ Skip: cannot resolve sender_id in {origin} (sender_type={type(sender).__name__})")
                _log(f"[{acc_name}] ⏭ Skip: cannot resolve sender_id in {origin}")
                return

            # --- диагностика "человек, но метаданные как у бота"
            my_id = await _get_my_id(client, acc_name)
            sender_is_bot = bool(getattr(sender, "bot", False))
            via_bot_id = getattr(msg_obj, "via_bot_id", None)
            fwd_from = getattr(msg_obj, "fwd_from", None)
            from_id = getattr(msg_obj, "from_id", None)

            # HARD режим: игнорим всех ботов, кроме себя
            if IGNORE_BOT_SENDERS_HARD and sender_is_bot and (my_id is None or sender_id != my_id):
                _log(f"[{acc_name}] 🤖 Игнор(HARD): sender.bot=True bot_id={sender_id} my_id={my_id}")
                return

            if sender_is_bot and (my_id is None or sender_id != my_id):
                uname = getattr(sender, "username", None)
                fn = getattr(sender, "first_name", None)
                ln = getattr(sender, "last_name", None)
                _log(
                    f"[{acc_name}] 🤖 sender.bot=True (soft) "
                    f"sender_id={sender_id} my_id={my_id} via_bot_id={via_bot_id} from_id={from_id} "
                    f"fwd={'1' if fwd_from else '0'} username=@{uname or '-'} name={(fn or '')} {(ln or '')}".strip()
                )

            # ✅ primary-only
            if not is_primary:
                return

            # ✅ whitelist применяем только к группам; лички не режем
            # (оставлено для совместимости, но источники уже отфильтрованы через sources.json)
            if is_group:
                allowed_chat_ids = _sources_allowed_ids_live(acc_name)
                if allowed_chat_ids and peer_id_norm not in allowed_chat_ids:
                    return

            # ---- классификация
            kind = classify_message(orig_text)
            if kind == "silent":
                _log(f"[{acc_name}] 🤐 Silent (reserve/bron) user={sender_id} origin={origin}")
                return
            if kind == "spam":
                save_spam(sender_id, orig_text, acc_name, origin, reason="classify:spam")
                _log(f"[{acc_name}] 🧹 Spam user={sender_id} origin={origin}")
                return

            # если sender.bot=True (не мы) — отвечаем только на product (+ buy-intent если включен)
            if sender_is_bot and (my_id is None or sender_id != my_id):
                if not ALLOW_BOT_SENDER_FOR_PRODUCTS:
                    return
                if kind != "product":
                    return
                if REQUIRE_BUY_INTENT and not BUY_INTENT_RE.search(orig_text.lower()) and kind != "product":
                    return

            if REQUIRE_BUY_INTENT and not BUY_INTENT_RE.search(orig_text.lower()) and kind != "product":
                return

            # ---- антидубликаты по входящему тексту
            if _blocked_globally(orig_text):
                return
            if _was_replied_user(sender_id, orig_text):
                return

            # ---- загрузка эталонов (из parsed_data.json)
            offers = _etalons_live()
            if not offers:
                _log(f"[{acc_name}] ⚠️ Нет эталонов (parsed_data.json пуст/не найден) — ответа не будет")
                _save_unmatched(sender_id, orig_text, acc_name, origin, parsed_attempts=[], kind="product", reason="no_etalons")
                return

            # ---- парсинг входящего текста (поддержка нескольких товаров в одном сообщении)
            parsed_attempts: list[dict] = []
            for ln in _split_candidate_lines(orig_text):
                parsed_attempts.extend(
                    _rbpg(
                        ln,
                        channel="__incoming__",
                        message_id=None,
                        date=datetime.now(timezone.utc).isoformat(),
                        path=None,
                    )
                    or []
                )

            parsed_attempts = [_canonize_parsed_watch(p) for p in parsed_attempts if isinstance(p, dict)]

            # ---- ограничения по path (если заданы)
            allowed_spec = _allowed_paths_live()
            parsed_filtered = _filter_parsed_by_allowed(parsed_attempts, allowed_spec)

            if not parsed_filtered:
                _save_unmatched(
                    sender_id,
                    orig_text,
                    acc_name,
                    origin,
                    parsed_attempts=parsed_attempts,
                    kind="product",
                    reason="no_parsed_or_not_allowed",
                )
                _log(f"[{acc_name}] ❓ Unmatched: не распарсилось/не разрешено по path user={sender_id} origin={origin}")
                return

            # ---- подбор офферов (поддержка нескольких товаров)
            reply_lines: list[str] = []
            matched_pairs: list[dict] = []
            reasons: dict[str, int] = {}

            for p in parsed_filtered:
                offer, reason = _match_best_offer(p, offers)
                if offer:
                    reply = _compose_reply(p, offer)
                    if reply and reply not in reply_lines:
                        reply_lines.append(reply)
                        matched_pairs.append({"parsed": p, "matched": offer})
                    continue
                reasons[reason or "no_match"] = reasons.get(reason or "no_match", 0) + 1

            if not reply_lines:
                last_reason = "no_match"
                if reasons:
                    last_reason = sorted(reasons.items(), key=lambda kv: kv[1], reverse=True)[0][0]
                _save_unmatched(
                    sender_id,
                    orig_text,
                    acc_name,
                    origin,
                    parsed_attempts=parsed_filtered,
                    kind="product",
                    reason=last_reason,
                )
                _log(f"[{acc_name}] ❌ Unmatched user={sender_id} reason={last_reason} origin={origin}")
                return

            reply_text = "\n".join(reply_lines)

            # антидубликат по сформированному ответу
            if _already_sent_recently(sender_id, reply_text):
                return

            # ✅ ВСЕГДА пишем matched — даже если enabled=False (DRY-RUN)
            _save_matched(
                sender_id,
                orig_text,
                acc_name,
                origin,
                reply_text,
                parsed_used=matched_pairs,
                matched_entry=matched_pairs,
                kind="product",
            )

            if not enabled:
                _log(f"[{acc_name}] 🧪 DRY-RUN (disabled): would DM user={sender_id} origin={origin}: {reply_text}")
                return

            # ======================
            # ✅ ОТПРАВКА ТОЛЬКО В ЛИЧКУ (DM)
            # в чат/группу не пишем никогда
            # ======================
            ok_dm, dm_reason = await _try_send_dm(client, sender_id, reply_text)
            if ok_dm:
                _log(f"[{acc_name}] ✅ Sent DM to user={sender_id} origin={origin}: {reply_text}")
                return

            if dm_reason == "dm_closed":
                _log(f"[{acc_name}] 🔒 DM closed for user={sender_id} — skip origin={origin}")
                return

            _log(f"[{acc_name}] ❌ DM send failed user={sender_id} reason={dm_reason} origin={origin}")
            _save_unmatched(
                sender_id,
                orig_text,
                acc_name,
                origin,
                parsed_attempts=parsed_filtered,
                kind="product",
                reason=dm_reason,
            )
            return

        except Exception as e:
            _log(f"[{acc_name}] ❌ Ошибка обработчика: {e}")
