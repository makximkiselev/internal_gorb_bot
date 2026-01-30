# telethon_manager.py
import os
import asyncio
import json
from pathlib import Path
from telethon import TelegramClient

# === Файлы/директории ===
SESSIONS_DIR = Path("sessions")
SESSIONS_DIR.mkdir(parents=True, exist_ok=True)

SOURCES_FILE = Path("sources.json")
PAID_AUTH_FILE = Path("data") / "auth_users.json"

# --- Тонкая настройка (через ENV) ---
# Сколько диалогов подгружать при вынужденном прогреве (когда numeric id не резолвится)
WARMUP_LIMIT = int(os.getenv("TG_WARMUP_LIMIT", "400"))  # 400 — быстро и достаточно для кэша
# Нужно ли прогревать на init вообще (мы выключаем)
WARMUP_ON_INIT = os.getenv("TG_WARMUP_ON_INIT", "0") == "1"
# Микропауза между повторными резолвами (мс → сек)
RESOLVE_RETRY_SLEEP = float(os.getenv("TG_RESOLVE_RETRY_SLEEP", "0.3"))  # 300 мс
# Максимум повторов при резолве entity
RESOLVE_MAX_RETRIES = int(os.getenv("TG_RESOLVE_MAX_RETRIES", "2"))


# === Работа с sources.json ===
def _ensure_sources_file():
    if not SOURCES_FILE.exists():
        SOURCES_FILE.write_text(
            '{\n'
            '  "channels": [],\n'
            '  "chats": [],\n'
            '  "bots": [],\n'
            '  "accounts": []\n'
            '}\n',
            encoding="utf-8"
        )


def _load_sources() -> dict:
    _ensure_sources_file()
    try:
        import json
        return json.loads(SOURCES_FILE.read_text(encoding="utf-8"))
    except Exception:
        # минимально валидная структура на случай битого файла
        return {"channels": [], "chats": [], "bots": [], "accounts": []}


# === Утилиты нормализации ===
def _norm(s: str | None) -> str:
    return (s or "").strip().lower()

def _strip_at(s: str | None) -> str:
    s = _norm(s)
    return s[1:] if s.startswith("@") else s


# === Глобальный пул клиентов (ключи в lower) ===
clients: dict[str, TelegramClient] = {}
_paid_clients: dict[int, TelegramClient] = {}

# === Кэш сущностей (ускоряет get_entity) ===
# Структура: {id(client): {cache_key: entity}}
# cache_key — строка: для int id используем str(id), для username — нормализованный.
_entity_cache: dict[int, dict[str, object]] = {}

def _client_cache(client: TelegramClient) -> dict[str, object]:
    return _entity_cache.setdefault(id(client), {})

def _entity_cache_get(client: TelegramClient, raw) -> object | None:
    cache = _client_cache(client)
    if isinstance(raw, int):
        return cache.get(str(raw))
    key = str(raw or "").strip()
    return cache.get(key)

def _entity_cache_put(client: TelegramClient, raw, entity: object) -> None:
    cache = _client_cache(client)
    if isinstance(raw, int):
        cache[str(raw)] = entity
    else:
        cache[str(raw).strip()] = entity


# === Вспомогательные утилиты Telethon ===
async def _limited_warmup_dialogs(client: TelegramClient, limit: int = WARMUP_LIMIT) -> None:
    """
    ЛЁГКИЙ прогрев кэша сущностей — подгружаем ограниченное число диалогов.
    Достаточно, чтобы get_entity(int_id) начал работать, и при этом быстро.
    """
    try:
        c = 0
        async for _ in client.iter_dialogs(limit=limit):
            c += 1
            if c >= limit:
                break
    except Exception as e:
        print(f"⚠️ Warmup dialogs (limited) error: {e}")

async def resolve_entity(client: TelegramClient, raw):
    """
    Унифицированный резолв чата/канала:
    - int/строковый int → пробуем напрямую; если не удалось — (опционально) лёгкий warmup и ещё раз
    - username / @username / t.me/ссылка → резолвится напрямую
    - всё кэшируем per-client
    - мягкие ретраи с микропаузой снижают риск FloodWait
    """
    # 0) кэш
    cached = _entity_cache_get(client, raw)
    if cached is not None:
        return cached

    # 1) нормализация raw
    is_numeric = False
    if isinstance(raw, str):
        s = raw.strip()
        if (s.startswith("-") and s[1:].isdigit()) or s.isdigit():
            try:
                raw = int(s)
                is_numeric = True
            except Exception:
                raw = s
        else:
            raw = s
    elif isinstance(raw, int):
        is_numeric = True

    # 2) гарантируем соединение и авторизацию
    if not client.is_connected():
        await client.connect()
    try:
        try:
            authorized = await client.is_user_authorized()  # type: ignore[misc]
        except TypeError:
            authorized = client.is_user_authorized()
    except Exception:
        authorized = False
    if not authorized:
        raise RuntimeError("Клиент не авторизован")

    # 3) прямая попытка + мягкий повтор без прогрева
    last_exc = None
    for attempt in range(RESOLVE_MAX_RETRIES):
        try:
            ent = await client.get_entity(raw)
            _entity_cache_put(client, raw, ent)
            return ent
        except Exception as e_first:
            last_exc = e_first
            # username/ссылка: повтор бесполезен — выходим
            if not is_numeric:
                break
            # numeric: мягкий повтор с микропаузой
            await asyncio.sleep(RESOLVE_RETRY_SLEEP)

    # 4) Числовой id: делаем ОГРАНИЧЕННЫЙ прогрев один раз, затем финальный retry
    if is_numeric:
        await _limited_warmup_dialogs(client, limit=WARMUP_LIMIT)
        try:
            ent = await client.get_entity(raw)
            _entity_cache_put(client, raw, ent)
            return ent
        except Exception as e_second:
            raise e_second

    # финально падаем
    if last_exc:
        raise last_exc
    raise RuntimeError("resolve_entity: unknown resolve failure")


async def ping_client_sendme(client: TelegramClient) -> bool:
    """
    Healthcheck: пробуем отправить тестовое сообщение в Saved Messages ('me').
    Возвращает True, если удалось.
    """
    try:
        if not client.is_connected():
            await client.connect()
        try:
            authorized = await client.is_user_authorized()
        except TypeError:
            authorized = client.is_user_authorized()
        if not authorized:
            return False
        await client.send_message("me", "✅ Telethon: healthcheck OK")
        return True
    except Exception:
        return False


async def init_clients(register_listeners: bool = True) -> dict[str, TelegramClient]:
    """
    Инициализация всех аккаунтов из sources.json → словарь {account_key_lower: TelegramClient}.
    Ключи: name.lower(), а также алиасы по username (без/с '@', тоже lower).
    Делается БЕЗ тяжёлого прогрева — он происходит лениво в resolve_entity при необходимости.
    """
    global clients, _entity_cache

    # Аккуратно закрываем старые соединения
    for c in list(clients.values()):
        try:
            await c.disconnect()
        except Exception:
            pass
    clients.clear()
    _entity_cache.clear()  # сбрасываем кэш при полной переинициализации

    sources = _load_sources()
    accounts = sources.get("accounts") or []
    if not isinstance(accounts, list):
        accounts = []

    for acc in accounts:
        try:
            name_raw = acc["name"]
            name_key = _norm(name_raw)
            api_id = int(acc["api_id"])
            api_hash = acc["api_hash"]
            session_path = Path(acc.get("session") or SESSIONS_DIR / f"{name_raw}.session")

            if not session_path.exists():
                print(f"⚠️ Сессия для {name_raw} не найдена ({session_path}), пропускаю.")
                continue

            # Клиент с мягкой защитой от флуд лимитов
            client = TelegramClient(
                session_path,
                api_id,
                api_hash,
                flood_sleep_threshold=60,   # пережидаем FloodWait до 60с автоматически
                connection_retries=3,
                request_retries=3,
            )

            # Подключение
            if not client.is_connected():
                await client.connect()

            # Авторизация
            try:
                authorized = await client.is_user_authorized()  # type: ignore[misc]
            except TypeError:
                authorized = client.is_user_authorized()

            if not authorized:
                print(f"❌ Аккаунт {name_raw} не авторизован. Нужен повторный логин.")
                try:
                    await client.disconnect()
                except Exception:
                    pass
                continue

            # ТЯЖЁЛЫЙ прогрев на init выключен по умолчанию.
            if WARMUP_ON_INIT:
                await _limited_warmup_dialogs(client, limit=WARMUP_LIMIT)

            me = await client.get_me()
            uname = getattr(me, "username", None) or ""
            uname_key = _strip_at(uname)

            print(f"✅ Аккаунт {name_raw} подключён: @{uname_key or me.id}")

            # Основной ключ — нормализованное name
            clients[name_key] = client

            # Алиасы по username
            if uname_key:
                clients.setdefault(uname_key, client)           # "apple_optom2"
                clients.setdefault(f"@{uname_key}", client)     # "@apple_optom2"

            # Дублируем «сырой» name в lower
            clients.setdefault(_norm(name_raw), client)

            # Здесь можно регать слушателей
            if register_listeners:
                # пример: client.add_event_handler(...)
                pass

        except Exception as e:
            print(f"❌ Ошибка подключения {acc.get('name','<no-name>')}: {e}")

    return clients


# === Утилиты доступа ===
def get_client(name: str) -> TelegramClient | None:
    """
    Вернуть клиента по имени/username (регистр и '@' игнорируются).
    Работают варианты: 'El_opt', 'el_opt', '@apple_optom2', 'apple_optom2'.
    """
    key = _strip_at(name)
    return clients.get(key) or clients.get(f"@{key}")

def get_all_clients() -> dict[str, TelegramClient]:
    """Вернуть все активные клиенты (ключи — lower, + алиасы)."""
    return clients


def _load_paid_account(user_id: int) -> dict | None:
    if not PAID_AUTH_FILE.exists():
        return None
    try:
        data = json.loads(PAID_AUTH_FILE.read_text(encoding="utf-8"))
    except Exception:
        return None
    u = (data.get("users") or {}).get(str(int(user_id))) or {}
    paid = u.get("paid_account") or {}
    if paid.get("status") != "ready":
        return None
    return paid


async def get_paid_client(user_id: int) -> TelegramClient | None:
    if user_id in _paid_clients:
        return _paid_clients[user_id]
    paid = _load_paid_account(user_id)
    if not paid:
        return None
    try:
        session_path = Path(paid.get("session") or SESSIONS_DIR / f"paid_{user_id}.session")
        api_id = int(paid["api_id"])
        api_hash = paid["api_hash"]
        client = TelegramClient(session_path, api_id, api_hash)
        await client.connect()
        _paid_clients[user_id] = client
        return client
    except Exception:
        return None


async def get_clients_for_user(user_id: int, include_default: bool = True) -> dict[str, TelegramClient]:
    out: dict[str, TelegramClient] = {}
    if include_default:
        out.update(get_all_clients())
    paid_client = await get_paid_client(user_id)
    if paid_client:
        key = f"paid_{user_id}"
        out[key] = paid_client
    return out

async def reload_clients(register_listeners: bool = True) -> dict[str, TelegramClient]:
    """Полностью перезагрузить клиентов из sources.json."""
    return await init_clients(register_listeners=register_listeners)
