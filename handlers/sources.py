import re
import json
from pathlib import Path
from aiogram import Router, F
from handlers.auth_utils import auth_get
from aiogram.types import CallbackQuery, Message, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from telethon_manager import get_all_clients, get_clients_for_user, get_paid_client

router = Router()

# === Путь к отдельному файлу sources.json ===
SOURCES_FILE = Path("sources.json")

# === Создание и загрузка/сохранение ===
def ensure_sources_file():
    """Создаёт файл sources.json, если его нет"""
    if not SOURCES_FILE.exists():
        data = {"accounts": [], "channels": [], "chats": [], "bots": []}
        SOURCES_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def load_sources() -> dict:
    ensure_sources_file()
    try:
        return json.loads(SOURCES_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {"accounts": [], "channels": [], "chats": [], "bots": []}


def save_sources(data: dict):
    SOURCES_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


# === FSM ===
class SourceStates(StatesGroup):
    waiting_for_channel = State()
    waiting_for_chat = State()
    waiting_for_bot = State()
    selecting_sources = State()

    # 👇 новые состояния для сценариев ботов
    waiting_bot_action_type = State()
    waiting_bot_action_value = State()


# === Хелперы ===
def _norm(s: str) -> str:
    if not s:
        return ""
    s = s.replace("@", "")
    return s.casefold().strip()


async def _get_user_ctx(user_id: int) -> dict:
    u = await auth_get(user_id)
    role = (u or {}).get("role")
    sources_mode = (u or {}).get("sources_mode", "default")
    return {"user": u, "role": role, "sources_mode": sources_mode}


def _filter_sources_by_user(db: dict, user_id: int | None, is_admin: bool) -> dict:
    def _ok(item: dict) -> bool:
        uid = item.get("user_id")
        if is_admin:
            return uid is None
        return uid == user_id

    out = {
        "channels": [s for s in db.get("channels", []) if isinstance(s, dict) and _ok(s)],
        "chats": [s for s in db.get("chats", []) if isinstance(s, dict) and _ok(s)],
        "bots": [s for s in db.get("bots", []) if isinstance(s, dict) and _ok(s)],
        "accounts": db.get("accounts", []) or [],
    }
    return out


def _is_broadcast_channel(dialog) -> bool:
    e = getattr(dialog, "entity", None)
    return bool(getattr(e, "broadcast", False))


def _is_chat(dialog) -> bool:
    if _is_broadcast_channel(dialog):
        return False
    if getattr(dialog, "is_group", False) or getattr(dialog, "is_user", False):
        return True
    e = getattr(dialog, "entity", None)
    if hasattr(e, "megagroup") and getattr(e, "megagroup", False):
        return True
    return False


def _is_bot(dialog) -> bool:
    user = getattr(dialog, "entity", None)
    return bool(getattr(user, "bot", False))


# === Главное меню ===
@router.callback_query(F.data == "sources")
async def show_sources_menu(callback: CallbackQuery, state: FSMContext):
    u = await auth_get(callback.from_user.id)
    if not u or not (u.get("role") == "admin" or (u.get("access") or {}).get("settings.sources")):
        await callback.answer("⛔️ Нет доступа", show_alert=True)
        return
    if u.get("role") != "admin" and u.get("sources_mode") not in ("own", "custom"):
        await callback.answer("⛔️ Источники недоступны в режиме 'По умолчанию'", show_alert=True)
        return
    await state.clear()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить канал", callback_data="add_channel")],
        [InlineKeyboardButton(text="💬 Добавить чат", callback_data="add_chat")],
        [InlineKeyboardButton(text="🤖 Добавить бота", callback_data="add_bot")],
        [InlineKeyboardButton(text="📋 Посмотреть источники", callback_data="list_sources")],
        # 👇 новая кнопка управления ботами
        [InlineKeyboardButton(text="🧩 Управление ботами", callback_data="manage_bots")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")]
    ])
    await callback.message.answer("📡 Управление источниками:", reply_markup=kb)


# === Универсальный поиск ===
async def _search_dialogs(query: str, src_type: str, *, user_id: int | None = None):
    if user_id is None:
        clients = get_all_clients()
    else:
        clients = await get_clients_for_user(user_id, include_default=False)
    db = load_sources()
    if user_id is not None:
        db = _filter_sources_by_user(db, user_id, False)

    # Сырые имена аккаунтов из sources.json
    sources_accounts = [a.get("name", "") for a in db.get("accounts", [])]
    # Нормализованные имена (убираем @, приводим к lower и т.п.)
    sources_accounts_norm = {_norm(name) for name in sources_accounts if name}
    if user_id is not None:
        sources_accounts_norm = set()

    print("========== SOURCES SEARCH ==========")
    print("🔗 Все клиенты (get_all_clients):", list(clients.keys()))
    print("📒 Аккаунты в sources.json (raw):", sources_accounts)
    print("📒 Аккаунты в sources.json (norm):", sources_accounts_norm)
    print("Тип поиска:", src_type)
    print("Запрос:", query)
    print("====================================")

    if not clients:
        print("⚠️ Нет активных подключений Telethon. Запусти init_clients().")
        return []

    if src_type == "channel":
        existing_ids = {s["channel_id"] for s in db.get("channels", [])}
    elif src_type == "chat":
        existing_ids = {s["channel_id"] for s in db.get("chats", [])}
    else:
        existing_ids = {s["channel_id"] for s in db.get("bots", [])}

    found = []

    seen_clients = set()
    for acc_name, client in clients.items():
        norm_acc = _norm(acc_name)

        # Фильтр по аккаунтам, но уже по нормализованным именам
        if sources_accounts_norm and norm_acc not in sources_accounts_norm:
            print(f"⛔ Пропущен аккаунт {acc_name} (norm='{norm_acc}') — нет в sources_accounts_norm")
            continue
        if user_id is not None:
            cid = id(client)
            if cid in seen_clients:
                continue
            seen_clients.add(cid)

        print(f"🔎 Ищу в аккаунте: {acc_name} (norm='{norm_acc}')")

        try:
            # Без limit — обходим все диалоги аккаунта
            async for d in client.iter_dialogs():
                if src_type == "channel" and not _is_broadcast_channel(d):
                    continue
                if src_type == "chat" and not _is_chat(d):
                    continue
                if src_type == "bot" and not _is_bot(d):
                    continue

                if d.entity.id in existing_ids:
                    continue

                title = _norm(d.name or "")
                username = _norm(getattr(d.entity, "username", "") or "")

                if query and (query in title or (username and query in username)):
                    print(f"   ✔ Найдено совпадение: {d.name} (id={d.entity.id})")
                    found.append((acc_name, d))
        except Exception as e:
            print(f"❌ Ошибка при поиске в {acc_name}: {e}")

    print(f"✔ Всего найдено диалогов: {len(found)}")
    print("====================================")

    return found


# === Построение клавиатуры множественного выбора ===
def _build_selection_keyboard(found, src_type: str, selected: set[str]):
    rows = []
    for acc, d in found:
        eid = int(d.entity.id)
        sel_key = f"{acc}:{eid}"
        icon = {"channel": "📺", "chat": "💬", "bot": "🤖"}[src_type]
        mark = "✅" if sel_key in selected else "☑️"
        uname = getattr(d.entity, "username", "") or ""
        title = d.name or ("@" + uname if uname else "без имени")
        suffix = f" (@{uname})" if uname else ""
        rows.append([
            InlineKeyboardButton(
                text=f"{mark} {icon} {acc} — {title[:45]}{suffix[:15]}",
                callback_data=f"toggle_select:{src_type}:{acc}:{eid}"
            )
        ])
    rows.append([InlineKeyboardButton(text="💾 Сохранить выбранные", callback_data=f"save_selected:{src_type}")])
    rows.append([InlineKeyboardButton(text="🔁 Новый поиск", callback_data=f"add_{src_type}"),
                 InlineKeyboardButton(text="⬅️ Назад", callback_data="sources")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# === Обработка выбора и отображения ===
async def _handle_search_results(msg: Message, state: FSMContext, src_type: str, query: str, user_id: int | None = None):
    search_user_id = user_id
    if user_id is not None:
        ctx = await _get_user_ctx(user_id)
        is_admin = ctx.get("role") == "admin"
        if is_admin:
            search_user_id = None
        else:
            client = await get_paid_client(user_id)
            if not client:
                await msg.answer("⚠️ Сессия не авторизована. Пройди регистрацию и попробуй снова.")
                await state.clear()
                return
    found = await _search_dialogs(query, src_type, user_id=search_user_id)
    if not found:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔁 Повторить поиск", callback_data=f"add_{src_type}")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="sources")]
        ])
        await msg.answer(f"⚠️ {src_type.capitalize()} не найден (или уже привязан).", reply_markup=kb)
        await state.clear()
        return

    await state.update_data(
        query=query,
        type=src_type,
        found=[
            (
                acc,
                int(d.entity.id),
                d.name or ("@" + (getattr(d.entity, "username", "") or "без имени")),
                getattr(d.entity, "username", "") or "",
            )
            for acc, d in found
        ],
        selected=[]
    )

    kb = _build_selection_keyboard(found, src_type, set())
    await msg.answer(f"✅ Найдено {len(found)} {src_type}(ов). Отметь нужные и нажми «💾 Сохранить выбранные».", reply_markup=kb)
    await state.set_state(SourceStates.selecting_sources)


# === Переключение выбора ===
@router.callback_query(F.data.startswith("toggle_select:"))
async def toggle_select(callback: CallbackQuery, state: FSMContext):
    _, src_type, acc, eid = callback.data.split(":")
    eid = int(eid)
    data = await state.get_data()
    found = data.get("found", [])
    selected = set(data.get("selected", []))
    sel_key = f"{acc}:{eid}"

    if sel_key in selected:
        selected.remove(sel_key)
    else:
        selected.add(sel_key)
    data["selected"] = list(selected)
    await state.update_data(**data)

    # Восстанавливаем объекты найденных
    from types import SimpleNamespace
    found_objs = [
        (
            acc_,
            SimpleNamespace(
                entity=SimpleNamespace(id=fid, username=uname),
                name=name,
            ),
        )
        for acc_, fid, name, uname in found
    ]
    kb = _build_selection_keyboard(found_objs, src_type, selected)
    await callback.message.edit_reply_markup(reply_markup=kb)


# === Сохранение выбранных ===
@router.callback_query(F.data.startswith("save_selected:"))
async def save_selected(callback: CallbackQuery, state: FSMContext):
    _, src_type = callback.data.split(":")
    data = await state.get_data()
    found = data.get("found", [])
    selected = set(data.get("selected", []))
    ctx = await _get_user_ctx(callback.from_user.id)
    is_admin = ctx.get("role") == "admin"

    if not selected:
        await callback.answer("⚠️ Ничего не выбрано", show_alert=True)
        return

    db = load_sources()
    count = 0
    for acc, eid, name, uname in found:
        sel_key = f"{acc}:{eid}"
        if sel_key in selected:
            entry = {"name": name, "channel_id": int(eid), "account": acc}
            if not is_admin:
                entry["user_id"] = callback.from_user.id
            if uname:
                entry["display_name"] = f"@{uname}"
            # для ботов тоже пишем в "bots"
            db[src_type + "s"].append(entry)
            count += 1

    save_sources(db)
    await state.clear()

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Посмотреть источники", callback_data="list_sources")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="sources")]
    ])
    await callback.message.edit_text(f"✅ Добавлено {count} {src_type}(ов).", reply_markup=kb)


# === Добавление каналов / чатов / ботов ===
@router.callback_query(F.data == "add_channel")
async def add_channel(callback: CallbackQuery, state: FSMContext):
    await state.set_state(SourceStates.waiting_for_channel)
    await callback.message.answer("🔍 Введи наименование или @username канала:")


@router.message(SourceStates.waiting_for_channel)
async def process_channel_name(msg: Message, state: FSMContext):
    query = _norm(msg.text.strip())
    await _handle_search_results(msg, state, "channel", query, user_id=msg.from_user.id)


@router.callback_query(F.data == "add_chat")
async def add_chat(callback: CallbackQuery, state: FSMContext):
    await state.set_state(SourceStates.waiting_for_chat)
    await callback.message.answer("💬 Введи наименование или @username чата:")


@router.message(SourceStates.waiting_for_chat)
async def process_chat_name(msg: Message, state: FSMContext):
    query = _norm(msg.text.strip())
    await _handle_search_results(msg, state, "chat", query, user_id=msg.from_user.id)


@router.callback_query(F.data == "add_bot")
async def add_bot(callback: CallbackQuery, state: FSMContext):
    await state.set_state(SourceStates.waiting_for_bot)
    await callback.message.answer("🤖 Введи @username бота для добавления:")


@router.message(SourceStates.waiting_for_bot)
async def process_bot_name(msg: Message, state: FSMContext):
    query = _norm(msg.text.strip())
    await _handle_search_results(msg, state, "bot", query, user_id=msg.from_user.id)


# === Просмотр всех источников ===
@router.callback_query(F.data == "list_sources")
async def list_sources(callback: CallbackQuery):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📺 Каналы", callback_data="src:list:channels")],
        [InlineKeyboardButton(text="💬 Чаты", callback_data="src:list:chats")],
        [InlineKeyboardButton(text="🤖 Боты", callback_data="src:list:bots")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="sources")],
    ])
    await callback.message.answer("📡 Источники — выбери раздел:", reply_markup=kb)


def _src_title(s: dict) -> str:
    disp = s.get("display_name") or ""
    disp_txt = f" {disp}" if disp else ""
    return f"{s.get('name') or '—'}{disp_txt}"


def _src_id(s: dict) -> int:
    for k in ("channel_id", "chat_id", "id"):
        try:
            v = s.get(k)
            if v is not None:
                return int(v)
        except Exception:
            continue
    return 0


def _delete_source(db: dict, src_type: str, src_id: int, user_id: int | None, is_admin: bool) -> int:
    items = db.get(src_type, []) or []
    kept = []
    removed = 0
    for s in items:
        if not isinstance(s, dict):
            continue
        sid = _src_id(s)
        if sid != int(src_id):
            kept.append(s)
            continue
        uid = s.get("user_id")
        if is_admin:
            if uid is None:
                removed += 1
            else:
                kept.append(s)
        else:
            if uid == user_id:
                removed += 1
            else:
                kept.append(s)
    db[src_type] = kept
    return removed


@router.callback_query(F.data.startswith("src:list:"))
async def list_sources_by_type(callback: CallbackQuery):
    _, _, src_type = callback.data.split(":")
    await _render_sources_type(callback, src_type)


async def _render_sources_type(callback: CallbackQuery, src_type: str) -> None:
    db = load_sources()
    ctx = await _get_user_ctx(callback.from_user.id)
    is_admin = ctx.get("role") == "admin"
    db = _filter_sources_by_user(db, callback.from_user.id, is_admin)
    items = db.get(src_type, []) or []
    rows = []
    if items:
        for s in items:
            sid = _src_id(s)
            title = _src_title(s)
            rows.append([InlineKeyboardButton(text=title[:60], callback_data=f"src:item:{src_type}:{sid}")])
    else:
        rows.append([InlineKeyboardButton(text="(пусто)", callback_data="src:noop")])
    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="list_sources")])
    await callback.message.answer(f"📡 {src_type.capitalize()}:", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))


@router.callback_query(F.data.startswith("src:item:"))
async def source_item_menu(callback: CallbackQuery):
    _, _, src_type, sid = callback.data.split(":")
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗑 Удалить", callback_data=f"src:del:{src_type}:{sid}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=f"src:list:{src_type}")],
    ])
    await callback.message.answer("Действия с источником:", reply_markup=kb)


@router.callback_query(F.data.startswith("src:del:"))
async def source_item_delete(callback: CallbackQuery):
    _, _, src_type, sid = callback.data.split(":")
    db = load_sources()
    ctx = await _get_user_ctx(callback.from_user.id)
    is_admin = ctx.get("role") == "admin"
    removed = _delete_source(db, src_type, int(sid), callback.from_user.id, is_admin)
    save_sources(db)
    if removed:
        await callback.answer("Удалено", show_alert=True)
    else:
        await callback.answer("Не найдено", show_alert=True)
    await _render_sources_type(callback, src_type)


@router.callback_query(F.data == "src:noop")
async def _src_noop(callback: CallbackQuery):
    await callback.answer()


# ========= УПРАВЛЕНИЕ БОТАМИ / СЦЕНАРИЯМИ =========

async def _show_bot_scenario(message: Message, bot_id: int):
    """Рендер сценария одного бота по его channel_id."""
    db = load_sources()
    ctx = await _get_user_ctx(message.from_user.id)
    is_admin = ctx.get("role") == "admin"
    db = _filter_sources_by_user(db, message.from_user.id, is_admin)
    bots = db.get("bots", []) or []
    bot = next((b for b in bots if int(b.get("channel_id")) == int(bot_id)), None)
    if not bot:
        await message.answer("⚠️ Бот не найден.")
        return

    scenario = bot.get("scenario") or []

    # Человеческие подписи типов
    type_labels = {
        "command": "команда",
        "inline": "inline-кнопка",
        "reply": "reply-кнопка",
    }

    text_lines = [
        f"🤖 <b>Сценарий для бота:</b> {bot.get('name')}",
        f"ID: <code>{bot.get('channel_id')}</code>",
        "────────"
    ]

    if not scenario:
        text_lines.append("Пока сценарий пуст.")
    else:
        for i, step in enumerate(scenario, 1):
            kind = step.get("kind") or step.get("type")  # на будущее
            value = step.get("value", "")
            label = type_labels.get(kind, kind or "?")
            text_lines.append(f"{i}. [{label}] {value}")

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить действие", callback_data=f"bot_add_action:{bot_id}")],
        [InlineKeyboardButton(text="⬅️ Назад к списку ботов", callback_data="manage_bots")],
        [InlineKeyboardButton(text="⬅️ В меню источников", callback_data="sources")]
    ])

    await message.answer("\n".join(text_lines), reply_markup=kb)


@router.callback_query(F.data == "manage_bots")
async def manage_bots(callback: CallbackQuery, state: FSMContext):
    """Показ списка ботов для управления сценариями."""
    await state.clear()
    db = load_sources()
    ctx = await _get_user_ctx(callback.from_user.id)
    is_admin = ctx.get("role") == "admin"
    db = _filter_sources_by_user(db, callback.from_user.id, is_admin)
    bots = db.get("bots", []) or []

    if not bots:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="➕ Добавить бота", callback_data="add_bot")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="sources")]
        ])
        await callback.message.answer("⚠️ Ботов пока нет. Сначала добавь бота в источники.", reply_markup=kb)
        return

    rows = []
    for b in bots:
        name = b.get("name") or "без имени"
        bid = int(b.get("channel_id"))
        rows.append([
            InlineKeyboardButton(
                text=f"🤖 {name}",
                callback_data=f"manage_bot:{bid}"
            )
        ])

    rows.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="sources")])
    kb = InlineKeyboardMarkup(inline_keyboard=rows)
    await callback.message.answer("🤖 Выбери бота для управления сценарием:", reply_markup=kb)


@router.callback_query(F.data.startswith("manage_bot:"))
async def manage_bot(callback: CallbackQuery):
    """Выбор конкретного бота. Если сценария нет — создаём /start."""
    _, bot_id_str = callback.data.split(":", 1)
    bot_id = int(bot_id_str)

    db = load_sources()
    ctx = await _get_user_ctx(callback.from_user.id)
    is_admin = ctx.get("role") == "admin"
    bots = db.get("bots", []) or []
    changed = False

    target_bot = None
    for b in bots:
        if int(b.get("channel_id")) == bot_id:
            if is_admin and b.get("user_id") is not None:
                await callback.answer("⛔️ Нет доступа", show_alert=True)
                return
            if not is_admin and b.get("user_id") != callback.from_user.id:
                await callback.answer("⛔️ Нет доступа", show_alert=True)
                return
            target_bot = b
            scenario = b.get("scenario") or []
            if not scenario:
                # 2) если путь эмуляции действий нет, то автоматом создается команда "/start"
                b["scenario"] = [{"kind": "command", "value": "/start"}]
                changed = True
            break
    if not target_bot:
        await callback.answer("⚠️ Бот не найден", show_alert=True)
        return

    if changed:
        db["bots"] = bots
        save_sources(db)

    # показываем актуальный сценарий
    await _show_bot_scenario(callback.message, bot_id)


@router.callback_query(F.data.startswith("bot_add_action:"))
async def bot_add_action(callback: CallbackQuery, state: FSMContext):
    """Шаг 3 — выбор типа действия: inline / reply / команда."""
    _, bot_id_str = callback.data.split(":", 1)
    bot_id = int(bot_id_str)

    await state.update_data(bot_id=bot_id)
    await state.set_state(SourceStates.waiting_bot_action_type)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="💬 Команда", callback_data="bot_action_type:command"),
        ],
        [
            InlineKeyboardButton(text="🧷 Inline-кнопка", callback_data="bot_action_type:inline"),
            InlineKeyboardButton(text="📎 Reply-кнопка", callback_data="bot_action_type:reply"),
        ],
        [InlineKeyboardButton(text="⬅️ Отмена", callback_data=f"manage_bot:{bot_id}")]
    ])

    await callback.message.answer(
        "Выбери тип действия, которое нужно добавить в сценарий:",
        reply_markup=kb
    )


@router.callback_query(SourceStates.waiting_bot_action_type, F.data.startswith("bot_action_type:"))
async def bot_action_type(callback: CallbackQuery, state: FSMContext):
    """После выбора типа действия — просим ввести текст кнопки/команды."""
    _, kind = callback.data.split(":", 1)
    data = await state.get_data()
    bot_id = data.get("bot_id")

    if bot_id is None:
        await callback.message.answer("⚠️ Неизвестный бот, попробуй ещё раз.")
        await state.clear()
        return

    await state.update_data(action_kind=kind)
    await state.set_state(SourceStates.waiting_bot_action_value)

    label = {
        "command": "команду (например, /start или /price)",
        "inline": "текст inline-кнопки",
        "reply": "текст reply-кнопки",
    }.get(kind, "текст действия")

    await callback.message.answer(f"✏️ Введи {label}:")


@router.message(SourceStates.waiting_bot_action_value)
async def bot_action_value(msg: Message, state: FSMContext):
    """Шаг 4 — сохраняем действие и показываем обновлённый сценарий."""
    data = await state.get_data()
    bot_id = data.get("bot_id")
    kind = data.get("action_kind")
    value = msg.text.strip() if msg.text else ""

    if not bot_id or not kind or not value:
        await msg.answer("⚠️ Не удалось сохранить действие, попробуй ещё раз.")
        await state.clear()
        return

    db = load_sources()
    bots = db.get("bots", []) or []
    updated = False
    ctx = await _get_user_ctx(msg.from_user.id)
    is_admin = ctx.get("role") == "admin"

    for b in bots:
        if int(b.get("channel_id")) == int(bot_id):
            if is_admin and b.get("user_id") is not None:
                await msg.answer("⛔️ Нет доступа")
                await state.clear()
                return
            if not is_admin and b.get("user_id") != msg.from_user.id:
                await msg.answer("⛔️ Нет доступа")
                await state.clear()
                return
            scenario = b.get("scenario") or []
            scenario.append({"kind": kind, "value": value})
            b["scenario"] = scenario
            updated = True
            break

    if updated:
        db["bots"] = bots
        save_sources(db)
        await msg.answer("✅ Действие добавлено в сценарий.")
    else:
        await msg.answer("⚠️ Бот не найден, действие не сохранено.")

    await state.clear()
    # показываем обновлённый сценарий
    await _show_bot_scenario(msg, int(bot_id))
