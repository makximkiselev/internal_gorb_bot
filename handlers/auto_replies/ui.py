# handlers/auto_replies/ui.py
from aiogram import Router, F
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    Message,
)
from storage import load_data, save_data

router = Router()


# ========================= МЕНЮ НАСТРОЕК =========================


def auto_replies_menu(enabled: bool) -> InlineKeyboardMarkup:
    status = "✅ Включены" if enabled else "❌ Выключены"
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=f"Автоответы: {status}", callback_data="toggle_auto_replies")],
            [InlineKeyboardButton(text="📂 Категории автоответов", callback_data="auto_replies_categories")],
            [InlineKeyboardButton(text="🧱 Чёрный список", callback_data="auto_replies_blacklist")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")],
        ]
    )


def _format_blacklist_text(blacklist: list[str]) -> str:
    header = "📛 Чёрный список автоответов\n\n"
    if not blacklist:
        body = "Сейчас чёрный список пуст — бот может писать всем.\n"
    else:
        # показываем максимум 30, чтобы не раздувать сообщение
        shown = blacklist[:30]
        body = "Бот НЕ будет писать этим аккаунтам:\n"
        body += "\n".join(f"• {u}" for u in shown)
        if len(blacklist) > len(shown):
            body += f"\n\n…и ещё {len(blacklist) - len(shown)} аккаунтов."

    instructions = (
        "\n\n"
        "➕ Чтобы ДОБАВИТЬ аккаунты в чёрный список — ответьте на это сообщение и напишите username через пробел:\n"
        "@user1 @user2\n\n"
        "➖ Чтобы УДАЛИТЬ аккаунты из чёрного списка — ответьте на это сообщение и напишите их с минусом:\n"
        "-@user1 -@user2\n"
    )

    return header + body + instructions


async def _render_settings_message(callback: CallbackQuery, *, edit: bool = False):
    db = load_data()
    enabled = db.get("auto_replies_enabled", False)
    text = "⚙️ Настройки автоответов:"
    markup = auto_replies_menu(enabled)
    try:
        if edit:
            await callback.message.edit_text(text, reply_markup=markup)
        else:
            await callback.message.answer(text, reply_markup=markup)
    except Exception:
        # fallback: если редактировать нельзя (старое сообщение/другой тип) — отправим новое
        await callback.message.answer(text, reply_markup=markup)


@router.callback_query(F.data == "auto_replies")
async def show_auto_replies(callback: CallbackQuery):
    await callback.answer()
    await _render_settings_message(callback, edit=False)


@router.callback_query(F.data == "toggle_auto_replies")
async def toggle_auto_replies(callback: CallbackQuery):
    db = load_data()
    enabled = not db.get("auto_replies_enabled", False)
    db["auto_replies_enabled"] = enabled
    save_data(db)

    # ✅ при ВКЛЮЧЕНИИ — чистим логи/кэши автоответчика
    if enabled:
        try:
            # импорт внутрь функции, чтобы не словить циклические импорты
            from handlers.auto_replies.listener import _clear_all_logs_and_state
            _clear_all_logs_and_state()
        except Exception:
            pass

    await callback.answer("Готово")
    await _render_settings_message(callback, edit=True)



# ========================= ЧЁРНЫЙ СПИСОК =========================


@router.callback_query(F.data == "auto_replies_blacklist")
async def show_blacklist(callback: CallbackQuery):
    """
    Показываем/редактируем чёрный список автоответов.
    """
    await callback.answer()

    db = load_data()
    blacklist: list[str] = db.get("auto_replies_blacklist", [])

    text = _format_blacklist_text(blacklist)

    # отдельное сообщение, чтобы можно было на него отвечать
    await callback.message.answer(text)  # без parse_mode, чтобы не ловить ошибки Markdown


@router.message(
    F.reply_to_message
    & F.reply_to_message.text.startswith("📛 Чёрный список автоответов")
)
async def edit_blacklist_from_reply(message: Message):
    """
    Обработка ответов на сообщение с заголовком «📛 Чёрный список автоответов».

    Формат:
    - Добавить:  @user1 @user2
    - Удалить:   -@user1 -@user2
    """
    db = load_data()
    blacklist: list[str] = db.get("auto_replies_blacklist", [])
    current = set(blacklist)

    text = (message.text or "").strip()
    tokens = text.split()

    added = []
    removed = []

    for raw in tokens:
        token = raw.strip()
        if not token:
            continue

        # Удаление: -@user
        if token.startswith("-@") and len(token) > 2:
            uname = token[1:]  # убираем минус → @user
            if uname in current:
                current.remove(uname)
                removed.append(uname)
            continue

        # Добавление: @user
        if token.startswith("@") and len(token) > 1:
            uname = token
            if uname not in current:
                current.add(uname)
                added.append(uname)
            continue

        # всё остальное игнорируем
        continue

    db["auto_replies_blacklist"] = sorted(current)
    save_data(db)

    # === Текст изменений ===
    parts = []
    if added:
        parts.append("➕ Добавлено:\n" + "\n".join(f"• {u}" for u in added))
    if removed:
        parts.append("➖ Удалено:\n" + "\n".join(f"• {u}" for u in removed))
    if not parts:
        parts.append(
            "Ничего не изменилось. Используйте формат:\n@user1 @user2 или -@user1 -@user2."
        )

    # === Кнопки навигации ===
    nav = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="auto_replies_blacklist")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")],
        ]
    )

    await message.answer("\n\n".join(parts), reply_markup=nav)

    # === Обновлённый список (с теми же кнопками навигации) ===
    text_new = _format_blacklist_text(db.get("auto_replies_blacklist", []))
    await message.answer(text_new, reply_markup=nav)


# ========================= КАТЕГОРИИ ДЛЯ АВТООТВЕТОВ =========================


def _get_catalog_tree(db: dict) -> dict:
    """
    Берём дерево каталога из data.json:
    - в приоритете db["catalog"]
    - если нет — пробуем db["etalon"]
    """
    catalog = db.get("catalog")
    if isinstance(catalog, dict) and catalog:
        return catalog
    etalon = db.get("etalon")
    if isinstance(etalon, dict) and etalon:
        return etalon
    return {}


def _load_allowed_paths_spec(db: dict) -> list[list[str]]:
    """
    Читаем auto_replies_allowed_paths как список строк "A|B|C"
    и приводим к виду: [["A","B","C"], ...]
    """
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


def _store_allowed_paths_spec(db: dict, spec: list[list[str]]):
    """
    Обратно сохраняем spec в data.json как список строк "A|B|C".
    """
    lines = ["|".join(p) for p in spec if p]
    db["auto_replies_allowed_paths"] = lines
    save_data(db)


def _path_has_any_allowed(path: list[str], allowed_spec: list[list[str]]) -> bool:
    """
    path помечаем галочкой, если:
    - хотя бы один разрешённый путь лежит внутри этого узла:
        allowed = ["Смартфоны","Apple","iPhone","iPhone 17 Pro"]
        path    = ["Смартфоны","Apple","iPhone"]  -> ✅
        path    = ["Смартфоны","Apple","iPhone","iPhone 17 Pro"] -> ✅
    """
    if not allowed_spec or not path:
        return False

    for ap in allowed_spec:
        n = min(len(path), len(ap))
        if ap[:n] == path[:n]:
            return True
    return False


def _toggle_path_in_spec(spec: list[list[str]], target: list[str]) -> list[list[str]]:
    """
    Клик по чекбоксу:
    - если под target уже есть выбранные пути → снимаем ВСЁ в этом поддереве;
    - если ничего нет → добавляем сам target как разрешённый путь.
    """
    if not target:
        return spec

    has_any = False
    new_spec: list[list[str]] = []
    for p in spec:
        if p[:len(target)] == target:
            has_any = True
            continue
        new_spec.append(p)

    if not has_any:
        new_spec.append(target)

    return new_spec


def _get_node_by_path(tree: dict, path: list[str]):
    node = tree
    for key in path:
        if not isinstance(node, dict):
            return {}
        node = node.get(key) or {}
    return node if isinstance(node, dict) else {}


def _build_categories_keyboard(
    tree: dict,
    current_path: list[str],
    allowed_spec: list[list[str]],
) -> InlineKeyboardMarkup:
    """
    Рендерим текущий уровень дерева.
    В КАЖДОЙ строке:
    [ ✅ ] [ 📁 iPhone 17 Pro ]
      ^ чекбокс      ^ навигация
    """
    node = _get_node_by_path(tree, current_path)
    rows: list[list[InlineKeyboardButton]] = []

    if isinstance(node, dict) and node:
        for name in sorted(node.keys(), key=lambda x: str(x)):
            if str(name).startswith("_"):
                continue

            child_path = current_path + [str(name)]
            checked = _path_has_any_allowed(child_path, allowed_spec)
            checkbox_text = "✅" if checked else "⬜️"

            # чекбокс — только переключает
            cb_toggle = "ar_cat_toggle:" + "|".join(child_path)
            # папка — только открывает уровень
            cb_open = "ar_cat_open:" + "|".join(child_path)

            row = [
                InlineKeyboardButton(text=checkbox_text, callback_data=cb_toggle),
                InlineKeyboardButton(text=f"📁 {name}", callback_data=cb_open),
            ]
            rows.append(row)
    else:
        rows.append([InlineKeyboardButton(text="(Нет дочерних категорий)", callback_data="noop")])

    nav_row: list[InlineKeyboardButton] = []
    if current_path:
        parent_path = current_path[:-1]
        parent_data = "ar_cat_back:" + "|".join(parent_path)
        nav_row.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=parent_data))
    else:
        nav_row.append(InlineKeyboardButton(text="⬅️ Назад", callback_data="auto_replies"))

    rows.append(nav_row)
    rows.append([InlineKeyboardButton(text="🏠 Автоответы", callback_data="auto_replies")])

    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _render_categories_tree(callback: CallbackQuery, current_path: list[str], *, edit: bool = True):
    db = load_data()
    tree = _get_catalog_tree(db)
    allowed_spec = _load_allowed_paths_spec(db)

    if current_path:
        title = "📂 Категории автоответов\n" + " / ".join(current_path)
    else:
        title = "📂 Категории автоответов\n(выберите категорию или модель)"

    markup = _build_categories_keyboard(tree, current_path, allowed_spec)

    try:
        if edit:
            await callback.message.edit_text(title, reply_markup=markup)
        else:
            await callback.message.answer(title, reply_markup=markup)
    except Exception:
        await callback.message.answer(title, reply_markup=markup)


@router.callback_query(F.data == "auto_replies_categories")
async def show_auto_reply_categories(callback: CallbackQuery):
    """
    Корень дерева категорий.
    """
    await callback.answer()
    await _render_categories_tree(callback, current_path=[], edit=False)


@router.callback_query(F.data.startswith("ar_cat_open:"))
async def open_category(callback: CallbackQuery):
    """
    Просто провалиться внутрь узла (без изменения флагов).
    """
    data = callback.data or ""
    _, _, raw_path = data.partition("ar_cat_open:")
    path = [p for p in raw_path.split("|") if p]

    await callback.answer()
    await _render_categories_tree(callback, current_path=path, edit=True)


@router.callback_query(F.data.startswith("ar_cat_toggle:"))
async def toggle_category(callback: CallbackQuery):
    """
    Переключить чекбокс для узла (target), но остаться на том же уровне (parent).
    """
    data = callback.data or ""
    _, _, raw_path = data.partition("ar_cat_toggle:")
    path = [p for p in raw_path.split("|") if p]

    db = load_data()
    spec = _load_allowed_paths_spec(db)
    spec_new = _toggle_path_in_spec(spec, path)
    _store_allowed_paths_spec(db, spec_new)

    # остаёмся на уровне родителя
    parent_path = path[:-1]
    await callback.answer("Обновлено")
    await _render_categories_tree(callback, current_path=parent_path, edit=True)


@router.callback_query(F.data.startswith("ar_cat_back:"))
async def back_in_categories(callback: CallbackQuery):
    """
    Шаг назад по дереву категорий.
    """
    data = callback.data or ""
    _, _, raw_path = data.partition("ar_cat_back:")
    path = [p for p in raw_path.split("|") if p]

    await callback.answer()
    await _render_categories_tree(callback, current_path=path, edit=True)
