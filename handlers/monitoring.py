from aiogram import Router, F
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from pathlib import Path
import json
import asyncio
from datetime import datetime
from storage import load_data, save_data

router = Router()

db = load_data()

# === Главное меню мониторинга ===
@router.callback_query(F.data == "monitoring")
async def monitoring_menu(callback: CallbackQuery):
    mon = db.get("monitoring", {"enabled": False, "period": 30, "work_hours": {"start": 10, "end": 18}, "history": []})
    status = "🔵 ВКЛЮЧЕН" if mon["enabled"] else "🔴 ВЫКЛЮЧЕН"

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=("🔴 Отключить" if mon["enabled"] else "🔵 Включить"),
                              callback_data="toggle_monitoring")],
        [InlineKeyboardButton(text=f"⏱ Периодичность: {mon['period']} мин", callback_data="set_period")],
        [InlineKeyboardButton(text=f"🕐 Часы работы: {mon['work_hours']['start']}–{mon['work_hours']['end']}",
                              callback_data="set_hours")],
        [InlineKeyboardButton(text="📜 История мониторинга", callback_data="monitoring_history")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="collect")]   # 🟢 исправлено
    ])

    await callback.message.answer(f"⏱ Мониторинг: {status}", reply_markup=kb)

# === Вкл/выкл мониторинг ===
@router.callback_query(F.data == "toggle_monitoring")
async def toggle_monitoring(callback: CallbackQuery):
    db["monitoring"]["enabled"] = not db["monitoring"]["enabled"]

    if not db["monitoring"]["enabled"]:
        # 🔥 при выключении мониторинга очищаем историю
        db["monitoring"]["history"] = []

    save_data(db)
    await monitoring_menu(callback)

# === Выбор периодичности ===
@router.callback_query(F.data == "set_period")
async def set_period(callback: CallbackQuery):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"{p} мин", callback_data=f"choose_period:{p}")]
        for p in [30, 45, 60, 90, 120]
    ] + [[InlineKeyboardButton(text="⬅️ Назад", callback_data="monitoring")]])
    await callback.message.answer("Выбери периодичность мониторинга:", reply_markup=kb)

@router.callback_query(F.data.startswith("choose_period:"))
async def choose_period(callback: CallbackQuery):
    _, val = callback.data.split(":")
    db["monitoring"]["period"] = int(val)
    save_data(db)
    await monitoring_menu(callback)

# === Настройка часов работы ===
@router.callback_query(F.data == "set_hours")
async def set_hours(callback: CallbackQuery):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Старт -1ч", callback_data="hour_start:-1"),
         InlineKeyboardButton(text="Старт +1ч", callback_data="hour_start:+1")],
        [InlineKeyboardButton(text="Финиш -1ч", callback_data="hour_end:-1"),
         InlineKeyboardButton(text="Финиш +1ч", callback_data="hour_end:+1")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="monitoring")]
    ])
    h = db["monitoring"]["work_hours"]
    await callback.message.answer(f"🕐 Рабочие часы: {h['start']}–{h['end']}", reply_markup=kb)

@router.callback_query(F.data.startswith("hour_start:"))
async def set_hour_start(callback: CallbackQuery):
    _, diff = callback.data.split(":")
    db["monitoring"]["work_hours"]["start"] += int(diff)
    save_data(db)
    await set_hours(callback)

@router.callback_query(F.data.startswith("hour_end:"))
async def set_hour_end(callback: CallbackQuery):
    _, diff = callback.data.split(":")
    db["monitoring"]["work_hours"]["end"] += int(diff)
    save_data(db)
    await set_hours(callback)

# === История мониторинга ===
@router.callback_query(F.data == "monitoring_history")
async def monitoring_history(callback: CallbackQuery):
    history = db["monitoring"].get("history", [])
    if not history:
        await callback.message.answer("📜 История пуста.",
                                      reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                          [InlineKeyboardButton(text="⬅️ Назад", callback_data="monitoring")]
                                      ]))
        return

    text = "📜 Последние мониторинги:\n\n"
    kb = []
    for item in history[::-1]:  # только последние 20 храним
        ts = item["time"]
        status = item["status"]
        text += f"• {ts} — {status}\n"
        kb.append([InlineKeyboardButton(text=f"{ts}", callback_data=f"monitoring_log:{ts}")])

    kb.append([InlineKeyboardButton(text="🧹 Очистить историю", callback_data="clear_history")])
    kb.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="monitoring")])
    await callback.message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

# === Подробный лог мониторинга ===
@router.callback_query(F.data.startswith("monitoring_log:"))
async def monitoring_log(callback: CallbackQuery):
    _, ts = callback.data.split(":", 1)
    history = db["monitoring"].get("history", [])
    item = next((h for h in history if h["time"] == ts), None)
    if not item:
        await callback.answer("⚠️ Лог не найден", show_alert=True)
        return

    text = (
        f"📊 Мониторинг {item['time']}\n\n"
        f"📡 Источников: {item.get('sources', '?')}\n"
        f"💬 Сообщений: {item.get('messages', '?')}\n"
        f"🏷 Цен: {item.get('prices', '?')}\n"
        f"⚙️ Статус: {item['status']}"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="monitoring_history")]
    ])
    await callback.message.answer(text, reply_markup=kb)

# === Очистка истории вручную ===
@router.callback_query(F.data == "clear_history")
async def clear_history(callback: CallbackQuery):
    db["monitoring"]["history"] = []
    save_data(db)
    await callback.message.answer("🧹 История мониторинга очищена.",
                                  reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                                      [InlineKeyboardButton(text="⬅️ Назад", callback_data="monitoring")]
                                  ]))

# === Фоновый таск мониторинга ===
async def monitoring_loop():
    while True:
        # гарантируем, что monitoring всегда есть
        mon = db.setdefault("monitoring", {
            "enabled": False,
            "period": 30,
            "work_hours": {"start": 10, "end": 18},
            "history": []
        })

        if mon.get("enabled"):
            now = datetime.now()
            start = mon["work_hours"].get("start", 10)
            end = mon["work_hours"].get("end", 18)

            if start <= now.hour < end:
                print(f"⏱ Запуск парсинга в {now}")

                # TODO: сюда подключить реальный парсер
                sources_count = len(db.get("sources", []))
                messages_count = sources_count * 10  # заглушка
                prices_count = sources_count * 7     # заглушка

                entry = {
                    "time": now.strftime("%Y-%m-%d %H:%M:%S"),
                    "status": "ok",
                    "sources": sources_count,
                    "messages": messages_count,
                    "prices": prices_count
                }
                mon.setdefault("history", []).append(entry)
                # ✨ Храним только последние 20
                mon["history"] = mon["history"][-20:]
                save_data(db)

        # безопасное ожидание
        await asyncio.sleep(mon.get("period", 30) * 60)

