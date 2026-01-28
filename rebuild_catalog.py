"""
rebuild_catalog.py
Восстанавливает структуру каталога (catalog) в data.json на основе ключей из etalon.
Файл data.json должен лежать в корне проекта.
"""

import json
from pathlib import Path

# === Путь к файлу data.json ===
DATA_PATH = Path("data.json")

if not DATA_PATH.exists():
    raise FileNotFoundError(f"❌ Файл не найден: {DATA_PATH.resolve()}")

# === Загружаем текущие данные ===
data = json.loads(DATA_PATH.read_text(encoding="utf-8"))
etalon = data.get("etalon", {})

if not etalon:
    raise ValueError("❌ В файле data.json отсутствует ключ 'etalon' — нечего восстанавливать.")

# === Функция рекурсивного построения каталога ===
def build_catalog_from_etalon(node):
    """
    Преобразует дерево эталона в структуру каталога:
      Категория → Бренд → Серия → Модель
    """
    if isinstance(node, dict):
        catalog_branch = {}
        for key, value in node.items():
            if isinstance(value, dict):
                catalog_branch[key] = build_catalog_from_etalon(value)
            elif isinstance(value, list):
                # Список моделей → создаём под каждый элемент пустой словарь
                branch = {}
                for item in value:
                    if isinstance(item, str) and item.strip():
                        branch[item.strip()] = {}
                catalog_branch[key] = branch
        return catalog_branch
    return {}

# === Формируем новый каталог ===
catalog = build_catalog_from_etalon(etalon)
data["catalog"] = catalog

# === Сохраняем обновлённый data.json ===
DATA_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

print("✅ Каталог успешно восстановлен из эталона!")
print(f"📁 Файл обновлён: {DATA_PATH.resolve()}")
