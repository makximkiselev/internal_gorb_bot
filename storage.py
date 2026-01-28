# storage.py
import json
from pathlib import Path

DATA_FILE = Path("data.json")

def load_data():
    if DATA_FILE.exists():
        try:
            content = DATA_FILE.read_text(encoding="utf-8").strip()
            if content:
                return json.loads(content)
        except json.JSONDecodeError:
            print("⚠️ data.json поврежден, пересоздаю...")
    # если файл пустой или битый
    return {
        "catalog": {},
        "etalon": {},
        "brands": [],
        "sources": [],
        "accounts": [],
        "prices": {}
    }

def save_data(data):
    DATA_FILE.write_text(
        json.dumps(data, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
