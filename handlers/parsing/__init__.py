from pathlib import Path
import json
from datetime import datetime, timezone

# === База строго в handlers/parsing/data ===
MODULE_DIR = Path(__file__).resolve().parent
BASE_DIR   = (MODULE_DIR / "data").resolve()
BASE_DIR.mkdir(parents=True, exist_ok=True)

def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

# === Единые файлы ===
PARSED_FILE   = BASE_DIR / "parsed_data.json"       # финал (список мэтчей + timestamp)
CACHE_FILE    = BASE_DIR / "parsed_cache.json"      # кэш метаданных
MESSAGES_FILE = BASE_DIR / "parsed_messages.json"   # собранные сообщения (после парсинга могут содержать parsed_raw)

# ⬇️ Новые диагностические файлы (вместо unmatched.json)
UNMATCHED_ETALON_FILE = BASE_DIR / "unmatched_etalon.json"   # эталонные позиции без найденной цены
UNMATCHED_PARSED_FILE = BASE_DIR / "unmatched_parsed.json"   # распарсенные товары без пары в эталоне

# === Создаём файлы, если их нет, с корректными дефолтами по типу ===
if not PARSED_FILE.exists():
    PARSED_FILE.write_text(
        json.dumps({"etalon_with_prices": [], "timestamp": _utcnow_iso()}, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

if not CACHE_FILE.exists():
    CACHE_FILE.write_text(
        json.dumps({"meta": {"etalon_hash": None, "last_updated": _utcnow_iso()}}, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )

if not MESSAGES_FILE.exists():
    MESSAGES_FILE.write_text(json.dumps([], ensure_ascii=False, indent=2), encoding="utf-8")

# новые файлы несопоставлений
if not UNMATCHED_ETALON_FILE.exists():
    UNMATCHED_ETALON_FILE.write_text(json.dumps([], ensure_ascii=False, indent=2), encoding="utf-8")

if not UNMATCHED_PARSED_FILE.exists():
    UNMATCHED_PARSED_FILE.write_text(json.dumps([], ensure_ascii=False, indent=2), encoding="utf-8")
