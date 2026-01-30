from __future__ import annotations

from pathlib import Path
import json


MODULE_DIR = Path(__file__).resolve().parent
DEFAULT_BASE_DIR = (MODULE_DIR / "data").resolve()


def user_data_dir(user_id: int) -> Path:
    return (DEFAULT_BASE_DIR / str(int(user_id))).resolve()


def _ensure_file(path: Path, default_obj) -> None:
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(default_obj, ensure_ascii=False, indent=2), encoding="utf-8")


def set_parsing_data_dir(base_dir: Path) -> None:
    base_dir = Path(base_dir).resolve()
    base_dir.mkdir(parents=True, exist_ok=True)

    # handlers/parsing/__init__.py
    from handlers import parsing as parsing_mod

    parsing_mod.BASE_DIR = base_dir
    parsing_mod.PARSED_FILE = base_dir / "parsed_data.json"
    parsing_mod.CACHE_FILE = base_dir / "parsed_cache.json"
    parsing_mod.MESSAGES_FILE = base_dir / "parsed_messages.json"
    parsing_mod.UNMATCHED_ETALON_FILE = base_dir / "unmatched_etalon.json"
    parsing_mod.UNMATCHED_PARSED_FILE = base_dir / "unmatched_parsed.json"

    _ensure_file(parsing_mod.PARSED_FILE, {"etalon_with_prices": [], "timestamp": ""})
    _ensure_file(parsing_mod.CACHE_FILE, {"meta": {"etalon_hash": None, "last_updated": ""}})
    _ensure_file(parsing_mod.MESSAGES_FILE, [])
    _ensure_file(parsing_mod.UNMATCHED_ETALON_FILE, [])
    _ensure_file(parsing_mod.UNMATCHED_PARSED_FILE, [])

    # handlers/parsing/parser.py
    from handlers.parsing import parser as parser_mod
    parser_mod.DATA_DIR = base_dir
    parser_mod.MESSAGES_FILE = base_dir / "parsed_messages.json"

    # handlers/normalizers/entry.py
    from handlers.normalizers import entry as entry_mod
    entry_mod.DATA_DIR = base_dir
    entry_mod.PARSED_ETALON_JSON = base_dir / "parsed_etalon.json"
    entry_mod.PARSED_MESSAGES_JSON = base_dir / "parsed_messages.json"
    entry_mod.PARSED_GOODS_JSON = base_dir / "parsed_goods.json"
    entry_mod.ETALON_STATS_JSON = base_dir / "etalon_stats.json"
    entry_mod.MODEL_ALIASES_JSON = base_dir / "model_aliases.json"
    entry_mod.MODEL_INDEX_JSON = base_dir / "model_index.json"
    entry_mod.CODE_INDEX_JSON = base_dir / "code_index.json"
    entry_mod.LEARNED_TOKENS_JSON = base_dir / "etalon_learned_tokens.json"
    entry_mod.ALIAS_COLLISIONS_JSON = base_dir / "alias_collisions.json"
    entry_mod.PARSED_MATCHED_JSON = base_dir / "parsed_matched.json"
    entry_mod.UNMATCHED_PARSED_JSON = base_dir / "unmatched_parsed.json"
    entry_mod.MATCH_STATS_JSON = base_dir / "match_stats.json"
    entry_mod.UNMATCHED_ETALON_JSON = base_dir / "unmatched_etalon.json"
    entry_mod.UNMATCHED_PARSED_FROM_MATCHER_JSON = base_dir / "unmatched_parsed_from_matcher.json"

    # handlers/parsing/matcher.py
    from handlers.parsing import matcher as matcher_mod
    matcher_mod.DATA_DIR = base_dir
    matcher_mod.ETALON_FILE = base_dir / "parsed_etalon.json"
    matcher_mod.GOODS_FILE = base_dir / "parsed_goods.json"
    matcher_mod.MATCHED_FILE = base_dir / "parsed_matched.json"
    matcher_mod.UNMATCHED_ETALON_FILE = base_dir / "unmatched_etalon.json"
    matcher_mod.UNMATCHED_PARSED_FILE = base_dir / "unmatched_parsed.json"
    matcher_mod.MATCH_STATS_FILE = base_dir / "match_stats.json"

    # handlers/parsing/results.py
    from handlers.parsing import results as results_mod
    results_mod.DATA_DIR = base_dir
    results_mod.MATCHED_FILE = base_dir / "parsed_matched.json"
    results_mod.PARSED_FILE = base_dir / "parsed_data.json"
