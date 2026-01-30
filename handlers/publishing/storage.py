from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from storage import load_data, save_data

BASE_DIR = Path(__file__).resolve().parent / "data"
CHANNELS_DIR = BASE_DIR / "channels"
BASE_DIR.mkdir(parents=True, exist_ok=True)
CHANNELS_DIR.mkdir(parents=True, exist_ok=True)

MANAGED_CHANNELS_FILE = BASE_DIR / "managed_channels.json"
STATUS_EXTRA_FILE = BASE_DIR / "channel_status_extra.json"


def _read_json(path: Path, default: Any):
    try:
        if not path.exists():
            return default
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def _peer_dir(peer_id: str) -> Path:
    return CHANNELS_DIR / str(peer_id)


def _migrate_managed_channels() -> dict:
    if MANAGED_CHANNELS_FILE.exists():
        return _read_json(MANAGED_CHANNELS_FILE, {})
    db = load_data()
    reg = db.get("managed_channels") or {}
    if isinstance(reg, dict):
        _write_json(MANAGED_CHANNELS_FILE, reg)
        db.pop("managed_channels", None)
        save_data(db)
        return reg
    return {}


def load_managed_channels() -> dict:
    return _migrate_managed_channels()


def save_managed_channels(reg: dict) -> None:
    _write_json(MANAGED_CHANNELS_FILE, reg if isinstance(reg, dict) else {})


def _migrate_status_extra() -> dict:
    if STATUS_EXTRA_FILE.exists():
        return _read_json(STATUS_EXTRA_FILE, {})
    db = load_data()
    cfg = db.get("channel_status_extra") or {}
    if isinstance(cfg, dict):
        _write_json(STATUS_EXTRA_FILE, cfg)
        db.pop("channel_status_extra", None)
        save_data(db)
        return cfg
    return {}


def load_status_extra() -> dict:
    return _migrate_status_extra()


def save_status_extra(cfg: dict) -> None:
    _write_json(STATUS_EXTRA_FILE, cfg if isinstance(cfg, dict) else {})


def _migrate_peer(peer_id: str) -> None:
    db = load_data()
    changed = False
    pid = str(peer_id)

    posts = (db.get("channel_posts") or {}).get(pid)
    if isinstance(posts, dict) and posts:
        save_channel_posts(pid, posts, migrate=False)
        db.get("channel_posts", {}).pop(pid, None)
        changed = True

    menu_state = (db.get("channel_menu_state") or {}).get(pid)
    if isinstance(menu_state, dict) and menu_state:
        save_channel_menu_state(pid, menu_state, migrate=False)
        db.get("channel_menu_state", {}).pop(pid, None)
        changed = True

    group_posts = (db.get("channel_group_posts") or {}).get(pid)
    if isinstance(group_posts, dict) and group_posts:
        save_channel_group_posts(pid, group_posts, migrate=False)
        db.get("channel_group_posts", {}).pop(pid, None)
        changed = True

    group_nav = (db.get("channel_group_nav") or {}).get(pid)
    if isinstance(group_nav, dict) and group_nav:
        save_channel_group_nav(pid, group_nav, migrate=False)
        db.get("channel_group_nav", {}).pop(pid, None)
        changed = True

    if changed:
        save_data(db)


def _channel_file(peer_id: str, name: str) -> Path:
    return _peer_dir(peer_id) / name


def load_channel_posts(peer_id: str) -> dict:
    _migrate_peer(peer_id)
    return _read_json(_channel_file(peer_id, "posts.json"), {})


def save_channel_posts(peer_id: str, posts: dict, *, migrate: bool = True) -> None:
    if migrate:
        _migrate_peer(peer_id)
    _write_json(_channel_file(peer_id, "posts.json"), posts if isinstance(posts, dict) else {})


def load_channel_menu_state(peer_id: str) -> dict:
    _migrate_peer(peer_id)
    return _read_json(_channel_file(peer_id, "menu_state.json"), {})


def save_channel_menu_state(peer_id: str, state: dict, *, migrate: bool = True) -> None:
    if migrate:
        _migrate_peer(peer_id)
    _write_json(_channel_file(peer_id, "menu_state.json"), state if isinstance(state, dict) else {})


def load_channel_group_posts(peer_id: str) -> dict:
    _migrate_peer(peer_id)
    return _read_json(_channel_file(peer_id, "group_posts.json"), {})


def save_channel_group_posts(peer_id: str, posts: dict, *, migrate: bool = True) -> None:
    if migrate:
        _migrate_peer(peer_id)
    _write_json(_channel_file(peer_id, "group_posts.json"), posts if isinstance(posts, dict) else {})


def load_channel_group_nav(peer_id: str) -> dict:
    _migrate_peer(peer_id)
    return _read_json(_channel_file(peer_id, "group_nav.json"), {})


def save_channel_group_nav(peer_id: str, nav: dict, *, migrate: bool = True) -> None:
    if migrate:
        _migrate_peer(peer_id)
    _write_json(_channel_file(peer_id, "group_nav.json"), nav if isinstance(nav, dict) else {})


def purge_channel_storage(peer_id: str) -> None:
    _migrate_peer(peer_id)
    ch_dir = _peer_dir(peer_id)
    if not ch_dir.exists():
        return
    for p in ch_dir.glob("*"):
        try:
            p.unlink()
        except Exception:
            pass
    try:
        ch_dir.rmdir()
    except Exception:
        pass
