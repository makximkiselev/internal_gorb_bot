#!/usr/bin/env python3
"""
Reorder etalon tree to match catalog ordering, optionally remove catalog.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict


DATA_FILE = Path("data.json")


def _read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, data: Dict[str, Any]) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _reorder_etalon(et_node: Any, cat_node: Any) -> Any:
    if isinstance(et_node, list):
        return list(et_node)

    if not isinstance(et_node, dict):
        return et_node

    if not isinstance(cat_node, dict):
        return {k: _reorder_etalon(v, None) for k, v in et_node.items()}

    out: Dict[str, Any] = {}

    # 1) keys in catalog order
    for k in cat_node.keys():
        if k in et_node:
            out[k] = _reorder_etalon(et_node[k], cat_node.get(k))

    # 2) remaining etalon keys (preserve original order)
    for k, v in et_node.items():
        if k not in out:
            out[k] = _reorder_etalon(v, None)

    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--keep-catalog", action="store_true", help="Do not delete catalog")
    args = parser.parse_args()

    if not DATA_FILE.exists():
        raise SystemExit("data.json not found")

    data = _read_json(DATA_FILE)
    etalon = data.get("etalon") or {}
    catalog = data.get("catalog") or {}

    if not isinstance(etalon, dict):
        raise SystemExit("etalon is not a dict")
    if not isinstance(catalog, dict):
        catalog = {}

    if catalog:
        data["etalon"] = _reorder_etalon(etalon, catalog)

    if not args.keep_catalog:
        data.pop("catalog", None)

    _write_json(DATA_FILE, data)


if __name__ == "__main__":
    main()
