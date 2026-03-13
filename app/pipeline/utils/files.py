from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def resolve_path(project_root: Path, path_str: str) -> Path:
    path = Path(path_str)
    if path.is_absolute():
        return path
    return project_root / path


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def validate_json_file(path: Path) -> None:
    try:
        with path.open("r", encoding="utf-8") as f:
            json.load(f)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Invalid JSON file: {path} (line {exc.lineno}, column {exc.colno}): {exc.msg}"
        ) from exc


def save_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False))
        f.write("\n")


def load_jsonl_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            item = line.strip()
            if not item:
                continue
            try:
                payload = json.loads(item)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                rows.append(payload)
    return rows
