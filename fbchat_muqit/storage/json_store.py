"""JSON file–backed session storage (default)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional

import aiofiles


def _safe_client_id(client_id: str) -> str:
    return "".join(c if c.isalnum() or c in "-_" else "_" for c in client_id)


class JsonSessionStorage:
    """Store one JSON file per ``client_id`` under ``directory``."""

    __slots__ = ("_dir",)

    def __init__(self, directory: str | Path) -> None:
        self._dir = Path(directory)
        self._dir.mkdir(parents=True, exist_ok=True)

    def _path(self, client_id: str) -> Path:
        return self._dir / f"{_safe_client_id(client_id)}.json"

    async def save(self, client_id: str, snapshot: Dict[str, Any]) -> None:
        path = self._path(client_id)
        async with aiofiles.open(path, "w", encoding="utf-8") as f:
            await f.write(json.dumps(snapshot, indent=2))

    async def load(self, client_id: str) -> Optional[Dict[str, Any]]:
        path = self._path(client_id)
        if not path.is_file():
            return None
        async with aiofiles.open(path, "r", encoding="utf-8") as f:
            return json.loads(await f.read())

    async def delete(self, client_id: str) -> None:
        path = self._path(client_id)
        try:
            path.unlink()
        except OSError:
            pass
