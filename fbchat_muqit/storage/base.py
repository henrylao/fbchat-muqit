"""Abstract session persistence API."""

from __future__ import annotations

from typing import Any, Dict, Optional, Protocol, runtime_checkable


@runtime_checkable
class SessionStorage(Protocol):
    """Persist serialized session snapshots (cookies + metadata)."""

    async def save(self, client_id: str, snapshot: Dict[str, Any]) -> None: ...

    async def load(self, client_id: str) -> Optional[Dict[str, Any]]: ...

    async def delete(self, client_id: str) -> None: ...
