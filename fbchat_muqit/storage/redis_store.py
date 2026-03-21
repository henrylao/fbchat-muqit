"""Optional Redis-backed session storage."""

from __future__ import annotations

import json
from typing import Any, Dict, Optional


try:
    import redis.asyncio as redis  # type: ignore[import-not-found]
except ImportError:  # pragma: no cover - optional dependency
    redis = None  # type: ignore[assignment]


class RedisSessionStorage:
    """Persist session JSON blobs in Redis."""

    __slots__ = ("_url", "_prefix", "_client")

    def __init__(self, url: str, *, key_prefix: str = "fbchat:session:") -> None:
        if redis is None:
            raise ImportError(
                "RedisSessionStorage requires the 'redis' package. "
                "Install with: pip install 'fbchat-muqit[redis]' or pip install redis"
            )
        self._url = url
        self._prefix = key_prefix
        self._client: Any = None

    async def _conn(self) -> Any:
        if self._client is None:
            self._client = redis.from_url(self._url, decode_responses=True)
        return self._client

    async def save(self, client_id: str, snapshot: Dict[str, Any]) -> None:
        r = await self._conn()
        await r.set(self._prefix + client_id, json.dumps(snapshot))

    async def load(self, client_id: str) -> Optional[Dict[str, Any]]:
        r = await self._conn()
        raw = await r.get(self._prefix + client_id)
        if raw is None:
            return None
        return json.loads(raw)

    async def delete(self, client_id: str) -> None:
        r = await self._conn()
        await r.delete(self._prefix + client_id)
