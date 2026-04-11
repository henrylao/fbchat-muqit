"""Multi-account orchestration and manager-scoped event routing."""

from __future__ import annotations

import asyncio
import inspect
from collections import defaultdict
from typing import Awaitable, Callable, Dict, List, Optional, Tuple

from .client import Client
from .events.dispatcher import EventType
from .exception.errors import FBChatError
from .logging.logger import FBChatLogger, get_logger
from .state import State
from .storage import JsonSessionStorage, SessionStorage

EventCallback = Callable[..., Awaitable[None]]

__all__ = ["ClientManager"]


class ClientManager:
    """Run multiple :class:`~fbchat_muqit.client.Client` instances with isolated sessions."""

    def __init__(
        self,
        *,
        storage: Optional[SessionStorage] = None,
        storage_dir: Optional[str] = None,
    ) -> None:
        self.clients: Dict[str, Client] = {}
        if storage is not None:
            self._storage: Optional[SessionStorage] = storage
        elif storage_dir is not None:
            self._storage = JsonSessionStorage(storage_dir)
        else:
            self._storage = None
        self.logger: FBChatLogger = get_logger()
        self._listeners: Dict[Tuple[str, EventType], List[EventCallback]] = defaultdict(
            list
        )
        self._max_concurrent_handlers = 25
        self._semaphore = asyncio.Semaphore(self._max_concurrent_handlers)

    def event(self, *, client_id: str, event: Optional[EventType] = None):
        """Register a handler for a specific account, e.g. ``@manager.event(client_id=\"acc1\")``."""

        def decorator(func: EventCallback) -> EventCallback:
            resolved: EventType
            if event is None:
                name = func.__name__
                if not name.startswith("on_"):
                    raise FBChatError(
                        "Manager event handler must start with 'on_' or pass event=EventType explicitly"
                    )
                suffix = name[3:]
                try:
                    resolved = EventType(suffix.lower())
                except ValueError as e:
                    raise FBChatError(f"Unknown event type for handler {name}") from e
            else:
                resolved = event
            self._listeners[(client_id, resolved)].append(func)
            return func

        return decorator

    async def _route_manager_event(
        self, client_id: str, event_name: EventType, *args
    ) -> None:
        for listener in self._listeners.get((client_id, event_name), []):
            await self._safe_manager_listener(client_id, listener, event_name, *args)

    async def _safe_manager_listener(
        self, client_id: str, listener: EventCallback, event_name: EventType, *args
    ) -> None:
        async with self._semaphore:
            try:
                if inspect.iscoroutinefunction(listener):
                    await listener(*args)
                else:
                    await asyncio.to_thread(listener, *args)
            except Exception as e:
                self.logger.error(
                    f"Error in manager listener for client_id={client_id!r} event={event_name}: {e}",
                    exc_info=True,
                )

    async def add_client(
        self,
        client_id: str,
        cookies_path: str,
        *,
        user_agent: Optional[str] = None,
        proxy: Optional[str] = None,
        restore_from_storage: bool = False,
        log_level: str = "INFO",
        disable_logs: bool = False,
        online: bool = True,
    ) -> Client:
        if client_id in self.clients:
            raise FBChatError(f"Client id already registered: {client_id}")

        initial_state: Optional[State] = None
        if self._storage and restore_from_storage:
            snap = await self._storage.load(client_id)
            if snap and snap.get("cookies"):
                initial_state = await State.from_cookie_list(
                    snap["cookies"],
                    user_agent=user_agent,
                    proxy=proxy,
                )

        client = Client(
            cookies_path,
            userAgent=user_agent,
            proxy=proxy,
            log_level=log_level,
            disable_logs=disable_logs,
            online=online,
            initial_state=initial_state,
        )
        client._manager_client_id = client_id  # type: ignore[attr-defined]
        client._manager_route = self._route_manager_event  # type: ignore[attr-defined]
        self.clients[client_id] = client
        return client

    async def remove_client(self, client_id: str, *, persist: bool = False) -> None:
        client = self.clients.pop(client_id, None)
        if client is None:
            return
        client._manager_route = None  # type: ignore[attr-defined]
        client._manager_client_id = ""  # type: ignore[attr-defined]
        if persist and self._storage is not None and client._state is not None:
            await self._storage.save(client_id, client._state.dump_session_snapshot())
        await client.stop_listening()
        await client.close()

    async def save_session(self, client_id: str) -> None:
        if self._storage is None:
            raise FBChatError("No session storage configured on ClientManager")
        client = self.clients.get(client_id)
        if client is None or client._state is None:
            raise FBChatError(f"No active session for client id: {client_id}")
        await self._storage.save(client_id, client._state.dump_session_snapshot())

    async def broadcast(self, message: str, thread_id: str) -> None:
        for cid, client in list(self.clients.items()):
            try:
                if client._state is not None:
                    await client.send_message(message, thread_id)
            except Exception as e:
                self.logger.error(f"broadcast failed for {cid}: {e}", exc_info=True)

    async def start_all(self) -> None:
        for client in self.clients.values():
            await client.start()
            await client.start_listening()

    async def stop_all(self) -> None:
        for client in self.clients.values():
            await client.stop_listening()
