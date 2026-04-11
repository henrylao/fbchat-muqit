"""Tests for ClientManager and session storage."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from fbchat_muqit.events.dispatcher import EventType
from fbchat_muqit.manager import ClientManager
from fbchat_muqit.storage.json_store import JsonSessionStorage


@pytest.mark.asyncio
async def test_manager_event_routing_invokes_handler(tmp_path: Path) -> None:
    mgr = ClientManager(storage_dir=str(tmp_path))
    received: list[str] = []

    @mgr.event(client_id="acc1", event=EventType.MESSAGE)
    async def _on_message(msg: str) -> None:
        received.append(msg)

    await mgr._route_manager_event("acc1", EventType.MESSAGE, "hello")
    assert received == ["hello"]


@pytest.mark.asyncio
async def test_manager_event_isolation(tmp_path: Path) -> None:
    mgr = ClientManager(storage_dir=str(tmp_path))
    acc1: list[str] = []

    @mgr.event(client_id="acc1", event=EventType.MESSAGE)
    async def _h(msg: str) -> None:
        acc1.append(msg)

    await mgr._route_manager_event("acc2", EventType.MESSAGE, "x")
    assert acc1 == []


@pytest.mark.asyncio
async def test_start_stop_all(tmp_path: Path) -> None:
    mgr = ClientManager(storage_dir=str(tmp_path))
    m = MagicMock()
    m.start = AsyncMock()
    m.start_listening = AsyncMock()
    m.stop_listening = AsyncMock()
    mgr.clients["x"] = m  # type: ignore[assignment]
    await mgr.start_all()
    m.start.assert_awaited_once()
    m.start_listening.assert_awaited_once()
    await mgr.stop_all()
    m.stop_listening.assert_awaited_once()


@pytest.mark.asyncio
async def test_broadcast(tmp_path: Path) -> None:
    mgr = ClientManager(storage_dir=str(tmp_path))
    a = MagicMock()
    a._state = MagicMock()
    a.send_message = AsyncMock()
    mgr.clients["u1"] = a  # type: ignore[assignment]
    await mgr.broadcast("hi", "12345")
    a.send_message.assert_awaited_once_with("hi", "12345")


@pytest.mark.asyncio
async def test_json_session_storage_roundtrip(tmp_path: Path) -> None:
    store = JsonSessionStorage(tmp_path)
    snap = {"version": 1, "cookies": [{"name": "c_user", "value": "1", "path": "/"}]}
    await store.save("c1", snap)
    loaded = await store.load("c1")
    assert loaded == snap


@pytest.mark.asyncio
async def test_add_client_restore_from_storage(tmp_path: Path) -> None:
    snap = {"version": 1, "cookies": [{"name": "c_user", "value": "1", "path": "/"}]}
    store = JsonSessionStorage(tmp_path)
    await store.save("u1", snap)

    mock_state = MagicMock()
    with patch(
        "fbchat_muqit.manager.State.from_cookie_list", new_callable=AsyncMock
    ) as fc:
        fc.return_value = mock_state
        with patch("fbchat_muqit.manager.Client") as Ctor:
            inst = MagicMock()
            inst._manager_route = None
            inst._manager_client_id = ""
            Ctor.return_value = inst
            mgr = ClientManager(storage=store)
            await mgr.add_client("u1", "dummy.json", restore_from_storage=True)
            fc.assert_awaited_once()
            Ctor.assert_called_once()
            kw = Ctor.call_args.kwargs
            assert kw.get("initial_state") is mock_state
