"""Handshake watchdog: when the MCP client never calls a handler, write a
one-line stderr hint instead of sitting silent forever.

Reproduces and locks the behaviour from the v1.81.3 client report against
Codex/rmcp where stdout corruption made the host wait 5h+ for a frame that
was never coming.
"""
from __future__ import annotations

import asyncio

import pytest

from jcodemunch_mcp import server as srv


@pytest.fixture(autouse=True)
def _reset_event():
    srv._handshake_event = None
    yield
    srv._handshake_event = None


def _make_event_in_running_loop() -> asyncio.Event:
    """asyncio.Event must be created inside a running loop in 3.10+."""
    return asyncio.Event()


def test_signal_handshake_is_safe_when_event_is_none():
    # No event yet (e.g. tests, or non-stdio transport): must not raise.
    srv._handshake_event = None
    srv._signal_handshake()


def test_signal_handshake_sets_event_once():
    async def _run():
        srv._handshake_event = _make_event_in_running_loop()
        assert not srv._handshake_event.is_set()
        srv._signal_handshake()
        assert srv._handshake_event.is_set()
        # Idempotent — calling again is a no-op
        srv._signal_handshake()
        assert srv._handshake_event.is_set()

    asyncio.run(_run())


def test_list_tools_signals_handshake():
    async def _run():
        srv._handshake_event = _make_event_in_running_loop()
        await srv.list_tools()
        assert srv._handshake_event.is_set()

    asyncio.run(_run())


def test_list_resources_signals_handshake():
    async def _run():
        srv._handshake_event = _make_event_in_running_loop()
        await srv.list_resources()
        assert srv._handshake_event.is_set()

    asyncio.run(_run())


def test_list_prompts_signals_handshake():
    async def _run():
        srv._handshake_event = _make_event_in_running_loop()
        await srv.list_prompts()
        assert srv._handshake_event.is_set()

    asyncio.run(_run())
