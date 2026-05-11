"""Tests for storage.process_locks — the v1.106.0 multi-process coordination
primitive used by watcher slots, save_index, and migrate_from_json.

Existing watcher-specific behavior is also covered in test_watcher_lock.py;
this file focuses on the generic (scope, target) API and the new metadata
fields (client_id, scope, target).
"""
import json
import os
import sys
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from jcodemunch_mcp.storage import process_locks
from jcodemunch_mcp.storage.process_locks import (
    LockHolder,
    acquire,
    current_holder_diagnostic,
    held,
    inspect,
    lock_path,
    release,
    _client_id,
    _is_pid_alive,
    _path_hash,
)


# ---------------------------------------------------------------------------
# _path_hash
# ---------------------------------------------------------------------------

class TestPathHash:
    def test_same_target_same_hash(self):
        assert _path_hash("/foo/bar") == _path_hash("/foo/bar")

    def test_different_targets_different_hash(self):
        assert _path_hash("/foo/bar") != _path_hash("/foo/baz")

    def test_owner_slug_format(self):
        # Non-path targets like "owner/name" hash deterministically too.
        assert _path_hash("acme/widget") == _path_hash("acme/widget")
        assert _path_hash("acme/widget") != _path_hash("acme/gadget")


# ---------------------------------------------------------------------------
# _client_id
# ---------------------------------------------------------------------------

class TestClientId:
    def test_explicit_env_var_wins(self, monkeypatch):
        monkeypatch.setenv("JCODEMUNCH_CLIENT_ID", "my-test-client")
        assert _client_id() == "my-test-client"

    def test_falls_back_to_argv0_basename(self, monkeypatch):
        monkeypatch.delenv("JCODEMUNCH_CLIENT_ID", raising=False)
        monkeypatch.setattr(sys, "argv", ["/some/path/claude"])
        assert _client_id() == "claude"

    def test_unknown_when_nothing_useful(self, monkeypatch):
        monkeypatch.delenv("JCODEMUNCH_CLIENT_ID", raising=False)
        monkeypatch.setattr(sys, "argv", [""])
        # Empty argv falls through to "unknown"
        assert _client_id() in {"unknown", ""}  # Allow either; both are safe sentinels


# ---------------------------------------------------------------------------
# acquire / release / inspect round-trip
# ---------------------------------------------------------------------------

class TestAcquireReleaseRoundTrip:
    def test_acquire_then_release(self, tmp_path):
        assert acquire("test", "alpha", str(tmp_path)) is True
        release("test", "alpha", str(tmp_path))
        # Acquiring again after release must succeed
        assert acquire("test", "alpha", str(tmp_path)) is True
        release("test", "alpha", str(tmp_path))

    def test_acquire_blocks_duplicate(self, tmp_path):
        assert acquire("test", "alpha", str(tmp_path)) is True
        try:
            assert acquire("test", "alpha", str(tmp_path)) is False
        finally:
            release("test", "alpha", str(tmp_path))

    def test_different_scopes_independent(self, tmp_path):
        # watcher + indexwrite on the same target must not collide
        assert acquire("watcher", "alpha", str(tmp_path)) is True
        try:
            assert acquire("indexwrite", "alpha", str(tmp_path)) is True
            release("indexwrite", "alpha", str(tmp_path))
        finally:
            release("watcher", "alpha", str(tmp_path))

    def test_different_targets_independent(self, tmp_path):
        assert acquire("test", "alpha", str(tmp_path)) is True
        try:
            assert acquire("test", "beta", str(tmp_path)) is True
            release("test", "beta", str(tmp_path))
        finally:
            release("test", "alpha", str(tmp_path))


# ---------------------------------------------------------------------------
# inspect
# ---------------------------------------------------------------------------

class TestInspect:
    def test_no_holder_returns_none(self, tmp_path):
        assert inspect("test", "nothing", str(tmp_path)) is None

    def test_holder_metadata_complete(self, tmp_path, monkeypatch):
        monkeypatch.setenv("JCODEMUNCH_CLIENT_ID", "test-runner")
        acquire("test", "alpha", str(tmp_path))
        try:
            h = inspect("test", "alpha", str(tmp_path))
            assert h is not None
            assert h.pid == os.getpid()
            assert h.client_id == "test-runner"
            assert h.scope == "test"
            assert h.target == "alpha"
            assert h.started_at  # ISO timestamp populated
            assert h.lock_path  # file path populated
        finally:
            release("test", "alpha", str(tmp_path))

    def test_stale_holder_returns_none(self, tmp_path):
        # Manually write a lock file with a dead PID
        dead_pid = os.getpid() + 999_999
        lf = lock_path("test", "alpha", str(tmp_path))
        lf.write_text(json.dumps({
            "scope": "test",
            "target": "alpha",
            "pid": dead_pid,
            "client_id": "ghost",
            "started_at": "2026-01-01T00:00:00+00:00",
        }), encoding="utf-8")
        assert inspect("test", "alpha", str(tmp_path)) is None

    def test_corrupt_metadata_returns_none(self, tmp_path):
        lf = lock_path("test", "alpha", str(tmp_path))
        lf.write_text("not valid json {{{", encoding="utf-8")
        assert inspect("test", "alpha", str(tmp_path)) is None


# ---------------------------------------------------------------------------
# LockHolder
# ---------------------------------------------------------------------------

class TestLockHolder:
    def test_as_dict_omits_invalid_age(self):
        h = LockHolder(
            scope="test", target="alpha", pid=1, client_id="x",
            started_at="not-a-timestamp", lock_path="/tmp/x.lock",
        )
        d = h.as_dict()
        assert "age_seconds" not in d
        assert d["pid"] == 1

    def test_as_dict_includes_age_when_parseable(self):
        h = LockHolder(
            scope="test", target="alpha", pid=1, client_id="x",
            started_at="2026-01-01T00:00:00+00:00", lock_path="/tmp/x.lock",
        )
        d = h.as_dict()
        assert "age_seconds" in d
        assert isinstance(d["age_seconds"], float)
        assert d["age_seconds"] > 0  # 2026-01-01 is in the past


# ---------------------------------------------------------------------------
# held() context manager
# ---------------------------------------------------------------------------

class TestHeldContextManager:
    def test_acquire_release_via_ctxmgr(self, tmp_path):
        with held("test", "alpha", str(tmp_path)) as got:
            assert got is True
        # Released — next acquire succeeds
        with held("test", "alpha", str(tmp_path)) as got2:
            assert got2 is True

    def test_returns_false_when_busy_and_no_wait(self, tmp_path):
        acquire("test", "alpha", str(tmp_path))
        try:
            with held("test", "alpha", str(tmp_path)) as got:
                assert got is False
        finally:
            release("test", "alpha", str(tmp_path))

    def test_wait_seconds_polls_until_lock_released(self, tmp_path):
        """Lock acquired by another 'process' is released; held() with wait
        should acquire after a brief poll."""
        # Pre-acquire, then release on a timer via thread
        acquire("test", "alpha", str(tmp_path))
        import threading
        def _release_after():
            time.sleep(0.3)
            release("test", "alpha", str(tmp_path))
        threading.Thread(target=_release_after, daemon=True).start()

        start = time.monotonic()
        with held(
            "test", "alpha", str(tmp_path),
            wait_seconds=5.0, poll_seconds=0.1,
        ) as got:
            elapsed = time.monotonic() - start
            assert got is True
            assert 0.2 < elapsed < 2.0  # waited briefly, not instantaneous, not too long

    def test_wait_seconds_gives_up(self, tmp_path):
        """If lock stays held longer than wait_seconds, held() returns False."""
        acquire("test", "alpha", str(tmp_path))
        try:
            start = time.monotonic()
            with held(
                "test", "alpha", str(tmp_path),
                wait_seconds=0.3, poll_seconds=0.1,
            ) as got:
                elapsed = time.monotonic() - start
                assert got is False
                assert 0.25 <= elapsed < 1.5
        finally:
            release("test", "alpha", str(tmp_path))


# ---------------------------------------------------------------------------
# current_holder_diagnostic
# ---------------------------------------------------------------------------

class TestDiagnostic:
    def test_empty_when_no_holder(self, tmp_path):
        assert current_holder_diagnostic("test", "alpha", str(tmp_path)) == ""

    def test_includes_holder_details(self, tmp_path, monkeypatch):
        monkeypatch.setenv("JCODEMUNCH_CLIENT_ID", "diagnostic-test")
        acquire("test", "alpha", str(tmp_path))
        try:
            d = current_holder_diagnostic("test", "alpha", str(tmp_path))
            assert "pid" in d
            assert "diagnostic-test" in d
        finally:
            release("test", "alpha", str(tmp_path))


# ---------------------------------------------------------------------------
# Integration: save_index serialises across processes (simulated)
# ---------------------------------------------------------------------------

class TestSaveIndexLock:
    def test_save_index_acquires_indexwrite_lock(self, tmp_path):
        """If indexwrite lock for owner/name is already held, save_index raises."""
        from jcodemunch_mcp.storage.sqlite_store import SQLiteIndexStore
        store = SQLiteIndexStore(base_path=str(tmp_path))

        # Pre-acquire the indexwrite lock to simulate another process holding it
        assert acquire("indexwrite", "test/repo", str(tmp_path)) is True
        try:
            # Use a very short wait so the test doesn't hang
            with patch.object(
                process_locks, "held",
                lambda *a, **kw: held(*a, **{**kw, "wait_seconds": 0.3, "poll_seconds": 0.1}),
            ):
                with pytest.raises(RuntimeError, match="index-write lock"):
                    store.save_index(
                        owner="test", name="repo",
                        source_files=["x.py"], symbols=[],
                        raw_files={"x.py": "print(1)"},
                    )
        finally:
            release("indexwrite", "test/repo", str(tmp_path))
