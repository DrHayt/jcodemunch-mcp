"""Tests for the digest tool (v1.86.0)."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from jcodemunch_mcp.storage.index_store import IndexLoadStatus
from jcodemunch_mcp.tools import digest as digest_mod


class _FakeIndex:
    def __init__(self, source_root="/canonical/repo", n_symbols=100, n_files=10, langs=("python", "go")):
        self.source_root = source_root
        self.symbols = [{"language": langs[i % len(langs)], "name": f"sym_{i}"} for i in range(n_symbols)]
        self.source_files = [f"f_{i}.py" for i in range(n_files)]


@pytest.fixture
def isolated_state(monkeypatch, tmp_path):
    """Redirect CODE_INDEX_PATH so digest state goes to a clean dir."""
    monkeypatch.setenv("CODE_INDEX_PATH", str(tmp_path))
    return tmp_path


@pytest.fixture
def patched_index(monkeypatch):
    """Stub out IndexStore.load_index + resolve_repo so tests don't need real indexes."""
    fake = _FakeIndex()

    class _FakeStore:
        def __init__(self, base_path=None): pass
        def load_index(self, owner, name): return fake

    monkeypatch.setattr(digest_mod, "IndexStore", _FakeStore)
    monkeypatch.setattr(digest_mod, "resolve_repo", lambda repo, sp=None: ("local", "test-repo"))
    return fake


class TestComposeDigestBasic:
    def test_first_session_no_delta(self, isolated_state, patched_index, monkeypatch):
        """No prior state → render announces first session, no delta block."""
        monkeypatch.setattr(digest_mod, "_git_head", lambda root: "abc1234567890")
        # Mock the composers to return empty/safe data.
        monkeypatch.setattr(digest_mod, "_compose_hotspots", lambda *a, **kw: [])
        monkeypatch.setattr(digest_mod, "_compose_dead_code", lambda *a, **kw: [])

        result = digest_mod.compose_digest("anything")

        assert "error" not in result
        assert "first session" in result["briefing"]
        assert result["structured"]["repo"] == "local/test-repo"
        # State file should now exist for next call.
        state_path = digest_mod._state_path("local", "test-repo", None)
        assert state_path.exists()

    def test_second_session_computes_delta(
        self, isolated_state, patched_index, monkeypatch
    ):
        """Prior state present → _compose_delta is called with the prior SHA."""
        # Pre-write a state file as if we'd briefed once already.
        state_path = digest_mod._state_path("local", "test-repo", None)
        state_path.parent.mkdir(parents=True, exist_ok=True)
        state_path.write_text(
            json.dumps({"git_head": "old1111111", "session_at": "2026-05-08T00:00:00Z"}),
            encoding="utf-8",
        )

        monkeypatch.setattr(digest_mod, "_git_head", lambda root: "new2222222")

        delta_calls = []
        def fake_delta(owner, name, since, until, max_files, sp):
            delta_calls.append((since, until))
            return {
                "files": ["src/foo.py", "src/bar.py"],
                "added": [{"symbol_id": "src/foo.py::new_fn#function", "name": "new_fn"}],
                "modified": [],
                "removed": [],
                "from_sha": since,
                "to_sha": until,
            }
        monkeypatch.setattr(digest_mod, "_compose_delta", fake_delta)
        monkeypatch.setattr(digest_mod, "_compose_hotspots", lambda *a, **kw: [])
        monkeypatch.setattr(digest_mod, "_compose_dead_code", lambda *a, **kw: [])

        result = digest_mod.compose_digest("anything")

        assert delta_calls == [("old1111111", "new2222222")]
        assert "Files changed" in result["briefing"]
        assert "src/foo.py" in result["briefing"]
        assert "Added symbols" in result["briefing"]

    def test_state_persists_current_head(self, isolated_state, patched_index, monkeypatch):
        monkeypatch.setattr(digest_mod, "_git_head", lambda root: "fresh1234567")
        monkeypatch.setattr(digest_mod, "_compose_hotspots", lambda *a, **kw: [])
        monkeypatch.setattr(digest_mod, "_compose_dead_code", lambda *a, **kw: [])

        digest_mod.compose_digest("anything")

        state_path = digest_mod._state_path("local", "test-repo", None)
        state = json.loads(state_path.read_text(encoding="utf-8"))
        assert state["git_head"] == "fresh1234567"
        assert "session_at" in state


class TestComposeDigestSections:
    def test_hotspot_section_renders(self, isolated_state, patched_index, monkeypatch):
        monkeypatch.setattr(digest_mod, "_git_head", lambda root: "sha")
        monkeypatch.setattr(digest_mod, "_compose_dead_code", lambda *a, **kw: [])
        monkeypatch.setattr(
            digest_mod, "_compose_hotspots",
            lambda *a, **kw: [
                {"symbol_id": "src/server.py::call_tool#function", "name": "call_tool", "hotspot_score": 100.5},
            ],
        )

        result = digest_mod.compose_digest("anything")

        assert "Risk surface" in result["briefing"]
        assert "call_tool" in result["briefing"]
        assert "100.5" in result["briefing"]

    def test_dead_code_section_renders(self, isolated_state, patched_index, monkeypatch):
        monkeypatch.setattr(digest_mod, "_git_head", lambda root: "sha")
        monkeypatch.setattr(digest_mod, "_compose_hotspots", lambda *a, **kw: [])
        monkeypatch.setattr(
            digest_mod, "_compose_dead_code",
            lambda *a, **kw: [
                {"symbol_id": "src/legacy.py::old_helper#function", "name": "old_helper"},
            ],
        )

        result = digest_mod.compose_digest("anything")

        assert "Dead-code candidates" in result["briefing"]
        assert "old_helper" in result["briefing"]


class TestComposeDigestErrors:
    def test_returns_error_when_repo_unresolvable(self, monkeypatch, isolated_state):
        def boom(repo, sp=None):
            raise ValueError("Repository not found: bogus")
        monkeypatch.setattr(digest_mod, "resolve_repo", boom)

        result = digest_mod.compose_digest("bogus")

        assert "error" in result
        assert "not found" in result["error"]

    def test_returns_error_when_index_missing(self, monkeypatch, isolated_state):
        class _NullStore:
            def __init__(self, base_path=None): pass
            def load_index(self, *a, **kw): return None
            def inspect_index(self, owner, name):
                return IndexLoadStatus(
                    repo=f"{owner}/{name}",
                    owner=owner,
                    name=name,
                    backend="none",
                    index_present=False,
                    loadable=False,
                    status="missing",
                    load_error="missing",
                )

        monkeypatch.setattr(digest_mod, "IndexStore", _NullStore)
        monkeypatch.setattr(digest_mod, "resolve_repo", lambda r, sp=None: ("local", "name"))

        result = digest_mod.compose_digest("anything")
        assert "error" in result
        assert "not loadable" in result["error"]


class TestStateFileHelpers:
    def test_state_path_uses_env_var(self, monkeypatch, tmp_path):
        monkeypatch.setenv("CODE_INDEX_PATH", str(tmp_path))
        path = digest_mod._state_path("owner", "name")
        assert tmp_path in path.parents
        assert path.name == "owner--name.json"

    def test_read_state_returns_empty_for_missing(self, tmp_path):
        path = tmp_path / "no.json"
        assert digest_mod._read_state(path) == {}

    def test_read_state_tolerates_corrupt_json(self, tmp_path):
        path = tmp_path / "bad.json"
        path.write_text("not json at all", encoding="utf-8")
        assert digest_mod._read_state(path) == {}

    def test_write_then_read_roundtrip(self, tmp_path):
        path = tmp_path / "state.json"
        digest_mod._write_state(path, {"git_head": "abc", "session_at": "now"})
        assert digest_mod._read_state(path) == {"git_head": "abc", "session_at": "now"}


class TestRenderMarkdown:
    def test_renders_minimal_briefing(self):
        s = {
            "repo": "owner/name",
            "current_head": "abc1234567890",
            "prior_head": None,
            "n_symbols": 50,
            "n_files": 5,
            "languages": "python",
        }
        out = digest_mod._render_markdown(s)
        assert "owner/name" in out
        assert "50 symbols" in out
        assert "5 files" in out
        assert "first session" in out

    def test_truncates_long_symbol_ids(self):
        long_id = "src/" + ("nested/" * 20) + "tool.py::do_thing#function"
        truncated = digest_mod._truncate_symbol_id(long_id, max_len=40)
        assert len(truncated) <= 40
        assert truncated.startswith("...")
        assert truncated.endswith("do_thing#function")
