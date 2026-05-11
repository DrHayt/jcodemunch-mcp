"""Tests for assemble_task_context — task-aware orchestrator."""

from pathlib import Path

from jcodemunch_mcp.tools.assemble_task_context import (
    assemble_task_context,
    _classify_intent,
)
from jcodemunch_mcp.tools.index_folder import index_folder


def _make_repo(tmp_path: Path, files: dict) -> tuple[str, str]:
    for rel, content in files.items():
        p = tmp_path / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content, encoding="utf-8")
    storage = str(tmp_path / ".index")
    result = index_folder(str(tmp_path), use_ai_summaries=False, storage_path=storage)
    repo_id = result.get("repo", str(tmp_path))
    return repo_id, storage


_REPO = {
    "core.py": (
        "class IndexStore:\n"
        "    def load_index(self, owner, name):\n"
        "        return None\n\n"
        "    def save_index(self, owner, name, idx):\n"
        "        pass\n"
    ),
    "subclass.py": (
        "from core import IndexStore\n\n"
        "class FastIndexStore(IndexStore):\n"
        "    def load_index(self, owner, name):\n"
        "        return {'cached': True}\n"
    ),
    "consumer.py": (
        "from core import IndexStore\n\n"
        "def serve(store: IndexStore):\n"
        "    return store.load_index('a', 'b')\n"
    ),
}


class TestIntentClassification:
    def test_explore_keywords(self):
        intent, conf, kw = _classify_intent("show me the structure of this codebase")
        assert intent == "explore"
        assert conf >= 0.5

    def test_debug_keywords(self):
        intent, _, _ = _classify_intent("why does this throw an exception?")
        assert intent == "debug"

    def test_refactor_keywords(self):
        intent, _, _ = _classify_intent("refactor this to extract a helper")
        assert intent == "refactor"

    def test_extend_keywords(self):
        intent, _, _ = _classify_intent("add a new endpoint that supports DELETE")
        assert intent == "extend"

    def test_audit_keywords(self):
        intent, _, _ = _classify_intent("audit for dead code and unused symbols")
        assert intent == "audit"

    def test_review_keywords(self):
        intent, _, _ = _classify_intent("review this PR and check blast radius")
        assert intent == "review"

    def test_default_to_explore_when_unclear(self):
        intent, conf, kw = _classify_intent("just some random text 123")
        assert intent == "explore"
        assert kw == []


class TestAssembleHappyPath:
    def test_classifies_and_runs_stages(self, tmp_path):
        repo, storage = _make_repo(tmp_path, _REPO)
        result = assemble_task_context(
            repo=repo, task="refactor IndexStore class",
            token_budget=5000, storage_path=storage,
        )
        assert "error" not in result
        assert result["intent_detected"] == "refactor"
        assert "anchor" in result["stages_run"]
        # IndexStore should be auto-extracted as an anchor
        assert any("IndexStore" in a for a in result["anchors"])

    def test_entries_have_source_attribution(self, tmp_path):
        repo, storage = _make_repo(tmp_path, _REPO)
        result = assemble_task_context(
            repo=repo, task="refactor IndexStore",
            token_budget=5000, storage_path=storage,
        )
        for entry in result["entries"]:
            assert "stage" in entry
            assert "source_tool" in entry
            assert "tokens" in entry

    def test_explicit_intent_overrides_classification(self, tmp_path):
        repo, storage = _make_repo(tmp_path, _REPO)
        result = assemble_task_context(
            repo=repo, task="show me everything",
            intent="audit", token_budget=5000, storage_path=storage,
        )
        assert result["intent_detected"] == "audit"
        assert result["intent_confidence"] == 1.0


class TestAssembleBudget:
    def test_token_budget_honored(self, tmp_path):
        repo, storage = _make_repo(tmp_path, _REPO)
        result = assemble_task_context(
            repo=repo, task="explore this repo",
            token_budget=100, storage_path=storage,
        )
        assert result["total_tokens"] <= 100 or result["entry_count"] == 1

    def test_zero_budget_rejected(self, tmp_path):
        repo, storage = _make_repo(tmp_path, _REPO)
        result = assemble_task_context(
            repo=repo, task="anything",
            token_budget=0, storage_path=storage,
        )
        assert "error" in result


class TestAssembleAnchors:
    def test_auto_extracts_anchor_from_task(self, tmp_path):
        repo, storage = _make_repo(tmp_path, _REPO)
        result = assemble_task_context(
            repo=repo, task="debug why FastIndexStore fails",
            token_budget=5000, storage_path=storage,
        )
        assert "error" not in result
        assert any("FastIndexStore" in a for a in result["anchors"])

    def test_explicit_symbols_override_auto_extraction(self, tmp_path):
        repo, storage = _make_repo(tmp_path, _REPO)
        result = assemble_task_context(
            repo=repo, task="some unrelated task",
            symbols=["IndexStore"],
            token_budget=5000, storage_path=storage,
        )
        assert any("IndexStore" in a for a in result["anchors"])


class TestAssembleInclude:
    def test_include_restricts_stages(self, tmp_path):
        repo, storage = _make_repo(tmp_path, _REPO)
        result = assemble_task_context(
            repo=repo, task="refactor IndexStore",
            include=["anchor"], token_budget=5000, storage_path=storage,
        )
        assert "error" not in result
        # Only anchor stage should run
        assert set(result["stages_run"]).issubset({"anchor", "cross_repo"})


class TestAssembleErrors:
    def test_unindexed_repo(self, tmp_path):
        storage = str(tmp_path / ".index")
        result = assemble_task_context(
            repo="nope/repo", task="explore",
            storage_path=storage,
        )
        assert "error" in result
        assert result["loadable"] is False
        assert result["status"] == "missing"
        assert result["load_error"] == "missing"

    def test_invalid_intent(self, tmp_path):
        repo, storage = _make_repo(tmp_path, _REPO)
        result = assemble_task_context(
            repo=repo, task="anything",
            intent="bogus_intent", storage_path=storage,
        )
        assert "error" in result


class TestAssembleResultShape:
    def test_required_fields_present(self, tmp_path):
        repo, storage = _make_repo(tmp_path, _REPO)
        result = assemble_task_context(
            repo=repo, task="explore this repo",
            token_budget=5000, storage_path=storage,
        )
        assert "entries" in result
        assert "intent_detected" in result
        assert "intent_confidence" in result
        assert "intent_keywords_matched" in result
        assert "strategy_applied" in result
        assert "stages_run" in result
        assert "anchors" in result
        assert "total_tokens" in result
        assert "budget_tokens" in result
        assert "entry_count" in result
        assert "_meta" in result

    def test_strategy_applied_is_full_intent_strategy(self, tmp_path):
        """strategy_applied should reflect the full intent recipe even when
        include narrows what actually runs — so callers can see what was
        considered vs. what fired."""
        repo, storage = _make_repo(tmp_path, _REPO)
        result = assemble_task_context(
            repo=repo, task="explore",
            include=["orientation"], token_budget=5000, storage_path=storage,
        )
        # strategy_applied is the full intent strategy
        assert "orientation" in result["strategy_applied"]
        assert "hotspots" in result["strategy_applied"]
        # stages_run only contains what include allowed
        assert "hotspots" not in result["stages_run"]
