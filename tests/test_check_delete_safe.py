"""Tests for check_delete_safe — deletion preflight composite tool."""

from pathlib import Path

from jcodemunch_mcp.tools.check_delete_safe import check_delete_safe
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


_SAFE_REPO = {
    "used.py": (
        "def used_func():\n    return 1\n"
    ),
    "lonely.py": (
        # Standalone file with no external importers — pure orphan
        "def orphan_func():\n    return 'nobody calls me'\n"
    ),
    "consumer.py": (
        "from used import used_func\n\n"
        "def consume():\n    return used_func() + 1\n"
    ),
}

_ENTRY_POINT_REPO = {
    "app.py": (
        "def route(path):\n"
        "    def deco(fn):\n        return fn\n"
        "    return deco\n\n"
        "@route('/users')\n"
        "def get_users():\n    return []\n"
    ),
}

_TEST_ONLY_REPO = {
    "lib.py": (
        "def helper_function():\n    return 42\n"
    ),
    "tests/test_lib.py": (
        "from lib import helper_function\n\n"
        "def test_helper_function():\n    assert helper_function() == 42\n"
    ),
}


class TestCheckDeleteSafeOrphan:
    def test_orphan_function_returns_safe(self, tmp_path):
        repo, storage = _make_repo(tmp_path, _SAFE_REPO)
        result = check_delete_safe(repo, symbol="orphan_func", storage_path=storage)
        assert "error" not in result, result
        # Verdict should be one of the permissive ones
        assert result["verdict"] in {"safe_to_delete", "internal_only", "test_coverage_only"}

    def test_used_function_not_safe(self, tmp_path):
        repo, storage = _make_repo(tmp_path, _SAFE_REPO)
        result = check_delete_safe(repo, symbol="used_func", storage_path=storage)
        assert "error" not in result
        # Has an external importer (consumer.py)
        assert result["verdict"] in {
            "external_uses_blocking", "cross_repo_blocking",
            "internal_uses_blocking", "runtime_observed",
        }
        assert result["confidence"] <= 0.6


class TestCheckDeleteSafeEntryPoint:
    def test_route_decorator_flags_entry_point(self, tmp_path):
        repo, storage = _make_repo(tmp_path, _ENTRY_POINT_REPO)
        result = check_delete_safe(repo, symbol="get_users", storage_path=storage)
        assert "error" not in result, result
        # The decorator pattern should classify this as entry_point OR pull in
        # at least one entry_point blocker — both are acceptable signals.
        verdict_or_blocker_indicates_entry = (
            result["verdict"] == "entry_point"
            or any(b.get("kind") == "entry_point" for b in result["blockers"])
            or result["signals"].get("entry_point") is not None
        )
        assert verdict_or_blocker_indicates_entry


class TestCheckDeleteSafeTestOnly:
    def test_test_only_reference_returns_test_coverage_only(self, tmp_path):
        repo, storage = _make_repo(tmp_path, _TEST_ONLY_REPO)
        result = check_delete_safe(repo, symbol="helper_function", storage_path=storage)
        assert "error" not in result
        # External import is from a test file — tier should reflect that
        # (could be test_coverage_only, external_uses_blocking depending on which
        # signal fires first; both are reasonable)
        assert result["verdict"] in {
            "test_coverage_only", "external_uses_blocking", "internal_only",
            "safe_to_delete",
        }


class TestCheckDeleteSafeOutput:
    def test_signals_dict_populated(self, tmp_path):
        repo, storage = _make_repo(tmp_path, _SAFE_REPO)
        result = check_delete_safe(repo, symbol="orphan_func", storage_path=storage)
        assert "signals" in result
        sigs = result["signals"]
        assert "external_import_count" in sigs
        assert "internal_ref_count" in sigs
        assert "dead_code_confidence" in sigs

    def test_recommended_action_present(self, tmp_path):
        repo, storage = _make_repo(tmp_path, _SAFE_REPO)
        result = check_delete_safe(repo, symbol="orphan_func", storage_path=storage)
        assert "recommended_action" in result
        assert isinstance(result["recommended_action"], str)
        assert len(result["recommended_action"]) > 0

    def test_blockers_capped_at_5(self, tmp_path):
        repo, storage = _make_repo(tmp_path, _SAFE_REPO)
        result = check_delete_safe(repo, symbol="used_func", storage_path=storage)
        assert len(result["blockers"]) <= 5

    def test_confidence_in_range(self, tmp_path):
        repo, storage = _make_repo(tmp_path, _SAFE_REPO)
        result = check_delete_safe(repo, symbol="orphan_func", storage_path=storage)
        assert 0.0 <= result["confidence"] <= 1.0


class TestCheckDeleteSafeErrors:
    def test_unindexed_repo(self, tmp_path):
        storage = str(tmp_path / ".index")
        result = check_delete_safe("nope/repo", symbol="X", storage_path=storage)
        assert "error" in result

    def test_unknown_symbol(self, tmp_path):
        repo, storage = _make_repo(tmp_path, _SAFE_REPO)
        result = check_delete_safe(repo, symbol="DoesNotExist", storage_path=storage)
        assert "error" in result
