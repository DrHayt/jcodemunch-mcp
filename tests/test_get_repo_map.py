"""Tests for get_repo_map — the query-less, signature-level orientation map.

Covers:
  - Happy path: PageRank-ranked files with signatures only (no source bodies)
  - Token budget honored: large file stops emission when budget exhausted
  - max_per_file caps signatures per file
  - scope filter narrows to a subdirectory
  - include_kinds restricts kinds
  - error paths: missing repo, invalid budget
"""

from pathlib import Path

from jcodemunch_mcp.tools.get_repo_map import get_repo_map
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


# Three-file repo with a clear import hub: engine.py is imported by main.py and worker.py
# so PageRank should rank engine.py highest.
_HUB_REPO = {
    "engine.py": (
        "class Engine:\n"
        "    \"\"\"Core engine.\"\"\"\n"
        "    def run(self):\n"
        "        return 1\n\n"
        "    def stop(self):\n"
        "        return 0\n\n"
        "def boot():\n"
        "    return Engine()\n"
    ),
    "utils.py": (
        "def format_date(d):\n    return str(d)\n\n"
        "def parse_date(s):\n    return s\n"
    ),
    "main.py": (
        "from engine import Engine, boot\n"
        "from utils import format_date\n\n"
        "def main():\n    e = boot()\n    e.run()\n"
    ),
    "worker.py": (
        "from engine import Engine\n\n"
        "def work():\n    e = Engine()\n    return e.run()\n"
    ),
}


class TestGetRepoMapHappyPath:
    def test_returns_files_ranked_by_pagerank(self, tmp_path):
        repo, storage = _make_repo(tmp_path, _HUB_REPO)
        result = get_repo_map(repo, storage_path=storage)
        assert "error" not in result
        assert result["files_included"] > 0
        # engine.py has two importers; it should rank ahead of leaf files.
        paths = [f["path"] for f in result["files"]]
        assert "engine.py" in paths
        engine_rank = next(f["rank"] for f in result["files"] if f["path"] == "engine.py")
        # If a leaf like utils.py made it in, engine.py must outrank it.
        if "utils.py" in paths:
            utils_rank = next(f["rank"] for f in result["files"] if f["path"] == "utils.py")
            assert engine_rank < utils_rank

    def test_signatures_only_no_source_bodies(self, tmp_path):
        repo, storage = _make_repo(tmp_path, _HUB_REPO)
        result = get_repo_map(repo, storage_path=storage)
        assert "error" not in result
        for f in result["files"]:
            for sym in f["symbols"]:
                # Map output is signatures only — no `source` key.
                assert "source" not in sym
                assert "signature" in sym
                # Engine class body shouldn't leak through the signature.
                assert "return 1" not in sym["signature"]

    def test_meta_fields_present(self, tmp_path):
        repo, storage = _make_repo(tmp_path, _HUB_REPO)
        result = get_repo_map(repo, storage_path=storage)
        assert "_meta" in result
        assert "timing_ms" in result["_meta"]
        assert "tokens_saved" in result["_meta"]


class TestGetRepoMapBudget:
    def test_token_budget_honored(self, tmp_path):
        repo, storage = _make_repo(tmp_path, _HUB_REPO)
        # Tiny budget should produce at most a handful of signatures.
        result = get_repo_map(repo, token_budget=20, storage_path=storage)
        assert "error" not in result
        assert result["total_tokens"] <= 20
        assert result["budget_tokens"] == 20

    def test_zero_budget_rejected(self, tmp_path):
        repo, storage = _make_repo(tmp_path, _HUB_REPO)
        result = get_repo_map(repo, token_budget=0, storage_path=storage)
        assert "error" in result

    def test_max_per_file_caps_emissions(self, tmp_path):
        repo, storage = _make_repo(tmp_path, _HUB_REPO)
        result = get_repo_map(repo, max_per_file=1, storage_path=storage)
        assert "error" not in result
        for f in result["files"]:
            assert len(f["symbols"]) <= 1


class TestGetRepoMapFilters:
    def test_scope_filter_narrows_files(self, tmp_path):
        files = {
            "src/core/engine.py": "class Engine:\n    pass\n",
            "src/util/helper.py": "def helper():\n    return 1\n",
            "tests/test_engine.py": "from src.core.engine import Engine\n\ndef test_x():\n    Engine()\n",
        }
        repo, storage = _make_repo(tmp_path, files)
        result = get_repo_map(repo, scope="src/core/*", storage_path=storage)
        assert "error" not in result
        # Only src/core paths should make it through.
        for f in result["files"]:
            assert f["path"].startswith("src/core")

    def test_include_kinds_filters_symbols(self, tmp_path):
        repo, storage = _make_repo(tmp_path, _HUB_REPO)
        result = get_repo_map(repo, include_kinds=["class"], storage_path=storage)
        assert "error" not in result
        for f in result["files"]:
            for sym in f["symbols"]:
                assert sym["kind"] == "class"


class TestGetRepoMapErrors:
    def test_unindexed_repo_returns_error(self, tmp_path):
        storage = str(tmp_path / ".index")
        result = get_repo_map("nonexistent/repo", storage_path=storage)
        assert "error" in result
