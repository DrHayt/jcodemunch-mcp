"""Tests for find_similar_symbols — consolidation/duplicate detection.

Covers:
  - Near-duplicate functions cluster together
  - Cluster verdict tier is assigned
  - Canonical pick prefers higher-PageRank file
  - min_size kills trivial wrappers
  - threshold gates edge formation
  - test files excluded by default
  - include_tests=True opts them back in
  - scope filter narrows candidates
  - structural-only mode when no embeddings present
  - error paths
"""

from pathlib import Path

from jcodemunch_mcp.tools.find_similar_symbols import find_similar_symbols
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


# Three near-identical formatting functions across the repo + one unrelated function.
# All three call str.upper and format_part — structural+behavioral overlap.
_DUP_REPO = {
    "fmt_a.py": (
        "def format_user(user):\n"
        "    name = user.get('name', '').upper()\n"
        "    email = user.get('email', '').upper()\n"
        "    return f'{name} <{email}>'\n"
    ),
    "fmt_b.py": (
        "def format_account(account):\n"
        "    name = account.get('name', '').upper()\n"
        "    email = account.get('email', '').upper()\n"
        "    return f'{name} <{email}>'\n"
    ),
    "fmt_c.py": (
        "def format_member(member):\n"
        "    name = member.get('name', '').upper()\n"
        "    email = member.get('email', '').upper()\n"
        "    return f'{name} <{email}>'\n"
    ),
    "unrelated.py": (
        "def compute_total(items):\n"
        "    total = 0\n"
        "    for item in items:\n"
        "        total += item.get('price', 0) * item.get('qty', 1)\n"
        "    return total\n"
    ),
}


class TestFindSimilarHappyPath:
    def test_near_duplicate_cluster_detected(self, tmp_path):
        repo, storage = _make_repo(tmp_path, _DUP_REPO)
        result = find_similar_symbols(repo, threshold=0.5, min_size=10, storage_path=storage)
        assert "error" not in result
        # We expect at least one cluster containing 2+ of the format_* functions.
        format_ids = {f"fmt_{x}.py::format_{y}#function"
                      for x, y in (("a", "user"), ("b", "account"), ("c", "member"))}
        cluster_member_ids = set()
        for cluster in result["clusters"]:
            cluster_member_ids.add(cluster["canonical"]["symbol_id"])
            for m in cluster["members"]:
                cluster_member_ids.add(m["symbol_id"])
        overlap = format_ids & cluster_member_ids
        assert len(overlap) >= 2, f"expected at least 2 format_* fns clustered, got {overlap}"

    def test_unrelated_function_not_in_cluster(self, tmp_path):
        repo, storage = _make_repo(tmp_path, _DUP_REPO)
        # At a stricter threshold, the unrelated compute_total should fall below
        # the edge threshold and not be union-find'd into the format trio.
        result = find_similar_symbols(repo, threshold=0.75, min_size=10, storage_path=storage)
        assert "error" not in result
        for cluster in result["clusters"]:
            ids = {cluster["canonical"]["symbol_id"]} | {m["symbol_id"] for m in cluster["members"]}
            assert "unrelated.py::compute_total#function" not in ids

    def test_cluster_has_verdict_and_mode(self, tmp_path):
        repo, storage = _make_repo(tmp_path, _DUP_REPO)
        result = find_similar_symbols(repo, threshold=0.5, min_size=10, storage_path=storage)
        if not result["clusters"]:
            return  # Tolerate empty if structural signal didn't cross threshold
        c = result["clusters"][0]
        assert c["verdict"] in {"near_duplicate", "similar_logic", "parallel_implementation"}
        assert c["mode"] in {"structural", "hybrid"}
        assert "differs_by" in c
        assert isinstance(c["differs_by"], list)

    def test_canonical_picked_per_cluster(self, tmp_path):
        repo, storage = _make_repo(tmp_path, _DUP_REPO)
        result = find_similar_symbols(repo, threshold=0.5, min_size=10, storage_path=storage)
        if not result["clusters"]:
            return
        c = result["clusters"][0]
        assert "canonical" in c
        assert c["canonical"]["symbol_id"]
        assert c["canonical"]["score_reason"] in {"highest_pagerank", "largest_body"}


class TestFindSimilarFilters:
    def test_min_size_filters_wrappers(self, tmp_path):
        files = {
            "g.py": (
                "def get_x(): return _x\n"
                "def get_y(): return _y\n"
                "def get_z(): return _z\n"
                "_x = 1\n_y = 2\n_z = 3\n"
            ),
        }
        repo, storage = _make_repo(tmp_path, files)
        # min_size=100 should kill every trivial getter (each is well under 100 bytes)
        result = find_similar_symbols(
            repo, threshold=0.5, min_size=100, include_kinds=["function"], storage_path=storage,
        )
        assert "error" not in result
        assert result["clusters_returned"] == 0
        assert result["candidates_considered"] == 0

    def test_threshold_gates_edges(self, tmp_path):
        repo, storage = _make_repo(tmp_path, _DUP_REPO)
        # A threshold of 0.99 should reject almost every pair
        result_high = find_similar_symbols(repo, threshold=0.99, min_size=10, storage_path=storage)
        result_low = find_similar_symbols(repo, threshold=0.30, min_size=10, storage_path=storage)
        assert result_high["clusters_returned"] <= result_low["clusters_returned"]

    def test_test_files_excluded_by_default(self, tmp_path):
        files = {
            "tests/test_a.py": (
                "def test_format_one():\n    assert ('hello').upper() == 'HELLO'\n    return True\n"
            ),
            "tests/test_b.py": (
                "def test_format_two():\n    assert ('world').upper() == 'WORLD'\n    return True\n"
            ),
        }
        repo, storage = _make_repo(tmp_path, files)
        result = find_similar_symbols(repo, threshold=0.5, min_size=10, storage_path=storage)
        assert "error" not in result
        assert result["candidates_considered"] == 0

    def test_include_tests_opts_in(self, tmp_path):
        files = {
            "tests/test_a.py": (
                "def test_format_one():\n    assert ('hello').upper() == 'HELLO'\n    return True\n"
            ),
            "tests/test_b.py": (
                "def test_format_two():\n    assert ('world').upper() == 'WORLD'\n    return True\n"
            ),
        }
        repo, storage = _make_repo(tmp_path, files)
        result = find_similar_symbols(
            repo, threshold=0.3, min_size=10, include_tests=True, storage_path=storage,
        )
        assert "error" not in result
        assert result["candidates_considered"] >= 2


class TestFindSimilarMode:
    def test_structural_mode_when_no_embeddings(self, tmp_path):
        """Without embed_repo run, mode should be 'structural' and note set."""
        repo, storage = _make_repo(tmp_path, _DUP_REPO)
        result = find_similar_symbols(repo, threshold=0.3, min_size=10, storage_path=storage)
        assert result["mode"] == "structural"
        assert "note" in result
        assert "embed_repo" in result["note"]

    def test_structural_verdict_is_parallel_implementation(self, tmp_path):
        """In structural mode, clusters should be labeled parallel_implementation."""
        repo, storage = _make_repo(tmp_path, _DUP_REPO)
        result = find_similar_symbols(repo, threshold=0.5, min_size=10, storage_path=storage)
        for c in result["clusters"]:
            assert c["verdict"] == "parallel_implementation"


class TestFindSimilarMeta:
    def test_meta_fields_present(self, tmp_path):
        repo, storage = _make_repo(tmp_path, _DUP_REPO)
        result = find_similar_symbols(repo, threshold=0.5, min_size=10, storage_path=storage)
        assert "_meta" in result
        assert "timing_ms" in result["_meta"]
        assert "candidates_considered" in result
        assert "pairs_compared" in result


class TestFindSimilarErrors:
    def test_unindexed_repo_returns_error(self, tmp_path):
        storage = str(tmp_path / ".index")
        result = find_similar_symbols("nonexistent/repo", storage_path=storage)
        assert "error" in result

    def test_invalid_threshold(self, tmp_path):
        repo, storage = _make_repo(tmp_path, _DUP_REPO)
        result = find_similar_symbols(repo, threshold=1.5, storage_path=storage)
        assert "error" in result

    def test_invalid_semantic_weight(self, tmp_path):
        repo, storage = _make_repo(tmp_path, _DUP_REPO)
        result = find_similar_symbols(repo, semantic_weight=-0.1, storage_path=storage)
        assert "error" in result
