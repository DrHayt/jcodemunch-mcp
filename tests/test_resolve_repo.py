"""Tests for resolve_repo tool."""

import hashlib
import sqlite3

import pytest

from jcodemunch_mcp.storage import INDEX_VERSION, IndexStore
from jcodemunch_mcp.storage.sqlite_store import _cache_evict
from jcodemunch_mcp.tools.resolve_repo import resolve_repo, _compute_repo_id
from jcodemunch_mcp.watcher import _local_repo_id
from jcodemunch_mcp.tools.index_folder import index_folder


class TestComputeRepoId:
    def test_deterministic_id_matches_local_repo_id(self, tmp_path):
        """_compute_repo_id must produce the same ID as _local_repo_id."""
        folder = tmp_path / "my-project"
        folder.mkdir()
        from pathlib import Path
        assert _compute_repo_id(Path(folder)) == _local_repo_id(str(folder))

    def test_different_paths_produce_different_ids(self, tmp_path):
        left = tmp_path / "left" / "shared"
        right = tmp_path / "right" / "shared"
        left.mkdir(parents=True)
        right.mkdir(parents=True)
        from pathlib import Path
        assert _compute_repo_id(Path(left)) != _compute_repo_id(Path(right))


class TestResolveRepo:
    def _index_project(self, tmp_path, name="loadability"):
        project = tmp_path / name
        project.mkdir()
        (project / "main.py").write_text("def hello(): pass\n")
        store_path = str(tmp_path / "store")

        index_folder(str(project), use_ai_summaries=False, storage_path=store_path)
        repo_id = _compute_repo_id(project)
        owner, repo_name = repo_id.split("/", 1)
        return project, store_path, owner, repo_name

    def _mutate_sqlite_meta(self, store_path, owner, name, sql, params=()):
        store = IndexStore(base_path=store_path)
        db_path = store._sqlite._db_path(owner, name)
        conn = sqlite3.connect(str(db_path))
        try:
            conn.execute(sql, params)
            conn.commit()
        finally:
            conn.close()
        _cache_evict(owner, name)

    def test_resolve_exact_indexed_root(self, tmp_path):
        """Resolving an indexed root returns indexed: true with metadata."""
        project = tmp_path / "myproject"
        project.mkdir()
        (project / "main.py").write_text("def hello(): pass\n")
        store_path = str(tmp_path / "store")

        index_folder(str(project), use_ai_summaries=False, storage_path=store_path)

        result = resolve_repo(str(project), storage_path=store_path)
        assert result["found"] is True
        assert result["indexed"] is True
        assert result["repo"].startswith("local/myproject-")
        assert result["symbol_count"] >= 1
        assert result["file_count"] >= 1
        assert "hint" not in result

    def test_resolve_future_version_index_reports_unloadable(self, tmp_path):
        """A present but future-version SQLite index is not queryable."""
        project, store_path, owner, name = self._index_project(tmp_path, "futurever")
        self._mutate_sqlite_meta(
            store_path,
            owner,
            name,
            "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
            ("index_version", str(INDEX_VERSION + 100)),
        )

        result = resolve_repo(str(project), storage_path=store_path)

        assert result["found"] is True
        assert result["index_present"] is True
        assert result["indexed"] is False
        assert result["loadable"] is False
        assert result["status"] == "sqlite_future_version"
        assert result["load_error"] == "sqlite_future_version"
        assert result["hint"]

    def test_resolve_missing_meta_index_reports_unloadable(self, tmp_path):
        """A present SQLite index without metadata is not queryable."""
        project, store_path, owner, name = self._index_project(tmp_path, "missingmeta")
        self._mutate_sqlite_meta(store_path, owner, name, "DELETE FROM meta")

        result = resolve_repo(str(project), storage_path=store_path)

        assert result["found"] is True
        assert result["index_present"] is True
        assert result["indexed"] is False
        assert result["loadable"] is False
        assert result["status"] == "sqlite_missing_meta"
        assert result["load_error"] == "sqlite_missing_meta"
        assert result["hint"]

    def test_resolve_subdirectory_via_git(self, tmp_path, monkeypatch):
        """Resolving a subdirectory finds the repo via git root."""
        import subprocess
        project = tmp_path / "gitrepo"
        project.mkdir()
        subprocess.run(["git", "init"], cwd=str(project), capture_output=True)
        subdir = project / "src" / "pkg"
        subdir.mkdir(parents=True)
        (project / "main.py").write_text("def top(): pass\n")
        store_path = str(tmp_path / "store")

        index_folder(str(project), use_ai_summaries=False, storage_path=store_path)

        result = resolve_repo(str(subdir), storage_path=store_path)
        assert result["found"] is True
        assert result["indexed"] is True
        assert result["repo"].startswith("local/gitrepo-")

    def test_resolve_non_indexed_path(self, tmp_path):
        """Non-indexed path returns indexed: false with hint."""
        project = tmp_path / "unindexed"
        project.mkdir()
        store_path = str(tmp_path / "store")

        result = resolve_repo(str(project), storage_path=store_path)
        assert result["found"] is True
        assert result["indexed"] is False
        assert "repo" in result
        assert result["hint"] == "call index_folder to index this path"

    def test_resolve_nonexistent_path(self, tmp_path):
        """Nonexistent path returns found: false with error."""
        result = resolve_repo(str(tmp_path / "does-not-exist"))
        assert result["found"] is False
        assert result["indexed"] is False
        assert "error" in result

    def test_resolve_file_uses_parent(self, tmp_path):
        """Resolving a file path uses its parent directory."""
        project = tmp_path / "filetest"
        project.mkdir()
        pyfile = project / "app.py"
        pyfile.write_text("def run(): pass\n")
        store_path = str(tmp_path / "store")

        index_folder(str(project), use_ai_summaries=False, storage_path=store_path)

        result = resolve_repo(str(pyfile), storage_path=store_path)
        assert result["found"] is True
        assert result["indexed"] is True
        assert result["repo"].startswith("local/filetest-")

    def test_result_has_timing(self, tmp_path):
        """Result always includes _meta with timing_ms."""
        result = resolve_repo(str(tmp_path))
        assert "_meta" in result
        assert "timing_ms" in result["_meta"]


class TestWorktreeCanonicalCandidates:
    """Issue #277 — when a path is a Git worktree of an already-indexed
    canonical checkout, surface the canonical repo as a candidate instead
    of treating the worktree as a fresh unindexed target.
    """

    def _git(self, *args, cwd):
        import subprocess
        env = {
            "GIT_AUTHOR_NAME": "test",
            "GIT_AUTHOR_EMAIL": "t@example.com",
            "GIT_COMMITTER_NAME": "test",
            "GIT_COMMITTER_EMAIL": "t@example.com",
        }
        result = subprocess.run(
            ["git", *args],
            cwd=str(cwd),
            capture_output=True,
            text=True,
            env={**__import__("os").environ, **env},
        )
        assert result.returncode == 0, f"git {args} failed: {result.stderr}"
        return result.stdout

    def test_worktree_surfaces_canonical_candidate(self, tmp_path):
        """A worktree of an already-indexed repo lists the canonical as a candidate."""
        canonical = tmp_path / "canonical"
        canonical.mkdir()
        self._git("init", "-b", "main", cwd=canonical)
        (canonical / "main.py").write_text("def hello(): return 1\n", encoding="utf-8")
        self._git("add", "main.py", cwd=canonical)
        self._git("commit", "-m", "initial", cwd=canonical)

        store_path = str(tmp_path / "store")
        index_folder(str(canonical), use_ai_summaries=False, storage_path=store_path)

        # Create a linked worktree on a new branch (sibling path).
        worktree = tmp_path / "wt-feature"
        self._git(
            "worktree", "add", "-b", "feature", str(worktree), cwd=canonical
        )

        result = resolve_repo(str(worktree), storage_path=store_path)
        assert result["found"] is True
        assert result["indexed"] is False, (
            "worktree path itself isn't indexed — that's the whole point"
        )
        assert "canonical_candidates" in result, (
            f"expected canonical_candidates in {result}"
        )
        assert len(result["canonical_candidates"]) == 1
        cand = result["canonical_candidates"][0]
        assert cand["repo"].startswith("local/canonical-")
        assert cand["rationale"] == "shared --git-common-dir"
        assert "Git worktree" in result["hint"]

    def test_unrelated_unindexed_path_has_no_candidates(self, tmp_path):
        """A non-Git, non-worktree path stays on the original hint with no candidates."""
        canonical = tmp_path / "canonical"
        canonical.mkdir()
        self._git("init", cwd=canonical)
        (canonical / "main.py").write_text("def hello(): return 1\n", encoding="utf-8")
        self._git("add", "main.py", cwd=canonical)
        self._git("commit", "-m", "initial", cwd=canonical)

        store_path = str(tmp_path / "store")
        index_folder(str(canonical), use_ai_summaries=False, storage_path=store_path)

        # An unrelated empty directory — not a worktree, not indexed.
        unrelated = tmp_path / "unrelated"
        unrelated.mkdir()

        result = resolve_repo(str(unrelated), storage_path=store_path)
        assert result["indexed"] is False
        assert "canonical_candidates" not in result
        assert result["hint"] == "call index_folder to index this path"
