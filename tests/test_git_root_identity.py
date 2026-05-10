"""Tests for v1.95.0 git-root-aware index identity (#288 phase 1).

When a `.git/` is found above the indexed path and `git_root_identity`
is on (default), the storage identity comes from `git remote get-url
origin` so a clone of `elastic/kibana` indexes as `elastic/kibana`
regardless of the local folder name.  Repos without a usable origin
keep the v1.94 `local/<basename>-<hash>` identity but still record
the git_root for v1.96 merge logic.
"""

import subprocess
from pathlib import Path

import pytest

from jcodemunch_mcp import config as config_module
from jcodemunch_mcp.storage import IndexStore
from jcodemunch_mcp.storage.git_root import (
    GitRootIdentity,
    detect_git_root,
    _parse_owner_repo,
)
from jcodemunch_mcp.tools.index_folder import (
    _resolve_repo_identity,
    index_folder,
)


def _git(*args, cwd):
    subprocess.run(["git", *args], cwd=str(cwd), check=True, capture_output=True)


def _set_origin(path: Path, url: str) -> None:
    _git("remote", "add", "origin", url, cwd=path)


# ---------------------------------------------------------------------------
# detect_git_root unit tests
# ---------------------------------------------------------------------------


class TestDetectGitRoot:
    def test_no_git_returns_none(self, tmp_path):
        assert detect_git_root(str(tmp_path)) is None

    def test_git_root_with_origin_returns_owner_repo(self, tmp_path):
        repo = tmp_path / "kibana"
        repo.mkdir()
        _git("init", cwd=repo)
        _set_origin(repo, "https://github.com/elastic/kibana.git")

        ident = detect_git_root(str(repo))
        assert ident == GitRootIdentity(
            git_root=str(repo.resolve()),
            owner="elastic",
            name="kibana",
        )

    def test_git_root_without_origin_returns_local_basename(self, tmp_path):
        repo = tmp_path / "myproject"
        repo.mkdir()
        _git("init", cwd=repo)

        ident = detect_git_root(str(repo))
        assert ident is not None
        assert ident.owner == "local"
        assert ident.name == "myproject"
        assert ident.git_root == str(repo.resolve())

    def test_subdir_walks_up_to_git_root(self, tmp_path):
        repo = tmp_path / "kibana"
        repo.mkdir()
        _git("init", cwd=repo)
        _set_origin(repo, "git@github.com:elastic/kibana.git")
        deep = repo / "src" / "plugins" / "discover"
        deep.mkdir(parents=True)

        ident = detect_git_root(str(deep))
        assert ident is not None
        assert ident.owner == "elastic"
        assert ident.name == "kibana"
        assert ident.git_root == str(repo.resolve())


# ---------------------------------------------------------------------------
# Owner/repo URL parsing
# ---------------------------------------------------------------------------


class TestParseOwnerRepo:
    @pytest.mark.parametrize(
        "url,expected",
        [
            ("https://github.com/elastic/kibana", ("elastic", "kibana")),
            ("https://github.com/elastic/kibana.git", ("elastic", "kibana")),
            ("https://github.com/elastic/kibana/", ("elastic", "kibana")),
            ("git@github.com:elastic/kibana.git", ("elastic", "kibana")),
            ("ssh://git@github.com/elastic/kibana.git", ("elastic", "kibana")),
            ("https://gitlab.com/group/project.git", ("group", "project")),
            ("https://bitbucket.org/team/repo", ("team", "repo")),
        ],
    )
    def test_parse(self, url, expected):
        assert _parse_owner_repo(url) == expected

    def test_returns_none_for_unparseable(self):
        assert _parse_owner_repo("") is None
        assert _parse_owner_repo("not-a-url") is None


# ---------------------------------------------------------------------------
# _resolve_repo_identity end-to-end
# ---------------------------------------------------------------------------


class TestResolveRepoIdentity:
    def test_clone_with_origin_uses_owner_repo(self, tmp_path, monkeypatch):
        repo = tmp_path / "weirdly-named-clone"
        repo.mkdir()
        _git("init", cwd=repo)
        _set_origin(repo, "https://github.com/elastic/kibana.git")

        # Force config default in case a project config near the test cwd
        # has overridden it.
        monkeypatch.setattr(
            config_module, "get",
            lambda key, default=None, repo=None:
                True if key == "git_root_identity" else default,
        )

        owner, name, git_root = _resolve_repo_identity(repo)
        assert owner == "elastic"
        assert name == "kibana"
        assert git_root == str(repo.resolve())

    def test_clone_without_origin_keeps_basename_hash(self, tmp_path, monkeypatch):
        repo = tmp_path / "internal-tool"
        repo.mkdir()
        _git("init", cwd=repo)

        monkeypatch.setattr(
            config_module, "get",
            lambda key, default=None, repo=None:
                True if key == "git_root_identity" else default,
        )

        owner, name, git_root = _resolve_repo_identity(repo)
        assert owner == "local"
        assert name.startswith("internal-tool-")
        assert len(name.split("-")[-1]) == 8  # 8-char hash suffix
        assert git_root == str(repo.resolve())

    def test_no_git_uses_basename_hash(self, tmp_path, monkeypatch):
        plain = tmp_path / "plain-folder"
        plain.mkdir()

        monkeypatch.setattr(
            config_module, "get",
            lambda key, default=None, repo=None:
                True if key == "git_root_identity" else default,
        )

        owner, name, git_root = _resolve_repo_identity(plain)
        assert owner == "local"
        assert name.startswith("plain-folder-")
        assert git_root == ""

    def test_knob_off_uses_basename_hash_even_with_origin(self, tmp_path, monkeypatch):
        repo = tmp_path / "kibana-clone"
        repo.mkdir()
        _git("init", cwd=repo)
        _set_origin(repo, "https://github.com/elastic/kibana.git")

        monkeypatch.setattr(
            config_module, "get",
            lambda key, default=None, repo=None:
                False if key == "git_root_identity" else default,
        )

        owner, name, git_root = _resolve_repo_identity(repo)
        assert owner == "local"
        assert name.startswith("kibana-clone-")
        assert git_root == ""


# ---------------------------------------------------------------------------
# index_folder integration
# ---------------------------------------------------------------------------


class TestIndexFolderIdentity:
    def test_full_clone_index_uses_owner_repo(self, tmp_path):
        repo = tmp_path / "clone-named-anything"
        repo.mkdir()
        _git("init", cwd=repo)
        _set_origin(repo, "https://github.com/elastic/kibana.git")
        (repo / "main.py").write_text("def hello(): pass\n", encoding="utf-8")

        store = tmp_path / "store"
        result = index_folder(str(repo), use_ai_summaries=False, storage_path=str(store))
        assert result["success"] is True
        assert result["repo"] == "elastic/kibana"

    def test_git_root_field_round_trips(self, tmp_path):
        repo = tmp_path / "kibana"
        repo.mkdir()
        _git("init", cwd=repo)
        _set_origin(repo, "https://github.com/elastic/kibana.git")
        (repo / "main.py").write_text("def hello(): pass\n", encoding="utf-8")

        store_path = tmp_path / "store"
        index_folder(str(repo), use_ai_summaries=False, storage_path=str(store_path))

        store = IndexStore(base_path=str(store_path))
        loaded = store.load_index("elastic", "kibana")
        assert loaded is not None
        assert loaded.git_root == str(repo.resolve())

    def test_collision_detection_blocks_second_working_tree(self, tmp_path):
        repo_a = tmp_path / "kibana-a"
        repo_a.mkdir()
        _git("init", cwd=repo_a)
        _set_origin(repo_a, "https://github.com/elastic/kibana.git")
        (repo_a / "a.py").write_text("def a(): pass\n", encoding="utf-8")

        repo_b = tmp_path / "kibana-b"
        repo_b.mkdir()
        _git("init", cwd=repo_b)
        _set_origin(repo_b, "https://github.com/elastic/kibana.git")
        (repo_b / "b.py").write_text("def b(): pass\n", encoding="utf-8")

        store = tmp_path / "store"
        first = index_folder(str(repo_a), use_ai_summaries=False, storage_path=str(store))
        assert first["success"] is True

        second = index_folder(str(repo_b), use_ai_summaries=False, storage_path=str(store))
        assert second["success"] is False
        assert "already exists" in second["error"]
        assert str(repo_a.resolve()) in second["error"]
