"""Resolve a filesystem path to its indexed repo identifier."""

import hashlib
import json
import logging
import subprocess
import time
from pathlib import Path
from typing import Optional

from ..storage import IndexStore

logger = logging.getLogger(__name__)


def _compute_repo_id(folder_path: Path) -> str:
    """Compute the deterministic repo ID for a directory path.

    Same formula as _local_repo_id (watcher.py) and _local_repo_name (index_folder.py).
    """
    resolved = folder_path.resolve()
    digest = hashlib.sha1(str(resolved).encode("utf-8")).hexdigest()[:8]
    return f"local/{resolved.name}-{digest}"


def _git_common_dir(path: Path) -> Optional[Path]:
    """Return the canonical Git common-dir for a path, or None.

    For the main checkout this is the same as the `.git` directory; for
    a linked worktree it points at the main repo's `.git`. So all worktrees
    of a repository share a common-dir, which lets us match a worktree
    against an already-indexed canonical checkout (issue #277).

    Same env-neutralisation as `_git_toplevel` — system/global git config is
    disabled so a hostile workspace can't influence the probe.
    """
    import os as _os
    env = _os.environ.copy()
    env["GIT_CONFIG_NOSYSTEM"] = "1"
    env["GIT_CONFIG_GLOBAL"] = _os.devnull
    env["GIT_TERMINAL_PROMPT"] = "0"
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--git-common-dir"],
            capture_output=True,
            text=True,
            cwd=str(path),
            timeout=5,
            stdin=subprocess.DEVNULL,
            env=env,
        )
        if result.returncode == 0:
            raw = result.stdout.strip()
            if not raw:
                return None
            common = Path(raw)
            if not common.is_absolute():
                common = (path / common).resolve()
            return common.resolve()
    except (subprocess.TimeoutExpired, OSError):
        pass
    return None


def _git_toplevel(path: Path) -> Optional[Path]:
    """Get the git repository root for a path, or None.

    The caller's path is not yet trusted — the whole point of resolve_repo is
    to discover whether it's already indexed. Neutralise system/global git
    config and disable hook execution so a hostile workspace cannot influence
    this probe (defense-in-depth on top of git's safe.directory check).
    """
    import os as _os
    env = _os.environ.copy()
    env["GIT_CONFIG_NOSYSTEM"] = "1"
    env["GIT_CONFIG_GLOBAL"] = _os.devnull
    # GIT_TERMINAL_PROMPT=0 prevents accidental credential prompts on
    # workspaces whose .git/config points at remotes requiring auth.
    env["GIT_TERMINAL_PROMPT"] = "0"
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            cwd=str(path),
            timeout=5,
            stdin=subprocess.DEVNULL,
            env=env,
        )
        if result.returncode == 0:
            return Path(result.stdout.strip())
    except (subprocess.TimeoutExpired, OSError):
        pass
    return None


def resolve_repo(path: str, storage_path: Optional[str] = None) -> dict:
    """Resolve a filesystem path to its indexed repo identifier.

    Accepts a repo root, worktree, subdirectory, or file path.
    Returns whether the path is indexed and its computed repo ID.
    """
    start = time.perf_counter()
    p = Path(path)

    if not p.exists():
        elapsed = (time.perf_counter() - start) * 1000
        return {
            "found": False,
            "indexed": False,
            "error": f"Path does not exist: {path}",
            "_meta": {"timing_ms": round(elapsed, 1)},
        }

    # If it's a file, use parent directory
    if p.is_file():
        p = p.parent

    store = IndexStore(base_path=storage_path)

    # Try candidates: input path first, then git root
    candidates = [p]
    git_root = _git_toplevel(p)
    if git_root and git_root.resolve() != p.resolve():
        candidates.append(git_root)

    for candidate in candidates:
        repo_id = _compute_repo_id(candidate)
        owner, name = repo_id.split("/", 1)
        if store.has_index(owner, name):
            # Read metadata from sidecar or full index
            entry = _read_repo_metadata(store, owner, name)
            elapsed = (time.perf_counter() - start) * 1000
            result = {
                "found": True,
                "indexed": True,
                "repo": repo_id,
                "source_root": entry.get("source_root", ""),
                "display_name": entry.get("display_name", ""),
                "symbol_count": entry.get("symbol_count", 0),
                "file_count": entry.get("file_count", 0),
                "languages": entry.get("languages", {}),
                "indexed_at": entry.get("indexed_at", ""),
                "_meta": {"timing_ms": round(elapsed, 1)},
            }
            return result

    # Not indexed — return the computed ID for the best candidate
    best = candidates[0]
    repo_id = _compute_repo_id(best)

    # Worktree-aware canonical-index discovery (issue #277):
    # if the path is a Git worktree, look for already-indexed repos that
    # share the same --git-common-dir and surface them as candidates.
    canonical_candidates = _find_canonical_candidates(best, store)

    elapsed = (time.perf_counter() - start) * 1000
    response: dict = {
        "found": True,
        "indexed": False,
        "repo": repo_id,
        "hint": "call index_folder to index this path",
        "_meta": {"timing_ms": round(elapsed, 1)},
    }
    if canonical_candidates:
        response["canonical_candidates"] = canonical_candidates
        response["hint"] = (
            "this is a Git worktree of an already-indexed repo — use one of "
            "canonical_candidates for read-only lookups, or index this "
            "worktree explicitly if you need branch-local/uncommitted state"
        )
    return response


def _find_canonical_candidates(
    path: Path, store: IndexStore
) -> list[dict]:
    """Find indexed repos sharing this path's Git common-dir.

    Returns a list of `{repo, source_root, rationale}` dicts. Empty when the
    path isn't in a Git repo, has no common-dir, or no indexed repo matches.
    """
    common = _git_common_dir(path)
    if common is None:
        return []

    candidates: list[dict] = []
    try:
        repos = store.list_repos()
    except Exception:
        logger.debug("list_repos failed during worktree resolution", exc_info=True)
        return []

    for entry in repos:
        source_root = entry.get("source_root", "")
        if not source_root:
            continue
        try:
            other_path = Path(source_root)
            if not other_path.exists():
                continue
            other_common = _git_common_dir(other_path)
        except (OSError, ValueError):
            continue
        if other_common is None:
            continue
        if other_common == common:
            candidates.append({
                "repo": entry.get("repo", ""),
                "source_root": source_root,
                "rationale": "shared --git-common-dir",
            })
    return candidates


def _read_repo_metadata(store: IndexStore, owner: str, name: str) -> dict:
    """Read repo metadata from SQLite, sidecar, or full index JSON."""
    # Try SQLite first (primary backend since v1.9.0)
    if hasattr(store, '_sqlite'):
        db_path = store._sqlite._db_path(owner, name)
        if db_path.exists():
            entry = store._sqlite._list_repo_from_db(db_path)
            if entry:
                return entry

    slug = store._repo_slug(owner, name)

    # Try lightweight sidecar
    meta_path = store.base_path / f"{slug}.meta.json"
    if meta_path.exists():
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            entry = store._repo_entry_from_data(data)
            if entry:
                return entry
        except (json.JSONDecodeError, ValueError):
            logger.debug("Corrupted sidecar JSON at %s, skipping", meta_path)

    # Fall back to full index JSON
    index_path = store._index_path(owner, name)
    if index_path.exists():
        try:
            with open(index_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            entry = store._repo_entry_from_data(data)
            if entry:
                return entry
        except (json.JSONDecodeError, ValueError):
            logger.debug("Corrupted index JSON at %s, skipping", index_path)

    return {}
