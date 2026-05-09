"""Find all files that import from a given file path."""

import time
from collections import deque
from typing import Optional

from ..storage import IndexStore
from ..parser.imports import resolve_specifier
from ._utils import resolve_repo
from .package_registry import (
    build_package_registry,
    extract_root_package_from_specifier,
)


def _resolve_to_leaves(
    specifier: str,
    src_file: str,
    source_files: frozenset,
    alias_map,
    psr4_map,
    re_exports: dict[str, list[str]],
) -> set[str]:
    """Resolve `specifier` to its direct target plus every leaf reachable
    through `export * from` re-export chains. Used by find_importers so
    that importing a barrel attributes Ca to leaf definition files
    instead of stopping at the barrel.
    """
    direct = resolve_specifier(specifier, src_file, source_files, alias_map, psr4_map)
    if not direct:
        return set()
    leaves: set[str] = {direct}
    queue: deque = deque([direct])
    while queue:
        t = queue.popleft()
        for leaf in re_exports.get(t, ()):
            if leaf not in leaves:
                leaves.add(leaf)
                queue.append(leaf)
    return leaves


def _build_re_exports(
    imports: dict, source_files: frozenset, alias_map, psr4_map,
) -> dict[str, list[str]]:
    """Map each barrel file to the files it forwards via `export * from`.
    Mirrors get_dependency_graph._build_re_export_map; duplicated here to
    avoid a circular import."""
    re_exports: dict[str, list[str]] = {}
    for src_file, file_imports in imports.items():
        leaves: list[str] = []
        for imp in file_imports:
            if not imp.get("is_re_export"):
                continue
            target = resolve_specifier(imp["specifier"], src_file, source_files, alias_map, psr4_map)
            if target and target != src_file:
                leaves.append(target)
        if leaves:
            re_exports[src_file] = list(dict.fromkeys(leaves))
    return re_exports


def _find_importers_single(
    file_path: str,
    index,
    max_results: int,
    owner: str,
    name: str,
    start: float,
) -> dict:
    """Core logic for a single file_path query. Returns the original flat shape."""
    if index.imports is None:
        return {
            "repo": f"{owner}/{name}",
            "file_path": file_path,
            "importers": [],
            "note": "No import data available. Re-index with jcodemunch-mcp >= 1.3.0 to enable find_importers.",
            "_meta": {"timing_ms": round((time.perf_counter() - start) * 1000, 1)},
        }

    source_files = frozenset(index.source_files)
    alias_map = index.alias_map
    psr4_map = getattr(index, "psr4_map", None)

    # v1.93.0: barrel-aware resolution. When a file imports `@nestjs/common`
    # (which resolves to packages/common/index.ts) and that index.ts has
    # `export * from './decorators'`, the importer is also credited with
    # importing the leaf files reached transitively through the chain.
    # Old indexes without `is_re_export` data degrade to the prior behavior.
    re_exports = _build_re_exports(index.imports, source_files, alias_map, psr4_map)

    # Build a set of all files that are imported by at least one other file
    # (used for has_importers). Counts barrel-expanded leaves so a leaf
    # definition file isn't flagged as orphan just because its only
    # importers reach it via re-exports.
    files_that_are_imported: set[str] = set()
    for src_file, file_imports in index.imports.items():
        for imp in file_imports:
            if imp.get("is_re_export"):
                continue  # re-exports forward, not consume
            files_that_are_imported.update(
                _resolve_to_leaves(imp["specifier"], src_file, source_files, alias_map, psr4_map, re_exports)
            )

    results = []

    for src_file, file_imports in index.imports.items():
        if src_file == file_path:
            continue
        for imp in file_imports:
            if imp.get("is_re_export"):
                continue  # re-export-only files are forwarders, not consumers
            leaves = _resolve_to_leaves(imp["specifier"], src_file, source_files, alias_map, psr4_map, re_exports)
            if file_path in leaves:
                results.append({
                    "file": src_file,
                    "specifier": imp["specifier"],
                    "names": imp.get("names", []),
                    "has_importers": src_file in files_that_are_imported,
                })
                break  # one match per file is enough

    results.sort(key=lambda r: r["file"])

    elapsed = (time.perf_counter() - start) * 1000
    truncated = len(results) > max_results
    return {
        "repo": f"{owner}/{name}",
        "file_path": file_path,
        "importer_count": len(results),
        "importers": results[:max_results],
        "_meta": {
            "timing_ms": round(elapsed, 1),
            "truncated": truncated,
            "tip": "Tip: use file_paths=['{0}','...'] to query multiple files in one call.".format(file_path)
            if truncated
            else "Tip: use file_paths=['{0}','...'] to query multiple files in one call. "
                 "For usage-site matching beyond imports, also try check_references.".format(file_path),
        },
    }


def _find_importers_batch(
    file_paths: list[str],
    index,
    max_results: int,
    owner: str,
    name: str,
    start: float,
) -> dict:
    """Batch logic: loop over file_paths, return grouped results array."""
    if index.imports is None:
        return {
            "repo": f"{owner}/{name}",
            "results": [
                {
                    "file_path": fp,
                    "importers": [],
                    "note": "No import data available. Re-index with jcodemunch-mcp >= 1.3.0 to enable find_importers.",
                }
                for fp in file_paths
            ],
            "_meta": {"timing_ms": round((time.perf_counter() - start) * 1000, 1)},
        }

    source_files = frozenset(index.source_files)
    alias_map = index.alias_map
    psr4_map = getattr(index, "psr4_map", None)

    # v1.93.0: barrel-aware resolution. See _find_importers_single docstring.
    re_exports = _build_re_exports(index.imports, source_files, alias_map, psr4_map)

    # Pass 1: build files_that_are_imported (counts barrel-expanded leaves)
    files_that_are_imported: set[str] = set()
    for src_file, file_imports in index.imports.items():
        for imp in file_imports:
            if imp.get("is_re_export"):
                continue
            files_that_are_imported.update(
                _resolve_to_leaves(imp["specifier"], src_file, source_files, alias_map, psr4_map, re_exports)
            )

    # Pass 2: build import_map. Each importer is recorded under every leaf
    # its specifier reaches (including transitively through barrels).
    import_map: dict[str, list[dict]] = {}
    for src_file, file_imports in index.imports.items():
        for imp in file_imports:
            if imp.get("is_re_export"):
                continue
            leaves = _resolve_to_leaves(imp["specifier"], src_file, source_files, alias_map, psr4_map, re_exports)
            if not leaves:
                continue
            entry = {
                "file": src_file,
                "specifier": imp["specifier"],
                "names": imp.get("names", []),
                "has_importers": src_file in files_that_are_imported,
            }
            for leaf in leaves:
                import_map.setdefault(leaf, []).append(entry)

    results = []
    for file_path in file_paths:
        file_results = import_map.get(file_path, [])  # O(1) lookup
        file_results.sort(key=lambda r: r["file"])
        results.append({
            "file_path": file_path,
            "importer_count": len(file_results),
            "importers": file_results[:max_results],
        })

    return {
        "repo": f"{owner}/{name}",
        "results": results,
        "_meta": {"timing_ms": round((time.perf_counter() - start) * 1000, 1)},
    }


def _find_cross_repo_importers(
    file_path: str,
    repo_id: str,
    all_repos: list[dict],
    store: IndexStore,
    owner: str,
    name: str,
) -> list[dict]:
    """Search other indexed repos for files that import from this repo's package."""
    # Look up this repo's package names from its index
    current_index = store.load_index(owner, name)
    if not current_index:
        return []
    pkg_names = getattr(current_index, "package_names", []) or []
    if not pkg_names:
        return []

    cross_results: list[dict] = []

    for repo_entry in all_repos:
        other_repo_id = repo_entry.get("repo", "")
        if not other_repo_id or other_repo_id == repo_id or "/" not in other_repo_id:
            continue
        other_owner, other_name = other_repo_id.split("/", 1)
        other_index = store.load_index(other_owner, other_name)
        if not other_index or not other_index.imports:
            continue

        other_source_files = frozenset(other_index.source_files)

        for src_file, file_imports in other_index.imports.items():
            for imp in file_imports:
                specifier = imp.get("specifier", "")
                # Determine language from file extension
                lang = other_index.file_languages.get(src_file, "")
                root_pkg = extract_root_package_from_specifier(specifier, lang)
                if root_pkg and root_pkg in pkg_names:
                    cross_results.append({
                        "file": src_file,
                        "specifier": specifier,
                        "names": imp.get("names", []),
                        "has_importers": True,  # cross-repo — not analyzed further
                        "cross_repo": True,
                        "source_repo": other_repo_id,
                    })
                    break  # one match per file per other-repo is enough

    return cross_results


def find_importers(
    repo: str,
    file_path: Optional[str] = None,
    max_results: int = 50,
    storage_path: Optional[str] = None,
    file_paths: Optional[list[str]] = None,
    cross_repo: Optional[bool] = None,
) -> dict:
    """Find all indexed files that import from file_path.

    Supports two modes:
    - Singular: pass ``file_path`` to get the original flat response shape.
    - Batch: pass ``file_paths`` (list) to query multiple files at once,
      returning a grouped ``results`` array.

    Args:
        repo: Repository identifier (owner/repo or display name).
        file_path: Target file path within the repo (singular mode).
        file_paths: List of target file paths (batch mode).
        max_results: Maximum number of importers per file.
        storage_path: Custom storage path.
        cross_repo: When True, also search other indexed repos for cross-repo importers.
                    Defaults to the ``cross_repo_default`` config value (False).

    Returns:
        Singular mode: dict with flat ``importers`` list and _meta envelope.
        Batch mode: dict with ``results`` array (one entry per input file_path).

    Raises:
        ValueError: if neither or both of file_path and file_paths are provided.
    """
    # Normalize: some MCP clients send file_paths=[] alongside file_path when they mean singular mode
    if file_path is not None and file_paths is not None and len(file_paths) == 0:
        file_paths = None
    if (file_path is None and file_paths is None) or (file_path is not None and file_paths is not None):
        raise ValueError("Provide exactly one of 'file_path' or 'file_paths', not both and not neither.")

    # Resolve cross_repo default from config if not explicitly provided
    if cross_repo is None:
        from .. import config as _cfg
        cross_repo = bool(_cfg.get("cross_repo_default", False))

    start = time.perf_counter()
    max_results = max(1, min(max_results, 200))

    try:
        owner, name = resolve_repo(repo, storage_path)
    except ValueError as e:
        return {"error": str(e)}

    store = IndexStore(base_path=storage_path)
    index = store.load_index(owner, name)
    if not index:
        return {"error": f"Repository not indexed: {owner}/{name}"}

    repo_id = f"{owner}/{name}"

    if file_paths is not None:
        result = _find_importers_batch(file_paths, index, max_results, owner, name, start)
        if cross_repo:
            try:
                from .list_repos import list_repos
                all_repos = list_repos(storage_path=storage_path).get("repos", [])
                cross_results = _find_cross_repo_importers(
                    file_paths[0] if len(file_paths) == 1 else "",
                    repo_id, all_repos, store, owner, name,
                )
                if cross_results and "results" in result:
                    # Attach cross-repo results to the batch response
                    result["cross_repo_importers"] = cross_results[:max_results]
            except Exception:
                pass
        return result
    else:
        result = _find_importers_single(file_path, index, max_results, owner, name, start)
        if cross_repo and "importers" in result:
            try:
                from .list_repos import list_repos
                all_repos = list_repos(storage_path=storage_path).get("repos", [])
                cross_results = _find_cross_repo_importers(
                    file_path, repo_id, all_repos, store, owner, name,
                )
                if cross_results:
                    result["importers"] = result.get("importers", []) + cross_results[:max_results]
                    result["cross_repo_importer_count"] = len(cross_results)
            except Exception:
                pass
        return result
