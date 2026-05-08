"""Regression: `get_dependency_graph` exposes top-level `imports` and
`importers` arrays as siblings of `edges`/`neighbors`, mirroring the
queried file's depth-1 outgoing/incoming neighbors.

Locks the v1.83.x shape addition that made the response parser-friendly
for harvesters expecting a flat `imports: [...]` field.
"""
from __future__ import annotations

import textwrap

import pytest

from jcodemunch_mcp.tools.get_dependency_graph import get_dependency_graph
from jcodemunch_mcp.tools.index_folder import index_folder


@pytest.fixture
def chain_repo(tmp_path):
    """app.js → utils.js → core.js (chain of 3)."""
    src = tmp_path / "src"
    store = tmp_path / "store"
    src.mkdir()
    store.mkdir()
    (src / "core.js").write_text("export const v = 1;\n")
    (src / "utils.js").write_text(textwrap.dedent("""\
        import { v } from './core';
        export function helper() { return v; }
    """))
    (src / "app.js").write_text(textwrap.dedent("""\
        import { helper } from './utils';
        helper();
    """))
    r = index_folder(str(src), use_ai_summaries=False, storage_path=str(store))
    assert r["success"] is True
    return {"repo": r["repo"], "store": str(store)}


def test_imports_alias_lists_depth1_outgoing(chain_repo):
    out = get_dependency_graph(
        repo=chain_repo["repo"],
        file="app.js",
        direction="imports",
        depth=1,
        storage_path=chain_repo["store"],
    )
    assert "imports" in out, "imports alias missing from response"
    assert "utils.js" in out["imports"]
    # depth=1 must not pull in the transitive core.js
    assert "core.js" not in out["imports"]


def test_importers_alias_lists_depth1_incoming(chain_repo):
    out = get_dependency_graph(
        repo=chain_repo["repo"],
        file="utils.js",
        direction="importers",
        depth=1,
        storage_path=chain_repo["store"],
    )
    assert "importers" in out, "importers alias missing from response"
    assert "app.js" in out["importers"]


def test_imports_and_importers_both_present_for_direction_both(chain_repo):
    out = get_dependency_graph(
        repo=chain_repo["repo"],
        file="utils.js",
        direction="both",
        depth=1,
        storage_path=chain_repo["store"],
    )
    assert "imports" in out and "importers" in out
    assert "core.js" in out["imports"]
    assert "app.js" in out["importers"]


def test_aliases_are_lists_not_views(chain_repo):
    """The aliases must be plain lists so JSON serialization is stable."""
    out = get_dependency_graph(
        repo=chain_repo["repo"],
        file="app.js",
        direction="imports",
        depth=1,
        storage_path=chain_repo["store"],
    )
    assert isinstance(out["imports"], list)
    assert isinstance(out["importers"], list)
