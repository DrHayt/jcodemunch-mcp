"""Regression: each `find_references` match carries the line number of the
import statement, so downstream consumers (regex harvesters, IDE deeplinks)
can jump straight to the import site.

Locks the v1.83.x shape change that closed the missing-line gap surfaced by
the sverklo benchmark's strict file-AND-line regex parser.
"""
from __future__ import annotations

import textwrap

import pytest

from jcodemunch_mcp.tools.find_references import find_references, _find_import_line
from jcodemunch_mcp.tools.index_folder import index_folder


@pytest.fixture
def js_repo(tmp_path):
    """Two JS files: utils.js exports `helper`; app.js imports it on line 2."""
    src = tmp_path / "src"
    store = tmp_path / "store"
    src.mkdir()
    store.mkdir()
    (src / "utils.js").write_text(textwrap.dedent("""\
        export function helper(x) {
            return x + 1;
        }
    """))
    (src / "app.js").write_text(textwrap.dedent("""\
        // app entrypoint
        import { helper } from './utils';

        helper(41);
    """))
    r = index_folder(str(src), use_ai_summaries=False, storage_path=str(store))
    assert r["success"] is True
    return {"repo": r["repo"], "store": str(store)}


def test_find_import_line_basic_quoted_specifiers():
    content = textwrap.dedent("""\
        import os
        from foo import bar
        import { x } from './utils';
        require('lodash');
        const m = import("./dyn");
    """)
    assert _find_import_line(content, "./utils") == 3
    assert _find_import_line(content, "lodash") == 4
    assert _find_import_line(content, "./dyn") == 5
    assert _find_import_line(content, "missing") is None


def test_find_import_line_empty_inputs():
    assert _find_import_line("", "anything") is None
    assert _find_import_line("some content", "") is None


def test_find_references_emits_line_per_match(js_repo):
    """Each `matches[i].line` points at the line where the specifier is imported."""
    out = find_references(
        repo=js_repo["repo"],
        identifier="helper",
        storage_path=js_repo["store"],
    )
    assert out.get("reference_count", 0) >= 1
    refs = out["references"]
    by_file = {r["file"]: r for r in refs}
    app_ref = by_file.get("app.js")
    assert app_ref is not None, f"expected app.js in references; got {list(by_file)}"
    matches = app_ref["matches"]
    assert matches, "expected at least one match entry on app.js"
    # The import statement is on line 2 of app.js.
    assert matches[0].get("line") == 2
