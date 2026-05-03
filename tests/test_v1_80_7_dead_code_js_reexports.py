"""Regression tests for v1.80.7 — get_dead_code_v2 false positives on
JavaScript libraries that use CommonJS / ES module re-export patterns.

Reproduces the issue surfaced by the sverklo benchmark
(https://github.com/sverklo/sverklo/issues/25): Express's `createApplication`
was flagged as dead because:

  1. Express has no `app.py`/`main.py`-style entry point — it's a library
     whose entry is declared by `package.json`'s `main` field.
  2. `index.js` re-exports via `module.exports = require('./lib/express')`,
     which doesn't textually mention `createApplication`, so the barrel-
     export signal fires.
  3. `index.js` doesn't *call* `createApplication`, so the no-callers
     signal also fires.

All three signals firing → confidence 1.0 → reported as dead. Two fixes:
  - `package.json` `main`/`module`/`exports`/`bin` files become entry points.
  - Barrel-export scanning recursively follows CJS `module.exports = require`
    and ES `export * from` re-exports.
"""

from __future__ import annotations

import pytest

from jcodemunch_mcp.tools.get_dead_code_v2 import get_dead_code_v2
from jcodemunch_mcp.tools.index_folder import index_folder


def _build_express_like_repo(tmp_path):
    """Minimal repo modeled after Express's CJS re-export structure."""
    src = tmp_path / "src"
    src.mkdir()
    store = tmp_path / "store"
    store.mkdir()

    (src / "package.json").write_text(
        '{\n'
        '  "name": "express-like",\n'
        '  "main": "./index.js",\n'
        '  "version": "1.0.0"\n'
        '}\n',
        encoding="utf-8",
    )
    (src / "index.js").write_text(
        "module.exports = require('./lib/express');\n",
        encoding="utf-8",
    )
    lib = src / "lib"
    lib.mkdir()
    (lib / "express.js").write_text(
        "function createApplication() {\n"
        "  return { listen: function() {} };\n"
        "}\n"
        "function actuallyDead() {\n"
        "  return 'unused';\n"
        "}\n"
        "exports = module.exports = createApplication;\n",
        encoding="utf-8",
    )
    r = index_folder(str(src), use_ai_summaries=False, storage_path=str(store))
    assert r["success"] is True
    return r["repo"], str(store)


# NOTE: a direct end-to-end ESM `export * from` test was attempted but the
# JS import extractor doesn't currently recognize the `export ... from`
# syntax as an import edge — so `index.imports` ends up empty and
# get_dead_code_v2 short-circuits with "No import data". The regex-level
# behavior is verified by `TestBarrelExportRegexes` below; full e2e
# coverage will land when the import extractor is extended.


class TestExpressLikeFalsePositive:
    def test_create_application_not_flagged_as_dead(self, tmp_path):
        repo, store = _build_express_like_repo(tmp_path)
        result = get_dead_code_v2(repo=repo, storage_path=store)
        dead_names = {s["name"] for s in result.get("dead_symbols", [])}
        assert "createApplication" not in dead_names, (
            "createApplication should NOT be flagged: it is the package's "
            "main export, re-exported via `module.exports = require(...)`. "
            f"Got dead symbols: {sorted(dead_names)}"
        )

    def test_actually_dead_function_still_flagged(self, tmp_path):
        """Sanity check: the fix must not mask genuine dead code."""
        repo, store = _build_express_like_repo(tmp_path)
        result = get_dead_code_v2(repo=repo, min_confidence=0.33,
                                  storage_path=store)
        dead_names = {s["name"] for s in result.get("dead_symbols", [])}
        assert "actuallyDead" in dead_names, (
            "actuallyDead is genuinely unreferenced and should still be "
            f"flagged. Got dead symbols: {sorted(dead_names)}"
        )

    def test_package_json_entry_recorded_in_meta(self, tmp_path):
        repo, store = _build_express_like_repo(tmp_path)
        result = get_dead_code_v2(repo=repo, storage_path=store)
        entries = result.get("_meta", {}).get("package_json_entries") or []
        assert any(e.endswith("index.js") for e in entries), (
            f"package.json main should be detected as an entry point. "
            f"Got: {entries}"
        )


class TestBarrelExportRegexes:
    """Direct verification of the re-export pattern matchers added in
    1.80.7 — covers the cases the e2e tests can't easily exercise."""

    def test_cjs_module_exports_require_pattern(self):
        from jcodemunch_mcp.tools.get_dead_code_v2 import _CJS_REEXPORT_RE
        cases = [
            "module.exports = require('./lib/express')",
            'module.exports = require("./lib/express")',
            "exports = require('./X')",
            "module.exports.foo = require('./y')",
        ]
        for src in cases:
            m = _CJS_REEXPORT_RE.search(src)
            assert m is not None, f"failed to match: {src!r}"

    def test_esm_export_star_pattern(self):
        from jcodemunch_mcp.tools.get_dead_code_v2 import _ESM_REEXPORT_STAR_RE
        cases = [
            "export * from './lib/api'",
            'export * from "./lib/api"',
            "export * as ns from './lib/api'",
        ]
        for src in cases:
            m = _ESM_REEXPORT_STAR_RE.search(src)
            assert m is not None, f"failed to match: {src!r}"
            assert m.group(1) == "./lib/api"

    def test_esm_named_reexport_pattern(self):
        from jcodemunch_mcp.tools.get_dead_code_v2 import _ESM_REEXPORT_NAMED_RE
        cases = [
            "export { foo, bar } from './lib/api'",
            "export {foo as x} from './lib/api'",
        ]
        for src in cases:
            m = _ESM_REEXPORT_NAMED_RE.search(src)
            assert m is not None, f"failed to match: {src!r}"


class TestPaginationAndFilter:
    def test_max_results_caps_response(self, tmp_path):
        """`max_results` should cap the dead_symbols list and set
        `_meta.truncated` + `_meta.total_matches`."""
        src = tmp_path / "src"
        src.mkdir()
        store = tmp_path / "store"
        store.mkdir()
        # 20 orphan functions in 20 separate files — all qualify as dead.
        # `import os` lines just ensure the index has non-empty imports
        # data (the tool early-returns when index.imports is empty).
        for i in range(20):
            (src / f"orphan_{i}.py").write_text(
                f"import os\n\ndef orphan_{i}():\n    return {i}\n",
                encoding="utf-8",
            )
        r = index_folder(str(src), use_ai_summaries=False, storage_path=str(store))
        repo = r["repo"]

        result = get_dead_code_v2(repo=repo, min_confidence=0.33,
                                  max_results=5, storage_path=str(store))
        assert len(result["dead_symbols"]) == 5
        assert result["_meta"]["truncated"] is True
        assert result["_meta"]["total_matches"] >= 20

    def test_max_results_zero_means_unlimited(self, tmp_path):
        src = tmp_path / "src"
        src.mkdir()
        store = tmp_path / "store"
        store.mkdir()
        for i in range(15):
            (src / f"orphan_{i}.py").write_text(
                f"import os\n\ndef orphan_{i}():\n    return {i}\n",
                encoding="utf-8",
            )
        r = index_folder(str(src), use_ai_summaries=False, storage_path=str(store))
        result = get_dead_code_v2(repo=r["repo"], min_confidence=0.33,
                                  max_results=0, storage_path=str(store))
        assert result["_meta"]["truncated"] is False
        assert len(result["dead_symbols"]) >= 15

    def test_file_pattern_scopes_analysis(self, tmp_path):
        src = tmp_path / "src"
        (src / "keep").mkdir(parents=True)
        (src / "skip").mkdir()
        store = tmp_path / "store"
        store.mkdir()
        (src / "keep" / "a.py").write_text(
            "import os\n\ndef keep_dead():\n    pass\n", encoding="utf-8"
        )
        (src / "skip" / "b.py").write_text(
            "import os\n\ndef skip_dead():\n    pass\n", encoding="utf-8"
        )
        r = index_folder(str(src), use_ai_summaries=False, storage_path=str(store))
        result = get_dead_code_v2(
            repo=r["repo"], min_confidence=0.33,
            file_pattern="*keep*", storage_path=str(store),
        )
        names = {s["name"] for s in result["dead_symbols"]}
        assert "keep_dead" in names
        assert "skip_dead" not in names
        assert result["_meta"]["file_pattern"] == "*keep*"
