"""Hardened generic encoder — round-trip coverage for tools without a custom schema.

These shapes mirror real responses from tools that don't yet have per-tool
encoders (e.g. get_hotspots, get_related_symbols, get_untested_symbols, etc.).
Verifies: original keys preserved, column types preserved, nested list-of-dicts
flattened, _meta passthrough, fail-open on weird shapes.
"""

import pytest

from jcodemunch_mcp.encoding import encode_response
from jcodemunch_mcp.encoding.decoder import decode


def _rt(tool: str, response: dict) -> dict:
    payload, meta = encode_response(tool, response, "compact")
    assert isinstance(payload, str)
    assert meta["encoding"] == "gen1"
    return decode(payload)


def test_generic_preserves_int_column_types():
    resp = {
        "repo": "acme",
        "hotspots": [
            {"name": "foo", "file": "src/a.py", "complexity": 12, "churn": 5},
            {"name": "bar", "file": "src/a.py", "complexity": 7, "churn": 9},
            {"name": "baz", "file": "src/b.py", "complexity": 3, "churn": 11},
        ],
    }
    out = _rt("get_hotspots", resp)
    assert out["hotspots"][0]["complexity"] == 12
    assert isinstance(out["hotspots"][0]["complexity"], int)
    assert out["hotspots"][0]["churn"] == 5


def test_generic_preserves_bool_and_float_types():
    resp = {
        "results": [
            {"name": "a", "passed": True, "score": 0.82},
            {"name": "b", "passed": False, "score": 0.41},
        ],
    }
    out = _rt("some_tool", resp)
    assert out["results"][0]["passed"] is True
    assert out["results"][1]["passed"] is False
    assert out["results"][0]["score"] == pytest.approx(0.82)


def test_generic_preserves_top_level_scalar_types():
    resp = {
        "total": 42,
        "ratio": 0.5,
        "ok": True,
        "query": "foo",
        "results": [
            {"name": "a", "line": 1},
            {"name": "b", "line": 2},
        ],
    }
    out = _rt("search_something", resp)
    assert out["total"] == 42
    assert out["ratio"] == 0.5
    assert out["ok"] is True
    assert out["query"] == "foo"


def test_generic_interns_shared_path_prefix():
    long_prefix = "src/very/deeply/nested/module/submodule/"
    resp = {
        "refs": [
            {"file": long_prefix + f"file_{i:03d}.py", "line": i}
            for i in range(20)
        ],
    }
    payload, meta = encode_response("x", resp, "compact")
    # Savings proof: compact must be meaningfully smaller than JSON.
    assert meta["encoded_bytes"] < meta["json_bytes"]
    out = decode(payload)
    assert out["refs"][0]["file"] == long_prefix + "file_000.py"
    assert out["refs"][19]["file"] == long_prefix + "file_019.py"


def test_generic_flattens_nested_list_of_dicts():
    resp = {
        "summary": {
            "total": 3,
            "items": [
                {"name": "a", "count": 1},
                {"name": "b", "count": 2},
            ],
        },
    }
    out = _rt("tool_with_nested", resp)
    assert out["summary"]["items"][0]["name"] == "a"
    assert out["summary"]["items"][0]["count"] == 1


def test_generic_falls_through_for_mixed_arrays():
    # Mixed types in list — not table-eligible. Should round-trip via JSON blob.
    resp = {
        "mixed": [1, "two", {"three": 3}],
        "rows": [
            {"a": 1, "b": 2},
            {"a": 3, "b": 4},
        ],
    }
    out = _rt("weird", resp)
    assert out["mixed"] == [1, "two", {"three": 3}]
    assert out["rows"][0]["a"] == 1


def test_generic_handles_empty_tables_gracefully():
    # Lists shorter than _MIN_TABLE_ROWS should not be table-encoded.
    resp = {
        "one": [{"x": 1}],  # too short, falls to JSON blob
        "rows": [
            {"a": 1, "b": "x"},
            {"a": 2, "b": "y"},
        ],
    }
    out = _rt("short", resp)
    assert out["one"] == [{"x": 1}]
    assert out["rows"][1]["b"] == "y"


def test_generic_fail_open_when_gate_fails():
    # Tiny response — savings gate rejects encoding; dispatcher returns JSON.
    payload, meta = encode_response("tiny", {"ok": True}, "auto")
    assert meta["encoding"] == "json"


def test_generic_many_tables_does_not_crash():
    # Construct more tables than table-tag alphabet has — overflow should land
    # in JSON blobs, not explode.
    resp: dict = {}
    for i in range(30):
        resp[f"t{i}"] = [{"x": i, "y": i + 1}, {"x": i + 2, "y": i + 3}]
    payload, meta = encode_response("many_tables", resp, "compact")
    assert meta["encoding"] in ("gen1", "json")
    if meta["encoding"] == "gen1":
        out = decode(payload)
        # At least the first 26 should round-trip as tables.
        present = sum(1 for k in resp if k in out)
        assert present >= 20


# ── Audit finding F1: scalar newline escape ────────────────────────────────

@pytest.mark.parametrize("text", [
    "line1\n\nline3",          # blank-line trigger — the original bug.
    "single\n",
    "dos\r\nstyle",
    "tabs\there",
    "quote \"inside\" it",
    "trailing\n\n\n",
    "\n\n",                      # pathological: literal double-newline value.
    "pipe|and:colon,and%25",     # non-ASCII meta-chars alongside newlines.
])
def test_generic_scalar_newline_roundtrip(text):
    out = _rt("misc_tool", {"doc": text, "n": 1})
    assert out["doc"] == text
    assert out["n"] == 1


# ── Audit finding F11: schema-embed separator escape ──────────────────────

def test_generic_schema_key_with_colon_roundtrips():
    # Nested table under a key containing ':' used to corrupt on decode.
    resp = {
        "stats:by_file": {
            "rows": [
                {"file": "a.py", "n": 3},
                {"file": "b.py", "n": 5},
            ],
        },
    }
    out = _rt("weird_keys", resp)
    assert out["stats:by_file"]["rows"] == resp["stats:by_file"]["rows"]


def test_generic_schema_column_with_pipe_roundtrips():
    resp = {
        "rows": [
            {"col|weird": "a", "value": 1},
            {"col|weird": "b", "value": 2},
        ],
    }
    out = _rt("pipe_col", resp)
    assert out["rows"] == resp["rows"]


# ── Audit finding F5: nested table + __json.<parent> collision ─────────────

def test_generic_nested_table_coexists_with_scalar_json_parent():
    # Parent "foo" holds one nested table ("tbl") and one non-uniform list
    # ("other") which falls into the __json.foo blob. On decode, `foo` gets
    # restored first as whatever __json.foo produced; the nested-table code
    # must not silently drop `tbl` when the restored value isn't already a
    # dict.
    resp = {
        "foo": {
            "tbl": [{"x": 1, "y": 2}, {"x": 3, "y": 4}],
            "other": [1, "two", {"nested": True}],
        },
    }
    out = _rt("mixed_nest", resp)
    assert out["foo"]["tbl"] == resp["foo"]["tbl"]
    assert "other" in out["foo"]
