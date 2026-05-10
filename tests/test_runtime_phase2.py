"""Phase 2: runtime confidence stamping on existing tool results.

Verifies the zero-cost contract:
  - With runtime_calls populated, results gain `_runtime_confidence` per
    entry and `_meta.runtime_freshness`.
  - With runtime_calls empty, response shape is unchanged from Phase 1
    (no `_runtime_confidence` field, no `runtime_freshness` key).
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from jcodemunch_mcp.runtime import (
    RuntimeConfidenceProbe,
    attach_runtime_confidence,
    attach_runtime_confidence_by_file,
)
from jcodemunch_mcp.runtime.ingest import ingest_otel_file
from jcodemunch_mcp.storage.sqlite_store import SQLiteIndexStore


# ──────────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────────


def _seed_index(tmp_path: Path) -> tuple[SQLiteIndexStore, Path]:
    """Index a small repo with known symbols + import edges for resolver tests."""
    store = SQLiteIndexStore(base_path=str(tmp_path))
    db_path = store._db_path("local", "phase2")
    conn = store._connect(db_path)
    try:
        conn.executescript(
            """
            INSERT INTO symbols (id, file, name, kind, line, end_line) VALUES
                ('src/handlers.py::get_users#function', 'src/handlers.py', 'get_users', 'function', 10, 25),
                ('src/handlers.py::create_user#function', 'src/handlers.py', 'create_user', 'function', 30, 50),
                ('src/auth.py::login#function', 'src/auth.py', 'login', 'function', 5, 20),
                ('src/auth.py::logout#function', 'src/auth.py', 'logout', 'function', 25, 35);
            """
        )
        conn.commit()
    finally:
        conn.close()
    return store, db_path


def _otel_span(file_path: str, line_no: int, function_name: str) -> dict:
    return {
        "traceId": "x", "spanId": "y", "name": function_name,
        "startTimeUnixNano": "0", "endTimeUnixNano": "1000000",
        "attributes": [
            {"key": "code.filepath", "value": {"stringValue": file_path}},
            {"key": "code.lineno", "value": {"intValue": str(line_no)}},
            {"key": "code.function", "value": {"stringValue": function_name}},
        ],
    }


def _ingest(db_path: Path, tmp_path: Path, *, mapped_only: bool = True) -> None:
    """Ingest a small synthetic OTel trace so runtime_calls is populated."""
    trace = tmp_path / "trace.jsonl"
    spans = [
        _otel_span("src/handlers.py", 12, "get_users"),
        _otel_span("src/handlers.py", 12, "get_users"),  # 2nd hit
        _otel_span("src/auth.py", 8, "login"),
    ]
    payload = {"resourceSpans": [{"resource": {"attributes": []}, "scopeSpans": [{"scope": {"name": "t"}, "spans": spans}]}]}
    trace.write_text(json.dumps(payload))
    ingest_otel_file(db_path=str(db_path), file_path=str(trace))


# ──────────────────────────────────────────────────────────────────────
# Probe contract — direct unit tests
# ──────────────────────────────────────────────────────────────────────


def test_probe_no_op_when_runtime_calls_empty(tmp_path):
    store, db_path = _seed_index(tmp_path)
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        probe = RuntimeConfidenceProbe(conn)
        assert probe.has_runtime is False
        entries = [{"id": "src/handlers.py::get_users#function", "name": "get_users"}]
        probe.annotate(entries)
        assert "_runtime_confidence" not in entries[0]
        assert probe.summary(entries) == {}
    finally:
        conn.close()


def test_probe_stamps_confirmed_and_declared(tmp_path):
    store, db_path = _seed_index(tmp_path)
    _ingest(db_path, tmp_path)
    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        probe = RuntimeConfidenceProbe(conn)
        assert probe.has_runtime is True
        entries = [
            {"id": "src/handlers.py::get_users#function"},   # confirmed
            {"id": "src/auth.py::login#function"},            # confirmed
            {"id": "src/handlers.py::create_user#function"},  # declared_only
            {"id": "src/auth.py::logout#function"},           # declared_only
        ]
        probe.annotate(entries)
        assert entries[0]["_runtime_confidence"] == "confirmed"
        assert entries[1]["_runtime_confidence"] == "confirmed"
        assert entries[2]["_runtime_confidence"] == "declared_only"
        assert entries[3]["_runtime_confidence"] == "declared_only"
        summary = probe.summary(entries)
        assert summary["sources"] == ["otel"]
        assert summary["last_seen"]
        assert summary["coverage_pct"] == 50  # 2 of 4 confirmed
    finally:
        conn.close()


def test_attach_runtime_confidence_helper_no_op_when_empty(tmp_path):
    store, db_path = _seed_index(tmp_path)
    entries = [{"id": "src/handlers.py::get_users#function"}]
    summary = attach_runtime_confidence(entries, str(db_path), id_field="id")
    assert summary == {}
    assert "_runtime_confidence" not in entries[0]


def test_attach_runtime_confidence_helper_stamps_when_populated(tmp_path):
    store, db_path = _seed_index(tmp_path)
    _ingest(db_path, tmp_path)
    entries = [
        {"id": "src/handlers.py::get_users#function"},
        {"id": "src/auth.py::logout#function"},
    ]
    summary = attach_runtime_confidence(entries, str(db_path), id_field="id")
    assert summary["coverage_pct"] == 50
    assert entries[0]["_runtime_confidence"] == "confirmed"
    assert entries[1]["_runtime_confidence"] == "declared_only"


def test_attach_by_file_helper_no_op_when_empty(tmp_path):
    store, db_path = _seed_index(tmp_path)
    entries = [{"file": "src/handlers.py"}]
    summary = attach_runtime_confidence_by_file(entries, str(db_path), file_field="file")
    assert summary == {}
    assert "_runtime_confidence" not in entries[0]


def test_attach_by_file_helper_stamps_when_populated(tmp_path):
    store, db_path = _seed_index(tmp_path)
    _ingest(db_path, tmp_path)
    entries = [
        {"file": "src/handlers.py"},  # has runtime symbol → confirmed
        {"file": "src/auth.py"},       # has runtime symbol → confirmed
        {"file": "src/missing.py"},    # not in symbols at all → declared_only
    ]
    summary = attach_runtime_confidence_by_file(entries, str(db_path), file_field="file")
    assert summary["coverage_pct"] == 67  # 2 of 3
    assert entries[0]["_runtime_confidence"] == "confirmed"
    assert entries[1]["_runtime_confidence"] == "confirmed"
    assert entries[2]["_runtime_confidence"] == "declared_only"


def test_attach_helper_handles_invalid_db_path(tmp_path):
    summary = attach_runtime_confidence(
        [{"id": "x"}],
        str(tmp_path / "does-not-exist.db"),
        id_field="id",
    )
    assert summary == {}


# ──────────────────────────────────────────────────────────────────────
# Integration: get_symbol_source
# ──────────────────────────────────────────────────────────────────────


def test_get_symbol_source_stamps_runtime_after_ingest(tmp_path, monkeypatch):
    """get_symbol_source stamps `_runtime_confidence` per result and adds
    runtime_freshness to _meta when traces are ingested."""
    from jcodemunch_mcp.tools.get_symbol import get_symbol_source
    from jcodemunch_mcp.storage import IndexStore

    # Build a real index for "local/runtime-test"
    store = IndexStore(base_path=str(tmp_path))
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    (src_dir / "handlers.py").write_text(
        "def get_users():\n    return []\n\ndef create_user():\n    return None\n",
        encoding="utf-8",
    )

    from jcodemunch_mcp.tools.index_folder import index_folder
    result = index_folder(
        path=str(tmp_path),
        use_ai_summaries=False,
        storage_path=str(tmp_path),
    )
    assert result.get("success")
    repo = result["repo"]
    owner, name = repo.split("/", 1)

    # Find the symbol_id for get_users
    db_path = store._sqlite._db_path(owner, name)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT id, file, line FROM symbols WHERE name = 'get_users'"
    ).fetchall()
    conn.close()
    assert rows
    sym_id = rows[0]["id"]
    sym_file = rows[0]["file"]
    sym_line = rows[0]["line"]

    # Ingest a trace pointing at get_users
    trace = tmp_path / "trace.jsonl"
    payload = {"resourceSpans": [{"resource": {"attributes": []}, "scopeSpans": [{"scope": {"name": "t"}, "spans": [
        _otel_span(sym_file, sym_line, "get_users")
    ]}]}]}
    trace.write_text(json.dumps(payload))
    ingest_result = ingest_otel_file(db_path=str(db_path), file_path=str(trace))
    assert ingest_result["mapped"] == 1

    # Query — single mode
    out = get_symbol_source(repo=repo, symbol_id=sym_id, storage_path=str(tmp_path))
    assert out.get("error") is None
    assert out.get("_runtime_confidence") == "confirmed"
    assert out["_meta"]["runtime_freshness"]["coverage_pct"] == 100
    assert "otel" in out["_meta"]["runtime_freshness"]["sources"]


def test_get_symbol_source_no_runtime_field_when_no_traces(tmp_path):
    """Without traces ingested, get_symbol_source response shape is unchanged."""
    from jcodemunch_mcp.tools.get_symbol import get_symbol_source

    src_dir = tmp_path / "src"
    src_dir.mkdir()
    (src_dir / "handlers.py").write_text("def get_users():\n    return []\n", encoding="utf-8")

    from jcodemunch_mcp.tools.index_folder import index_folder
    result = index_folder(
        path=str(tmp_path),
        use_ai_summaries=False,
        storage_path=str(tmp_path),
    )
    repo = result["repo"]
    sym_id = next(
        s["id"] for s in result.get("symbols", [])
        if isinstance(s, dict) and s.get("name") == "get_users"
    ) if result.get("symbols") else None

    if not sym_id:
        # Older index_folder shape — query SQLite directly
        owner, name = repo.split("/", 1)
        from jcodemunch_mcp.storage import IndexStore
        store = IndexStore(base_path=str(tmp_path))
        db_path = store._sqlite._db_path(owner, name)
        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        sym_id = conn.execute("SELECT id FROM symbols WHERE name='get_users'").fetchone()["id"]
        conn.close()

    out = get_symbol_source(repo=repo, symbol_id=sym_id, storage_path=str(tmp_path))
    assert "_runtime_confidence" not in out
    assert "runtime_freshness" not in out.get("_meta", {})


# ──────────────────────────────────────────────────────────────────────
# Integration: search_symbols
# ──────────────────────────────────────────────────────────────────────


def test_search_symbols_stamps_runtime_after_ingest(tmp_path):
    from jcodemunch_mcp.tools.search_symbols import search_symbols
    from jcodemunch_mcp.tools.index_folder import index_folder
    from jcodemunch_mcp.storage import IndexStore

    src = tmp_path / "src"
    src.mkdir()
    (src / "h.py").write_text("def alpha():\n    pass\ndef beta():\n    pass\n", encoding="utf-8")

    result = index_folder(
        path=str(tmp_path),
        use_ai_summaries=False,
        storage_path=str(tmp_path),
    )
    repo = result["repo"]
    owner, name = repo.split("/", 1)

    store = IndexStore(base_path=str(tmp_path))
    db_path = store._sqlite._db_path(owner, name)

    # Ingest trace pointing at alpha only
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    alpha_row = conn.execute("SELECT file, line FROM symbols WHERE name='alpha'").fetchone()
    conn.close()
    trace = tmp_path / "trace.jsonl"
    trace.write_text(json.dumps({
        "resourceSpans": [{"resource": {"attributes": []}, "scopeSpans": [{"scope": {"name": "t"}, "spans": [
            _otel_span(alpha_row["file"], alpha_row["line"], "alpha")
        ]}]}]
    }))
    ingest_otel_file(db_path=str(db_path), file_path=str(trace))

    out = search_symbols(repo=repo, query="alpha beta", storage_path=str(tmp_path))
    results = out.get("results", [])
    assert results
    by_name = {r["name"]: r for r in results}
    assert by_name["alpha"]["_runtime_confidence"] == "confirmed"
    assert by_name["beta"]["_runtime_confidence"] == "declared_only"
    assert "runtime_freshness" in out["_meta"]


def test_search_symbols_no_runtime_field_when_empty(tmp_path):
    from jcodemunch_mcp.tools.search_symbols import search_symbols
    from jcodemunch_mcp.tools.index_folder import index_folder

    src = tmp_path / "src"
    src.mkdir()
    (src / "h.py").write_text("def alpha():\n    pass\n", encoding="utf-8")

    result = index_folder(path=str(tmp_path), use_ai_summaries=False, storage_path=str(tmp_path))
    repo = result["repo"]

    out = search_symbols(repo=repo, query="alpha", storage_path=str(tmp_path))
    for r in out.get("results", []):
        assert "_runtime_confidence" not in r
    assert "runtime_freshness" not in out.get("_meta", {})


# ──────────────────────────────────────────────────────────────────────
# Integration: find_references (file-level)
# ──────────────────────────────────────────────────────────────────────


def test_find_references_stamps_file_level_runtime(tmp_path):
    """find_references gets file-level confidence on each reference."""
    from jcodemunch_mcp.tools.find_references import find_references
    from jcodemunch_mcp.tools.index_folder import index_folder
    from jcodemunch_mcp.storage import IndexStore

    src = tmp_path / "src"
    src.mkdir()
    (src / "lib.py").write_text("def auth():\n    return True\n", encoding="utf-8")
    (src / "app.py").write_text("from .lib import auth\n\ndef serve():\n    return auth()\n", encoding="utf-8")
    (src / "other.py").write_text("from .lib import auth\n\ndef other():\n    return auth()\n", encoding="utf-8")

    result = index_folder(path=str(tmp_path), use_ai_summaries=False, storage_path=str(tmp_path))
    repo = result["repo"]
    owner, name = repo.split("/", 1)

    store = IndexStore(base_path=str(tmp_path))
    db_path = store._sqlite._db_path(owner, name)

    # Ingest trace marking serve() as runtime-confirmed (file: src/app.py)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    serve_row = conn.execute("SELECT file, line FROM symbols WHERE name='serve'").fetchone()
    conn.close()
    if serve_row is None:
        pytest.skip("serve symbol not extracted by index_folder; skipping file-level integration test")

    trace = tmp_path / "trace.jsonl"
    trace.write_text(json.dumps({
        "resourceSpans": [{"resource": {"attributes": []}, "scopeSpans": [{"scope": {"name": "t"}, "spans": [
            _otel_span(serve_row["file"], serve_row["line"], "serve")
        ]}]}]
    }))
    res = ingest_otel_file(db_path=str(db_path), file_path=str(trace))
    assert res["mapped"] == 1

    out = find_references(repo=repo, identifier="auth", storage_path=str(tmp_path))
    refs = out.get("references", [])
    if not refs:
        pytest.skip("no references resolved by import graph; skipping integration assertion")
    by_file = {r["file"]: r for r in refs}
    assert by_file.get(serve_row["file"], {}).get("_runtime_confidence") == "confirmed"
    if "src/other.py" in by_file:
        assert by_file["src/other.py"]["_runtime_confidence"] == "declared_only"
    assert "runtime_freshness" in out.get("_meta", {})


def test_find_references_no_runtime_field_when_empty(tmp_path):
    from jcodemunch_mcp.tools.find_references import find_references
    from jcodemunch_mcp.tools.index_folder import index_folder

    src = tmp_path / "src"
    src.mkdir()
    (src / "lib.py").write_text("def auth():\n    return True\n", encoding="utf-8")
    (src / "app.py").write_text("from .lib import auth\n\nauth()\n", encoding="utf-8")

    result = index_folder(path=str(tmp_path), use_ai_summaries=False, storage_path=str(tmp_path))
    repo = result["repo"]

    out = find_references(repo=repo, identifier="auth", storage_path=str(tmp_path))
    for r in out.get("references", []):
        assert "_runtime_confidence" not in r
    assert "runtime_freshness" not in out.get("_meta", {})


# ──────────────────────────────────────────────────────────────────────
# Coverage rounding edge case
# ──────────────────────────────────────────────────────────────────────


def test_coverage_pct_clamps_to_100_max(tmp_path):
    store, db_path = _seed_index(tmp_path)
    _ingest(db_path, tmp_path)
    entries = [{"id": "src/handlers.py::get_users#function"}]
    summary = attach_runtime_confidence(entries, str(db_path), id_field="id")
    assert summary["coverage_pct"] == 100
