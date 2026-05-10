"""Phase 1 tests: OTel JSON ingest end-to-end."""

from __future__ import annotations

import gzip
import json
import sqlite3
from pathlib import Path

import pytest

from jcodemunch_mcp.runtime import ingest_otel_file, parse_otel_file
from jcodemunch_mcp.storage.sqlite_store import SQLiteIndexStore


# ──────────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────────


def _otel_span(file_path: str, line_no: int, function_name: str, *, duration_ns: int = 1_000_000) -> dict:
    """Build a single OTLP span dict with code attributes."""
    return {
        "traceId": "abc",
        "spanId": "def",
        "name": f"GET /api/{function_name}",
        "startTimeUnixNano": "1000000000000000000",
        "endTimeUnixNano": str(1_000_000_000_000_000_000 + duration_ns),
        "attributes": [
            {"key": "code.filepath", "value": {"stringValue": file_path}},
            {"key": "code.lineno", "value": {"intValue": str(line_no)}},
            {"key": "code.function", "value": {"stringValue": function_name}},
            {"key": "http.status_code", "value": {"intValue": "200"}},
            {"key": "user.email", "value": {"stringValue": "alice@example.com"}},
        ],
    }


def _wrap_resource_spans(spans: list[dict]) -> dict:
    """Wrap a list of spans in the OTLP resourceSpans -> scopeSpans -> spans envelope."""
    return {
        "resourceSpans": [
            {
                "resource": {"attributes": []},
                "scopeSpans": [
                    {
                        "scope": {"name": "test"},
                        "spans": spans,
                    }
                ],
            }
        ]
    }


def _seed_index_with_symbols(tmp_path: Path) -> tuple[SQLiteIndexStore, Path]:
    """Index a small repo with known symbols for resolution tests."""
    store = SQLiteIndexStore(base_path=str(tmp_path))
    db_path = store._db_path("local", "phase1")
    conn = store._connect(db_path)
    try:
        conn.executescript(
            """
            INSERT INTO symbols (id, file, name, kind, line, end_line) VALUES
                ('src/handlers.py::get_users#function', 'src/handlers.py', 'get_users', 'function', 10, 25),
                ('src/handlers.py::create_user#function', 'src/handlers.py', 'create_user', 'function', 30, 50),
                ('src/handlers.py::delete_user#function', 'src/handlers.py', 'delete_user', 'function', 55, 70),
                ('src/auth.py::login#function', 'src/auth.py', 'login', 'function', 5, 20),
                ('src/auth.py::logout#function', 'src/auth.py', 'logout', 'function', 25, 35),
                ('src/db.py::query#function', 'src/db.py', 'query', 'function', 1, 100);
            """
        )
        conn.commit()
    finally:
        conn.close()
    return store, db_path


# ──────────────────────────────────────────────────────────────────────
# parse_otel_file
# ──────────────────────────────────────────────────────────────────────


def test_parse_otel_file_jsonlines(tmp_path):
    """JSON-Lines: each line is one resourceSpans envelope."""
    out = tmp_path / "trace.jsonl"
    spans = [
        _otel_span("src/handlers.py", 12, "get_users"),
        _otel_span("src/auth.py", 8, "login"),
    ]
    with out.open("w") as f:
        for span in spans:
            f.write(json.dumps(_wrap_resource_spans([span])))
            f.write("\n")
    parsed = list(parse_otel_file(str(out)))
    assert len(parsed) == 2
    assert parsed[0].file_path == "src/handlers.py"
    assert parsed[0].line_no == 12
    assert parsed[0].function_name == "get_users"


def test_parse_otel_file_single_object(tmp_path):
    """Single top-level object with all spans batched in one envelope."""
    out = tmp_path / "trace.json"
    spans = [
        _otel_span("src/handlers.py", 12, "get_users"),
        _otel_span("src/handlers.py", 35, "create_user"),
        _otel_span("src/auth.py", 8, "login"),
    ]
    out.write_text(json.dumps(_wrap_resource_spans(spans)))
    parsed = list(parse_otel_file(str(out)))
    assert len(parsed) == 3


def test_parse_otel_file_array(tmp_path):
    """Top-level array of records."""
    out = tmp_path / "trace.json"
    payload = [
        _wrap_resource_spans([_otel_span("src/handlers.py", 12, "get_users")]),
        _wrap_resource_spans([_otel_span("src/auth.py", 8, "login")]),
    ]
    out.write_text(json.dumps(payload))
    assert len(list(parse_otel_file(str(out)))) == 2


def test_parse_otel_file_gzipped(tmp_path):
    """Gzipped JSONL is decompressed transparently."""
    out = tmp_path / "trace.jsonl.gz"
    with gzip.open(out, "wt", encoding="utf-8") as f:
        f.write(json.dumps(_wrap_resource_spans([_otel_span("src/handlers.py", 12, "get_users")])))
        f.write("\n")
    parsed = list(parse_otel_file(str(out)))
    assert len(parsed) == 1
    assert parsed[0].function_name == "get_users"


def test_parse_otel_file_extracts_duration_ms(tmp_path):
    """endTimeUnixNano - startTimeUnixNano → duration_ms (float)."""
    out = tmp_path / "trace.jsonl"
    span = _otel_span("src/handlers.py", 12, "get_users", duration_ns=4_200_000)  # 4.2 ms
    out.write_text(json.dumps(_wrap_resource_spans([span])) + "\n")
    parsed = list(parse_otel_file(str(out)))
    assert parsed[0].duration_ms == pytest.approx(4.2)


def test_parse_otel_file_handles_missing_code_attrs(tmp_path):
    """Span without code.* attributes still yields an OtelSpan with None fields."""
    out = tmp_path / "trace.jsonl"
    span = {
        "traceId": "x", "spanId": "y", "name": "no-code-attrs",
        "startTimeUnixNano": "0", "endTimeUnixNano": "0",
        "attributes": [{"key": "user.id", "value": {"stringValue": "42"}}],
    }
    out.write_text(json.dumps(_wrap_resource_spans([span])) + "\n")
    parsed = list(parse_otel_file(str(out)))
    assert len(parsed) == 1
    assert parsed[0].file_path is None
    assert parsed[0].line_no is None
    assert parsed[0].function_name is None


def test_parse_otel_file_missing_path_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        list(parse_otel_file(str(tmp_path / "does-not-exist.jsonl")))


# ──────────────────────────────────────────────────────────────────────
# ingest_otel_file — happy path
# ──────────────────────────────────────────────────────────────────────


def test_ingest_populates_runtime_calls(tmp_path):
    """Ingesting a small file maps spans to symbol_ids and writes runtime_calls."""
    store, db_path = _seed_index_with_symbols(tmp_path)
    trace = tmp_path / "trace.jsonl"
    spans = [
        _otel_span("src/handlers.py", 12, "get_users"),
        _otel_span("src/handlers.py", 12, "get_users"),  # 2nd hit on same symbol
        _otel_span("src/auth.py", 8, "login"),
    ]
    with trace.open("w") as f:
        for s in spans:
            f.write(json.dumps(_wrap_resource_spans([s])) + "\n")

    result = ingest_otel_file(db_path=str(db_path), file_path=str(trace))

    assert result["records"] == 3
    assert result["mapped"] == 3
    assert result["unmapped"] == 0

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT symbol_id, source, count, p50_ms, p95_ms FROM runtime_calls ORDER BY symbol_id"
    ).fetchall()
    conn.close()
    assert len(rows) == 2
    by_id = {r["symbol_id"]: r for r in rows}
    assert by_id["src/handlers.py::get_users#function"]["count"] == 2
    assert by_id["src/handlers.py::get_users#function"]["source"] == "otel"
    assert by_id["src/handlers.py::get_users#function"]["p50_ms"] is not None
    assert by_id["src/auth.py::login#function"]["count"] == 1


def test_ingest_records_unmapped(tmp_path):
    """Spans whose (file, line) doesn't resolve land in runtime_unmapped."""
    store, db_path = _seed_index_with_symbols(tmp_path)
    trace = tmp_path / "trace.jsonl"
    spans = [
        _otel_span("src/nonexistent.py", 1, "missing"),
        _otel_span("src/another.py", 5, "ghost"),
    ]
    with trace.open("w") as f:
        for s in spans:
            f.write(json.dumps(_wrap_resource_spans([s])) + "\n")

    result = ingest_otel_file(db_path=str(db_path), file_path=str(trace))
    assert result["mapped"] == 0
    assert result["unmapped"] == 2
    assert result["unmapped_reasons"]["no_match"] == 2

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    n = conn.execute("SELECT COUNT(*) AS n FROM runtime_unmapped").fetchone()["n"]
    conn.close()
    assert n == 2


def test_ingest_no_code_attrs_reason(tmp_path):
    """Spans without any code attributes are reported under no_code_attrs."""
    store, db_path = _seed_index_with_symbols(tmp_path)
    trace = tmp_path / "trace.jsonl"
    span = {
        "traceId": "x", "spanId": "y", "name": "background-task",
        "startTimeUnixNano": "0", "endTimeUnixNano": "0",
        "attributes": [],
    }
    trace.write_text(json.dumps(_wrap_resource_spans([span])) + "\n")
    result = ingest_otel_file(db_path=str(db_path), file_path=str(trace))
    assert result["unmapped"] == 1
    assert result["unmapped_reasons"]["no_code_attrs"] == 1


def test_ingest_records_redaction_log(tmp_path):
    """Redaction labels fired during ingest are persisted in runtime_redaction_log."""
    store, db_path = _seed_index_with_symbols(tmp_path)
    trace = tmp_path / "trace.jsonl"
    # Span with an email in user.email — should fire email_address pattern
    spans = [_otel_span("src/handlers.py", 12, "get_users") for _ in range(3)]
    with trace.open("w") as f:
        for s in spans:
            f.write(json.dumps(_wrap_resource_spans([s])) + "\n")

    result = ingest_otel_file(db_path=str(db_path), file_path=str(trace))
    assert result["redactions_fired"]
    # email_address fires once per span
    assert "email_address" in result["redactions_fired"]
    assert result["redactions_fired"]["email_address"] == 3

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT pattern, redaction_count FROM runtime_redaction_log"
    ).fetchall()
    conn.close()
    by_pattern = {r["pattern"]: r["redaction_count"] for r in rows}
    assert by_pattern["email_address"] == 3


def test_ingest_idempotent_additive(tmp_path):
    """Re-ingesting the same file doubles the count (additive contract)."""
    store, db_path = _seed_index_with_symbols(tmp_path)
    trace = tmp_path / "trace.jsonl"
    trace.write_text(
        json.dumps(_wrap_resource_spans([_otel_span("src/handlers.py", 12, "get_users")])) + "\n"
    )
    ingest_otel_file(db_path=str(db_path), file_path=str(trace))
    ingest_otel_file(db_path=str(db_path), file_path=str(trace))

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT count FROM runtime_calls WHERE symbol_id = 'src/handlers.py::get_users#function'"
    ).fetchone()
    conn.close()
    assert row["count"] == 2


def test_ingest_fifo_eviction(tmp_path):
    """When runtime_calls exceeds max_rows, oldest entries are evicted."""
    store, db_path = _seed_index_with_symbols(tmp_path)
    trace = tmp_path / "trace.jsonl"
    # Six spans, one per symbol — should produce 6 runtime_calls rows.
    spans = [
        _otel_span("src/handlers.py", 12, "get_users"),
        _otel_span("src/handlers.py", 35, "create_user"),
        _otel_span("src/handlers.py", 60, "delete_user"),
        _otel_span("src/auth.py", 8, "login"),
        _otel_span("src/auth.py", 28, "logout"),
        _otel_span("src/db.py", 15, "query"),
    ]
    with trace.open("w") as f:
        for s in spans:
            f.write(json.dumps(_wrap_resource_spans([s])) + "\n")

    # Cap at 3 — should trim 3 oldest after the upsert.
    result = ingest_otel_file(db_path=str(db_path), file_path=str(trace), max_rows=3)
    assert result["mapped"] == 6
    assert result["evicted"] == 3

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    n = conn.execute("SELECT COUNT(*) AS n FROM runtime_calls").fetchone()["n"]
    conn.close()
    assert n == 3


def test_ingest_session_stats_reflects_rows(tmp_path):
    """After ingest, get_session_stats reports a non-zero runtime_signal."""
    from jcodemunch_mcp.storage.token_tracker import get_session_stats

    store, db_path = _seed_index_with_symbols(tmp_path)
    trace = tmp_path / "trace.jsonl"
    trace.write_text(
        json.dumps(_wrap_resource_spans([_otel_span("src/handlers.py", 12, "get_users")])) + "\n"
    )
    ingest_otel_file(db_path=str(db_path), file_path=str(trace))
    stats = get_session_stats(base_path=str(tmp_path))
    assert stats["runtime_signal"]["rows"] == 1
    assert stats["runtime_signal"]["by_source"]["otel"] == 1


def test_ingest_redact_disabled(tmp_path):
    """When redaction is off, redactions_fired is empty (PII would land if extras were stored)."""
    store, db_path = _seed_index_with_symbols(tmp_path)
    trace = tmp_path / "trace.jsonl"
    trace.write_text(
        json.dumps(_wrap_resource_spans([_otel_span("src/handlers.py", 12, "get_users")])) + "\n"
    )
    result = ingest_otel_file(
        db_path=str(db_path), file_path=str(trace), redact_enabled=False
    )
    assert result["redactions_fired"] == {}


def test_ingest_missing_file_raises(tmp_path):
    store, db_path = _seed_index_with_symbols(tmp_path)
    with pytest.raises(FileNotFoundError):
        ingest_otel_file(db_path=str(db_path), file_path=str(tmp_path / "missing.jsonl"))


# ──────────────────────────────────────────────────────────────────────
# Mapping-rate threshold (verification criterion in todo.md Phase 1)
# ──────────────────────────────────────────────────────────────────────


def test_import_runtime_signal_mcp_tool_wrapper(tmp_path):
    """The MCP-tool wrapper resolves the repo, runs ingest, returns success."""
    from jcodemunch_mcp.tools.import_runtime_signal import import_runtime_signal

    store, db_path = _seed_index_with_symbols(tmp_path)
    trace = tmp_path / "trace.jsonl"
    trace.write_text(
        json.dumps(_wrap_resource_spans([_otel_span("src/handlers.py", 12, "get_users")])) + "\n"
    )
    result = import_runtime_signal(
        source="otel",
        path=str(trace),
        repo="local/phase1",
        storage_path=str(tmp_path),
    )
    assert result["success"] is True
    assert result["source"] == "otel"
    assert result["repo"] == "local/phase1"
    assert result["records"] == 1
    assert result["mapped"] == 1


def test_import_runtime_signal_rejects_unknown_source(tmp_path):
    from jcodemunch_mcp.tools.import_runtime_signal import import_runtime_signal

    store, db_path = _seed_index_with_symbols(tmp_path)
    result = import_runtime_signal(
        source="bogus",
        path=str(tmp_path / "x.jsonl"),
        repo="local/phase1",
        storage_path=str(tmp_path),
    )
    assert result["success"] is False
    assert "unknown source" in result["error"]


def test_import_runtime_signal_rejects_phase4_sources(tmp_path):
    from jcodemunch_mcp.tools.import_runtime_signal import import_runtime_signal

    store, db_path = _seed_index_with_symbols(tmp_path)
    result = import_runtime_signal(
        source="sql_log",
        path=str(tmp_path / "x.jsonl"),
        repo="local/phase1",
        storage_path=str(tmp_path),
    )
    assert result["success"] is False
    assert "not yet implemented" in result["error"]


def test_import_runtime_signal_missing_index_returns_error(tmp_path):
    from jcodemunch_mcp.tools.import_runtime_signal import import_runtime_signal

    result = import_runtime_signal(
        source="otel",
        path=str(tmp_path / "trace.jsonl"),
        repo="local/no-such-repo",
        storage_path=str(tmp_path),
    )
    assert result["success"] is False
    assert "index database not found" in result["error"]


def test_ingest_mapping_rate_above_90_percent(tmp_path):
    """Mapping rate ≥90% on a synthetic FastAPI-style fixture.

    Per todo.md Phase 1 verification: a realistic trace with the same
    spans you would see from an instrumented FastAPI app maps cleanly.
    Synthetic here — we control both the index and the spans — so the
    threshold is tighter than in the wild (≥95% with this fixture).
    """
    store, db_path = _seed_index_with_symbols(tmp_path)
    trace = tmp_path / "trace.jsonl"

    # 100 spans: 95 against known symbols, 5 against unknown paths
    known = [
        ("src/handlers.py", 12, "get_users"),
        ("src/handlers.py", 35, "create_user"),
        ("src/handlers.py", 60, "delete_user"),
        ("src/auth.py", 8, "login"),
        ("src/auth.py", 28, "logout"),
        ("src/db.py", 15, "query"),
    ]
    unknown = [
        ("src/missing.py", 1, "ghost"),
        ("src/missing.py", 2, "phantom"),
    ]

    lines = []
    for i in range(95):
        f, l, n = known[i % len(known)]
        lines.append(json.dumps(_wrap_resource_spans([_otel_span(f, l, n)])))
    for i in range(5):
        f, l, n = unknown[i % len(unknown)]
        lines.append(json.dumps(_wrap_resource_spans([_otel_span(f, l, n)])))
    trace.write_text("\n".join(lines) + "\n")

    result = ingest_otel_file(db_path=str(db_path), file_path=str(trace))
    mapping_rate = result["mapped"] / max(1, result["records"])
    assert mapping_rate >= 0.90, f"mapping rate {mapping_rate:.2%} < 90% threshold"
    assert result["records"] == 100
    assert result["mapped"] == 95
    assert result["unmapped"] == 5
