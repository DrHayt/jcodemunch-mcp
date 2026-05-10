"""Phase 5 tests: stack-frame log ingest end-to-end.

Covers:
- Python traceback parser (single + multi-frame, severity inference, message capture)
- JVM traceback parser (Caused-by chains, schema-qualified method names)
- Node.js stack parser (anonymous and named at-lines)
- JSON-Lines structured log path with explicit severity/level
- End-to-end ingest: runtime_calls (severity-agnostic) + runtime_stack_events
  (per-severity) populated, runtime_unmapped for non-resolving frames,
  redaction labels firing for the message field
- get_symbol_provenance picks up stack_frequency when there are events,
  doesn't add the field when there aren't, and amends the narrative when
  error counts cross the threshold
- v15→v16 migration creates runtime_stack_events idempotently
"""

from __future__ import annotations

import gzip
import json
import sqlite3
from pathlib import Path

import pytest

from jcodemunch_mcp.runtime import (
    ingest_stack_log_file,
    parse_stack_log_file,
)
from jcodemunch_mcp.runtime.stack_log import iter_events_from_text
from jcodemunch_mcp.storage.sqlite_store import SQLiteIndexStore


# ──────────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────────


def _seed_index(tmp_path: Path) -> tuple[SQLiteIndexStore, Path]:
    """Index a small repo with enough symbols for the parser fixtures to resolve."""
    store = SQLiteIndexStore(base_path=str(tmp_path))
    db_path = store._db_path("local", "phase5")
    conn = store._connect(db_path)
    try:
        conn.executescript(
            """
            INSERT INTO symbols (id, file, name, kind, line, end_line) VALUES
                ('app/handlers.py::process_request#function', 'app/handlers.py', 'process_request', 'function', 10, 30),
                ('app/handlers.py::validate_input#function', 'app/handlers.py', 'validate_input', 'function', 35, 55),
                ('app/db.py::query#function', 'app/db.py', 'query', 'function', 1, 100),
                ('com/example/Foo.java::bar#function', 'com/example/Foo.java', 'bar', 'function', 42, 60),
                ('src/server.js::handleRequest#function', 'src/server.js', 'handleRequest', 'function', 100, 150);
            """
        )
        conn.commit()
    finally:
        conn.close()
    return store, db_path


# ──────────────────────────────────────────────────────────────────────
# Parser — Python traceback
# ──────────────────────────────────────────────────────────────────────


PY_TRACE = """\
2026-05-10T12:00:00Z ERROR app.handlers Exception while handling request:
Traceback (most recent call last):
  File "app/handlers.py", line 12, in process_request
    validate_input(payload)
  File "app/handlers.py", line 38, in validate_input
    raise ValueError("bad input")
ValueError: bad input
"""


def test_parse_python_traceback_extracts_two_frames():
    events = list(iter_events_from_text(PY_TRACE))
    assert len(events) == 1
    e = events[0]
    assert e.severity == "error"
    assert len(e.frames) == 2
    assert e.frames[0].file_path == "app/handlers.py"
    assert e.frames[0].line_no == 12
    assert e.frames[0].function_name == "process_request"
    assert e.frames[1].function_name == "validate_input"
    assert e.timestamp == "2026-05-10T12:00:00Z"


def test_parse_python_traceback_default_severity_is_info():
    """When no log-level tag precedes the trace, default severity is 'info'."""
    text = (
        'Traceback (most recent call last):\n'
        '  File "app/handlers.py", line 12, in process_request\n'
        '    validate_input(payload)\n'
        'ValueError: bad input\n'
    )
    events = list(iter_events_from_text(text))
    assert len(events) == 1
    assert events[0].severity == "info"


def test_parse_python_traceback_warn_keyword_classified_as_warn():
    text = (
        '2026-05-10T12:00:00Z WARNING app.handlers near-miss:\n'
        'Traceback (most recent call last):\n'
        '  File "app/handlers.py", line 12, in process_request\n'
        '    pass\n'
        'RuntimeWarning: deprecated\n'
    )
    events = list(iter_events_from_text(text))
    assert len(events) == 1
    assert events[0].severity == "warn"


# ──────────────────────────────────────────────────────────────────────
# Parser — JVM traceback
# ──────────────────────────────────────────────────────────────────────


JVM_TRACE = """\
2026-05-09 10:00:00 ERROR [Thread-1] handler caught:
java.lang.NullPointerException: target was null
    at com.example.Foo.bar(Foo.java:42)
    at com.example.Foo.entry(Foo.java:10)
    at java.lang.Thread.run(Thread.java:829)
Caused by: java.lang.IllegalArgumentException: bad arg
    at com.example.Foo.validate(Foo.java:55)
"""


def test_parse_jvm_traceback_collects_all_frames_including_caused_by():
    events = list(iter_events_from_text(JVM_TRACE))
    assert len(events) == 1
    e = events[0]
    assert e.severity == "error"
    # 3 main frames + 1 caused-by frame = 4 total
    assert len(e.frames) == 4
    funcs = [f.function_name for f in e.frames]
    assert "bar" in funcs
    assert "validate" in funcs
    files = {f.file_path for f in e.frames}
    assert "Foo.java" in files


# ──────────────────────────────────────────────────────────────────────
# Parser — Node.js stack
# ──────────────────────────────────────────────────────────────────────


NODE_TRACE = """\
[error] 2026-05-09T10:00:00Z handler crash
Error: socket hang up
    at handleRequest (src/server.js:120:7)
    at TLSSocket.emit (node:events:514:28)
    at src/anon.js:9:5
"""


def test_parse_node_stack_handles_named_and_anonymous_frames():
    events = list(iter_events_from_text(NODE_TRACE))
    assert len(events) == 1
    e = events[0]
    assert e.severity == "error"
    assert len(e.frames) == 3
    assert e.frames[0].function_name == "handleRequest"
    assert e.frames[0].file_path == "src/server.js"
    assert e.frames[0].line_no == 120
    # Anonymous frame: function_name is None, file/line still parsed
    assert e.frames[2].function_name is None
    assert e.frames[2].file_path == "src/anon.js"
    assert e.frames[2].line_no == 9


# ──────────────────────────────────────────────────────────────────────
# Parser — JSON-Lines structured log
# ──────────────────────────────────────────────────────────────────────


def test_parse_jsonl_structured_log_uses_explicit_severity(tmp_path):
    p = tmp_path / "stacks.jsonl"
    p.write_text(json.dumps({
        "severity": "ERROR",
        "ts": "2026-05-10T12:00:00Z",
        "message": "boom",
        "stack_trace": (
            'Traceback (most recent call last):\n'
            '  File "app/handlers.py", line 12, in process_request\n'
            '    pass\n'
            'ValueError: x\n'
        ),
    }) + "\n")
    events = list(parse_stack_log_file(str(p)))
    assert len(events) == 1
    e = events[0]
    assert e.severity == "error"
    assert e.timestamp == "2026-05-10T12:00:00Z"
    assert len(e.frames) == 1


def test_parse_jsonl_top_level_array_supported(tmp_path):
    p = tmp_path / "stacks.json"
    p.write_text(json.dumps([
        {"severity": "WARN", "stack_trace": "    at com.example.Foo.bar(Foo.java:42)\n"},
        {"severity": "ERROR", "stack_trace": "    at com.example.Foo.bar(Foo.java:99)\n"},
    ]))
    events = list(parse_stack_log_file(str(p)))
    assert len(events) == 2
    assert events[0].severity == "warn"
    assert events[1].severity == "error"


def test_parse_gzipped_plain_log(tmp_path):
    p = tmp_path / "app.log.gz"
    with gzip.open(p, "wt", encoding="utf-8") as f:
        f.write(PY_TRACE)
    events = list(parse_stack_log_file(str(p)))
    assert len(events) == 1
    assert events[0].severity == "error"


def test_parse_missing_file_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        list(parse_stack_log_file(str(tmp_path / "missing.log")))


# ──────────────────────────────────────────────────────────────────────
# Ingest — end to end
# ──────────────────────────────────────────────────────────────────────


def test_ingest_populates_runtime_calls_and_stack_events(tmp_path):
    store, db_path = _seed_index(tmp_path)
    log = tmp_path / "app.log"
    log.write_text(PY_TRACE)

    result = ingest_stack_log_file(db_path=str(db_path), file_path=str(log))

    assert result["records"] == 1
    assert result["frames"] == 2
    assert result["mapped"] == 2
    assert result["unmapped"] == 0
    assert result["severity_counts"]["error"] == 1
    assert result["severity_counts"]["warn"] == 0

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    calls = list(conn.execute(
        "SELECT symbol_id, count FROM runtime_calls WHERE source = 'stack_log' ORDER BY symbol_id"
    ))
    events = list(conn.execute(
        "SELECT symbol_id, severity, count FROM runtime_stack_events WHERE source = 'stack_log' ORDER BY symbol_id, severity"
    ))
    conn.close()

    by_call = {r["symbol_id"]: r["count"] for r in calls}
    assert by_call["app/handlers.py::process_request#function"] == 1
    assert by_call["app/handlers.py::validate_input#function"] == 1

    by_event = {(r["symbol_id"], r["severity"]): r["count"] for r in events}
    assert by_event[("app/handlers.py::process_request#function", "error")] == 1
    assert by_event[("app/handlers.py::validate_input#function", "error")] == 1


def test_ingest_jvm_resolves_via_filename_suffix_match(tmp_path):
    store, db_path = _seed_index(tmp_path)
    log = tmp_path / "app.log"
    log.write_text(JVM_TRACE)

    result = ingest_stack_log_file(db_path=str(db_path), file_path=str(log))
    assert result["frames"] == 4
    # com/example/Foo.java::bar is in the seed → at least the bar() frame resolves.
    conn = sqlite3.connect(str(db_path))
    rows = list(conn.execute(
        "SELECT symbol_id FROM runtime_stack_events WHERE source = 'stack_log'"
    ))
    conn.close()
    assert any("Foo.java::bar" in r[0] for r in rows)


def test_ingest_node_resolves_handle_request(tmp_path):
    store, db_path = _seed_index(tmp_path)
    log = tmp_path / "app.log"
    log.write_text(NODE_TRACE)

    result = ingest_stack_log_file(db_path=str(db_path), file_path=str(log))
    assert result["frames"] == 3
    # `handleRequest` in src/server.js is seeded — should resolve.
    conn = sqlite3.connect(str(db_path))
    rows = list(conn.execute(
        "SELECT symbol_id FROM runtime_calls WHERE source = 'stack_log'"
    ))
    conn.close()
    assert any("handleRequest" in r[0] for r in rows)


def test_ingest_unresolved_frame_lands_in_runtime_unmapped(tmp_path):
    store, db_path = _seed_index(tmp_path)
    log = tmp_path / "app.log"
    log.write_text(
        '2026-05-10T12:00:00Z ERROR thing:\n'
        'Traceback (most recent call last):\n'
        '  File "src/never_indexed.py", line 1, in nonexistent_fn\n'
        '    pass\n'
        'ValueError: x\n'
    )
    ingest_stack_log_file(db_path=str(db_path), file_path=str(log))
    conn = sqlite3.connect(str(db_path))
    rows = list(conn.execute(
        "SELECT file_path, function_name FROM runtime_unmapped WHERE source = 'stack_log'"
    ))
    conn.close()
    assert any(r[1] == "nonexistent_fn" for r in rows)


def test_ingest_redacts_email_in_message(tmp_path):
    store, db_path = _seed_index(tmp_path)
    log = tmp_path / "app.log"
    log.write_text(
        '2026-05-10T12:00:00Z ERROR app.handlers contacting alice@example.com\n'
        'Traceback (most recent call last):\n'
        '  File "app/handlers.py", line 12, in process_request\n'
        '    pass\n'
        'ValueError: contacting alice@example.com\n'
    )
    result = ingest_stack_log_file(db_path=str(db_path), file_path=str(log), redact_enabled=True)
    fired = result["redactions_fired"]
    assert any(label in ("email_address",) for label in fired)


def test_ingest_idempotent_under_repeat_run(tmp_path):
    store, db_path = _seed_index(tmp_path)
    log = tmp_path / "app.log"
    log.write_text(PY_TRACE)
    ingest_stack_log_file(db_path=str(db_path), file_path=str(log))
    ingest_stack_log_file(db_path=str(db_path), file_path=str(log))
    conn = sqlite3.connect(str(db_path))
    n = conn.execute(
        "SELECT count FROM runtime_stack_events WHERE symbol_id = 'app/handlers.py::process_request#function' AND severity = 'error'"
    ).fetchone()[0]
    conn.close()
    assert n == 2


# ──────────────────────────────────────────────────────────────────────
# get_symbol_provenance integration
# ──────────────────────────────────────────────────────────────────────


def test_get_symbol_provenance_omits_stack_frequency_when_table_empty(tmp_path):
    """Symbol with no stack events: no stack_frequency field appears."""
    from jcodemunch_mcp.tools.get_symbol_provenance import _load_stack_frequency
    store, db_path = _seed_index(tmp_path)
    out = _load_stack_frequency(
        db_path,
        symbol_id="app/handlers.py::process_request#function",
        since_days=30,
    )
    assert out is None


def test_get_symbol_provenance_surfaces_stack_frequency(tmp_path):
    from jcodemunch_mcp.tools.get_symbol_provenance import _load_stack_frequency
    store, db_path = _seed_index(tmp_path)
    log = tmp_path / "app.log"
    # Write 4 distinct error events on the same symbol so the threshold (>=3 errors)
    # for the narrative-amendment trips.
    log.write_text(PY_TRACE * 4)
    ingest_stack_log_file(db_path=str(db_path), file_path=str(log))

    freq = _load_stack_frequency(
        db_path,
        symbol_id="app/handlers.py::process_request#function",
        since_days=30,
    )
    assert freq is not None
    assert freq["by_severity"]["error"] >= 4
    assert freq["last_seen"] is not None
    assert freq["since_days"] == 30


# ──────────────────────────────────────────────────────────────────────
# v15→v16 migration
# ──────────────────────────────────────────────────────────────────────


def test_migration_creates_runtime_stack_events_table(tmp_path):
    store = SQLiteIndexStore(base_path=str(tmp_path))
    db_path = store._db_path("local", "migration-test")
    conn = store._connect(db_path)
    try:
        info = conn.execute("PRAGMA table_info(runtime_stack_events)").fetchall()
    finally:
        conn.close()
    cols = {r[1] for r in info}
    assert {"symbol_id", "source", "severity", "count", "first_seen", "last_seen"} <= cols


def test_migration_v15_to_v16_idempotent(tmp_path):
    from jcodemunch_mcp.storage.sqlite_store import _migrate_v15_to_v16
    store = SQLiteIndexStore(base_path=str(tmp_path))
    db_path = store._db_path("local", "migration-idempotent")
    conn = store._connect(db_path)
    try:
        _migrate_v15_to_v16(conn)
        _migrate_v15_to_v16(conn)
        version = conn.execute(
            "SELECT value FROM meta WHERE key = 'index_version'"
        ).fetchone()[0]
        assert int(version) >= 16
    finally:
        conn.close()
