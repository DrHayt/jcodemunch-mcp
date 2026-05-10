"""Phase 3: get_runtime_coverage, find_hot_paths, find_unused_paths."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from jcodemunch_mcp.runtime.ingest import ingest_otel_file
from jcodemunch_mcp.tools.find_hot_paths import find_hot_paths
from jcodemunch_mcp.tools.find_unused_paths import find_unused_paths
from jcodemunch_mcp.tools.get_runtime_coverage import get_runtime_coverage
from jcodemunch_mcp.storage.sqlite_store import SQLiteIndexStore


# ──────────────────────────────────────────────────────────────────────
# Fixture builder
# ──────────────────────────────────────────────────────────────────────


def _seed_index(tmp_path: Path) -> tuple[SQLiteIndexStore, Path, str]:
    """Index a small repo + return (store, db_path, repo_id)."""
    store = SQLiteIndexStore(base_path=str(tmp_path))
    repo_owner, repo_name = "local", "phase3"
    db_path = store._db_path(repo_owner, repo_name)
    conn = store._connect(db_path)
    try:
        conn.executescript(
            """
            INSERT INTO meta (key, value) VALUES
                ('repo', 'local/phase3'),
                ('owner', 'local'),
                ('name', 'phase3'),
                ('indexed_at', '2026-05-10T00:00:00Z'),
                ('source_root', ''),
                ('languages', '{"python": 6}');
            INSERT INTO symbols (id, file, name, kind, line, end_line) VALUES
                ('src/handlers.py::get_users#function', 'src/handlers.py', 'get_users', 'function', 10, 25),
                ('src/handlers.py::create_user#function', 'src/handlers.py', 'create_user', 'function', 30, 50),
                ('src/handlers.py::delete_user#function', 'src/handlers.py', 'delete_user', 'function', 55, 70),
                ('src/auth.py::login#function', 'src/auth.py', 'login', 'function', 5, 20),
                ('src/auth.py::logout#function', 'src/auth.py', 'logout', 'function', 25, 35),
                ('tests/test_users.py::test_get_users#function', 'tests/test_users.py', 'test_get_users', 'function', 1, 10),
                ('main.py::main#function', 'main.py', 'main', 'function', 1, 5);
            """
        )
        conn.commit()
    finally:
        conn.close()
    return store, db_path, f"{repo_owner}/{repo_name}"


def _otel_span(file_path: str, line_no: int, function_name: str, *, duration_ns: int = 1_000_000) -> dict:
    return {
        "traceId": "x", "spanId": "y", "name": function_name,
        "startTimeUnixNano": "0", "endTimeUnixNano": str(duration_ns),
        "attributes": [
            {"key": "code.filepath", "value": {"stringValue": file_path}},
            {"key": "code.lineno", "value": {"intValue": str(line_no)}},
            {"key": "code.function", "value": {"stringValue": function_name}},
        ],
    }


def _wrap(spans: list[dict]) -> dict:
    return {"resourceSpans": [{"resource": {"attributes": []}, "scopeSpans": [{"scope": {"name": "t"}, "spans": spans}]}]}


def _ingest_baseline(db_path: Path, tmp_path: Path) -> None:
    """Ingest 10 hits on get_users, 3 on login, 1 on logout — leaves create_user / delete_user dark."""
    trace = tmp_path / "trace.jsonl"
    spans = (
        [_otel_span("src/handlers.py", 12, "get_users") for _ in range(10)]
        + [_otel_span("src/auth.py", 8, "login") for _ in range(3)]
        + [_otel_span("src/auth.py", 28, "logout")]
    )
    trace.write_text(json.dumps(_wrap(spans)))
    ingest_otel_file(db_path=str(db_path), file_path=str(trace))


# ──────────────────────────────────────────────────────────────────────
# get_runtime_coverage
# ──────────────────────────────────────────────────────────────────────


def test_coverage_zero_when_no_traces(tmp_path):
    _seed_index(tmp_path)
    out = get_runtime_coverage(repo="local/phase3", storage_path=str(tmp_path))
    assert out["total_symbols"] == 7
    assert out["confirmed"] == 0
    assert out["coverage_pct"] == 0
    assert out["sources"] == []
    assert out["unmapped_runtime"] == []


def test_coverage_repo_wide_with_traces(tmp_path):
    _store, db_path, repo = _seed_index(tmp_path)
    _ingest_baseline(db_path, tmp_path)
    out = get_runtime_coverage(repo=repo, storage_path=str(tmp_path))
    # 3 distinct symbols got runtime hits: get_users, login, logout
    assert out["confirmed"] == 3
    assert out["total_symbols"] == 7
    assert out["coverage_pct"] == round(100 * 3 / 7)
    assert out["sources"] == ["otel"]
    assert out["last_seen"]


def test_coverage_scoped_to_file(tmp_path):
    _store, db_path, repo = _seed_index(tmp_path)
    _ingest_baseline(db_path, tmp_path)
    out = get_runtime_coverage(
        repo=repo,
        file_path="src/auth.py",
        storage_path=str(tmp_path),
    )
    assert out["scope"] == "file:src/auth.py"
    # auth.py has 2 symbols, both got hits → 100%
    assert out["total_symbols"] == 2
    assert out["confirmed"] == 2
    assert out["coverage_pct"] == 100


def test_coverage_lists_unmapped(tmp_path):
    """Spans pointing at code the AST doesn't have → runtime_unmapped → surface here."""
    _store, db_path, repo = _seed_index(tmp_path)
    trace = tmp_path / "trace.jsonl"
    spans = [_otel_span("src/missing.py", 1, "ghost"), _otel_span("src/missing.py", 1, "ghost")]
    trace.write_text(json.dumps(_wrap(spans)))
    ingest_otel_file(db_path=str(db_path), file_path=str(trace))
    out = get_runtime_coverage(repo=repo, storage_path=str(tmp_path))
    assert len(out["unmapped_runtime"]) == 1
    entry = out["unmapped_runtime"][0]
    assert entry["function_name"] == "ghost"
    assert entry["count"] == 2


def test_coverage_unknown_repo_errors(tmp_path):
    out = get_runtime_coverage(repo="local/missing", storage_path=str(tmp_path))
    assert "error" in out


# ──────────────────────────────────────────────────────────────────────
# find_hot_paths
# ──────────────────────────────────────────────────────────────────────


def test_hot_paths_empty_without_traces(tmp_path):
    _seed_index(tmp_path)
    out = find_hot_paths(repo="local/phase3", storage_path=str(tmp_path))
    assert out["results"] == []


def test_hot_paths_ranks_by_count(tmp_path):
    _store, db_path, repo = _seed_index(tmp_path)
    _ingest_baseline(db_path, tmp_path)
    out = find_hot_paths(repo=repo, storage_path=str(tmp_path))
    names = [r["name"] for r in out["results"]]
    assert names == ["get_users", "login", "logout"]
    counts = [r["runtime_count"] for r in out["results"]]
    assert counts == [10, 3, 1]
    # Each should carry sources + last_seen
    for r in out["results"]:
        assert r["sources"] == ["otel"]
        assert r["last_seen"]


def test_hot_paths_filters_by_query(tmp_path):
    _store, db_path, repo = _seed_index(tmp_path)
    _ingest_baseline(db_path, tmp_path)
    out = find_hot_paths(repo=repo, query="user", storage_path=str(tmp_path))
    names = [r["name"] for r in out["results"]]
    # Only 'get_users' has runtime hits AND matches "user" — create_user/delete_user have no hits
    assert names == ["get_users"]


def test_hot_paths_top_n_clamps(tmp_path):
    _store, db_path, repo = _seed_index(tmp_path)
    _ingest_baseline(db_path, tmp_path)
    out = find_hot_paths(repo=repo, top_n=2, storage_path=str(tmp_path))
    assert len(out["results"]) == 2


# ──────────────────────────────────────────────────────────────────────
# find_unused_paths
# ──────────────────────────────────────────────────────────────────────


def test_unused_paths_refuses_when_no_runtime(tmp_path):
    """With zero runtime data, every symbol would trivially qualify — refuse."""
    _seed_index(tmp_path)
    out = find_unused_paths(repo="local/phase3", storage_path=str(tmp_path))
    assert out["results"] == []
    assert out["_meta"]["runtime_data_present"] is False


def test_unused_paths_lists_dark_symbols(tmp_path):
    _store, db_path, repo = _seed_index(tmp_path)
    _ingest_baseline(db_path, tmp_path)
    out = find_unused_paths(repo=repo, storage_path=str(tmp_path))
    names = sorted(r["name"] for r in out["results"])
    # create_user and delete_user have no runtime hits.
    # logout/login/get_users were hit. test_* and main excluded by default.
    assert "create_user" in names
    assert "delete_user" in names
    assert "logout" not in names
    assert "test_get_users" not in names  # excluded as test file
    assert "main" not in names             # excluded as entry-point filename


def test_unused_paths_include_tests(tmp_path):
    _store, db_path, repo = _seed_index(tmp_path)
    _ingest_baseline(db_path, tmp_path)
    out = find_unused_paths(repo=repo, include_tests=True, storage_path=str(tmp_path))
    names = {r["name"] for r in out["results"]}
    assert "test_get_users" in names


def test_unused_paths_include_entry_points(tmp_path):
    _store, db_path, repo = _seed_index(tmp_path)
    _ingest_baseline(db_path, tmp_path)
    out = find_unused_paths(repo=repo, include_entry_points=True, storage_path=str(tmp_path))
    names = {r["name"] for r in out["results"]}
    assert "main" in names


def test_unused_paths_reason_classification(tmp_path):
    _store, db_path, repo = _seed_index(tmp_path)
    _ingest_baseline(db_path, tmp_path)
    out = find_unused_paths(repo=repo, storage_path=str(tmp_path))
    for entry in out["results"]:
        assert entry["reason"] == "no_runtime_evidence"
        assert entry["last_seen"] == ""


def test_unused_paths_meta_counts(tmp_path):
    _store, db_path, repo = _seed_index(tmp_path)
    _ingest_baseline(db_path, tmp_path)
    out = find_unused_paths(repo=repo, storage_path=str(tmp_path))
    assert out["_meta"]["total_symbols_scanned"] == 7
    assert out["_meta"]["excluded_test_files"] >= 1
    assert out["_meta"]["excluded_entry_points"] >= 1
    assert out["_meta"]["runtime_data_present"] is True
