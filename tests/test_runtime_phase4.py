"""Phase 4 tests: SQL query log ingest end-to-end.

Covers:
- pg_stat_statements CSV parser (header detection, missing optional columns)
- Generic SQL JSON-Lines parser (.jsonl, top-level array, .gz)
- Table reference extraction (FROM/JOIN/UPDATE/INSERT INTO/DELETE FROM/MERGE INTO)
- Column reference extraction (qualified alias.col, bare SELECT-list, predicate blocks)
- Resolution against a seeded index (file-stem match, exact-name match)
- runtime_columns upsert via dbt-style declared metadata
- find_unused_paths surfacing dbt models with no column reads
- find_unused_paths rescuing models that *do* have column reads
- v14→v15 migration creates runtime_columns idempotently
"""

from __future__ import annotations

import csv
import gzip
import io
import json
import sqlite3
from pathlib import Path

import pytest

from jcodemunch_mcp.runtime import (
    ingest_sql_log_file,
    parse_sql_log_file,
)
from jcodemunch_mcp.runtime.sql_log import _build_record  # type: ignore
from jcodemunch_mcp.storage.sqlite_store import SQLiteIndexStore
from jcodemunch_mcp.tools.find_unused_paths import find_unused_paths


# ──────────────────────────────────────────────────────────────────────
# Fixtures
# ──────────────────────────────────────────────────────────────────────


def _seed_dbt_index(tmp_path: Path) -> tuple[SQLiteIndexStore, Path]:
    """Index a small dbt-shaped repo with three models + dbt_columns metadata."""
    store = SQLiteIndexStore(base_path=str(tmp_path))
    db_path = store._db_path("local", "phase4")
    conn = store._connect(db_path)
    try:
        conn.executescript(
            """
            INSERT INTO symbols (id, file, name, kind, line, end_line) VALUES
                ('models/fact_orders.sql::fact_orders#table', 'models/fact_orders.sql', 'fact_orders', 'table', 1, 50),
                ('models/dim_customers.sql::dim_customers#table', 'models/dim_customers.sql', 'dim_customers', 'table', 1, 30),
                ('models/dim_products.sql::dim_products#table', 'models/dim_products.sql', 'dim_products', 'table', 1, 25),
                ('src/api.py::get_orders#function', 'src/api.py', 'get_orders', 'function', 10, 25);
            """
        )
        ctx_meta = {
            "dbt_columns": {
                "fact_orders": {
                    "order_id": "Primary key",
                    "customer_id": "FK to dim_customers",
                    "product_id": "FK to dim_products",
                    "order_total": "Order total in USD",
                    "deprecated_legacy_field": "Slated for removal",
                },
                "dim_customers": {
                    "customer_id": "Primary key",
                    "email": "Customer email",
                    "signup_date": "Account creation timestamp",
                },
                "dim_products": {
                    "product_id": "Primary key",
                    "sku": "Stock keeping unit",
                    "list_price": "MSRP",
                },
            }
        }
        conn.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES ('context_metadata', ?)",
            (json.dumps(ctx_meta),),
        )
        conn.commit()
    finally:
        conn.close()
    return store, db_path


def _write_jsonl_log(path: Path, queries: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for q in queries:
            f.write(json.dumps(q))
            f.write("\n")


def _write_pg_stat_csv(path: Path, rows: list[dict]) -> None:
    fieldnames = sorted({k for r in rows for k in r.keys()})
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)


# ──────────────────────────────────────────────────────────────────────
# parse_sql_log_file: pg_stat_statements CSV
# ──────────────────────────────────────────────────────────────────────


def test_parse_pg_stat_statements_csv_basic(tmp_path):
    p = tmp_path / "pg_stat.csv"
    _write_pg_stat_csv(p, [
        {"query": "SELECT * FROM fact_orders", "calls": "100", "mean_exec_time": "1.5"},
        {"query": "SELECT email FROM dim_customers", "calls": "42", "mean_exec_time": "0.8"},
    ])
    records = list(parse_sql_log_file(str(p)))
    assert len(records) == 2
    assert records[0].calls == 100
    assert records[0].mean_ms == pytest.approx(1.5)
    assert "fact_orders" in records[0].tables


def test_parse_pg_stat_statements_csv_tolerates_missing_optional_columns(tmp_path):
    p = tmp_path / "pg_stat.csv"
    # Only `query` is required; calls/mean_time absent.
    _write_pg_stat_csv(p, [{"query": "SELECT 1 FROM fact_orders"}])
    records = list(parse_sql_log_file(str(p)))
    assert len(records) == 1
    assert records[0].calls == 1   # default
    assert records[0].mean_ms is None


def test_parse_pg_stat_statements_csv_skips_blank_query_rows(tmp_path):
    p = tmp_path / "pg_stat.csv"
    _write_pg_stat_csv(p, [
        {"query": "SELECT 1 FROM fact_orders", "calls": "5"},
        {"query": "", "calls": "9999"},  # noise row → skipped
        {"query": "   ", "calls": "9999"},  # whitespace-only → skipped
    ])
    records = list(parse_sql_log_file(str(p)))
    assert len(records) == 1


def test_parse_pg_stat_statements_csv_uses_total_time_alias(tmp_path):
    """Older Postgres exposes ``total_time``; newer exposes ``total_exec_time``.
    Either alias should be accepted."""
    p = tmp_path / "pg_stat.csv"
    _write_pg_stat_csv(p, [{"query": "SELECT 1 FROM fact_orders", "calls": "1", "total_time": "12.3"}])
    records = list(parse_sql_log_file(str(p)))
    assert records[0].total_ms == pytest.approx(12.3)


# ──────────────────────────────────────────────────────────────────────
# parse_sql_log_file: JSON-Lines + array fallback
# ──────────────────────────────────────────────────────────────────────


def test_parse_jsonl_log_single_record(tmp_path):
    p = tmp_path / "queries.jsonl"
    _write_jsonl_log(p, [
        {"sql": "SELECT * FROM fact_orders", "duration_ms": 0.5, "ts": "2026-05-09T12:00:00Z"},
    ])
    records = list(parse_sql_log_file(str(p)))
    assert len(records) == 1
    assert records[0].mean_ms == pytest.approx(0.5)
    assert records[0].timestamp == "2026-05-09T12:00:00Z"


def test_parse_jsonl_log_top_level_array(tmp_path):
    p = tmp_path / "queries.json"
    p.write_text(json.dumps([
        {"sql": "SELECT 1 FROM fact_orders"},
        {"sql": "SELECT 2 FROM dim_customers"},
    ]))
    records = list(parse_sql_log_file(str(p)))
    assert len(records) == 2


def test_parse_jsonl_log_skips_blank_and_comment_lines(tmp_path):
    p = tmp_path / "queries.jsonl"
    p.write_text(
        "# a comment\n"
        "\n"
        "// another comment\n"
        '{"sql": "SELECT 1 FROM fact_orders"}\n'
        '{"sql": "SELECT 2 FROM dim_customers"}\n'
    )
    records = list(parse_sql_log_file(str(p)))
    assert len(records) == 2


def test_parse_jsonl_log_gzipped(tmp_path):
    p = tmp_path / "queries.jsonl.gz"
    with gzip.open(p, "wt", encoding="utf-8") as f:
        f.write(json.dumps({"sql": "SELECT * FROM fact_orders"}) + "\n")
    records = list(parse_sql_log_file(str(p)))
    assert len(records) == 1
    assert "fact_orders" in records[0].tables


def test_parse_jsonl_log_missing_path_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        list(parse_sql_log_file(str(tmp_path / "missing.jsonl")))


def test_parse_jsonl_log_skips_malformed_lines(tmp_path):
    p = tmp_path / "queries.jsonl"
    p.write_text(
        '{"sql": "SELECT 1 FROM fact_orders"}\n'
        'NOT JSON HERE\n'
        '{"sql": "SELECT 2 FROM dim_customers"}\n'
    )
    records = list(parse_sql_log_file(str(p)))
    assert len(records) == 2


# ──────────────────────────────────────────────────────────────────────
# Reference extraction
# ──────────────────────────────────────────────────────────────────────


def test_extract_tables_from_join():
    rec = _build_record(sql="SELECT o.* FROM fact_orders o JOIN dim_customers c ON o.customer_id = c.customer_id")
    assert "fact_orders" in rec.tables
    assert "dim_customers" in rec.tables


def test_extract_tables_strips_schema_qualifier():
    rec = _build_record(sql="SELECT * FROM analytics.fact_orders")
    assert rec.tables == ["fact_orders"]


def test_extract_tables_handles_quoted_identifiers():
    rec = _build_record(sql='SELECT * FROM "analytics"."fact_orders"')
    assert "fact_orders" in rec.tables


def test_extract_tables_from_update_delete_insert():
    rec_u = _build_record(sql="UPDATE fact_orders SET status = 'shipped' WHERE order_id = 1")
    assert "fact_orders" in rec_u.tables
    rec_d = _build_record(sql="DELETE FROM dim_customers WHERE signup_date < '2020-01-01'")
    assert "dim_customers" in rec_d.tables
    rec_i = _build_record(sql="INSERT INTO dim_products (sku) VALUES ('X')")
    assert "dim_products" in rec_i.tables


def test_extract_columns_qualified_and_bare():
    rec = _build_record(
        sql="SELECT o.order_id, o.order_total, c.email FROM fact_orders o JOIN dim_customers c ON o.customer_id = c.customer_id WHERE o.order_total > 100"
    )
    qualified = [(t, c) for (t, c) in rec.columns if t]
    assert ("o", "order_id") in qualified
    assert ("o", "order_total") in qualified
    assert ("c", "email") in qualified


def test_extract_columns_skips_keywords_and_star():
    rec = _build_record(sql="SELECT * FROM fact_orders WHERE order_id IS NULL")
    # No column should be the literal '*'; keywords like NULL/IS shouldn't surface.
    cols = {c for (_, c) in rec.columns}
    assert "*" not in cols
    assert "NULL" not in {c.upper() for c in cols}


# ──────────────────────────────────────────────────────────────────────
# ingest_sql_log_file: end-to-end
# ──────────────────────────────────────────────────────────────────────


def test_ingest_populates_runtime_calls_for_resolved_models(tmp_path):
    store, db_path = _seed_dbt_index(tmp_path)
    log = tmp_path / "queries.jsonl"
    _write_jsonl_log(log, [
        {"sql": "SELECT * FROM fact_orders WHERE order_id = 1", "calls": 10},
        {"sql": "SELECT email FROM dim_customers", "calls": 5},
        {"sql": "SELECT * FROM unknown_table", "calls": 3},  # unmapped
    ])

    result = ingest_sql_log_file(db_path=str(db_path), file_path=str(log))

    assert result["records"] == 3
    assert result["mapped"] >= 15  # 10 + 5 from the two resolved tables
    assert result["unmapped"] == 3   # the unknown_table row

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    rows = {r["symbol_id"]: r["count"] for r in conn.execute(
        "SELECT symbol_id, count FROM runtime_calls WHERE source = 'sql_log'"
    )}
    conn.close()
    assert rows.get("models/fact_orders.sql::fact_orders#table") == 10
    assert rows.get("models/dim_customers.sql::dim_customers#table") == 5


def test_ingest_records_runtime_columns_against_dbt_metadata(tmp_path):
    store, db_path = _seed_dbt_index(tmp_path)
    log = tmp_path / "queries.jsonl"
    _write_jsonl_log(log, [
        {"sql": "SELECT order_id, order_total FROM fact_orders", "calls": 4},
        {"sql": "SELECT email FROM dim_customers", "calls": 7},
    ])

    result = ingest_sql_log_file(db_path=str(db_path), file_path=str(log))
    assert result["columns_recorded"] >= 1

    conn = sqlite3.connect(str(db_path))
    rows = list(conn.execute(
        "SELECT model_name, column_name, count FROM runtime_columns WHERE source = 'sql_log'"
    ))
    conn.close()
    seen = {(r[0], r[1]): r[2] for r in rows}
    assert ("fact_orders", "order_id") in seen
    assert ("fact_orders", "order_total") in seen
    assert ("dim_customers", "email") in seen
    # Counts respect the input query's calls multiplier.
    assert seen[("dim_customers", "email")] == 7


def test_ingest_does_not_record_columns_not_in_dbt_metadata(tmp_path):
    store, db_path = _seed_dbt_index(tmp_path)
    log = tmp_path / "queries.jsonl"
    # `not_a_real_column` is referenced but not declared in dbt_columns.
    _write_jsonl_log(log, [
        {"sql": "SELECT not_a_real_column FROM fact_orders", "calls": 1},
    ])
    ingest_sql_log_file(db_path=str(db_path), file_path=str(log))
    conn = sqlite3.connect(str(db_path))
    rows = list(conn.execute(
        "SELECT model_name, column_name FROM runtime_columns WHERE column_name = 'not_a_real_column'"
    ))
    conn.close()
    assert rows == []


def test_ingest_redaction_strips_string_literals(tmp_path):
    store, db_path = _seed_dbt_index(tmp_path)
    log = tmp_path / "queries.jsonl"
    _write_jsonl_log(log, [
        {"sql": "SELECT * FROM fact_orders WHERE customer_email = 'alice@example.com'", "calls": 1},
    ])
    result = ingest_sql_log_file(db_path=str(db_path), file_path=str(log), redact_enabled=True)
    fired = result["redactions_fired"]
    # The string-literal pattern must have fired against the email value.
    assert any("sql_string_literal" in label or "email_address" in label for label in fired)


def test_ingest_unmapped_table_lands_in_runtime_unmapped(tmp_path):
    store, db_path = _seed_dbt_index(tmp_path)
    log = tmp_path / "queries.jsonl"
    _write_jsonl_log(log, [
        {"sql": "SELECT * FROM never_indexed_table", "calls": 4},
    ])
    ingest_sql_log_file(db_path=str(db_path), file_path=str(log))
    conn = sqlite3.connect(str(db_path))
    rows = list(conn.execute(
        "SELECT function_name, count FROM runtime_unmapped WHERE source = 'sql_log'"
    ))
    conn.close()
    assert ("never_indexed_table", 4) in [(r[0], r[1]) for r in rows]


def test_ingest_idempotent_under_repeat_run(tmp_path):
    """Re-importing the same log adds counts; nothing else changes."""
    store, db_path = _seed_dbt_index(tmp_path)
    log = tmp_path / "queries.jsonl"
    _write_jsonl_log(log, [
        {"sql": "SELECT * FROM fact_orders", "calls": 3},
    ])
    ingest_sql_log_file(db_path=str(db_path), file_path=str(log))
    ingest_sql_log_file(db_path=str(db_path), file_path=str(log))
    conn = sqlite3.connect(str(db_path))
    n = conn.execute(
        "SELECT count FROM runtime_calls WHERE symbol_id = 'models/fact_orders.sql::fact_orders#table'"
    ).fetchone()[0]
    conn.close()
    assert n == 6


# ──────────────────────────────────────────────────────────────────────
# find_unused_paths integration
# ──────────────────────────────────────────────────────────────────────


def test_find_unused_paths_surfaces_dbt_model_with_no_column_reads(tmp_path):
    """A model whose declared columns get no SQL-log hits surfaces with reason='dbt_model_no_column_reads'."""
    store, db_path = _seed_dbt_index(tmp_path)
    log = tmp_path / "queries.jsonl"
    # Only fact_orders gets reads; dim_products is never queried.
    _write_jsonl_log(log, [
        {"sql": "SELECT order_id FROM fact_orders", "calls": 2},
    ])
    ingest_sql_log_file(db_path=str(db_path), file_path=str(log))

    result = find_unused_paths(repo="local/phase4", since_days=365, storage_path=str(tmp_path))
    assert "error" not in result, result
    names = {r["name"]: r for r in result["results"]}
    assert "dim_products" in names
    assert names["dim_products"]["reason"] == "dbt_model_no_column_reads"
    assert "list_price" in names["dim_products"]["unused_columns"]
    # fact_orders was queried — should NOT be in the unused list.
    assert "fact_orders" not in names


def test_find_unused_paths_meta_flags_runtime_columns_present(tmp_path):
    """When runtime_columns has rows, _meta.runtime_columns_present is True.
    When a model has column reads but no symbol-level runtime_calls hit
    (column-only audit log shape), it must be rescued from the unused list."""
    store, db_path = _seed_dbt_index(tmp_path)
    log = tmp_path / "queries.jsonl"
    _write_jsonl_log(log, [
        {"sql": "SELECT order_id FROM fact_orders", "calls": 1},
    ])
    ingest_sql_log_file(db_path=str(db_path), file_path=str(log))

    # Simulate a column-only signal: drop the model-level call row but
    # leave the runtime_columns rows in place. This reproduces the
    # pg_audit / column-trace shape where reads are logged per column,
    # never per query.
    conn = sqlite3.connect(str(db_path))
    conn.execute("DELETE FROM runtime_calls WHERE source = 'sql_log'")
    conn.commit()
    conn.close()

    result = find_unused_paths(repo="local/phase4", since_days=365, storage_path=str(tmp_path))
    # Defensive: runtime_calls is empty after the delete, so the tool refuses
    # to enumerate. Re-seed with a non-sql_log row to keep the tool live.
    if not result.get("results") and result["_meta"].get("runtime_data_present") is False:
        conn = sqlite3.connect(str(db_path))
        conn.execute(
            "INSERT INTO runtime_calls (symbol_id, source, count, first_seen, last_seen) "
            "VALUES ('src/api.py::get_orders#function', 'otel', 1, '2026-05-09T00:00:00Z', '2026-05-09T00:00:00Z')"
        )
        conn.commit()
        conn.close()
        result = find_unused_paths(repo="local/phase4", since_days=365, storage_path=str(tmp_path))

    assert result["_meta"]["runtime_columns_present"] is True
    # fact_orders should be rescued from the unused list because
    # runtime_columns has rows for it, even though runtime_calls doesn't.
    assert result["_meta"]["rescued_by_column_hit"] >= 1
    names = {r["name"] for r in result["results"]}
    assert "fact_orders" not in names


# ──────────────────────────────────────────────────────────────────────
# v14→v15 migration
# ──────────────────────────────────────────────────────────────────────


def test_migration_creates_runtime_columns_table(tmp_path):
    """A fresh DB at the current INDEX_VERSION must have the runtime_columns table."""
    store = SQLiteIndexStore(base_path=str(tmp_path))
    db_path = store._db_path("local", "migrate-test")
    conn = store._connect(db_path)
    try:
        info = conn.execute("PRAGMA table_info(runtime_columns)").fetchall()
    finally:
        conn.close()
    cols = {r[1] for r in info}
    assert {"model_name", "column_name", "source", "count", "first_seen", "last_seen"} <= cols


def test_migration_v14_to_v15_idempotent(tmp_path):
    """Running v14→v15 twice does not raise; `IF NOT EXISTS` guards every DDL."""
    from jcodemunch_mcp.storage.sqlite_store import _migrate_v14_to_v15
    store = SQLiteIndexStore(base_path=str(tmp_path))
    db_path = store._db_path("local", "migrate-idempotent")
    conn = store._connect(db_path)
    try:
        _migrate_v14_to_v15(conn)
        _migrate_v14_to_v15(conn)
        version = conn.execute(
            "SELECT value FROM meta WHERE key = 'index_version'"
        ).fetchone()[0]
        assert int(version) >= 15
    finally:
        conn.close()
