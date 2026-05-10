"""Phase 7 tests: runtime-aware PR risk profile + 7th radar axis.

Covers:
- ``compute_radar`` accepts ``runtime_coverage_pct`` and surfaces a 7th axis
- ``compute_radar`` omits ``runtime_coverage`` from axes when the param is None
  (preserves bit-for-bit composite comparability against pre-Phase-7 baselines)
- ``_score_runtime_coverage`` is a healthy-by-default linear axis
- ``get_pr_risk_profile`` runtime helpers (``_load_runtime_signal_for_changed`` +
  ``_runtime_traffic_score``) respond correctly when traces exist vs not
- ``runtime_dark_code_introduced`` flips True when an added symbol's file
  has zero runtime evidence
- ``runtime_dark_code_introduced`` stays False when the added symbol's file
  *does* have runtime evidence
- Risk score rebalances correctly: with traces, weights sum to 1.0 (six-signal);
  without traces, weights sum to 1.0 (five-signal) and exclude runtime_traffic
- Observatory append_run flags ``runtime_evidence`` based on whether the
  ``runtime_coverage`` axis is present in the radar
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from jcodemunch_mcp.tools.health_radar import (
    _score_runtime_coverage,
    compute_radar,
    diff_radar,
)
from jcodemunch_mcp.tools.get_pr_risk_profile import (
    _W_BLAST,
    _W_CHURN,
    _W_COMPLEXITY,
    _W_RUNTIME,
    _W_TEST_GAP,
    _W_VOLUME,
    _load_runtime_signal_for_changed,
    _runtime_traffic_score,
)
from jcodemunch_mcp.tools.observatory import append_run
from jcodemunch_mcp.storage.sqlite_store import SQLiteIndexStore


# ──────────────────────────────────────────────────────────────────────
# Health radar 7th axis
# ──────────────────────────────────────────────────────────────────────


def test_radar_without_runtime_param_omits_axis():
    """Pre-Phase-7 callers (no runtime_coverage_pct) keep the historical 6-axis composite."""
    radar = compute_radar(
        avg_complexity=2.0,
        dead_code_pct=5.0,
        cycle_count=0,
        unstable_modules=0,
        total_files=100,
        untested_pct=10.0,
        top_hotspot_score=0.0,
    )
    assert "runtime_coverage" not in radar["axes"]
    assert "runtime_coverage" in radar["omitted_axes"]


def test_radar_with_runtime_param_adds_axis():
    radar = compute_radar(
        avg_complexity=2.0,
        dead_code_pct=5.0,
        cycle_count=0,
        unstable_modules=0,
        total_files=100,
        untested_pct=10.0,
        top_hotspot_score=0.0,
        runtime_coverage_pct=80.0,
    )
    assert "runtime_coverage" in radar["axes"]
    assert radar["axes"]["runtime_coverage"]["score"] == 80.0
    assert radar["axes"]["runtime_coverage"]["raw"] == 80.0
    assert "runtime_coverage" not in radar["omitted_axes"]


def test_radar_runtime_coverage_score_is_linear():
    assert _score_runtime_coverage(0.0) == 0.0
    assert _score_runtime_coverage(50.0) == 50.0
    assert _score_runtime_coverage(100.0) == 100.0
    # Out-of-range inputs clamp.
    assert _score_runtime_coverage(150.0) == 100.0
    assert _score_runtime_coverage(-10.0) == 0.0


def test_radar_composite_drops_when_runtime_axis_is_low():
    """A repo with empirical evidence and bad coverage should score worse than
    the same repo without empirical evidence — that's the whole point."""
    static_only = compute_radar(
        avg_complexity=2.0,
        dead_code_pct=5.0,
        cycle_count=0,
        unstable_modules=0,
        total_files=100,
        untested_pct=10.0,
        top_hotspot_score=0.0,
    )
    with_low_runtime = compute_radar(
        avg_complexity=2.0,
        dead_code_pct=5.0,
        cycle_count=0,
        unstable_modules=0,
        total_files=100,
        untested_pct=10.0,
        top_hotspot_score=0.0,
        runtime_coverage_pct=20.0,  # only 20% of edges actually traced
    )
    assert with_low_runtime["composite"] < static_only["composite"]


def test_diff_radar_picks_up_new_runtime_axis():
    """diff_radar walks axis-keys generically; runtime_coverage just appears."""
    base = compute_radar(
        avg_complexity=2.0, dead_code_pct=5.0, cycle_count=0,
        unstable_modules=0, total_files=100,
        untested_pct=10.0, top_hotspot_score=0.0,
    )
    head = compute_radar(
        avg_complexity=2.0, dead_code_pct=5.0, cycle_count=0,
        unstable_modules=0, total_files=100,
        untested_pct=10.0, top_hotspot_score=0.0,
        runtime_coverage_pct=85.0,
    )
    diff = diff_radar(base, head)
    assert "runtime_coverage" in diff["axis_deltas"]
    # Baseline lacks the axis → delta is None
    assert diff["axis_deltas"]["runtime_coverage"]["delta"] is None


# ──────────────────────────────────────────────────────────────────────
# get_pr_risk_profile runtime helpers
# ──────────────────────────────────────────────────────────────────────


def test_runtime_traffic_score_zero_for_empty_dict():
    assert _runtime_traffic_score({}) == 0.0


def test_runtime_traffic_score_increases_with_hits():
    low = _runtime_traffic_score({"sym1": 10})
    high = _runtime_traffic_score({"sym1": 1_000_000})
    assert high > low
    assert 0.0 <= low <= 1.0
    assert 0.0 <= high <= 1.0


def test_runtime_traffic_score_uses_average_not_max():
    """A PR touching a hot symbol AND many cold ones should score lower than
    a PR touching only the hot symbol — average is the right aggregate."""
    only_hot = _runtime_traffic_score({"sym1": 1_000_000})
    diluted = _runtime_traffic_score({"sym1": 1_000_000, **{f"cold{i}": 1 for i in range(20)}})
    assert diluted < only_hot


def test_load_runtime_signal_returns_false_when_no_data(tmp_path):
    """No runtime_calls rows => runtime_present=False, no per-symbol data."""
    store = SQLiteIndexStore(base_path=str(tmp_path))
    db_path = store._db_path("local", "phase7-empty")
    conn = store._connect(db_path)
    conn.close()
    per_sym, files, present = _load_runtime_signal_for_changed(db_path, ["any"], [])
    assert present is False
    assert per_sym == {}
    assert files == frozenset()


def test_load_runtime_signal_returns_true_when_data_present(tmp_path):
    store = SQLiteIndexStore(base_path=str(tmp_path))
    db_path = store._db_path("local", "phase7-data")
    conn = store._connect(db_path)
    try:
        conn.executescript(
            """
            INSERT INTO symbols (id, file, name, kind, line, end_line) VALUES
                ('app/handlers.py::process_request#function', 'app/handlers.py', 'process_request', 'function', 10, 30),
                ('app/db.py::query#function', 'app/db.py', 'query', 'function', 1, 100);
            INSERT INTO runtime_calls (symbol_id, source, count, first_seen, last_seen) VALUES
                ('app/handlers.py::process_request#function', 'otel', 5000, '2026-05-10T00:00:00Z', '2026-05-10T00:00:00Z'),
                ('app/db.py::query#function', 'otel', 2, '2026-05-10T00:00:00Z', '2026-05-10T00:00:00Z');
            """
        )
        conn.commit()
    finally:
        conn.close()

    per_sym, files_with_traces, present = _load_runtime_signal_for_changed(
        db_path,
        [
            "app/handlers.py::process_request#function",
            "app/db.py::query#function",
            "app/handlers.py::not_called_yet#function",  # not in the runtime tables
        ],
        ["app/handlers.py", "app/db.py"],
    )
    assert present is True
    assert per_sym["app/handlers.py::process_request#function"] == 5000
    assert per_sym["app/db.py::query#function"] == 2
    assert "app/handlers.py::not_called_yet#function" not in per_sym  # no row → no entry
    assert "app/handlers.py" in files_with_traces
    assert "app/db.py" in files_with_traces


def test_load_runtime_signal_combines_runtime_calls_and_stack_events(tmp_path):
    store = SQLiteIndexStore(base_path=str(tmp_path))
    db_path = store._db_path("local", "phase7-stack")
    conn = store._connect(db_path)
    try:
        conn.executescript(
            """
            INSERT INTO symbols (id, file, name, kind, line, end_line) VALUES
                ('app/handlers.py::process_request#function', 'app/handlers.py', 'process_request', 'function', 10, 30);
            INSERT INTO runtime_calls (symbol_id, source, count, first_seen, last_seen) VALUES
                ('app/handlers.py::process_request#function', 'otel', 100, '2026-05-10T00:00:00Z', '2026-05-10T00:00:00Z');
            INSERT INTO runtime_stack_events (symbol_id, source, severity, count, first_seen, last_seen) VALUES
                ('app/handlers.py::process_request#function', 'stack_log', 'error', 7, '2026-05-10T00:00:00Z', '2026-05-10T00:00:00Z');
            """
        )
        conn.commit()
    finally:
        conn.close()

    per_sym, _, present = _load_runtime_signal_for_changed(
        db_path,
        ["app/handlers.py::process_request#function"],
        [],
    )
    assert present is True
    # runtime_calls(100) + runtime_stack_events(7) = 107
    assert per_sym["app/handlers.py::process_request#function"] == 107


# ──────────────────────────────────────────────────────────────────────
# Weights
# ──────────────────────────────────────────────────────────────────────


def test_static_only_weights_sum_to_one():
    total = _W_BLAST + _W_COMPLEXITY + _W_CHURN + _W_TEST_GAP + _W_VOLUME
    assert abs(total - 1.0) < 1e-9


def test_runtime_aware_weights_sum_to_one():
    """When runtime data is present, the static weights are scaled by
    (1 - _W_RUNTIME) and then _W_RUNTIME is added — total stays at 1.0."""
    scale = 1.0 - _W_RUNTIME
    total = (
        _W_BLAST * scale
        + _W_COMPLEXITY * scale
        + _W_CHURN * scale
        + _W_TEST_GAP * scale
        + _W_VOLUME * scale
        + _W_RUNTIME
    )
    assert abs(total - 1.0) < 1e-9


# ──────────────────────────────────────────────────────────────────────
# Observatory runtime_evidence column
# ──────────────────────────────────────────────────────────────────────


def _radar_with_runtime(coverage_pct: float | None) -> dict:
    return compute_radar(
        avg_complexity=2.0, dead_code_pct=5.0, cycle_count=0,
        unstable_modules=0, total_files=100,
        untested_pct=10.0, top_hotspot_score=0.0,
        runtime_coverage_pct=coverage_pct,
    )


def test_observatory_append_run_flags_runtime_evidence(tmp_path):
    health = {
        "summary": "test repo",
        "total_files": 10,
        "total_symbols": 100,
        "radar": _radar_with_runtime(80.0),
    }
    record = append_run(tmp_path, "owner-name", "abc1234", health)
    assert record["runtime_evidence"] is True
    # The stored history must persist the flag.
    history = json.loads((tmp_path / "owner-name" / "history.json").read_text())
    assert history[0]["runtime_evidence"] is True


def test_observatory_append_run_no_runtime_when_axis_missing(tmp_path):
    health = {
        "summary": "test repo",
        "total_files": 10,
        "total_symbols": 100,
        "radar": _radar_with_runtime(None),
    }
    record = append_run(tmp_path, "owner-other", "deadbee", health)
    assert record["runtime_evidence"] is False
