"""Tests for the six-axis health radar + diff helper (v1.87.0)."""

from __future__ import annotations

import pytest

from jcodemunch_mcp.tools.health_radar import (
    _letter_grade,
    _score_complexity,
    _score_coupling,
    _score_cycles,
    _score_dead_code,
    _score_test_gap,
    _score_churn_surface,
    compute_radar,
    diff_health_radar,
    diff_radar,
)


class TestComplexityScorer:
    def test_low_complexity_full_score(self):
        assert _score_complexity(0.0) == 100.0
        assert _score_complexity(2.5) == 100.0
        assert _score_complexity(3.0) == 100.0

    def test_medium_penalty(self):
        # avg=10 -> 100 - 6*7 = 58
        assert _score_complexity(10.0) == 58.0

    def test_high_complexity_floor(self):
        assert _score_complexity(50.0) == 0.0


class TestDeadCodeScorer:
    @pytest.mark.parametrize("pct,expected", [(0, 100), (10, 60), (15, 40), (25, 0), (100, 0)])
    def test_linear_penalty(self, pct, expected):
        assert _score_dead_code(pct) == expected


class TestCyclesScorer:
    def test_no_cycles_full_score(self):
        assert _score_cycles(0) == 100.0

    @pytest.mark.parametrize("count,expected", [(1, 95), (5, 75), (10, 50), (20, 0), (100, 0)])
    def test_linear_penalty(self, count, expected):
        assert _score_cycles(count) == expected


class TestCouplingScorer:
    def test_no_unstable(self):
        assert _score_coupling(0, 100) == 100.0

    def test_zero_total_files_returns_full(self):
        # Edge case: empty repo. Don't divide by zero, treat as healthy.
        assert _score_coupling(0, 0) == 100.0

    def test_partial_unstable(self):
        # 25% unstable -> 50.0
        assert _score_coupling(25, 100) == 50.0

    def test_fully_unstable_floor(self):
        assert _score_coupling(100, 100) == 0.0


class TestTestGapScorer:
    @pytest.mark.parametrize("pct,expected", [(0, 100), (50, 50), (75, 25), (100, 0)])
    def test_linear(self, pct, expected):
        assert _score_test_gap(pct) == expected


class TestChurnSurfaceScorer:
    @pytest.mark.parametrize("score,expected", [
        (0.0, 100.0),
        (50.0, 80.0),
        (200.0, 60.0),
        (700.0, 40.0),
        (1500.0, 20.0),
        (3000.0, 0.0),
    ])
    def test_buckets(self, score, expected):
        assert _score_churn_surface(score) == expected


class TestLetterGrade:
    @pytest.mark.parametrize("composite,grade", [
        (95, "A"), (90, "A"), (89.9, "B"),
        (80, "B"), (79.9, "C"),
        (70, "C"), (69.9, "D"),
        (60, "D"), (59.9, "F"),
        (0, "F"),
    ])
    def test_bands(self, composite, grade):
        assert _letter_grade(composite) == grade


class TestComputeRadar:
    def test_healthy_repo_grades_high(self):
        out = compute_radar(
            avg_complexity=2.0,
            dead_code_pct=1.0,
            cycle_count=0,
            unstable_modules=0,
            total_files=100,
            untested_pct=5.0,
            top_hotspot_score=20.0,
        )
        assert out["composite"] >= 90
        assert out["grade"] == "A"
        # Phase 7 introduced the optional runtime_coverage axis; when the
        # caller doesn't pass runtime_coverage_pct, the axis is omitted.
        # All other six axes remain present.
        assert out["omitted_axes"] == ["runtime_coverage"]
        assert set(out["axes"].keys()) == {
            "complexity", "dead_code", "cycles", "coupling", "test_gap", "churn_surface",
        }

    def test_unhealthy_repo_grades_low(self):
        out = compute_radar(
            avg_complexity=20.0,
            dead_code_pct=30.0,
            cycle_count=15,
            unstable_modules=60,
            total_files=100,
            untested_pct=80.0,
            top_hotspot_score=2500.0,
        )
        assert out["composite"] <= 30
        assert out["grade"] == "F"

    def test_test_gap_omitted_when_unknown(self):
        out = compute_radar(
            avg_complexity=5.0,
            dead_code_pct=5.0,
            cycle_count=0,
            unstable_modules=0,
            total_files=100,
            untested_pct=None,
            top_hotspot_score=50.0,
        )
        assert "test_gap" in out["omitted_axes"]
        assert "test_gap" not in out["axes"]
        # Composite still computed from remaining 5 axes.
        assert out["composite"] > 0

    def test_churn_omitted_when_unknown(self):
        out = compute_radar(
            avg_complexity=5.0,
            dead_code_pct=5.0,
            cycle_count=0,
            unstable_modules=0,
            total_files=100,
            untested_pct=10.0,
            top_hotspot_score=None,
        )
        assert "churn_surface" in out["omitted_axes"]


class TestDiffRadar:
    def _radar(self, **overrides):
        defaults = dict(
            avg_complexity=5.0, dead_code_pct=5.0, cycle_count=0,
            unstable_modules=0, total_files=100,
            untested_pct=10.0, top_hotspot_score=50.0,
        )
        defaults.update(overrides)
        return compute_radar(**defaults)

    def test_no_change_returns_zero_deltas(self):
        a = self._radar()
        b = self._radar()
        d = diff_radar(a, b)
        assert d["composite_delta"] == 0.0
        assert d["regressions"] == []
        assert d["improvements"] == []
        assert "no meaningful change" in d["verdict"]

    def test_regression_detected(self):
        baseline = self._radar(avg_complexity=3.0, dead_code_pct=1.0)
        current = self._radar(avg_complexity=15.0, dead_code_pct=20.0)
        d = diff_radar(baseline, current)
        assert d["composite_delta"] < 0
        assert "complexity" in d["regressions"]
        assert "dead_code" in d["regressions"]
        assert "REGRESSION" in d["verdict"]

    def test_improvement_detected(self):
        baseline = self._radar(avg_complexity=15.0, cycle_count=10)
        current = self._radar(avg_complexity=4.0, cycle_count=0)
        d = diff_radar(baseline, current)
        assert d["composite_delta"] > 0
        assert "complexity" in d["improvements"]
        assert "cycles" in d["improvements"]
        assert "improvement" in d["verdict"]

    def test_grade_change_string(self):
        baseline = self._radar(avg_complexity=2.0, dead_code_pct=1.0, cycle_count=0,
                               unstable_modules=0, untested_pct=2.0, top_hotspot_score=10.0)
        current = self._radar(avg_complexity=20.0, dead_code_pct=30.0, cycle_count=20,
                              unstable_modules=80, untested_pct=90.0, top_hotspot_score=3000.0)
        d = diff_radar(baseline, current)
        assert "->" in d["grade_change"]
        assert d["grade_change"].startswith("A")  # Original grade A
        assert d["grade_change"].endswith("F")    # Final grade F

    def test_axis_missing_from_one_side(self):
        full = self._radar()
        partial = self._radar(top_hotspot_score=None)  # churn_surface omitted
        d = diff_radar(full, partial)
        churn = d["axis_deltas"]["churn_surface"]
        assert churn["delta"] is None
        assert "missing" in churn["note"]


class TestDiffHealthRadarMcpEntry:
    def test_returns_diff_for_valid_payloads(self):
        a = compute_radar(
            avg_complexity=3.0, dead_code_pct=1.0, cycle_count=0,
            unstable_modules=0, total_files=100,
        )
        b = compute_radar(
            avg_complexity=15.0, dead_code_pct=20.0, cycle_count=10,
            unstable_modules=20, total_files=100,
        )
        result = diff_health_radar(a, b)
        assert "axis_deltas" in result
        assert "composite_delta" in result
        assert "verdict" in result

    def test_rejects_non_dict_inputs(self):
        result = diff_health_radar("not a dict", {})
        assert "error" in result

    def test_rejects_missing_axes_field(self):
        # Likely cause: user passed the full get_repo_health response
        # instead of its `radar` sub-field.
        bogus_response = {"summary": "...", "avg_complexity": 5.0}
        result = diff_health_radar(bogus_response, bogus_response)
        assert "error" in result
        assert "radar" in result["error"].lower()
