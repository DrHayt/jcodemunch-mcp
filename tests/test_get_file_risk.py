"""Tests for get_file_risk (v1.89.0)."""

from __future__ import annotations

import pytest

from jcodemunch_mcp.tools.get_file_risk import (
    _level_for,
    _score_churn,
    _score_complexity,
    _score_exposure,
    _score_test_gap,
)


class TestLevelFor:
    @pytest.mark.parametrize("composite,level", [
        (95, "green"),
        (85, "green"),
        (84.9, "yellow"),
        (70, "yellow"),
        (69.9, "orange"),
        (55, "orange"),
        (54.9, "red"),
        (0, "red"),
    ])
    def test_band_thresholds(self, composite, level):
        assert _level_for(composite) == level


class TestScoreComplexity:
    @pytest.mark.parametrize("cy,score", [
        (0, 100), (3, 100), (5, 88), (10, 58), (20, 0), (50, 0),
    ])
    def test_linear_penalty(self, cy, score):
        assert _score_complexity(cy) == score


class TestScoreExposure:
    @pytest.mark.parametrize("incoming,score", [
        (0, 100), (5, 75), (10, 50), (20, 0), (50, 0),
    ])
    def test_linear_penalty(self, incoming, score):
        assert _score_exposure(incoming) == score


class TestScoreChurn:
    @pytest.mark.parametrize("commits,score", [
        (0, 100), (5, 75), (10, 50), (20, 0), (50, 0),
    ])
    def test_linear_penalty(self, commits, score):
        assert _score_churn(commits) == score


class TestScoreTestGap:
    def test_with_tests(self):
        assert _score_test_gap(True) == 100.0

    def test_without_tests(self):
        assert _score_test_gap(False) == 0.0


class TestComposite:
    """Spot-check the four-axis arithmetic-mean composite."""

    def _composite(self, *, cy, incoming, churn, has_tests):
        # Mirror of the inlined computation in get_file_risk.
        from jcodemunch_mcp.tools.get_file_risk import (
            _score_complexity, _score_exposure, _score_churn, _score_test_gap,
        )
        return round(
            (
                _score_complexity(cy)
                + _score_exposure(incoming)
                + _score_churn(churn)
                + _score_test_gap(has_tests)
            ) / 4.0,
            1,
        )

    def test_healthy_function_grades_green(self):
        c = self._composite(cy=2, incoming=0, churn=0, has_tests=True)
        assert c >= 85
        assert _level_for(c) == "green"

    def test_landmine_grades_red(self):
        # High complexity + many callers + recent churn + no tests
        c = self._composite(cy=18, incoming=15, churn=12, has_tests=False)
        assert c < 55
        assert _level_for(c) == "red"

    def test_borderline_function(self):
        # Medium-complexity function in a tested file
        c = self._composite(cy=6, incoming=2, churn=2, has_tests=True)
        # complexity ~82, exposure ~90, churn ~90, test_gap 100 -> ~90
        assert _level_for(c) in ("green", "yellow")
