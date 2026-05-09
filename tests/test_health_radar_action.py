"""Tests for the health-radar GitHub Action's renderer (v1.88.0).

The action's shell + YAML steps can only be exercised by running the
Action in a real CI environment. The Python piece (``render_comment.py``)
is the load-bearing part for output quality and is unit-testable here.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

# Load the renderer by file path — it lives under .github/actions/, not
# the importable package tree.
_REPO_ROOT = Path(__file__).resolve().parent.parent
_RENDERER_PATH = _REPO_ROOT / ".github" / "actions" / "health-radar" / "render_comment.py"


def _load_renderer():
    spec = importlib.util.spec_from_file_location("render_comment", _RENDERER_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules["render_comment"] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def renderer():
    return _load_renderer()


def _radar(grade="C", composite=70.0, axes=None) -> dict:
    if axes is None:
        axes = {
            "complexity":    {"score": 70.0, "raw": 6.0},
            "dead_code":     {"score": 80.0, "raw": 5.0},
            "cycles":        {"score": 100.0, "raw": 0},
            "coupling":      {"score": 50.0, "raw_unstable": 25, "raw_total_files": 100},
            "test_gap":      {"score": 60.0, "raw": 40.0},
            "churn_surface": {"score": 60.0, "raw": 200.0},
        }
    return {
        "axes": axes,
        "composite": composite,
        "grade": grade,
        "omitted_axes": [],
    }


class TestRender:
    def test_marker_on_first_line(self, renderer):
        out = renderer.render(_radar(), _radar())
        assert out.splitlines()[0] == "<!-- jcm-health-radar -->"

    def test_no_change_verdict(self, renderer):
        out = renderer.render(_radar(), _radar())
        assert "no meaningful change" in out

    def test_regression_summary(self, renderer):
        baseline = _radar(grade="B", composite=85.0)
        current_axes = dict(baseline["axes"])
        current_axes["complexity"] = {"score": 50.0, "raw": 12.0}
        current = {
            "axes": current_axes,
            "composite": 70.0,
            "grade": "C",
            "omitted_axes": [],
        }
        out = renderer.render(baseline, current)
        assert "B → C" in out
        assert "-15.0" in out
        assert "complexity" in out
        # Regressions section present
        assert "### Regressions" in out

    def test_improvement_summary(self, renderer):
        baseline_axes = {
            "complexity":    {"score": 30.0, "raw": 15.0},
            "dead_code":     {"score": 80.0, "raw": 5.0},
            "cycles":        {"score": 100.0, "raw": 0},
            "coupling":      {"score": 50.0, "raw_unstable": 25, "raw_total_files": 100},
            "test_gap":      {"score": 60.0, "raw": 40.0},
            "churn_surface": {"score": 60.0, "raw": 200.0},
        }
        baseline = {"axes": baseline_axes, "composite": 63.3, "grade": "D", "omitted_axes": []}
        current_axes = dict(baseline_axes)
        current_axes["complexity"] = {"score": 90.0, "raw": 4.0}
        current = {"axes": current_axes, "composite": 73.3, "grade": "C", "omitted_axes": []}
        out = renderer.render(baseline, current)
        assert "D → C" in out
        assert "+10.0" in out or "+10" in out
        assert "### Improvements" in out

    def test_axis_table_renders_all_rows(self, renderer):
        out = renderer.render(_radar(), _radar())
        for axis in ("complexity", "dead_code", "cycles", "coupling", "test_gap", "churn_surface"):
            assert f"`{axis}`" in out

    def test_version_appears_in_footer(self, renderer):
        out = renderer.render(_radar(), _radar(), version="1.88.0")
        assert "1.88.0" in out


class TestLoadRadar:
    def test_accepts_full_health_response(self, renderer, tmp_path: Path):
        full_response = {
            "summary": "...",
            "avg_complexity": 5.0,
            "radar": _radar(),
        }
        path = tmp_path / "h.json"
        path.write_text(json.dumps(full_response), encoding="utf-8")
        radar = renderer._load_radar(path)
        assert "axes" in radar
        assert "complexity" in radar["axes"]

    def test_accepts_radar_only_payload(self, renderer, tmp_path: Path):
        path = tmp_path / "r.json"
        path.write_text(json.dumps(_radar()), encoding="utf-8")
        radar = renderer._load_radar(path)
        assert "axes" in radar

    def test_rejects_unrelated_json(self, renderer, tmp_path: Path):
        path = tmp_path / "x.json"
        path.write_text(json.dumps({"foo": "bar"}), encoding="utf-8")
        with pytest.raises(ValueError):
            renderer._load_radar(path)


class TestArrowsAndSigns:
    def test_signed_positive(self, renderer):
        assert renderer._signed(2.5) == "+2.5"

    def test_signed_negative(self, renderer):
        assert renderer._signed(-4.0) == "-4.0"

    def test_signed_none(self, renderer):
        assert renderer._signed(None) == "—"

    def test_arrow_thresholds(self, renderer):
        assert renderer._arrow(5.0) == "↑"
        assert renderer._arrow(-5.0) == "↓"
        assert renderer._arrow(2.0) == "·"
        assert renderer._arrow(-2.0) == "·"
