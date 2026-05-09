"""Tests for the observatory pipeline (v1.90.0).

The clone-and-index half needs network + git; we exercise the
rendering + state-management half here. The pipeline orchestrator
is integration-tested by ``observatory build`` against a real config
in CI.
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from jcodemunch_mcp.tools import observatory as obs
from jcodemunch_mcp.tools import observatory_render as render


class TestRepoSlug:
    @pytest.mark.parametrize("url,slug", [
        ("https://github.com/expressjs/express", "expressjs--express"),
        ("https://github.com/expressjs/express/", "expressjs--express"),
        ("https://github.com/expressjs/express.git", "expressjs--express"),
        ("git@github.com:expressjs/express.git", "expressjs--express"),
        ("ssh://git@host:foo/bar.git", "foo--bar"),
    ])
    def test_known_shapes(self, url, slug):
        assert obs.repo_slug(url) == slug


class TestHistoryAppend:
    def _health_payload(self, sha="abc1234567890", grade="B", composite=82.5):
        return {
            "summary": "ok",
            "total_files": 50,
            "total_symbols": 400,
            "radar": {
                "grade": grade,
                "composite": composite,
                "axes": {
                    "complexity":    {"score": 80.0},
                    "dead_code":     {"score": 90.0},
                    "cycles":        {"score": 100.0},
                    "coupling":      {"score": 70.0},
                    "test_gap":      {"score": 80.0},
                    "churn_surface": {"score": 75.0},
                },
            },
        }

    def test_first_run_creates_history(self, tmp_path: Path):
        rec = obs.append_run(tmp_path, "owner--repo", "deadbeef", self._health_payload())
        history = obs.load_history(tmp_path, "owner--repo")
        assert len(history) == 1
        assert history[0]["sha"] == "deadbeef"
        assert history[0]["grade"] == "B"
        assert "axes" in history[0]
        assert rec == history[0]

    def test_same_sha_is_noop(self, tmp_path: Path):
        obs.append_run(tmp_path, "owner--repo", "deadbeef", self._health_payload())
        obs.append_run(tmp_path, "owner--repo", "deadbeef", self._health_payload())  # same SHA
        history = obs.load_history(tmp_path, "owner--repo")
        assert len(history) == 1

    def test_new_sha_prepends(self, tmp_path: Path):
        obs.append_run(tmp_path, "owner--repo", "deadbeef", self._health_payload())
        obs.append_run(tmp_path, "owner--repo", "newsha", self._health_payload(grade="A"))
        history = obs.load_history(tmp_path, "owner--repo")
        assert len(history) == 2
        assert history[0]["sha"] == "newsha"
        assert history[1]["sha"] == "deadbeef"

    def test_history_cap_trims_oldest(self, tmp_path: Path):
        for i in range(10):
            obs.append_run(tmp_path, "owner--repo", f"sha{i:02d}", self._health_payload(), cap=5)
        history = obs.load_history(tmp_path, "owner--repo")
        assert len(history) == 5
        # Newest first; the 5 most recent are sha09..sha05.
        assert history[0]["sha"] == "sha09"
        assert history[-1]["sha"] == "sha05"

    def test_corrupt_history_returns_empty(self, tmp_path: Path):
        path = obs.history_path(tmp_path, "owner--repo")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("not json", encoding="utf-8")
        assert obs.load_history(tmp_path, "owner--repo") == []


class TestRenderSparkline:
    def test_empty_returns_empty(self):
        assert render.render_sparkline([]) == ""

    def test_single_point_renders_circle(self):
        out = render.render_sparkline([85.0])
        assert "<svg" in out
        assert "<circle" in out

    def test_multi_point_renders_polyline(self):
        out = render.render_sparkline([60.0, 70.0, 65.0, 80.0])
        assert "<polyline" in out
        # Width should be present.
        assert 'width="120"' in out


class TestRenderRepoPage:
    def _config(self):
        return SimpleNamespace(
            url="https://github.com/owner/repo",
            label="OwnerRepo",
            blurb="A test repo.",
            branch=None,
        )

    def _history(self):
        return [
            {
                "timestamp": "2026-05-09T12:00:00+00:00",
                "sha": "abc1234567890",
                "summary": "Healthy.",
                "grade": "B",
                "composite": 82.5,
                "total_files": 50,
                "total_symbols": 400,
                "axes": {
                    "complexity": 80.0, "dead_code": 90.0, "cycles": 100.0,
                    "coupling": 70.0, "test_gap": 80.0, "churn_surface": 75.0,
                },
            },
        ]

    def _health(self):
        return {
            "summary": "Healthy.",
            "total_files": 50,
            "total_symbols": 400,
            "radar": {
                "grade": "B",
                "composite": 82.5,
                "axes": {
                    "complexity": {"score": 80.0},
                    "dead_code":  {"score": 90.0},
                    "cycles":     {"score": 100.0},
                    "coupling":   {"score": 70.0},
                    "test_gap":   {"score": 80.0},
                    "churn_surface": {"score": 75.0},
                },
            },
        }

    def test_writes_index_html(self, tmp_path: Path):
        render.render_repo_page(
            tmp_path, "owner--repo", "OwnerRepo", self._config(), self._history(), self._health(),
        )
        page = (tmp_path / "owner--repo" / "index.html").read_text(encoding="utf-8")
        assert "OwnerRepo" in page
        assert "82.5" in page  # composite shown
        assert "B</span>" in page  # grade badge
        assert "Six-axis radar" in page

    def test_writes_rss_feed(self, tmp_path: Path):
        render.render_repo_page(
            tmp_path, "owner--repo", "OwnerRepo", self._config(), self._history(), self._health(),
        )
        feed = (tmp_path / "owner--repo" / "feed.xml").read_text(encoding="utf-8")
        assert "<rss" in feed
        assert "OwnerRepo" in feed
        # Item title format: "<grade> (<composite>) at <sha-short>"
        assert "abc1234" in feed

    def test_no_history_skips_render(self, tmp_path: Path):
        # Empty history -> no files written (no crash).
        render.render_repo_page(
            tmp_path, "owner--repo", "OwnerRepo", self._config(), [], self._health(),
        )
        assert not (tmp_path / "owner--repo" / "index.html").exists()


class TestRenderIndexPage:
    def test_leaderboard_sorts_by_composite_desc(self, tmp_path: Path):
        # Pre-populate history files for two repos.
        for slug, comp in (("a--a", 90.0), ("b--b", 70.0)):
            obs.append_run(tmp_path, slug, "abc", {
                "summary": "", "total_files": 1, "total_symbols": 1,
                "radar": {"grade": "A", "composite": comp, "axes": {}},
            })
        summaries = [
            {"slug": "b--b", "label": "B", "status": "ok", "url": "u", "sha": "abc", "grade": "C", "composite": 70.0},
            {"slug": "a--a", "label": "A", "status": "ok", "url": "u", "sha": "abc", "grade": "A", "composite": 90.0},
        ]
        render.render_index_page(tmp_path, summaries)
        html = (tmp_path / "index.html").read_text(encoding="utf-8")
        # A should come before B in the rendered tile order.
        assert html.find("A</h3>") < html.find("B</h3>")

    def test_renders_skipped_section_for_failures(self, tmp_path: Path):
        summaries = [
            {"slug": "a--a", "label": "Worked", "status": "ok", "composite": 80.0, "grade": "B"},
            {"slug": "f--f", "label": "Failed", "status": "clone_failed"},
        ]
        # ok summary needs a history file for the sparkline.
        obs.append_run(tmp_path, "a--a", "abc", {
            "summary": "", "total_files": 1, "total_symbols": 1,
            "radar": {"grade": "B", "composite": 80.0, "axes": {}},
        })
        render.render_index_page(tmp_path, summaries)
        html = (tmp_path / "index.html").read_text(encoding="utf-8")
        assert "Skipped" in html
        assert "Failed" in html
        assert "clone_failed" in html

    def test_writes_machine_readable_json(self, tmp_path: Path):
        summaries = [{"slug": "a--a", "label": "A", "status": "ok", "composite": 80.0, "grade": "B"}]
        obs.append_run(tmp_path, "a--a", "abc", {
            "summary": "", "total_files": 1, "total_symbols": 1,
            "radar": {"grade": "B", "composite": 80.0, "axes": {}},
        })
        render.render_index_page(tmp_path, summaries)
        payload = json.loads((tmp_path / "index.json").read_text(encoding="utf-8"))
        assert "summaries" in payload
        assert payload["summaries"][0]["slug"] == "a--a"


class TestIndexAndHealth:
    """Regression: get_repo_health must receive the indexed owner/name,
    not the absolute filesystem checkout path. Pre-1.90.1 the orchestrator
    passed str(repo_path), tripping the path-separator guard in
    sqlite_store._safe_repo_component for every repo in CI."""

    def test_passes_indexed_repo_id_not_path(self, tmp_path, monkeypatch):
        captured = {}

        def fake_index_folder(path, **kw):
            captured["index_path"] = path
            return {"success": True, "repo": "spf13/cobra"}

        def fake_get_repo_health(repo, storage_path=None):
            captured["health_repo"] = repo
            return {"summary": "ok", "radar": {}}

        monkeypatch.setattr(
            "jcodemunch_mcp.tools.index_folder.index_folder",
            fake_index_folder,
        )
        monkeypatch.setattr(
            "jcodemunch_mcp.tools.get_repo_health.get_repo_health",
            fake_get_repo_health,
        )

        repo_path = tmp_path / "checkouts" / "spf13--cobra"
        repo_path.mkdir(parents=True)
        result = obs.index_and_health(repo_path)

        assert result is not None
        assert captured["index_path"] == str(repo_path)
        assert captured["health_repo"] == "spf13/cobra"
        assert "/" not in captured["health_repo"].split("/", 1)[1]  # no path components

    def test_returns_none_when_index_returns_no_repo_id(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "jcodemunch_mcp.tools.index_folder.index_folder",
            lambda path, **kw: {"success": True},  # no "repo" key
        )
        result = obs.index_and_health(tmp_path)
        assert result is None


class TestLoadConfig:
    def test_minimal(self, tmp_path: Path):
        cfg_path = tmp_path / "obs.json"
        cfg_path.write_text(json.dumps({
            "output_dir": str(tmp_path / "out"),
            "repos": [
                {"url": "https://github.com/owner/repo", "label": "OwnerRepo"},
            ],
        }), encoding="utf-8")
        cfg = obs.load_config(cfg_path)
        assert cfg.output_dir == (tmp_path / "out").resolve()
        assert len(cfg.repos) == 1
        assert cfg.repos[0].url == "https://github.com/owner/repo"
        assert cfg.history_cap == 52  # default
