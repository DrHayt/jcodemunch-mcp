"""Tests for the whatsnew CLI helper (v1.84.0)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from jcodemunch_mcp.cli.whatsnew import (
    parse_changelog,
    render_readme_block,
    update_readme,
    write_whatsnew_json,
)


_SAMPLE_CHANGELOG = """\
# Changelog

All notable changes to jcodemunch-mcp are documented here.

## [1.84.0] — 2026-05-09 — One-click install badges + auto-recency block

Top-of-fold UX wins. Three additive surfaces, zero behaviour change.

### Added
- One-click install badges.

## [1.83.2] — 2026-05-08 — Docs: Codex CLI install workaround

### Changed
- README — Codex CLI config block rewritten.

## [1.83.1] — 2026-05-08 — Reference-tool response shapes

### Added
- find_references matches now carry line.

## [1.83.0] — 2026-05-08 — get_file_outline no longer drops nested symbols

### Fixed
- get_file_outline silently dropped every nested symbol.
"""


class TestParseChangelog:
    def test_returns_top_n_entries(self, tmp_path: Path):
        changelog = tmp_path / "CHANGELOG.md"
        changelog.write_text(_SAMPLE_CHANGELOG, encoding="utf-8")
        entries = parse_changelog(changelog, max_entries=3)

        assert len(entries) == 3
        assert entries[0]["version"] == "1.84.0"
        assert entries[0]["date"] == "2026-05-09"
        assert "install badges" in entries[0]["title"].lower()
        assert entries[1]["version"] == "1.83.2"
        assert entries[2]["version"] == "1.83.1"

    def test_summary_captures_first_paragraph(self, tmp_path: Path):
        changelog = tmp_path / "CHANGELOG.md"
        changelog.write_text(_SAMPLE_CHANGELOG, encoding="utf-8")
        entries = parse_changelog(changelog, max_entries=1)

        assert "Top-of-fold UX wins" in entries[0]["summary"]

    def test_returns_empty_when_changelog_missing(self, tmp_path: Path):
        assert parse_changelog(tmp_path / "missing.md") == []


class TestRenderReadmeBlock:
    def test_renders_markdown_list_with_links(self):
        entries = [
            {"version": "1.84.0", "date": "2026-05-09", "title": "thing one", "summary": ""},
            {"version": "1.83.2", "date": "2026-05-08", "title": "thing two", "summary": ""},
        ]
        block = render_readme_block(entries, "jgravelle/jcodemunch-mcp")

        assert "What's new" in block
        assert "v1.84.0" in block
        assert "releases/tag/v1.84.0" in block
        assert "thing one" in block

    def test_empty_entries_returns_empty(self):
        assert render_readme_block([], "j/repo") == ""


class TestUpdateReadme:
    def test_replaces_content_between_markers(self, tmp_path: Path):
        readme = tmp_path / "README.md"
        readme.write_text(
            "head\n<!-- WHATSNEW:START -->\nold content\n<!-- WHATSNEW:END -->\ntail\n",
            encoding="utf-8",
        )
        changed = update_readme(readme, "new content")

        assert changed is True
        text = readme.read_text(encoding="utf-8")
        assert "old content" not in text
        assert "new content" in text
        assert text.startswith("head\n")
        assert text.rstrip().endswith("tail")

    def test_no_change_when_markers_missing(self, tmp_path: Path):
        readme = tmp_path / "README.md"
        readme.write_text("no markers here\n", encoding="utf-8")
        assert update_readme(readme, "anything") is False

    def test_idempotent_when_block_already_current(self, tmp_path: Path):
        readme = tmp_path / "README.md"
        readme.write_text(
            "<!-- WHATSNEW:START -->\nthe content\n<!-- WHATSNEW:END -->\n",
            encoding="utf-8",
        )
        update_readme(readme, "the content")
        # Second call with identical content should be a no-op
        assert update_readme(readme, "the content") is False


class TestWriteWhatsnewJson:
    def test_writes_valid_json_payload(self, tmp_path: Path):
        out = tmp_path / "whatsnew.json"
        entries = [
            {"version": "1.84.0", "date": "2026-05-09", "title": "x", "summary": "y"},
        ]
        write_whatsnew_json(out, entries, current_version="1.84.0")

        payload = json.loads(out.read_text(encoding="utf-8"))
        assert payload["current"] == "1.84.0"
        assert payload["entries"] == entries
