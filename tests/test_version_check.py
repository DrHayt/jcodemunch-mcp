"""Tests for the first-launch version-drift probe (v1.84.0)."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from jcodemunch_mcp import version_check


@pytest.fixture
def isolated_storage(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """Redirect CODE_INDEX_PATH and unset the disable env var."""
    monkeypatch.setenv("CODE_INDEX_PATH", str(tmp_path))
    monkeypatch.delenv("JCODEMUNCH_NO_VERSION_HINT", raising=False)
    return tmp_path


class TestCheckAndAnnounce:
    def test_first_launch_writes_file_silently(
        self, isolated_storage: Path, capsys: pytest.CaptureFixture, monkeypatch
    ):
        monkeypatch.setattr(version_check, "_current_version", lambda: "1.84.0")
        version_check.check_and_announce()

        out = capsys.readouterr()
        assert out.err == ""  # No announcement on first launch
        assert (isolated_storage / "last_seen_version").read_text(encoding="utf-8") == "1.84.0"

    def test_no_drift_no_message(
        self, isolated_storage: Path, capsys: pytest.CaptureFixture, monkeypatch
    ):
        seen = isolated_storage / "last_seen_version"
        seen.write_text("1.84.0", encoding="utf-8")
        monkeypatch.setattr(version_check, "_current_version", lambda: "1.84.0")

        version_check.check_and_announce()

        out = capsys.readouterr()
        assert out.err == ""

    def test_drift_emits_one_line_hint_to_stderr(
        self, isolated_storage: Path, capsys: pytest.CaptureFixture, monkeypatch
    ):
        seen = isolated_storage / "last_seen_version"
        seen.write_text("1.83.2", encoding="utf-8")
        monkeypatch.setattr(version_check, "_current_version", lambda: "1.84.0")

        version_check.check_and_announce()

        out = capsys.readouterr()
        assert "upgraded 1.83.2 → 1.84.0" in out.err
        assert "releases/tag/v1.84.0" in out.err
        # File got bumped to current version
        assert seen.read_text(encoding="utf-8") == "1.84.0"

    def test_disable_env_var_silences_probe(
        self, isolated_storage: Path, capsys: pytest.CaptureFixture, monkeypatch
    ):
        seen = isolated_storage / "last_seen_version"
        seen.write_text("1.83.2", encoding="utf-8")
        monkeypatch.setattr(version_check, "_current_version", lambda: "1.84.0")
        monkeypatch.setenv("JCODEMUNCH_NO_VERSION_HINT", "1")

        version_check.check_and_announce()

        out = capsys.readouterr()
        assert out.err == ""

    def test_unknown_version_is_silent(
        self, isolated_storage: Path, capsys: pytest.CaptureFixture, monkeypatch
    ):
        monkeypatch.setattr(version_check, "_current_version", lambda: None)

        version_check.check_and_announce()

        out = capsys.readouterr()
        assert out.err == ""
        # File should not have been created
        assert not (isolated_storage / "last_seen_version").exists()
