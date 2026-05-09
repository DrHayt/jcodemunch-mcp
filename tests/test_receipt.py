"""Tests for the receipt CLI helper (v1.85.0)."""

from __future__ import annotations

import datetime as _dt
import json
from pathlib import Path

import pytest

from jcodemunch_mcp.cli.receipt import (
    _BYTES_PER_TOKEN,
    _DEFAULT_MULTIPLIER,
    _MODEL_PRICES_USD_PER_MTOK,
    _TOOL_MULTIPLIERS,
    _result_byte_length,
    aggregate,
    dollar_savings,
    iter_calls,
    render_csv,
    render_explain,
    render_json,
    render_text,
)


def _write_session(path: Path, events: list[dict]) -> None:
    """Write a synthetic Claude transcript file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for ev in events:
            f.write(json.dumps(ev) + "\n")


def _make_call(tool: str, tu_id: str, result_text: str, ts: str = "2026-05-09T12:00:00Z") -> list[dict]:
    """Synthesize a (tool_use, tool_result) pair as two transcript events."""
    return [
        {
            "type": "assistant",
            "timestamp": ts,
            "message": {
                "role": "assistant",
                "content": [
                    {"type": "tool_use", "id": tu_id, "name": tool, "input": {}},
                ],
            },
        },
        {
            "type": "user",
            "timestamp": ts,
            "message": {
                "role": "user",
                "content": [
                    {"type": "tool_result", "tool_use_id": tu_id, "content": result_text},
                ],
            },
        },
    ]


class TestResultByteLength:
    def test_string_content(self):
        assert _result_byte_length("hello") == 5

    def test_text_blocks(self):
        content = [
            {"type": "text", "text": "abc"},
            {"type": "text", "text": "defg"},
        ]
        assert _result_byte_length(content) == 7

    def test_non_text_blocks_ignored(self):
        content = [
            {"type": "image", "source": "..."},
            {"type": "text", "text": "ok"},
        ]
        assert _result_byte_length(content) == 2

    def test_none_and_empty(self):
        assert _result_byte_length(None) == 0
        assert _result_byte_length([]) == 0
        assert _result_byte_length("") == 0


class TestIterCalls:
    def test_pairs_tool_use_with_result(self, tmp_path: Path):
        events = _make_call(
            "mcp__jcodemunch__search_symbols",
            "tu_1",
            "x" * 400,
        )
        _write_session(tmp_path / "session1.jsonl", events)

        calls = list(iter_calls(tmp_path))
        assert len(calls) == 1
        assert calls[0]["tool"] == "search_symbols"
        # 400 bytes / 4 bytes/token = 100 tokens.
        assert calls[0]["result_tokens"] == 100

    def test_ignores_non_jcodemunch_tools(self, tmp_path: Path):
        events = (
            _make_call("Read", "tu_1", "irrelevant")
            + _make_call("mcp__claude_ai_Notion__create", "tu_2", "irrelevant")
            + _make_call("mcp__jcodemunch__resolve_repo", "tu_3", "result")
        )
        _write_session(tmp_path / "session.jsonl", events)
        calls = list(iter_calls(tmp_path))
        assert len(calls) == 1
        assert calls[0]["tool"] == "resolve_repo"

    def test_handles_orphan_tool_use_without_result(self, tmp_path: Path):
        """Tool calls that never got a result are silently dropped."""
        events = [
            {
                "type": "assistant",
                "timestamp": "2026-05-09T12:00:00Z",
                "message": {
                    "role": "assistant",
                    "content": [
                        {"type": "tool_use", "id": "tu_orphan", "name": "mcp__jcodemunch__search_symbols", "input": {}},
                    ],
                },
            },
            # No matching tool_result
        ]
        _write_session(tmp_path / "session.jsonl", events)
        assert list(iter_calls(tmp_path)) == []

    def test_tolerates_corrupt_lines(self, tmp_path: Path):
        path = tmp_path / "session.jsonl"
        events = _make_call("mcp__jcodemunch__find_references", "tu_1", "result")
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            for ev in events[:1]:
                f.write(json.dumps(ev) + "\n")
            f.write("not valid json\n")
            for ev in events[1:]:
                f.write(json.dumps(ev) + "\n")
        calls = list(iter_calls(tmp_path))
        assert len(calls) == 1
        assert calls[0]["tool"] == "find_references"

    def test_returns_empty_for_missing_root(self, tmp_path: Path):
        assert list(iter_calls(tmp_path / "does-not-exist")) == []


class TestAggregate:
    def test_applies_per_tool_multipliers(self):
        # search_symbols multiplier is 20×.
        calls = [
            {"tool": "search_symbols", "result_tokens": 100, "timestamp": "", "session_file": "x"},
            {"tool": "search_symbols", "result_tokens": 200, "timestamp": "", "session_file": "x"},
        ]
        agg = aggregate(calls)
        assert agg["totals"]["calls"] == 2
        assert agg["totals"]["actual_tokens"] == 300
        assert agg["totals"]["baseline_tokens"] == 300 * 20
        assert agg["totals"]["savings_tokens"] == 300 * 19  # baseline - actual

    def test_default_multiplier_for_unknown_tools(self):
        calls = [{"tool": "made_up_tool", "result_tokens": 100, "timestamp": "", "session_file": "x"}]
        agg = aggregate(calls)
        assert agg["totals"]["baseline_tokens"] == 100 * _DEFAULT_MULTIPLIER
        assert agg["per_tool"]["made_up_tool"]["calls"] == 1

    def test_empty_calls_returns_zeros(self):
        agg = aggregate([])
        assert agg["totals"]["calls"] == 0
        assert agg["totals"]["savings_tokens"] == 0


class TestDollarSavings:
    def test_sonnet_rate(self):
        # $3/MTok → 1M tokens = $3.
        assert dollar_savings(1_000_000, "sonnet") == pytest.approx(3.0)

    def test_opus_rate(self):
        assert dollar_savings(1_000_000, "opus") == pytest.approx(15.0)

    def test_haiku_rate(self):
        assert dollar_savings(1_000_000, "haiku") == pytest.approx(0.80)

    def test_unknown_model_zero(self):
        assert dollar_savings(1_000_000, "made-up") == 0.0


class TestRenderText:
    def _simple_agg(self):
        return aggregate([
            {"tool": "search_symbols", "result_tokens": 1000, "timestamp": "", "session_file": "x"},
            {"tool": "find_references", "result_tokens": 500, "timestamp": "", "session_file": "x"},
        ])

    def test_includes_dollar_headline(self):
        agg = self._simple_agg()
        out = render_text(agg, days=30, model="sonnet")
        assert "Sonnet pricing" in out
        # search_symbols: 1000 × 20 = 20000; find_references: 500 × 25 = 12500.
        # savings = (20000 - 1000) + (12500 - 500) = 31000.
        # $3/MTok × 31000 / 1e6 = $0.093 → rounds to $0.09.
        assert "$0.09" in out

    def test_empty_data_message(self):
        out = render_text(aggregate([]), days=30, model="sonnet")
        assert "No jcodemunch tool calls found" in out

    def test_top_tools_table(self):
        out = render_text(self._simple_agg(), days=30, model="sonnet")
        assert "search_symbols" in out
        assert "find_references" in out


class TestExplain:
    def test_lists_every_known_tool(self):
        out = render_explain()
        for tool in _TOOL_MULTIPLIERS:
            assert tool in out

    def test_includes_default_multiplier(self):
        out = render_explain()
        assert f"{_DEFAULT_MULTIPLIER}" in out


class TestExports:
    def _agg(self):
        return aggregate([
            {"tool": "search_symbols", "result_tokens": 1000, "timestamp": "", "session_file": "x"},
        ])

    def test_csv_has_header_and_row(self):
        out = render_csv(self._agg())
        assert out.splitlines()[0] == "tool,calls,actual_tokens,baseline_tokens,savings_tokens"
        assert "search_symbols" in out

    def test_json_payload_shape(self):
        out = render_json(self._agg(), model="sonnet")
        payload = json.loads(out)
        assert payload["model"] == "sonnet"
        assert "savings_usd" in payload
        assert payload["totals"]["calls"] == 1
        assert "search_symbols" in payload["per_tool"]


class TestModelPriceTable:
    def test_known_models_present(self):
        for m in ("sonnet", "opus", "haiku"):
            assert m in _MODEL_PRICES_USD_PER_MTOK
            assert _MODEL_PRICES_USD_PER_MTOK[m] > 0

    def test_opus_more_expensive_than_sonnet(self):
        assert _MODEL_PRICES_USD_PER_MTOK["opus"] > _MODEL_PRICES_USD_PER_MTOK["sonnet"]

    def test_haiku_cheaper_than_sonnet(self):
        assert _MODEL_PRICES_USD_PER_MTOK["haiku"] < _MODEL_PRICES_USD_PER_MTOK["sonnet"]
