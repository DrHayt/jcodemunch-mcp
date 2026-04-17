"""Dispatcher + gate + generic encoder integration tests."""

import json

from jcodemunch_mcp.encoding import encode_response
from jcodemunch_mcp.encoding.decoder import decode as decode_munch


def _big_response():
    return {
        "repo": "myapp",
        "depth": 3,
        "references": [
            {"file": "src/service/auth.py", "line": 12, "kind": "call"},
            {"file": "src/service/auth.py", "line": 88, "kind": "call"},
            {"file": "src/service/user.py", "line": 21, "kind": "ref"},
            {"file": "src/service/user.py", "line": 44, "kind": "ref"},
            {"file": "tests/integration/auth_test.py", "line": 9, "kind": "call"},
            {"file": "tests/integration/auth_test.py", "line": 15, "kind": "call"},
        ],
    }


def test_auto_falls_back_to_json_for_tiny_responses():
    payload, meta = encode_response("demo", {"ok": True}, "auto")
    assert meta["encoding"] == "json"


def test_force_json():
    payload, meta = encode_response("demo", _big_response(), "json")
    assert meta["encoding"] == "json"
    assert isinstance(payload, dict)


def test_compact_always_encodes_and_is_smaller():
    resp = _big_response()
    payload, meta = encode_response("demo", resp, "compact")
    assert meta["encoding"] != "json"
    assert meta["encoded_bytes"] < meta["json_bytes"]


def test_generic_encoding_round_trip():
    resp = _big_response()
    payload, meta = encode_response("demo", resp, "compact")
    assert isinstance(payload, str)
    rehydrated = decode_munch(payload)
    # Generic decoder preserves original top-level keys and column types.
    assert rehydrated["repo"] == "myapp"
    assert rehydrated["depth"] == 3
    assert "references" in rehydrated
    rows = rehydrated["references"]
    assert any(r["file"] == "src/service/auth.py" for r in rows)
    assert all(isinstance(r["line"], int) for r in rows)


def test_auto_emits_compact_on_big_response():
    resp = _big_response()
    # Artificially inflate the response so the savings gate trips.
    resp["references"] *= 10
    payload, meta = encode_response("demo", resp, "auto")
    assert meta["encoding"] != "json", meta


def test_json_decoder_falls_through_for_json_payloads():
    raw = json.dumps({"hello": 1})
    assert decode_munch(raw) == {"hello": 1}
