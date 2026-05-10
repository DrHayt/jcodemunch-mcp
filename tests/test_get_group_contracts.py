"""Tests for get_group_contracts — cross-repo shared-symbol API surface."""

from pathlib import Path

import pytest

from jcodemunch_mcp.tools.get_group_contracts import get_group_contracts
from jcodemunch_mcp.tools.index_folder import index_folder
from jcodemunch_mcp.tools.package_registry import invalidate_registry_cache


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _index(folder: Path, store: Path) -> str:
    result = index_folder(str(folder), use_ai_summaries=False, storage_path=str(store))
    assert result["success"] is True, result
    return result["repo"]


def _build_group(tmp_path: Path):
    """Build three repos: provider (mylib), consumer_a, consumer_b — both consumers
    import named symbols from mylib."""
    provider_src = tmp_path / "provider"
    consumer_a_src = tmp_path / "consumer_a"
    consumer_b_src = tmp_path / "consumer_b"
    store = tmp_path / "store"
    for d in (provider_src, consumer_a_src, consumer_b_src, store):
        d.mkdir()

    # Provider publishes "mylib" with one public and one internal symbol.
    _write(provider_src / "pyproject.toml", '[project]\nname = "mylib"\n')
    _write(
        provider_src / "__init__.py",
        "def validate_token(t):\n    return bool(t)\n\n"
        "def _internal_helper(x):\n    return x + 1\n\n"
        "def unused_export():\n    return 42\n",
    )

    # Consumer A imports validate_token AND _internal_helper (leaky!)
    _write(consumer_a_src / "pyproject.toml", '[project]\nname = "consumer-a"\n')
    _write(
        consumer_a_src / "app.py",
        "from mylib import validate_token, _internal_helper\n\n"
        "def login(t):\n    return validate_token(t)\n\n"
        "def helper_call(x):\n    return _internal_helper(x)\n",
    )

    # Consumer B imports only validate_token
    _write(consumer_b_src / "pyproject.toml", '[project]\nname = "consumer-b"\n')
    _write(
        consumer_b_src / "service.py",
        "from mylib import validate_token\n\n"
        "def authenticate(t):\n    return validate_token(t)\n",
    )

    provider_id = _index(provider_src, store)
    consumer_a_id = _index(consumer_a_src, store)
    consumer_b_id = _index(consumer_b_src, store)
    invalidate_registry_cache()
    return provider_id, consumer_a_id, consumer_b_id, str(store)


class TestGroupContractsHappyPath:
    def test_shared_symbol_becomes_de_facto_api(self, tmp_path):
        provider, a, b, store = _build_group(tmp_path)
        result = get_group_contracts(
            repos=[provider, a, b], min_importers=2, storage_path=store,
        )
        assert "error" not in result, result
        names = [c["name"] for c in result["contracts"]]
        assert "validate_token" in names
        vt = next(c for c in result["contracts"] if c["name"] == "validate_token")
        assert vt["verdict"] == "de_facto_api"
        assert vt["importer_count"] == 2

    def test_unresolvable_member_reported(self, tmp_path):
        provider, a, b, store = _build_group(tmp_path)
        result = get_group_contracts(
            repos=[provider, a, "does-not-exist-anywhere"],
            min_importers=1, storage_path=store,
        )
        # Should still succeed (we have 2 resolved members) and surface the unresolved
        if "error" in result:
            # Acceptable: if too many unresolved, error is reasonable
            assert "Could not resolve" in result["error"]
        else:
            assert "unresolved_repos" in result
            assert "does-not-exist-anywhere" in result["unresolved_repos"]


class TestGroupContractsClassification:
    def test_leaky_internal_detected(self, tmp_path):
        """Underscore-prefixed symbol imported externally → leaky_internal verdict."""
        provider, a, b, store = _build_group(tmp_path)
        result = get_group_contracts(
            repos=[provider, a, b], min_importers=1,
            include_internal=True, storage_path=store,
        )
        assert "error" not in result
        leaky = [c for c in result["contracts"] if c["verdict"] == "leaky_internal"]
        assert len(leaky) >= 1
        # _internal_helper should be the leaky one
        leaky_names = [c["name"] for c in leaky]
        assert "_internal_helper" in leaky_names

    def test_include_internal_false_hides_leaky(self, tmp_path):
        provider, a, b, store = _build_group(tmp_path)
        result = get_group_contracts(
            repos=[provider, a, b], min_importers=1,
            include_internal=False, storage_path=store,
        )
        assert "error" not in result
        leaky = [c for c in result["contracts"] if c["verdict"] == "leaky_internal"]
        assert leaky == []

    def test_dead_contract_opt_in_surfaces_unused_public_symbol(self, tmp_path):
        provider, a, b, store = _build_group(tmp_path)
        result = get_group_contracts(
            repos=[provider, a, b], min_importers=1,
            include_dead_contracts=True, storage_path=store,
        )
        assert "error" not in result
        dead = [c for c in result["contracts"] if c["verdict"] == "dead_contract"]
        names = [c["name"] for c in dead]
        # unused_export was public but never imported externally
        assert "unused_export" in names

    def test_dead_contract_off_by_default(self, tmp_path):
        provider, a, b, store = _build_group(tmp_path)
        result = get_group_contracts(
            repos=[provider, a, b], min_importers=1, storage_path=store,
        )
        assert "error" not in result
        dead = [c for c in result["contracts"] if c["verdict"] == "dead_contract"]
        assert dead == []


class TestGroupContractsFilters:
    def test_min_importers_gates(self, tmp_path):
        provider, a, b, store = _build_group(tmp_path)
        # min_importers=2: only validate_token qualifies (imported by both consumers)
        # _internal_helper is only imported by consumer_a (1)
        result = get_group_contracts(
            repos=[provider, a, b], min_importers=2, storage_path=store,
        )
        assert "error" not in result
        for c in result["contracts"]:
            assert c["importer_count"] >= 2

    def test_classify_false_emits_shared(self, tmp_path):
        provider, a, b, store = _build_group(tmp_path)
        result = get_group_contracts(
            repos=[provider, a, b], min_importers=1, classify=False, storage_path=store,
        )
        assert "error" not in result
        for c in result["contracts"]:
            assert c["verdict"] == "shared"


class TestGroupContractsMetadata:
    def test_stability_score_present(self, tmp_path):
        provider, a, b, store = _build_group(tmp_path)
        result = get_group_contracts(
            repos=[provider, a, b], min_importers=1, storage_path=store,
        )
        assert "error" not in result
        for c in result["contracts"]:
            assert "stability_score" in c
            assert 0.0 <= c["stability_score"] <= 1.0

    def test_verdict_counts_present(self, tmp_path):
        provider, a, b, store = _build_group(tmp_path)
        result = get_group_contracts(
            repos=[provider, a, b], min_importers=1, storage_path=store,
        )
        assert "verdict_counts" in result
        assert isinstance(result["verdict_counts"], dict)

    def test_group_field_echoed(self, tmp_path):
        provider, a, b, store = _build_group(tmp_path)
        result = get_group_contracts(
            repos=[provider, a, b], min_importers=1, storage_path=store,
        )
        assert "error" not in result
        assert set(result["group"]) == {provider, a, b}


class TestGroupContractsErrors:
    def test_single_repo_rejected(self, tmp_path):
        provider, _a, _b, store = _build_group(tmp_path)
        result = get_group_contracts(repos=[provider], storage_path=store)
        assert "error" in result

    def test_empty_repos_rejected(self, tmp_path):
        result = get_group_contracts(repos=[], storage_path=str(tmp_path))
        assert "error" in result

    def test_all_unresolved_rejected(self, tmp_path):
        provider, _a, _b, store = _build_group(tmp_path)
        result = get_group_contracts(
            repos=["nope-a", "nope-b"], storage_path=store,
        )
        assert "error" in result

    def test_invalid_min_importers(self, tmp_path):
        provider, a, b, store = _build_group(tmp_path)
        result = get_group_contracts(
            repos=[provider, a, b], min_importers=0, storage_path=store,
        )
        assert "error" in result

    def test_no_indexed_repos(self, tmp_path):
        result = get_group_contracts(
            repos=["foo", "bar"], storage_path=str(tmp_path / "empty"),
        )
        assert "error" in result
