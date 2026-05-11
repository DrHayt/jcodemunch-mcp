"""Tests for FQN ↔ symbol_id translation."""

import json
from pathlib import Path

from jcodemunch_mcp.parser.fqn import symbol_to_fqn, fqn_to_symbol
from jcodemunch_mcp.tools._utils import resolve_fqn
from jcodemunch_mcp.tools.index_folder import index_folder
from jcodemunch_mcp.storage import IndexStore


PSR4_MAP = {
    "App\\": "app/",
    "Database\\": "database/",
}


class TestSymbolToFqn:

    def test_class_symbol(self):
        result = symbol_to_fqn("app/Models/User.php::User#class", PSR4_MAP)
        assert result == "App\\Models\\User"

    def test_nested_namespace(self):
        result = symbol_to_fqn(
            "app/Http/Controllers/Admin/UserController.php::UserController#class",
            PSR4_MAP,
        )
        assert result == "App\\Http\\Controllers\\Admin\\UserController"

    def test_method_symbol(self):
        result = symbol_to_fqn("app/Models/User.php::User.posts#method", PSR4_MAP)
        assert result == "App\\Models\\User::posts"

    def test_non_php_returns_none(self):
        result = symbol_to_fqn("src/main.py::main#function", PSR4_MAP)
        assert result is None

    def test_no_matching_prefix_returns_none(self):
        result = symbol_to_fqn("vendor/Foo.php::Foo#class", PSR4_MAP)
        assert result is None

    def test_no_separator_returns_none(self):
        result = symbol_to_fqn("just-a-string", PSR4_MAP)
        assert result is None

    def test_database_prefix(self):
        result = symbol_to_fqn(
            "database/Seeders/UserSeeder.php::UserSeeder#class", PSR4_MAP
        )
        assert result == "Database\\Seeders\\UserSeeder"

    def test_function_kind_returns_none(self):
        """Only class/method symbols translate to FQNs."""
        result = symbol_to_fqn("app/helpers.php::formatDate#function", PSR4_MAP)
        assert result is None


class TestFqnToSymbol:

    SOURCE_FILES = {
        "app/Models/User.php",
        "app/Models/Post.php",
        "app/Http/Controllers/UserController.php",
        "database/Seeders/UserSeeder.php",
    }

    def test_simple_class(self):
        result = fqn_to_symbol("App\\Models\\User", PSR4_MAP, self.SOURCE_FILES)
        assert result == "app/Models/User.php::User#class"

    def test_nested_class(self):
        result = fqn_to_symbol(
            "App\\Http\\Controllers\\UserController", PSR4_MAP, self.SOURCE_FILES
        )
        assert result == "app/Http/Controllers/UserController.php::UserController#class"

    def test_with_method(self):
        result = fqn_to_symbol("App\\Models\\User::posts", PSR4_MAP, self.SOURCE_FILES)
        assert result == "app/Models/User.php::User.posts#method"

    def test_missing_file_returns_none(self):
        result = fqn_to_symbol(
            "App\\Models\\Order", PSR4_MAP, self.SOURCE_FILES
        )
        assert result is None

    def test_database_prefix(self):
        result = fqn_to_symbol(
            "Database\\Seeders\\UserSeeder", PSR4_MAP, self.SOURCE_FILES
        )
        assert result == "database/Seeders/UserSeeder.php::UserSeeder#class"

    def test_roundtrip_class(self):
        """symbol_to_fqn and fqn_to_symbol should be inverses for class symbols."""
        symbol_id = "app/Models/Post.php::Post#class"
        fqn = symbol_to_fqn(symbol_id, PSR4_MAP)
        assert fqn == "App\\Models\\Post"
        back = fqn_to_symbol(fqn, PSR4_MAP, self.SOURCE_FILES)
        assert back == symbol_id

    def test_roundtrip_method(self):
        symbol_id = "app/Models/User.php::User.posts#method"
        fqn = symbol_to_fqn(symbol_id, PSR4_MAP)
        assert fqn == "App\\Models\\User::posts"
        back = fqn_to_symbol(fqn, PSR4_MAP, self.SOURCE_FILES)
        assert back == symbol_id

    def test_empty_psr4_map(self):
        result = fqn_to_symbol("App\\Models\\User", {}, self.SOURCE_FILES)
        assert result is None

    def test_empty_source_files(self):
        result = fqn_to_symbol("App\\Models\\User", PSR4_MAP, set())
        assert result is None

    def test_fqn_without_backslash(self):
        """Simple class name without namespace should not resolve."""
        result = fqn_to_symbol("User", PSR4_MAP, self.SOURCE_FILES)
        assert result is None


# ---------------------------------------------------------------------------
# resolve_fqn integration (tools/_utils.py)
# ---------------------------------------------------------------------------

def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


class TestResolveFqnIntegration:
    def test_resolves_indexed_class(self, tmp_path):
        store_path = tmp_path / "store"
        _write(tmp_path / "artisan", "<?php\n")
        _write(tmp_path / "composer.json", json.dumps({
            "require": {"laravel/framework": "^11.0"},
            "autoload": {"psr-4": {"App\\": "app/"}},
        }))
        _write(tmp_path / "app" / "Models" / "User.php",
               "<?php\nnamespace App\\Models;\nclass User {}\n")

        result = index_folder(str(tmp_path), use_ai_summaries=False,
                              storage_path=str(store_path))
        assert result["success"]

        resolved, err = resolve_fqn(result["repo"], "App\\Models\\User",
                                    str(store_path))
        assert err is None
        assert resolved == "app/Models/User.php::User#class"

    def test_unresolvable_fqn(self, tmp_path):
        store_path = tmp_path / "store"
        _write(tmp_path / "composer.json", json.dumps({
            "autoload": {"psr-4": {"App\\": "app/"}},
        }))
        _write(tmp_path / "app" / "Foo.php", "<?php\nclass Foo {}\n")

        result = index_folder(str(tmp_path), use_ai_summaries=False,
                              storage_path=str(store_path))
        assert result["success"]

        resolved, err = resolve_fqn(result["repo"], "App\\Models\\Missing",
                                    str(store_path))
        assert resolved is None
        assert "could not be resolved" in err.lower()

    def test_no_psr4_returns_error(self, tmp_path):
        store_path = tmp_path / "store"
        _write(tmp_path / "main.py", "print('hello')\n")

        result = index_folder(str(tmp_path), use_ai_summaries=False,
                              storage_path=str(store_path))
        assert result["success"]

        resolved, err = resolve_fqn(result["repo"], "App\\Models\\User",
                                    str(store_path))
        assert resolved is None
        assert "PSR-4" in err

    def test_repo_not_found(self):
        resolved, err = resolve_fqn("nonexistent/repo", "App\\Models\\User")
        assert resolved is None
        assert "not loadable" in err.lower()
