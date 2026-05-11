"""Tests for find_implementations — multi-source impl discovery."""

from pathlib import Path

from jcodemunch_mcp.tools.find_implementations import find_implementations
from jcodemunch_mcp.tools.index_folder import index_folder


def _make_repo(tmp_path: Path, files: dict) -> tuple[str, str]:
    for rel, content in files.items():
        p = tmp_path / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content, encoding="utf-8")
    storage = str(tmp_path / ".index")
    result = index_folder(str(tmp_path), use_ai_summaries=False, storage_path=storage)
    repo_id = result.get("repo", str(tmp_path))
    return repo_id, storage


_INHERITANCE_REPO = {
    "shapes.py": (
        "class Shape:\n"
        "    def area(self):\n"
        "        raise NotImplementedError\n\n"
        "    def perimeter(self):\n"
        "        raise NotImplementedError\n"
    ),
    "circle.py": (
        "from shapes import Shape\n\n"
        "class Circle(Shape):\n"
        "    def __init__(self, r):\n"
        "        self.r = r\n\n"
        "    def area(self):\n"
        "        return 3.14 * self.r * self.r\n\n"
        "    def perimeter(self):\n"
        "        return 2 * 3.14 * self.r\n"
    ),
    "square.py": (
        "from shapes import Shape\n\n"
        "class Square(Shape):\n"
        "    def __init__(self, s):\n"
        "        self.s = s\n\n"
        "    def area(self):\n"
        "        return self.s * self.s\n\n"
        "    def perimeter(self):\n"
        "        return 4 * self.s\n"
    ),
    "unrelated.py": (
        "class Animal:\n"
        "    def speak(self):\n"
        "        return 'noise'\n"
    ),
}


class TestFindImplementationsClasses:
    def test_subclasses_found_for_base_class(self, tmp_path):
        repo, storage = _make_repo(tmp_path, _INHERITANCE_REPO)
        result = find_implementations(repo, symbol="Shape", storage_path=storage)
        assert "error" not in result
        names = {i["name"] for i in result["implementations"]}
        assert "Circle" in names
        assert "Square" in names
        # Sanity: unrelated class should not appear
        assert "Animal" not in names

    def test_subclass_relationship_kind_emitted(self, tmp_path):
        repo, storage = _make_repo(tmp_path, _INHERITANCE_REPO)
        result = find_implementations(repo, symbol="Shape", storage_path=storage)
        for impl in result["implementations"]:
            assert impl["relationship"] in {"subclass", "subclass_override", "interface_impl", "duck_typed", "decorator_handler"}

    def test_confidence_attached(self, tmp_path):
        repo, storage = _make_repo(tmp_path, _INHERITANCE_REPO)
        result = find_implementations(repo, symbol="Shape", storage_path=storage)
        for impl in result["implementations"]:
            assert 0.0 < impl["confidence"] <= 1.0


class TestFindImplementationsMethods:
    def test_method_finds_overrides(self, tmp_path):
        repo, storage = _make_repo(tmp_path, _INHERITANCE_REPO)
        # Target the base-class method explicitly so all three area() overrides are candidates
        # other than the target itself.
        result = find_implementations(
            repo, symbol="shapes.py::Shape.area#method", storage_path=storage,
        )
        assert "error" not in result
        impl_files = {i["file"] for i in result["implementations"]}
        # When the target is the base, both circle.py and square.py overrides should appear.
        assert "circle.py" in impl_files
        assert "square.py" in impl_files

    def test_differs_by_present_for_methods(self, tmp_path):
        repo, storage = _make_repo(tmp_path, _INHERITANCE_REPO)
        result = find_implementations(repo, symbol="area", storage_path=storage)
        for impl in result["implementations"]:
            assert "differs_by" in impl
            assert isinstance(impl["differs_by"], list)


class TestFindImplementationsFilters:
    def test_relationship_kinds_filter(self, tmp_path):
        repo, storage = _make_repo(tmp_path, _INHERITANCE_REPO)
        result = find_implementations(
            repo, symbol="Shape",
            relationship_kinds=["subclass"],
            storage_path=storage,
        )
        for impl in result["implementations"]:
            assert impl["relationship"] == "subclass"

    def test_include_subclasses_false_for_class(self, tmp_path):
        repo, storage = _make_repo(tmp_path, _INHERITANCE_REPO)
        result = find_implementations(
            repo, symbol="Shape", include_subclasses=False, storage_path=storage,
        )
        # Without subclass walk, class target yields nothing structurally
        assert result["implementations_returned"] == 0

    def test_max_results_caps(self, tmp_path):
        repo, storage = _make_repo(tmp_path, _INHERITANCE_REPO)
        result = find_implementations(
            repo, symbol="area", max_results=1, storage_path=storage,
        )
        assert result["implementations_returned"] <= 1


class TestFindImplementationsMeta:
    def test_target_echoed(self, tmp_path):
        repo, storage = _make_repo(tmp_path, _INHERITANCE_REPO)
        result = find_implementations(repo, symbol="Shape", storage_path=storage)
        assert result["target"]["name"] == "Shape"
        assert result["target"]["kind"] in ("class", "type")

    def test_relationship_counts_present(self, tmp_path):
        repo, storage = _make_repo(tmp_path, _INHERITANCE_REPO)
        result = find_implementations(repo, symbol="Shape", storage_path=storage)
        assert "relationship_counts" in result
        assert isinstance(result["relationship_counts"], dict)


class TestFindImplementationsErrors:
    def test_unindexed_repo(self, tmp_path):
        storage = str(tmp_path / ".index")
        result = find_implementations("nope/repo", symbol="X", storage_path=storage)
        assert "error" in result
        assert result["loadable"] is False
        assert result["status"] == "missing"
        assert result["load_error"] == "missing"

    def test_unknown_symbol(self, tmp_path):
        repo, storage = _make_repo(tmp_path, _INHERITANCE_REPO)
        result = find_implementations(repo, symbol="NoSuchSymbol", storage_path=storage)
        assert "error" in result

    def test_invalid_max_results(self, tmp_path):
        repo, storage = _make_repo(tmp_path, _INHERITANCE_REPO)
        result = find_implementations(repo, symbol="Shape", max_results=0, storage_path=storage)
        assert "error" in result
