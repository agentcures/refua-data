from pathlib import Path
from typing import Any

from refua_data.cache import DataCache
from refua_data.catalog import DatasetCatalog
from refua_data.models import DatasetDefinition
from refua_data.pipeline import DatasetManager


class ProxyCache:
    """Simple delegating cache backend used to validate pluggable cache wiring."""

    def __init__(self, root: Path):
        self._inner = DataCache(root)
        self.root = self._inner.root
        self.ensure_calls = 0
        self.write_calls = 0

    def ensure(self) -> None:
        self.ensure_calls += 1
        self._inner.ensure()

    def raw_file(self, dataset: DatasetDefinition) -> Path:
        return self._inner.raw_file(dataset)

    def raw_meta(self, dataset: DatasetDefinition) -> Path:
        return self._inner.raw_meta(dataset)

    def parquet_dir(self, dataset: DatasetDefinition) -> Path:
        return self._inner.parquet_dir(dataset)

    def parquet_manifest(self, dataset: DatasetDefinition) -> Path:
        return self._inner.parquet_manifest(dataset)

    def read_json(self, path: Path) -> dict[str, Any] | None:
        return self._inner.read_json(path)

    def write_json(self, path: Path, payload: dict[str, Any]) -> None:
        self.write_calls += 1
        self._inner.write_json(path, payload)


def test_dataset_manager_accepts_pluggable_cache_backend(tmp_path: Path) -> None:
    source = tmp_path / "source.csv"
    source.write_text("smiles,label\nCCO,1\nCCC,0\n", encoding="utf-8")

    dataset = DatasetDefinition(
        dataset_id="toy",
        name="Toy",
        description="Toy test dataset",
        source="unit-test",
        homepage="https://example.test",
        license_name="test",
        license_url=None,
        urls=(source.resolve().as_uri(),),
        file_format="csv",
        category="test",
        tags=("unit",),
    )
    catalog = DatasetCatalog.from_entries([dataset])
    cache = ProxyCache(tmp_path / "cache")
    manager = DatasetManager(catalog=catalog, cache=cache)

    first = manager.fetch("toy")
    second = manager.fetch("toy")

    assert cache.ensure_calls >= 1
    assert cache.write_calls >= 1
    assert first.cache_hit is False
    assert second.cache_hit is True
