from pathlib import Path

import pytest

from refua_data.cache import DataCache
from refua_data.catalog import DatasetCatalog
from refua_data.models import DatasetDefinition
from refua_data.pipeline import DatasetManager


def _build_manager(source_path: Path, cache_root: Path) -> DatasetManager:
    dataset = DatasetDefinition(
        dataset_id="toy",
        name="Toy",
        description="Toy test dataset",
        source="unit-test",
        homepage="https://example.test",
        license_name="test",
        license_url=None,
        urls=(source_path.resolve().as_uri(),),
        file_format="csv",
        category="test",
        tags=("unit",),
    )
    catalog = DatasetCatalog.from_entries([dataset])
    cache = DataCache(cache_root)
    return DatasetManager(catalog=catalog, cache=cache)


def _build_concat_manager(source_paths: tuple[Path, ...], cache_root: Path) -> DatasetManager:
    dataset = DatasetDefinition(
        dataset_id="toy_concat",
        name="Toy Concat",
        description="Toy multi-source concat dataset",
        source="unit-test",
        homepage="https://example.test",
        license_name="test",
        license_url=None,
        urls=tuple(path.resolve().as_uri() for path in source_paths),
        file_format="csv",
        category="test",
        tags=("unit",),
        url_mode="concat",
    )
    catalog = DatasetCatalog.from_entries([dataset])
    cache = DataCache(cache_root)
    return DatasetManager(catalog=catalog, cache=cache)


def test_fetch_uses_local_cache_when_available(tmp_path: Path) -> None:
    source = tmp_path / "source.csv"
    source.write_text("smiles,label\nCCO,1\nCCC,0\n", encoding="utf-8")

    manager = _build_manager(source, tmp_path / "cache")

    first = manager.fetch("toy")
    second = manager.fetch("toy")
    meta = manager.cache.read_json(first.metadata_path)

    assert first.cache_hit is False
    assert second.cache_hit is True
    assert first.sha256 == second.sha256
    assert isinstance(meta, dict)
    dataset_meta = meta.get("dataset")
    assert isinstance(dataset_meta, dict)
    assert dataset_meta.get("description") == "Toy test dataset"
    assert dataset_meta.get("usage_notes") == ["Toy test dataset"]


def test_fetch_refresh_detects_updated_file_url(tmp_path: Path) -> None:
    source = tmp_path / "source.csv"
    source.write_text("smiles,label\nCCO,1\n", encoding="utf-8")

    manager = _build_manager(source, tmp_path / "cache")

    first = manager.fetch("toy")
    source.write_text("smiles,label\nCCO,1\nCCN,0\n", encoding="utf-8")
    refreshed = manager.fetch("toy", refresh=True)

    assert first.sha256 != refreshed.sha256
    assert refreshed.cache_hit is False
    assert refreshed.bytes_downloaded > 0


def test_fetch_refresh_raises_if_source_is_unavailable(tmp_path: Path) -> None:
    source = tmp_path / "source.csv"
    source.write_text("smiles,label\nCCO,1\n", encoding="utf-8")

    manager = _build_manager(source, tmp_path / "cache")
    manager.fetch("toy")

    source.unlink()
    with pytest.raises(RuntimeError, match="Failed to fetch dataset 'toy'"):
        manager.fetch("toy", refresh=True)


def test_fetch_concat_mode_merges_multiple_sources_and_skips_duplicate_headers(
    tmp_path: Path,
) -> None:
    source_a = tmp_path / "part_a.csv"
    source_b = tmp_path / "part_b.csv"
    source_a.write_text("smiles,label\nCCO,1\n", encoding="utf-8")
    source_b.write_text("smiles,label\nCCC,0\n", encoding="utf-8")

    manager = _build_concat_manager((source_a, source_b), tmp_path / "cache")
    fetched = manager.fetch("toy_concat")

    merged_lines = fetched.raw_path.read_text(encoding="utf-8").strip().splitlines()
    assert fetched.cache_hit is False
    assert merged_lines == ["smiles,label", "CCO,1", "CCC,0"]
