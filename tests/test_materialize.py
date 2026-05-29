from pathlib import Path

import pandas as pd

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


def _build_bundle_manager(
    source_paths: tuple[Path, ...], cache_root: Path
) -> DatasetManager:
    dataset = DatasetDefinition(
        dataset_id="toy_bundle",
        name="Toy Bundle",
        description="Toy parquet bundle dataset",
        source="unit-test",
        homepage="https://example.test",
        license_name="test",
        license_url=None,
        urls=tuple(path.resolve().as_uri() for path in source_paths),
        file_format="parquet",
        category="test",
        tags=("unit",),
        filename="toy_bundle",
        url_mode="bundle",
    )
    catalog = DatasetCatalog.from_entries([dataset])
    cache = DataCache(cache_root)
    return DatasetManager(catalog=catalog, cache=cache)


def _build_excel_manager(source_path: Path, cache_root: Path) -> DatasetManager:
    dataset = DatasetDefinition(
        dataset_id="toy_excel",
        name="Toy Excel",
        description="Toy excel dataset",
        source="unit-test",
        homepage="https://example.test",
        license_name="test",
        license_url=None,
        urls=(source_path.resolve().as_uri(),),
        file_format="xlsx",
        category="test",
        tags=("unit",),
    )
    catalog = DatasetCatalog.from_entries([dataset])
    cache = DataCache(cache_root)
    return DatasetManager(catalog=catalog, cache=cache)


def test_materialize_writes_parquet_and_manifest(tmp_path: Path) -> None:
    source = tmp_path / "source.csv"
    source.write_text("smiles,label\nCCO,1\nCCC,0\nCCN,1\n", encoding="utf-8")

    manager = _build_manager(source, tmp_path / "cache")

    first = manager.materialize("toy", chunksize=2)
    second = manager.materialize("toy", chunksize=2)

    assert first.cache_hit is False
    assert second.cache_hit is True
    assert first.row_count == 3
    assert len(first.parts) == 2
    assert first.manifest_path.exists()
    manifest = manager.cache.read_json(first.manifest_path)

    loaded = pd.concat(
        [pd.read_parquet(part) for part in first.parts], ignore_index=True
    )
    assert len(loaded) == 3
    assert set(loaded.columns) == {"smiles", "label"}
    assert isinstance(manifest, dict)
    dataset_meta = manifest.get("dataset")
    assert isinstance(dataset_meta, dict)
    assert dataset_meta.get("description") == "Toy test dataset"
    assert dataset_meta.get("usage_notes") == ["Toy test dataset"]


def test_materialize_reads_parquet_bundle_sources(tmp_path: Path) -> None:
    source_a = tmp_path / "part_a.parquet"
    source_b = tmp_path / "part_b.parquet"
    pd.DataFrame({"target": ["SRC"], "score": [0.8]}).to_parquet(source_a, index=False)
    pd.DataFrame({"target": ["EGFR"], "score": [0.9]}).to_parquet(source_b, index=False)

    manager = _build_bundle_manager((source_a, source_b), tmp_path / "cache")

    result = manager.materialize("toy_bundle", chunksize=1)

    loaded = pd.concat(
        [pd.read_parquet(part) for part in result.parts], ignore_index=True
    )
    assert result.row_count == 2
    assert len(result.parts) == 2
    assert set(loaded["target"]) == {"SRC", "EGFR"}


def test_materialize_reads_excel_sources(tmp_path: Path) -> None:
    source = tmp_path / "source.xlsx"
    pd.DataFrame({"cell_line": ["A673", "PFSK-1"], "auc": [0.61, 0.93]}).to_excel(
        source, index=False
    )

    manager = _build_excel_manager(source, tmp_path / "cache")

    result = manager.materialize("toy_excel", chunksize=1)

    loaded = pd.concat(
        [pd.read_parquet(part) for part in result.parts], ignore_index=True
    )
    assert result.row_count == 2
    assert len(result.parts) == 2
    assert set(loaded["cell_line"]) == {"A673", "PFSK-1"}
