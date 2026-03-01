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
