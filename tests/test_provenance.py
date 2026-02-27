from __future__ import annotations

import json
from pathlib import Path

from refua_data.provenance import (
    build_data_provenance_record,
    load_materialized_manifest,
    summarize_materialized_dataset,
)


def test_summarize_materialized_dataset(tmp_path: Path) -> None:
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "dataset_id": "toy",
                "version": "latest",
                "generated_at": "2026-02-26T00:00:00+00:00",
                "source": {
                    "url": "https://example.test/toy.csv",
                    "sha256": "abc123",
                },
                "row_count": 12,
                "parts": ["part-00000.parquet", "part-00001.parquet"],
                "dataset": {
                    "name": "Toy Dataset",
                    "category": "admet",
                    "license_name": "Upstream terms",
                },
            }
        ),
        encoding="utf-8",
    )

    payload = summarize_materialized_dataset(manifest_path)
    assert payload["dataset_id"] == "toy"
    assert payload["parts_count"] == 2
    assert payload["row_count"] == 12
    assert payload["dataset_name"] == "Toy Dataset"
    assert payload["manifest_path"] == str(manifest_path.resolve())


def test_build_data_provenance_record_handles_sparse_manifest() -> None:
    payload = build_data_provenance_record({"dataset_id": "x", "parts": []})
    assert payload["dataset_id"] == "x"
    assert payload["parts_count"] == 0
    assert payload["source_url"] is None


def test_load_materialized_manifest_rejects_missing(tmp_path: Path) -> None:
    missing = tmp_path / "missing.json"
    try:
        load_materialized_manifest(missing)
    except ValueError as exc:
        assert "does not exist" in str(exc)
    else:
        raise AssertionError("expected ValueError for missing manifest")
