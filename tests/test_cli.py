from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd
import pytest
from _pytest.capture import CaptureFixture
from _pytest.monkeypatch import MonkeyPatch

from refua_data import DataCache, DatasetManager
from refua_data.cli import main


def _write_materialized_fixture(cache_root: Path, *, dataset_id: str = "tox21") -> None:
    cache = DataCache(cache_root)
    manager = DatasetManager(cache=cache)
    dataset = manager.catalog.get(dataset_id)

    parquet_dir = cache.parquet_dir(dataset)
    parquet_dir.mkdir(parents=True, exist_ok=True)
    part_path = parquet_dir / "part-00000.parquet"

    frame = pd.DataFrame(
        [
            {"smiles": "CCO", "label": 1, "split": "train"},
            {"smiles": "CCN", "label": 0, "split": "train"},
            {"smiles": "CCC", "label": 1, "split": "valid"},
        ]
    )
    frame.to_parquet(part_path, index=False)

    cache.write_json(
        cache.parquet_manifest(dataset),
        {
            "dataset_id": dataset.dataset_id,
            "version": dataset.version,
            "row_count": int(len(frame)),
            "parts": [part_path.name],
            "source": {"sha256": "fixture"},
            "dataset": dataset.metadata_snapshot(),
        },
    )


def test_cli_query_reads_manifest_without_materialize(
    tmp_path: Path,
    capsys: CaptureFixture[str],
    monkeypatch: MonkeyPatch,
) -> None:
    _write_materialized_fixture(tmp_path)

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "refua-data",
            "--cache-root",
            str(tmp_path),
            "query",
            "tox21",
            "--columns",
            "smiles,label",
            "--filters",
            '{"label":{"eq":1}}',
            "--limit",
            "10",
            "--no-materialize-if-missing",
        ],
    )
    rc = main()

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["dataset_id"] == "tox21"
    assert payload["returned_rows"] == 2
    assert payload["scanned_parts"] == 1
    assert payload["columns"] == ["smiles", "label"]
    assert all(set(row) == {"smiles", "label"} for row in payload["rows"])
    assert {row["smiles"] for row in payload["rows"]} == {"CCO", "CCC"}


def test_cli_query_without_manifest_returns_error(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "refua-data",
            "--cache-root",
            str(tmp_path),
            "query",
            "tox21",
            "--no-materialize-if-missing",
        ],
    )
    with pytest.raises(ValueError, match="has no parquet manifest"):
        main()


def test_cli_query_rejects_invalid_filter_json(
    tmp_path: Path,
    monkeypatch: MonkeyPatch,
) -> None:
    _write_materialized_fixture(tmp_path)

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "refua-data",
            "--cache-root",
            str(tmp_path),
            "query",
            "tox21",
            "--filters",
            "{not-json",
            "--no-materialize-if-missing",
        ],
    )
    with pytest.raises(ValueError, match="filters must be a valid JSON object"):
        main()
