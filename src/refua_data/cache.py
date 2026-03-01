"""Filesystem cache primitives for dataset files and metadata."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Any, Protocol

from .config import default_cache_root
from .models import DatasetDefinition


class CacheBackend(Protocol):
    """Protocol for pluggable cache backends used by the pipeline."""

    root: Path

    def ensure(self) -> None: ...

    def raw_file(self, dataset: DatasetDefinition) -> Path: ...

    def raw_meta(self, dataset: DatasetDefinition) -> Path: ...

    def parquet_dir(self, dataset: DatasetDefinition) -> Path: ...

    def parquet_manifest(self, dataset: DatasetDefinition) -> Path: ...

    def read_json(self, path: Path) -> dict[str, Any] | None: ...

    def write_json(self, path: Path, payload: dict[str, Any]) -> None: ...


class DataCache:
    """Filesystem-backed cache backend for raw + parquet artifacts."""

    def __init__(self, root: Path | None = None):
        self.root = (root or default_cache_root()).expanduser().resolve()

    def ensure(self) -> None:
        """Create required cache root directories."""
        self.root.mkdir(parents=True, exist_ok=True)
        self.root.joinpath("raw").mkdir(parents=True, exist_ok=True)
        self.root.joinpath("parquet").mkdir(parents=True, exist_ok=True)
        self.root.joinpath("_meta", "raw").mkdir(parents=True, exist_ok=True)
        self.root.joinpath("_meta", "parquet").mkdir(parents=True, exist_ok=True)

    def raw_file(self, dataset: DatasetDefinition) -> Path:
        """Return raw file path for a dataset."""
        filename = dataset.preferred_filename()
        return self.root.joinpath("raw", dataset.dataset_id, dataset.version, filename)

    def raw_meta(self, dataset: DatasetDefinition) -> Path:
        """Return raw metadata path for a dataset."""
        filename = f"{dataset.preferred_filename()}.json"
        return self.root.joinpath(
            "_meta", "raw", dataset.dataset_id, dataset.version, filename
        )

    def parquet_dir(self, dataset: DatasetDefinition) -> Path:
        """Return parquet output directory for a dataset."""
        return self.root.joinpath("parquet", dataset.dataset_id, dataset.version)

    def parquet_manifest(self, dataset: DatasetDefinition) -> Path:
        """Return parquet manifest metadata path for a dataset."""
        return self.root.joinpath(
            "_meta",
            "parquet",
            dataset.dataset_id,
            dataset.version,
            "manifest.json",
        )

    def read_json(self, path: Path) -> dict[str, Any] | None:
        """Read JSON metadata if it exists."""
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))

    def write_json(self, path: Path, payload: dict[str, Any]) -> None:
        """Write JSON metadata atomically."""
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        tmp_path.write_text(
            json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8"
        )
        os.replace(tmp_path, path)


_CHUNK_SIZE = 4 * 1024 * 1024


def sha256_file(path: Path) -> str:
    """Compute the SHA256 checksum of a file."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(_CHUNK_SIZE)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()
