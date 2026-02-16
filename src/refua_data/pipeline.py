"""High-level dataset fetch and parquet materialization pipeline."""

from __future__ import annotations

import shutil
from datetime import UTC, datetime
from pathlib import Path

from .cache import CacheBackend, DataCache
from .catalog import DatasetCatalog, get_default_catalog
from .downloader import fetch_dataset
from .io import iter_dataset_chunks
from .models import DatasetDefinition, FetchResult, MaterializeResult
from .validation import SourceValidationResult, validate_dataset_sources

_DEFAULT_CHUNKSIZE = 100_000


def _utcnow_iso() -> str:
    return datetime.now(UTC).isoformat()


class DatasetManager:
    """Entrypoint for catalog lookup, downloading, and parquet conversion."""

    def __init__(
        self,
        *,
        catalog: DatasetCatalog | None = None,
        cache: CacheBackend | None = None,
    ):
        self.catalog = catalog or get_default_catalog()
        self.cache: CacheBackend = cache or DataCache()

    def list_datasets(self, *, tag: str | None = None) -> list[DatasetDefinition]:
        """List available datasets, optionally filtered by tag."""
        if tag is None:
            return self.catalog.list()
        return self.catalog.filter_by_tag(tag)

    def fetch(
        self,
        dataset_id: str,
        *,
        force: bool = False,
        refresh: bool = False,
        timeout_seconds: float = 120.0,
    ) -> FetchResult:
        """Fetch a dataset to local cache."""
        dataset = self.catalog.get(dataset_id)
        return fetch_dataset(
            dataset,
            cache=self.cache,
            force=force,
            refresh=refresh,
            timeout_seconds=timeout_seconds,
        )

    def materialize(
        self,
        dataset_id: str,
        *,
        force: bool = False,
        refresh: bool = False,
        chunksize: int = _DEFAULT_CHUNKSIZE,
        timeout_seconds: float = 120.0,
    ) -> MaterializeResult:
        """Fetch a dataset and materialize chunked parquet output."""
        if chunksize < 1:
            raise ValueError("chunksize must be >= 1")

        dataset = self.catalog.get(dataset_id)
        fetch_result = self.fetch(
            dataset_id,
            force=force,
            refresh=refresh,
            timeout_seconds=timeout_seconds,
        )

        parquet_dir = self.cache.parquet_dir(dataset)
        manifest_path = self.cache.parquet_manifest(dataset)

        if not force:
            cached = self._manifest_cache_hit(
                dataset=dataset,
                source_sha256=fetch_result.sha256,
                parquet_dir=parquet_dir,
                manifest_path=manifest_path,
            )
            if cached is not None:
                return cached

        if parquet_dir.exists():
            shutil.rmtree(parquet_dir)
        parquet_dir.mkdir(parents=True, exist_ok=True)

        parts: list[Path] = []
        row_count = 0

        for index, chunk in enumerate(
            iter_dataset_chunks(fetch_result.raw_path, dataset=dataset, chunksize=chunksize)
        ):
            part_path = parquet_dir.joinpath(f"part-{index:05d}.parquet")
            chunk.to_parquet(part_path, index=False)
            parts.append(part_path)
            row_count += int(len(chunk))

        if not parts:
            raise ValueError(
                f"No tabular rows found while materializing dataset '{dataset_id}'."
            )

        manifest = {
            "dataset_id": dataset.dataset_id,
            "version": dataset.version,
            "generated_at": _utcnow_iso(),
            "source": {
                "url": fetch_result.source_url,
                "raw_path": str(fetch_result.raw_path),
                "sha256": fetch_result.sha256,
            },
            "row_count": row_count,
            "parts": [str(path.name) for path in parts],
            "dataset": dataset.metadata_snapshot(),
        }
        self.cache.write_json(manifest_path, manifest)

        return MaterializeResult(
            dataset_id=dataset.dataset_id,
            version=dataset.version,
            parquet_dir=parquet_dir,
            manifest_path=manifest_path,
            parts=tuple(parts),
            row_count=row_count,
            cache_hit=False,
            source_sha256=fetch_result.sha256,
        )

    def _manifest_cache_hit(
        self,
        *,
        dataset: DatasetDefinition,
        source_sha256: str,
        parquet_dir: Path,
        manifest_path: Path,
    ) -> MaterializeResult | None:
        manifest = self.cache.read_json(manifest_path)
        if not manifest or not parquet_dir.exists():
            return None

        source = manifest.get("source")
        if not isinstance(source, dict):
            return None
        if source.get("sha256") != source_sha256:
            return None

        parts_raw = manifest.get("parts")
        if not isinstance(parts_raw, list) or not parts_raw:
            return None

        parts = tuple(parquet_dir.joinpath(str(name)) for name in parts_raw)
        if not all(path.exists() for path in parts):
            return None

        row_count_raw = manifest.get("row_count")
        row_count = int(row_count_raw) if isinstance(row_count_raw, int | float | str) else 0

        return MaterializeResult(
            dataset_id=dataset.dataset_id,
            version=dataset.version,
            parquet_dir=parquet_dir,
            manifest_path=manifest_path,
            parts=parts,
            row_count=row_count,
            cache_hit=True,
            source_sha256=source_sha256,
        )

    def fetch_many(
        self,
        dataset_ids: list[str],
        *,
        force: bool = False,
        refresh: bool = False,
    ) -> list[FetchResult]:
        """Fetch multiple datasets."""
        return [self.fetch(dataset_id, force=force, refresh=refresh) for dataset_id in dataset_ids]

    def materialize_many(
        self,
        dataset_ids: list[str],
        *,
        force: bool = False,
        refresh: bool = False,
        chunksize: int = _DEFAULT_CHUNKSIZE,
    ) -> list[MaterializeResult]:
        """Materialize multiple datasets."""
        return [
            self.materialize(
                dataset_id,
                force=force,
                refresh=refresh,
                chunksize=chunksize,
            )
            for dataset_id in dataset_ids
        ]

    def validate_sources(
        self,
        *,
        dataset_ids: list[str] | None = None,
        tag: str | None = None,
        timeout_seconds: float = 20.0,
    ) -> list[SourceValidationResult]:
        """Validate dataset source accessibility for configured datasets."""
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be > 0")

        datasets: list[DatasetDefinition]
        if dataset_ids is not None:
            datasets = [self.catalog.get(dataset_id) for dataset_id in dataset_ids]
        else:
            datasets = self.list_datasets(tag=tag)

        results: list[SourceValidationResult] = []
        for dataset in datasets:
            results.extend(
                validate_dataset_sources(
                    dataset,
                    timeout_seconds=timeout_seconds,
                )
            )
        return results
