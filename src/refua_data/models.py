"""Core data models for refua-data."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlparse

TabularFormat = Literal["csv", "tsv", "jsonl"]
Compression = Literal["none", "gzip", "zip", "infer"]
ApiPaginationMode = Literal["none", "chembl", "link_header"]
UrlMode = Literal["fallback", "concat"]

_CATEGORY_USAGE_DEFAULTS: dict[str, str] = {
    "compound_library": "Use for compound library curation, screening, and molecular pretraining.",
    "target_activity": "Use for ligand-target activity modeling and potency benchmarking.",
    "toxicity": "Use for toxicity risk prediction and safety classification tasks.",
    "admet": "Use for ADMET property prediction and developability screening.",
    "safety": "Use for pharmacovigilance and safety endpoint modeling.",
    "virtual_screening": "Use for virtual screening and hit prioritization workflows.",
    "physchem": "Use for physicochemical property modeling and feature engineering.",
    "assays": "Use for assay landscape analysis and protocol-level benchmarking.",
    "targets": "Use for target selection, annotation, and target-space definition.",
    "target_families": "Use for family-focused target programs and panel design.",
}


@dataclass(frozen=True, slots=True)
class ApiDatasetConfig:
    """Configuration for API-backed datasets."""

    endpoint: str
    params: dict[str, str | int | float | bool] = field(default_factory=dict)
    headers: dict[str, str] = field(default_factory=dict)
    pagination: ApiPaginationMode = "none"
    items_path: str = "results"
    page_size_param: str | None = None
    page_size: int | None = None
    max_pages: int | None = 100
    max_rows: int | None = 10_000

    def request_signature(self) -> dict[str, Any]:
        """Return a stable signature used for cache compatibility checks."""
        return {
            "endpoint": self.endpoint,
            "params": dict(sorted(self.params.items())),
            "headers": dict(sorted(self.headers.items())),
            "pagination": self.pagination,
            "items_path": self.items_path,
            "page_size_param": self.page_size_param,
            "page_size": self.page_size,
            "max_pages": self.max_pages,
            "max_rows": self.max_rows,
        }


@dataclass(frozen=True, slots=True)
class DatasetDefinition:
    """Dataset metadata used by fetch/materialize workflows."""

    dataset_id: str
    name: str
    description: str
    source: str
    homepage: str
    license_name: str
    license_url: str | None
    file_format: TabularFormat
    category: str
    urls: tuple[str, ...] = ()
    api: ApiDatasetConfig | None = None
    usage_notes: tuple[str, ...] = ()
    tags: tuple[str, ...] = ()
    delimiter: str | None = None
    compression: Compression = "infer"
    version: str = "latest"
    filename: str | None = None
    url_mode: UrlMode = "fallback"

    def preferred_filename(self) -> str:
        """Return a filesystem-safe filename for the raw file."""
        if self.filename:
            return self.filename
        if self.api is not None:
            return f"{self.dataset_id}.jsonl"
        if self.urls:
            first_url = self.urls[0]
            parsed = urlparse(first_url)
            from_url = Path(parsed.path).name
            if from_url:
                return from_url
        fallback_ext = {
            "csv": ".csv",
            "tsv": ".tsv",
            "jsonl": ".jsonl",
        }[self.file_format]
        return f"{self.dataset_id}{fallback_ext}"

    def resolved_usage_notes(self) -> tuple[str, ...]:
        """Return explicit usage notes or a category-derived fallback note."""
        if self.usage_notes:
            return self.usage_notes
        fallback = _CATEGORY_USAGE_DEFAULTS.get(self.category)
        if fallback:
            return (fallback,)
        return (self.description,)

    def metadata_snapshot(self) -> dict[str, Any]:
        """Return normalized metadata suitable for cache/manifests and CLI output."""
        return {
            "dataset_id": self.dataset_id,
            "name": self.name,
            "description": self.description,
            "usage_notes": list(self.resolved_usage_notes()),
            "category": self.category,
            "source_type": "api" if self.api is not None else "file",
            "source": self.source,
            "homepage": self.homepage,
            "license_name": self.license_name,
            "license_url": self.license_url,
            "version": self.version,
            "file_format": self.file_format,
            "compression": self.compression,
            "delimiter": self.delimiter,
            "filename": self.filename or self.preferred_filename(),
            "url_mode": self.url_mode,
            "tags": list(self.tags),
            "urls": list(self.urls),
            "api": self.api.request_signature() if self.api is not None else None,
        }


@dataclass(frozen=True, slots=True)
class FetchResult:
    """Result of a dataset fetch call."""

    dataset_id: str
    version: str
    raw_path: Path
    metadata_path: Path
    source_url: str
    cache_hit: bool
    refreshed: bool
    bytes_downloaded: int
    sha256: str


@dataclass(frozen=True, slots=True)
class MaterializeResult:
    """Result of parquet materialization."""

    dataset_id: str
    version: str
    parquet_dir: Path
    manifest_path: Path
    parts: tuple[Path, ...]
    row_count: int
    cache_hit: bool
    source_sha256: str
