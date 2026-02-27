"""Helpers for converting materialized datasets into provenance records."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping


def load_materialized_manifest(manifest_path: Path) -> dict[str, Any]:
    """Load and validate a refua-data parquet manifest."""
    resolved = manifest_path.expanduser().resolve()
    if not resolved.exists() or not resolved.is_file():
        raise ValueError(f"Manifest file does not exist: {resolved}")

    import json

    try:
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Manifest is not valid JSON: {resolved}") from exc

    if not isinstance(payload, dict):
        raise ValueError(f"Manifest must be a JSON object: {resolved}")
    return payload


def build_data_provenance_record(
    manifest: Mapping[str, Any],
    *,
    manifest_path: Path | None = None,
) -> dict[str, Any]:
    """Return a normalized provenance record from a parquet manifest payload."""
    source = manifest.get("source")
    if not isinstance(source, Mapping):
        source = {}

    dataset = manifest.get("dataset")
    if not isinstance(dataset, Mapping):
        dataset = {}

    parts = manifest.get("parts")
    if not isinstance(parts, list):
        parts = []

    record: dict[str, Any] = {
        "dataset_id": _as_text(manifest.get("dataset_id")),
        "version": _as_text(manifest.get("version")),
        "row_count": _as_int(manifest.get("row_count")),
        "parts_count": len(parts),
        "source_url": _as_text(source.get("url")),
        "sha256": _as_text(source.get("sha256")),
        "license_name": _as_text(dataset.get("license_name")),
        "generated_at": _as_text(manifest.get("generated_at")),
        "dataset_name": _as_text(dataset.get("name")),
        "category": _as_text(dataset.get("category")),
    }
    if manifest_path is not None:
        record["manifest_path"] = str(manifest_path.expanduser().resolve())
    return record


def summarize_materialized_dataset(manifest_path: Path) -> dict[str, Any]:
    """Load a manifest file and return a provenance summary."""
    payload = load_materialized_manifest(manifest_path)
    return build_data_provenance_record(payload, manifest_path=manifest_path)


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _as_int(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None
