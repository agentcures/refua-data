"""CLI for refua-data."""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .cache import DataCache
from .pipeline import DatasetManager


def build_parser() -> argparse.ArgumentParser:
    """Build command-line parser."""
    parser = argparse.ArgumentParser(
        prog="refua-data", description="Refua dataset tooling"
    )
    parser.add_argument(
        "--cache-root",
        type=Path,
        default=None,
        help="Override cache root (default: $REFUA_DATA_HOME or ~/.cache/refua-data)",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list", help="List available datasets")
    list_parser.add_argument("--tag", default=None, help="Filter datasets by tag")
    list_parser.add_argument("--json", action="store_true", help="Emit JSON output")

    fetch_parser = subparsers.add_parser("fetch", help="Fetch one dataset")
    fetch_parser.add_argument("dataset_id", help="Dataset ID")
    fetch_parser.add_argument("--force", action="store_true", help="Force re-download")
    fetch_parser.add_argument(
        "--refresh",
        action="store_true",
        help="Refresh against remote with conditional HTTP requests",
    )
    fetch_parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=120.0,
        help="Download timeout in seconds",
    )

    materialize_parser = subparsers.add_parser(
        "materialize", help="Fetch and materialize one dataset to parquet"
    )
    materialize_parser.add_argument("dataset_id", help="Dataset ID")
    materialize_parser.add_argument(
        "--force", action="store_true", help="Force reprocessing"
    )
    materialize_parser.add_argument(
        "--refresh",
        action="store_true",
        help="Refresh raw file against remote metadata",
    )
    materialize_parser.add_argument(
        "--chunksize",
        type=int,
        default=100_000,
        help="Rows per chunk for parquet writing",
    )
    materialize_parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=120.0,
        help="Download timeout in seconds",
    )

    mat_all_parser = subparsers.add_parser(
        "materialize-all", help="Materialize all datasets (or by tag)"
    )
    mat_all_parser.add_argument("--tag", default=None, help="Filter datasets by tag")
    mat_all_parser.add_argument(
        "--force", action="store_true", help="Force reprocessing"
    )
    mat_all_parser.add_argument(
        "--refresh",
        action="store_true",
        help="Refresh raw files against remote metadata",
    )
    mat_all_parser.add_argument(
        "--chunksize",
        type=int,
        default=100_000,
        help="Rows per chunk for parquet writing",
    )

    query_parser = subparsers.add_parser(
        "query",
        help="Query rows from materialized parquet",
    )
    query_parser.add_argument("dataset_id", help="Dataset ID")
    query_parser.add_argument(
        "--columns",
        default=None,
        help="Comma-separated columns to project (default: all)",
    )
    query_parser.add_argument(
        "--filters",
        default=None,
        help=(
            "JSON object filters (for example "
            '\'{"label":{"eq":1},"split":["train","valid"]}\')'
        ),
    )
    query_parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum rows to return (1-5000)",
    )
    query_parser.add_argument(
        "--materialize-if-missing",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Materialize parquet if missing (default: true)",
    )
    query_parser.add_argument(
        "--force-materialize",
        action="store_true",
        help="Force re-materialization when materializing for query",
    )
    query_parser.add_argument(
        "--refresh",
        action="store_true",
        help="Refresh raw source metadata/content when materializing",
    )
    query_parser.add_argument(
        "--chunksize",
        type=int,
        default=100_000,
        help="Rows per chunk for parquet writing during materialization",
    )
    query_parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=120.0,
        help="Download/materialization timeout in seconds",
    )

    validate_parser = subparsers.add_parser(
        "validate-sources",
        help="Validate dataset source endpoints (file/http/api)",
    )
    validate_parser.add_argument(
        "dataset_ids",
        nargs="*",
        help="Optional dataset IDs to validate (defaults to all datasets)",
    )
    validate_parser.add_argument("--tag", default=None, help="Filter datasets by tag")
    validate_parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=20.0,
        help="Per-source probe timeout in seconds",
    )
    validate_parser.add_argument("--json", action="store_true", help="Emit JSON output")
    validate_parser.add_argument(
        "--fail-on-error",
        action="store_true",
        help="Exit with code 1 if any source probe fails",
    )

    return parser


def _build_manager(cache_root: Path | None) -> DatasetManager:
    cache = DataCache(cache_root) if cache_root else DataCache()
    return DatasetManager(cache=cache)


def _run_list(manager: DatasetManager, *, tag: str | None, as_json: bool) -> int:
    datasets = manager.list_datasets(tag=tag)
    if as_json:
        payload = [
            {
                "dataset_id": dataset.dataset_id,
                "name": dataset.name,
                "description": dataset.description,
                "usage_notes": list(dataset.resolved_usage_notes()),
                "category": dataset.category,
                "source_type": "api" if dataset.api is not None else "file",
                "source": dataset.source,
                "tags": list(dataset.tags),
                "license": dataset.license_name,
            }
            for dataset in datasets
        ]
        print(json.dumps(payload, indent=2))  # noqa: T201
        return 0

    print(f"Datasets ({len(datasets)}):")  # noqa: T201
    for dataset in datasets:
        tags = ", ".join(dataset.tags)
        source_type = "api" if dataset.api is not None else "file"
        print(  # noqa: T201
            f"- {dataset.dataset_id:<32} {source_type:<4} {dataset.category:<18} "
            f"{dataset.name} [{tags}]"
        )
        print(f"  desc: {dataset.description}")  # noqa: T201
        print(f"  use:  {dataset.resolved_usage_notes()[0]}")  # noqa: T201
    return 0


def _run_fetch(
    manager: DatasetManager,
    *,
    dataset_id: str,
    force: bool,
    refresh: bool,
    timeout_seconds: float,
) -> int:
    dataset = manager.catalog.get(dataset_id)
    result = manager.fetch(
        dataset_id,
        force=force,
        refresh=refresh,
        timeout_seconds=timeout_seconds,
    )
    print(  # noqa: T201
        json.dumps(
            {
                "dataset_id": result.dataset_id,
                "version": result.version,
                "raw_path": str(result.raw_path),
                "metadata_path": str(result.metadata_path),
                "source_url": result.source_url,
                "cache_hit": result.cache_hit,
                "refreshed": result.refreshed,
                "bytes_downloaded": result.bytes_downloaded,
                "sha256": result.sha256,
                "dataset": dataset.metadata_snapshot(),
            },
            indent=2,
        )
    )
    return 0


def _run_materialize(
    manager: DatasetManager,
    *,
    dataset_id: str,
    force: bool,
    refresh: bool,
    chunksize: int,
    timeout_seconds: float,
) -> int:
    dataset = manager.catalog.get(dataset_id)
    result = manager.materialize(
        dataset_id,
        force=force,
        refresh=refresh,
        chunksize=chunksize,
        timeout_seconds=timeout_seconds,
    )
    print(  # noqa: T201
        json.dumps(
            {
                "dataset_id": result.dataset_id,
                "version": result.version,
                "parquet_dir": str(result.parquet_dir),
                "manifest_path": str(result.manifest_path),
                "parts": [str(path) for path in result.parts],
                "row_count": result.row_count,
                "cache_hit": result.cache_hit,
                "source_sha256": result.source_sha256,
                "dataset": dataset.metadata_snapshot(),
            },
            indent=2,
        )
    )
    return 0


def _run_materialize_all(
    manager: DatasetManager,
    *,
    tag: str | None,
    force: bool,
    refresh: bool,
    chunksize: int,
) -> int:
    dataset_ids = [dataset.dataset_id for dataset in manager.list_datasets(tag=tag)]
    results = manager.materialize_many(
        dataset_ids,
        force=force,
        refresh=refresh,
        chunksize=chunksize,
    )
    payload = [
        {
            "dataset_id": result.dataset_id,
            "parquet_dir": str(result.parquet_dir),
            "row_count": result.row_count,
            "cache_hit": result.cache_hit,
        }
        for result in results
    ]
    print(json.dumps(payload, indent=2))  # noqa: T201
    return 0


def _parse_query_columns(raw_columns: str | None) -> list[str] | None:
    if raw_columns is None:
        return None
    seen: set[str] = set()
    columns: list[str] = []
    for raw_value in raw_columns.split(","):
        column = raw_value.strip()
        if not column or column in seen:
            continue
        seen.add(column)
        columns.append(column)
    if not columns:
        raise ValueError("columns must contain at least one non-empty name.")
    return columns


def _parse_query_filters(raw_filters: str | None) -> dict[str, Any]:
    if raw_filters is None or not raw_filters.strip():
        return {}
    try:
        parsed = json.loads(raw_filters)
    except json.JSONDecodeError as exc:
        raise ValueError("filters must be a valid JSON object.") from exc
    if not isinstance(parsed, Mapping):
        raise ValueError("filters must be a JSON object.")
    return {str(key): value for key, value in parsed.items()}


def _apply_query_filters(frame: Any, filters: Mapping[str, Any]) -> Any:
    if not filters:
        return frame

    filtered = frame
    for column, condition in filters.items():
        if column not in filtered.columns:
            raise ValueError(f"Unknown filter column '{column}'.")
        series = filtered[column]

        if isinstance(condition, Mapping):
            for op, raw_value in condition.items():
                op_name = str(op).strip().lower()
                if op_name == "eq":
                    filtered = filtered[series == raw_value]
                elif op_name == "ne":
                    filtered = filtered[series != raw_value]
                elif op_name == "gt":
                    filtered = filtered[series > raw_value]
                elif op_name in {"gte", "ge"}:
                    filtered = filtered[series >= raw_value]
                elif op_name == "lt":
                    filtered = filtered[series < raw_value]
                elif op_name in {"lte", "le"}:
                    filtered = filtered[series <= raw_value]
                elif op_name == "in":
                    if not isinstance(raw_value, (list, tuple, set)):
                        raise ValueError(f"filters.{column}.in must be an array value.")
                    filtered = filtered[series.isin(list(raw_value))]
                elif op_name == "contains":
                    pattern = str(raw_value)
                    filtered = filtered[
                        series.astype(str).str.contains(
                            pattern,
                            case=False,
                            na=False,
                            regex=False,
                        )
                    ]
                else:
                    raise ValueError(
                        f"Unsupported filter operation '{op_name}' for column '{column}'."
                    )
                series = filtered[column]
            continue

        if isinstance(condition, (list, tuple, set)):
            filtered = filtered[series.isin(list(condition))]
        else:
            filtered = filtered[series == condition]

    return filtered


def _run_query(
    manager: DatasetManager,
    *,
    dataset_id: str,
    columns: str | None,
    filters: str | None,
    limit: int,
    materialize_if_missing: bool,
    force_materialize: bool,
    refresh: bool,
    chunksize: int,
    timeout_seconds: float,
) -> int:
    if limit < 1:
        raise ValueError("limit must be >= 1.")
    if limit > 5000:
        raise ValueError("limit must be <= 5000.")
    if chunksize < 1:
        raise ValueError("chunksize must be >= 1.")
    if timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be > 0.")

    dataset_key = str(dataset_id).strip()
    if not dataset_key:
        raise ValueError("dataset_id is required.")

    query_columns = _parse_query_columns(columns)
    query_filters = _parse_query_filters(filters)

    manifest: dict[str, Any] = {}
    manifest_path_text: str | None = None
    if materialize_if_missing:
        materialized = manager.materialize(
            dataset_key,
            force=force_materialize,
            refresh=refresh,
            chunksize=chunksize,
            timeout_seconds=timeout_seconds,
        )
        parts = list(materialized.parts)
        manifest_path_text = str(materialized.manifest_path)
        manifest_raw = manager.cache.read_json(materialized.manifest_path)
        if isinstance(manifest_raw, dict):
            manifest = dict(manifest_raw)
        dataset_meta = manager.catalog.get(dataset_key).metadata_snapshot()
    else:
        dataset = manager.catalog.get(dataset_key)
        dataset_meta = dataset.metadata_snapshot()
        parquet_dir = manager.cache.parquet_dir(dataset)
        manifest_path = manager.cache.parquet_manifest(dataset)
        manifest_raw = manager.cache.read_json(manifest_path)
        if not isinstance(manifest_raw, dict):
            raise ValueError(
                f"Dataset '{dataset_key}' has no parquet manifest. Set materialize_if_missing=true."
            )
        manifest_path_text = str(manifest_path)
        manifest = dict(manifest_raw)
        parts_raw = manifest.get("parts")
        if not isinstance(parts_raw, list) or not parts_raw:
            raise ValueError(f"Dataset '{dataset_key}' parquet manifest has no parts.")
        parts = [parquet_dir.joinpath(str(name)) for name in parts_raw]
        if not all(path.exists() for path in parts):
            raise ValueError(
                "Dataset "
                f"'{dataset_key}' parquet parts are missing. "
                "Re-materialize with force_materialize=true."
            )

    import pandas as pd

    rows: list[dict[str, Any]] = []
    scanned_rows = 0
    scanned_parts = 0
    for part in parts:
        frame = pd.read_parquet(part, columns=query_columns)
        scanned_parts += 1
        scanned_rows += int(len(frame))

        filtered = _apply_query_filters(frame, query_filters)
        if filtered.empty:
            continue
        if len(rows) >= limit:
            break
        remaining = int(limit) - len(rows)
        batch = filtered.head(remaining)
        rows.extend(batch.to_dict(orient="records"))
        if len(rows) >= limit:
            break

    row_count_estimate = manifest.get("row_count")
    row_count: int | None = None
    if isinstance(row_count_estimate, int | float | str):
        try:
            row_count = int(row_count_estimate)
        except (TypeError, ValueError):
            row_count = None

    payload = {
        "dataset_id": dataset_key,
        "columns": query_columns,
        "filters": query_filters,
        "limit": int(limit),
        "returned_rows": len(rows),
        "scanned_rows": scanned_rows,
        "scanned_parts": scanned_parts,
        "row_count_estimate": row_count,
        "rows": rows,
        "dataset": dataset_meta,
        "cache_root": str(manager.cache.root),
        "manifest_path": manifest_path_text,
    }
    print(json.dumps(payload, indent=2))  # noqa: T201
    return 0


def _run_validate_sources(
    manager: DatasetManager,
    *,
    dataset_ids: list[str],
    tag: str | None,
    timeout_seconds: float,
    as_json: bool,
    fail_on_error: bool,
) -> int:
    results = manager.validate_sources(
        dataset_ids=dataset_ids or None,
        tag=tag,
        timeout_seconds=timeout_seconds,
    )

    failures = [result for result in results if not result.ok]
    if as_json:
        payload = {
            "summary": {
                "checked": len(results),
                "ok": len(results) - len(failures),
                "failed": len(failures),
            },
            "results": [
                {
                    "dataset_id": result.dataset_id,
                    "source_type": result.source_type,
                    "source": result.source,
                    "ok": result.ok,
                    "status_code": result.status_code,
                    "latency_ms": round(result.latency_ms, 3),
                    "error": result.error,
                    "details": result.details,
                }
                for result in results
            ],
        }
        print(json.dumps(payload, indent=2))  # noqa: T201
    else:
        print(  # noqa: T201
            f"Source validation: checked={len(results)} ok={len(results) - len(failures)} "
            f"failed={len(failures)}"
        )
        for result in results:
            status = "OK " if result.ok else "ERR"
            code = str(result.status_code) if result.status_code is not None else "-"
            line = (
                f"{status} {result.dataset_id:<32} {result.source_type:<4} "
                f"code={code:<4} latency_ms={result.latency_ms:8.2f} {result.source}"
            )
            if result.error:
                line = f"{line} :: {result.error}"
            print(line)  # noqa: T201

    if fail_on_error and failures:
        return 1
    return 0


def main() -> int:
    """CLI entrypoint."""
    parser = build_parser()
    args = parser.parse_args()
    manager = _build_manager(args.cache_root)

    if args.command == "list":
        return _run_list(manager, tag=args.tag, as_json=args.json)
    if args.command == "fetch":
        return _run_fetch(
            manager,
            dataset_id=args.dataset_id,
            force=args.force,
            refresh=args.refresh,
            timeout_seconds=args.timeout_seconds,
        )
    if args.command == "materialize":
        return _run_materialize(
            manager,
            dataset_id=args.dataset_id,
            force=args.force,
            refresh=args.refresh,
            chunksize=args.chunksize,
            timeout_seconds=args.timeout_seconds,
        )
    if args.command == "materialize-all":
        return _run_materialize_all(
            manager,
            tag=args.tag,
            force=args.force,
            refresh=args.refresh,
            chunksize=args.chunksize,
        )
    if args.command == "query":
        return _run_query(
            manager,
            dataset_id=args.dataset_id,
            columns=args.columns,
            filters=args.filters,
            limit=args.limit,
            materialize_if_missing=args.materialize_if_missing,
            force_materialize=args.force_materialize,
            refresh=args.refresh,
            chunksize=args.chunksize,
            timeout_seconds=args.timeout_seconds,
        )
    if args.command == "validate-sources":
        return _run_validate_sources(
            manager,
            dataset_ids=args.dataset_ids,
            tag=args.tag,
            timeout_seconds=args.timeout_seconds,
            as_json=args.json,
            fail_on_error=args.fail_on_error,
        )

    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
