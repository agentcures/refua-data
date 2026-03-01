"""CLI for refua-data."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

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
