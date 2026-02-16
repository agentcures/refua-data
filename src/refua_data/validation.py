"""Dataset source validation utilities."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal
from urllib.parse import unquote, urlparse

import requests

from .models import ApiDatasetConfig, DatasetDefinition

_DEFAULT_USER_AGENT = "refua-data/0.6.0"


@dataclass(frozen=True, slots=True)
class SourceValidationResult:
    """Result of probing a single dataset source endpoint."""

    dataset_id: str
    source_type: Literal["file", "http", "api", "unknown"]
    source: str
    ok: bool
    status_code: int | None
    latency_ms: float
    error: str | None = None
    details: dict[str, Any] = field(default_factory=dict)


def validate_dataset_sources(
    dataset: DatasetDefinition,
    *,
    timeout_seconds: float,
) -> list[SourceValidationResult]:
    """Validate configured sources for a dataset.

    For datasets with multiple URLs in `fallback` mode, probes are attempted in
    order and the dataset is considered healthy once one source succeeds.
    For datasets in `concat` mode, every configured source must be reachable.
    """
    if dataset.api is not None:
        return [_probe_api(dataset, dataset.api, timeout_seconds=timeout_seconds)]

    if not dataset.urls:
        return [
            SourceValidationResult(
                dataset_id=dataset.dataset_id,
                source_type="unknown",
                source="",
                ok=False,
                status_code=None,
                latency_ms=0.0,
                error="Dataset has no configured URLs.",
            )
        ]

    if dataset.url_mode == "concat":
        concat_attempts = [
            _probe_url(dataset, url, timeout_seconds=timeout_seconds) for url in dataset.urls
        ]
        return [_collapse_concat_attempts(dataset, concat_attempts)]

    attempts: list[SourceValidationResult] = []
    for url in dataset.urls:
        result = _probe_url(dataset, url, timeout_seconds=timeout_seconds)
        if result.ok:
            return [_with_fallback_failures(result, failures=attempts)]
        attempts.append(result)

    return [_collapse_failed_attempts(dataset, attempts)]


def _with_fallback_failures(
    result: SourceValidationResult,
    *,
    failures: list[SourceValidationResult],
) -> SourceValidationResult:
    if not failures:
        return result

    details = dict(result.details)
    details["fallback_failures"] = [_attempt_details(attempt) for attempt in failures]
    return SourceValidationResult(
        dataset_id=result.dataset_id,
        source_type=result.source_type,
        source=result.source,
        ok=result.ok,
        status_code=result.status_code,
        latency_ms=result.latency_ms,
        error=result.error,
        details=details,
    )


def _collapse_failed_attempts(
    dataset: DatasetDefinition,
    attempts: list[SourceValidationResult],
) -> SourceValidationResult:
    primary = attempts[0]
    total_latency_ms = sum(attempt.latency_ms for attempt in attempts)
    details = {
        "attempts": [_attempt_details(attempt) for attempt in attempts],
        "attempt_count": len(attempts),
    }
    return SourceValidationResult(
        dataset_id=dataset.dataset_id,
        source_type=primary.source_type,
        source=primary.source,
        ok=False,
        status_code=primary.status_code,
        latency_ms=total_latency_ms,
        error=f"All configured sources failed ({len(attempts)} attempts).",
        details=details,
    )


def _collapse_concat_attempts(
    dataset: DatasetDefinition,
    attempts: list[SourceValidationResult],
) -> SourceValidationResult:
    primary = attempts[0]
    total_latency_ms = sum(attempt.latency_ms for attempt in attempts)
    failed = [attempt for attempt in attempts if not attempt.ok]

    if not failed:
        details: dict[str, Any] = {
            "source_count": len(attempts),
            "url_mode": dataset.url_mode,
        }
        return SourceValidationResult(
            dataset_id=dataset.dataset_id,
            source_type=primary.source_type,
            source=primary.source,
            ok=True,
            status_code=primary.status_code,
            latency_ms=total_latency_ms,
            error=None,
            details=details,
        )

    error_details: dict[str, Any] = {
        "source_count": len(attempts),
        "failed_count": len(failed),
        "url_mode": dataset.url_mode,
        "failed_sources": [_attempt_details(attempt) for attempt in failed[:10]],
    }
    return SourceValidationResult(
        dataset_id=dataset.dataset_id,
        source_type=primary.source_type,
        source=primary.source,
        ok=False,
        status_code=primary.status_code,
        latency_ms=total_latency_ms,
        error=f"{len(failed)} of {len(attempts)} required sources failed.",
        details=error_details,
    )


def _attempt_details(result: SourceValidationResult) -> dict[str, Any]:
    return {
        "source": result.source,
        "source_type": result.source_type,
        "ok": result.ok,
        "status_code": result.status_code,
        "latency_ms": round(result.latency_ms, 3),
        "error": result.error,
    }


def _probe_url(
    dataset: DatasetDefinition,
    url: str,
    *,
    timeout_seconds: float,
) -> SourceValidationResult:
    parsed = urlparse(url)
    scheme = parsed.scheme.lower()

    if scheme in {"", "file"}:
        return _probe_file_url(dataset, url)
    if scheme in {"http", "https"}:
        return _probe_http_url(dataset, url, timeout_seconds=timeout_seconds)

    return SourceValidationResult(
        dataset_id=dataset.dataset_id,
        source_type="unknown",
        source=url,
        ok=False,
        status_code=None,
        latency_ms=0.0,
        error=f"Unsupported URL scheme: {scheme}",
    )


def _probe_file_url(dataset: DatasetDefinition, url: str) -> SourceValidationResult:
    started = time.perf_counter()
    parsed = urlparse(url)
    source_path = Path(unquote(parsed.path)).expanduser().resolve()
    ok = source_path.exists()
    latency_ms = (time.perf_counter() - started) * 1000.0

    details: dict[str, Any] = {}
    if ok:
        stat = source_path.stat()
        details["size_bytes"] = int(stat.st_size)

    return SourceValidationResult(
        dataset_id=dataset.dataset_id,
        source_type="file",
        source=url,
        ok=ok,
        status_code=200 if ok else 404,
        latency_ms=latency_ms,
        error=None if ok else f"File not found: {source_path}",
        details=details,
    )


def _probe_http_url(
    dataset: DatasetDefinition,
    url: str,
    *,
    timeout_seconds: float,
) -> SourceValidationResult:
    started = time.perf_counter()
    headers = {
        "User-Agent": _DEFAULT_USER_AGENT,
        "Range": "bytes=0-0",
    }

    try:
        with requests.get(
            url,
            timeout=timeout_seconds,
            headers=headers,
            stream=True,
            allow_redirects=True,
        ) as response:
            ok = response.status_code < 400
            latency_ms = (time.perf_counter() - started) * 1000.0
            return SourceValidationResult(
                dataset_id=dataset.dataset_id,
                source_type="http",
                source=url,
                ok=ok,
                status_code=response.status_code,
                latency_ms=latency_ms,
                error=None if ok else f"HTTP {response.status_code}",
                details={
                    "content_type": response.headers.get("Content-Type"),
                    "content_length": response.headers.get("Content-Length"),
                },
            )
    except Exception as exc:  # noqa: BLE001
        latency_ms = (time.perf_counter() - started) * 1000.0
        return SourceValidationResult(
            dataset_id=dataset.dataset_id,
            source_type="http",
            source=url,
            ok=False,
            status_code=None,
            latency_ms=latency_ms,
            error=str(exc),
        )


def _probe_api(
    dataset: DatasetDefinition,
    api: ApiDatasetConfig,
    *,
    timeout_seconds: float,
) -> SourceValidationResult:
    started = time.perf_counter()
    params = dict(api.params)
    if api.page_size_param is not None:
        params.setdefault(api.page_size_param, 1)
    if api.pagination == "chembl":
        params.setdefault("limit", 1)
        params.setdefault("offset", 0)

    headers = {"User-Agent": _DEFAULT_USER_AGENT, **api.headers}

    try:
        response = requests.get(
            api.endpoint,
            params=params,
            timeout=timeout_seconds,
            headers=headers,
        )
        response.raise_for_status()
        payload = response.json()
        items = _extract_items(payload, api.items_path)
        latency_ms = (time.perf_counter() - started) * 1000.0

        return SourceValidationResult(
            dataset_id=dataset.dataset_id,
            source_type="api",
            source=api.endpoint,
            ok=True,
            status_code=response.status_code,
            latency_ms=latency_ms,
            error=None,
            details={
                "items_path": api.items_path,
                "sample_items": len(items),
                "pagination": api.pagination,
            },
        )
    except Exception as exc:  # noqa: BLE001
        latency_ms = (time.perf_counter() - started) * 1000.0
        return SourceValidationResult(
            dataset_id=dataset.dataset_id,
            source_type="api",
            source=api.endpoint,
            ok=False,
            status_code=None,
            latency_ms=latency_ms,
            error=str(exc),
            details={
                "items_path": api.items_path,
                "pagination": api.pagination,
            },
        )


def _extract_items(payload: Any, items_path: str) -> list[Any]:
    if items_path == "":
        if isinstance(payload, list):
            return payload
        raise ValueError("API payload must be a list when items_path is empty.")

    value: Any = payload
    for segment in items_path.split("."):
        if not isinstance(value, dict):
            raise ValueError(
                f"Cannot resolve items_path '{items_path}'; segment '{segment}' is not a mapping."
            )
        value = value.get(segment)

    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError(f"items_path '{items_path}' did not resolve to a list.")
    return value
