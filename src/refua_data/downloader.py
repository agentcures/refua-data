"""Download utilities with intelligent caching and metadata refresh."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urljoin, urlparse

import requests

from .cache import CacheBackend, sha256_file
from .models import ApiDatasetConfig, DatasetDefinition, FetchResult

_DEFAULT_TIMEOUT = 120.0
_DEFAULT_USER_AGENT = "refua-data/0.7.2"
_CHUNK_SIZE = 4 * 1024 * 1024
_MAX_DOWNLOAD_WORKERS = 8


def _utcnow_iso() -> str:
    return datetime.now(UTC).isoformat()


def _build_session() -> requests.Session:
    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=_MAX_DOWNLOAD_WORKERS,
        pool_maxsize=_MAX_DOWNLOAD_WORKERS,
    )
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def _download_worker_count(url_count: int) -> int:
    return max(1, min(_MAX_DOWNLOAD_WORKERS, url_count))


def _write_bytes_to_path(
    *,
    dest_path: Path,
    chunks: Any,
) -> tuple[int, str]:
    digest = hashlib.sha256()
    bytes_written = 0
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with dest_path.open("wb") as handle:
            for chunk in chunks:
                if not chunk:
                    continue
                handle.write(chunk)
                digest.update(chunk)
                bytes_written += len(chunk)
    except Exception:
        dest_path.unlink(missing_ok=True)
        raise
    return bytes_written, digest.hexdigest()


def _copy_file_to_path(source_path: Path, dest_path: Path) -> tuple[int, str]:
    source_path = source_path.expanduser().resolve()
    with source_path.open("rb") as handle:
        bytes_written, checksum = _write_bytes_to_path(
            dest_path=dest_path,
            chunks=iter(lambda: handle.read(_CHUNK_SIZE), b""),
        )
    shutil.copystat(source_path, dest_path)
    return bytes_written, checksum


def _conditional_headers(meta: dict[str, Any]) -> dict[str, str]:
    headers: dict[str, str] = {}
    etag = meta.get("etag") or meta.get("first_page_etag")
    if isinstance(etag, str) and etag:
        headers["If-None-Match"] = etag
    last_modified = meta.get("last_modified") or meta.get("first_page_last_modified")
    if isinstance(last_modified, str) and last_modified:
        headers["If-Modified-Since"] = last_modified
    return headers


def _with_dataset_metadata(
    meta: dict[str, Any], dataset: DatasetDefinition
) -> dict[str, Any]:
    merged = dict(meta)
    merged["dataset"] = dataset.metadata_snapshot()
    return merged


def _write_raw_metadata(
    cache: CacheBackend,
    meta_path: Path,
    *,
    dataset: DatasetDefinition,
    meta: dict[str, Any],
) -> None:
    cache.write_json(meta_path, _with_dataset_metadata(meta, dataset))


def _ensure_sha256(
    raw_path: Path,
    meta: dict[str, Any],
    cache: CacheBackend,
    meta_path: Path,
    *,
    dataset: DatasetDefinition,
) -> str:
    snapshot = dataset.metadata_snapshot()
    checksum = meta.get("sha256")
    if isinstance(checksum, str) and checksum:
        if meta.get("dataset") != snapshot:
            updated_meta = dict(meta)
            updated_meta["dataset"] = snapshot
            updated_meta["observed_at"] = _utcnow_iso()
            _write_raw_metadata(cache, meta_path, dataset=dataset, meta=updated_meta)
        return checksum
    checksum = sha256_file(raw_path)
    updated_meta = dict(meta)
    updated_meta["sha256"] = checksum
    updated_meta["observed_at"] = _utcnow_iso()
    _write_raw_metadata(cache, meta_path, dataset=dataset, meta=updated_meta)
    return checksum


def _default_source_url(
    dataset: DatasetDefinition, existing_meta: dict[str, Any]
) -> str:
    source_url = existing_meta.get("source_url")
    if isinstance(source_url, str) and source_url:
        return source_url
    if dataset.api is not None:
        return dataset.api.endpoint
    if dataset.urls:
        return dataset.urls[0]
    return ""


def fetch_dataset(
    dataset: DatasetDefinition,
    *,
    cache: CacheBackend,
    force: bool = False,
    refresh: bool = False,
    timeout_seconds: float = _DEFAULT_TIMEOUT,
) -> FetchResult:
    """Fetch a dataset using local cache and optional conditional refresh."""
    cache.ensure()

    raw_path = cache.raw_file(dataset)
    meta_path = cache.raw_meta(dataset)
    existing_meta = cache.read_json(meta_path) or {}

    if dataset.api is None and raw_path.exists() and not force and not refresh:
        checksum = _ensure_sha256(
            raw_path,
            existing_meta,
            cache,
            meta_path,
            dataset=dataset,
        )
        return FetchResult(
            dataset_id=dataset.dataset_id,
            version=dataset.version,
            raw_path=raw_path,
            metadata_path=meta_path,
            source_url=_default_source_url(dataset, existing_meta),
            cache_hit=True,
            refreshed=False,
            bytes_downloaded=0,
            sha256=checksum,
        )

    try:
        with _build_session() as session:
            if dataset.api is not None:
                return _fetch_api_dataset(
                    dataset=dataset,
                    cache=cache,
                    raw_path=raw_path,
                    meta_path=meta_path,
                    existing_meta=existing_meta,
                    force=force,
                    refresh=refresh,
                    timeout_seconds=timeout_seconds,
                    session=session,
                )

            if not dataset.urls:
                raise ValueError(
                    f"Dataset '{dataset.dataset_id}' has no configured URL sources."
                )

            if dataset.url_mode == "concat":
                return _fetch_concat_urls(
                    dataset=dataset,
                    cache=cache,
                    raw_path=raw_path,
                    meta_path=meta_path,
                    refresh=refresh,
                    timeout_seconds=timeout_seconds,
                )

            if dataset.url_mode == "bundle":
                return _fetch_bundle_urls(
                    dataset=dataset,
                    cache=cache,
                    raw_path=raw_path,
                    meta_path=meta_path,
                    refresh=refresh,
                    timeout_seconds=timeout_seconds,
                )

            errors: list[str] = []
            for url in dataset.urls:
                try:
                    return _fetch_from_url(
                        dataset=dataset,
                        cache=cache,
                        raw_path=raw_path,
                        meta_path=meta_path,
                        existing_meta=existing_meta,
                        url=url,
                        force=force,
                        refresh=refresh,
                        timeout_seconds=timeout_seconds,
                        session=session,
                    )
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"{url}: {exc}")

            details = "\n".join(errors)
            raise RuntimeError(
                f"Failed to download dataset '{dataset.dataset_id}'.\n{details}"
            )
    except Exception as exc:
        if raw_path.exists() and not force and not refresh:
            checksum = _ensure_sha256(
                raw_path,
                existing_meta,
                cache,
                meta_path,
                dataset=dataset,
            )
            return FetchResult(
                dataset_id=dataset.dataset_id,
                version=dataset.version,
                raw_path=raw_path,
                metadata_path=meta_path,
                source_url=_default_source_url(dataset, existing_meta),
                cache_hit=True,
                refreshed=refresh,
                bytes_downloaded=0,
                sha256=checksum,
            )
        raise RuntimeError(
            f"Failed to fetch dataset '{dataset.dataset_id}': {exc}"
        ) from exc


def _fetch_from_url(
    *,
    dataset: DatasetDefinition,
    cache: CacheBackend,
    raw_path: Path,
    meta_path: Path,
    existing_meta: dict[str, Any],
    url: str,
    force: bool,
    refresh: bool,
    timeout_seconds: float,
    session: requests.Session,
) -> FetchResult:
    parsed = urlparse(url)
    scheme = parsed.scheme.lower()

    if scheme in {"", "file"}:
        return _fetch_file_url(
            dataset=dataset,
            cache=cache,
            raw_path=raw_path,
            meta_path=meta_path,
            existing_meta=existing_meta,
            url=url,
            force=force,
            refresh=refresh,
        )

    if scheme in {"http", "https"}:
        return _fetch_http_url(
            dataset=dataset,
            cache=cache,
            raw_path=raw_path,
            meta_path=meta_path,
            existing_meta=existing_meta,
            url=url,
            force=force,
            refresh=refresh,
            timeout_seconds=timeout_seconds,
            session=session,
        )

    raise ValueError(f"Unsupported URL scheme for {url}")


def _download_url_to_path(
    *,
    url: str,
    dest_path: Path,
    timeout_seconds: float,
    session: requests.Session | None = None,
) -> dict[str, Any]:
    parsed = urlparse(url)
    scheme = parsed.scheme.lower()
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    if scheme in {"", "file"}:
        source_path = Path(unquote(parsed.path)).expanduser().resolve()
        if not source_path.exists():
            raise FileNotFoundError(f"Local source path does not exist: {source_path}")
        source_stat = source_path.stat()
        source_size, checksum = _copy_file_to_path(source_path, dest_path)
        return {
            "source_url": url,
            "source_type": "file",
            "status_code": 200,
            "source_size": source_size,
            "source_mtime_ns": int(source_stat.st_mtime_ns),
            "bytes_downloaded": source_size,
            "sha256": checksum,
        }

    if scheme in {"http", "https"}:
        headers = {"User-Agent": _DEFAULT_USER_AGENT}
        active_session = session
        owns_session = active_session is None
        if active_session is None:
            active_session = _build_session()
        try:
            with active_session.get(
                url,
                stream=True,
                timeout=timeout_seconds,
                headers=headers,
            ) as response:
                response.raise_for_status()
                bytes_downloaded, checksum = _write_bytes_to_path(
                    dest_path=dest_path,
                    chunks=response.iter_content(chunk_size=_CHUNK_SIZE),
                )
                return {
                    "source_url": url,
                    "source_type": "http",
                    "status_code": response.status_code,
                    "etag": response.headers.get("ETag"),
                    "last_modified": response.headers.get("Last-Modified"),
                    "content_length": response.headers.get("Content-Length"),
                    "bytes_downloaded": bytes_downloaded,
                    "sha256": checksum,
                }
        except Exception:
            dest_path.unlink(missing_ok=True)
            raise
        finally:
            if owns_session and active_session is not None:
                active_session.close()

    raise ValueError(f"Unsupported URL scheme for {url}")


def _remove_path(path: Path) -> None:
    if path.is_dir():
        shutil.rmtree(path)
        return
    path.unlink(missing_ok=True)


def _fetch_concat_urls(
    *,
    dataset: DatasetDefinition,
    cache: CacheBackend,
    raw_path: Path,
    meta_path: Path,
    refresh: bool,
    timeout_seconds: float,
) -> FetchResult:
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = raw_path.with_suffix(raw_path.suffix + ".tmp")
    part_paths: list[Path] = []

    first_header: bytes | None = None
    dedupe_header = dataset.file_format in {"csv", "tsv"}
    source_details: list[dict[str, Any]] = [{} for _ in dataset.urls]

    try:
        futures: dict[Future[dict[str, Any]], tuple[int, str, Path]] = {}
        with ThreadPoolExecutor(
            max_workers=_download_worker_count(len(dataset.urls))
        ) as executor:
            for index, url in enumerate(dataset.urls):
                part_path = raw_path.with_suffix(
                    f"{raw_path.suffix}.part-{index:04d}.tmp"
                )
                part_paths.append(part_path)
                future = executor.submit(
                    _download_url_to_path,
                    url=url,
                    dest_path=part_path,
                    timeout_seconds=timeout_seconds,
                    session=None,
                )
                futures[future] = (index, url, part_path)

            errors: list[str] = []
            for future, (index, url, _) in futures.items():
                try:
                    source_details[index] = future.result()
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"{url}: {exc}")

            if errors:
                details = "\n".join(errors)
                raise RuntimeError(
                    f"Failed to download dataset '{dataset.dataset_id}'.\n{details}"
                )

        bytes_downloaded = 0
        digest = hashlib.sha256()
        with tmp_path.open("wb") as merged:
            for index, part_path in enumerate(part_paths):
                detail = source_details[index]
                bytes_downloaded += int(detail.get("bytes_downloaded", 0))

                with part_path.open("rb") as source_handle:
                    if dedupe_header:
                        first_line = source_handle.readline()
                        if index == 0:
                            first_header = first_line
                            if first_line:
                                merged.write(first_line)
                                digest.update(first_line)
                        elif first_header is None or first_line != first_header:
                            if first_line:
                                merged.write(first_line)
                                digest.update(first_line)

                    while True:
                        chunk = source_handle.read(_CHUNK_SIZE)
                        if not chunk:
                            break
                        merged.write(chunk)
                        digest.update(chunk)

                part_path.unlink(missing_ok=True)
            part_paths.clear()

        os.replace(tmp_path, raw_path)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        for part_path in part_paths:
            part_path.unlink(missing_ok=True)
        raise

    checksum = digest.hexdigest()
    source_url = dataset.urls[0]
    meta = {
        "dataset_id": dataset.dataset_id,
        "version": dataset.version,
        "source_type": "multi_url",
        "source_url": source_url,
        "source_urls": list(dataset.urls),
        "url_mode": dataset.url_mode,
        "source_count": len(dataset.urls),
        "fetched_at": _utcnow_iso(),
        "refreshed": refresh,
        "bytes_downloaded": bytes_downloaded,
        "sources": source_details,
        "sha256": checksum,
    }
    _write_raw_metadata(cache, meta_path, dataset=dataset, meta=meta)

    return FetchResult(
        dataset_id=dataset.dataset_id,
        version=dataset.version,
        raw_path=raw_path,
        metadata_path=meta_path,
        source_url=source_url,
        cache_hit=False,
        refreshed=refresh,
        bytes_downloaded=bytes_downloaded,
        sha256=checksum,
    )


def _fetch_bundle_urls(
    *,
    dataset: DatasetDefinition,
    cache: CacheBackend,
    raw_path: Path,
    meta_path: Path,
    refresh: bool,
    timeout_seconds: float,
) -> FetchResult:
    raw_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = raw_path.with_name(f"{raw_path.name}.tmp")
    if tmp_path.exists():
        _remove_path(tmp_path)
    tmp_path.mkdir(parents=True, exist_ok=True)

    bytes_downloaded = 0
    source_details: list[dict[str, Any]] = [{} for _ in dataset.urls]

    try:
        futures: dict[Future[dict[str, Any]], tuple[int, str]] = {}
        with ThreadPoolExecutor(
            max_workers=_download_worker_count(len(dataset.urls))
        ) as executor:
            for index, url in enumerate(dataset.urls):
                candidate_name = Path(urlparse(url).path).name
                filename = candidate_name or f"part-{index:05d}"
                future = executor.submit(
                    _download_url_to_path,
                    url=url,
                    dest_path=tmp_path / filename,
                    timeout_seconds=timeout_seconds,
                    session=None,
                )
                futures[future] = (index, url)

            errors: list[str] = []
            for future, (index, url) in futures.items():
                try:
                    detail = future.result()
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"{url}: {exc}")
                    continue
                source_details[index] = detail
                bytes_downloaded += int(detail.get("bytes_downloaded", 0))

            if errors:
                details = "\n".join(errors)
                raise RuntimeError(
                    f"Failed to download dataset '{dataset.dataset_id}'.\n{details}"
                )

        if raw_path.exists():
            _remove_path(raw_path)
        os.replace(tmp_path, raw_path)
    except Exception:
        _remove_path(tmp_path)
        raise

    checksum = sha256_file(raw_path)
    source_url = dataset.urls[0]
    meta = {
        "dataset_id": dataset.dataset_id,
        "version": dataset.version,
        "source_type": "multi_url",
        "source_url": source_url,
        "source_urls": list(dataset.urls),
        "url_mode": dataset.url_mode,
        "source_count": len(dataset.urls),
        "fetched_at": _utcnow_iso(),
        "refreshed": refresh,
        "bytes_downloaded": bytes_downloaded,
        "sources": source_details,
        "sha256": checksum,
    }
    _write_raw_metadata(cache, meta_path, dataset=dataset, meta=meta)

    return FetchResult(
        dataset_id=dataset.dataset_id,
        version=dataset.version,
        raw_path=raw_path,
        metadata_path=meta_path,
        source_url=source_url,
        cache_hit=False,
        refreshed=refresh,
        bytes_downloaded=bytes_downloaded,
        sha256=checksum,
    )


def _fetch_file_url(
    *,
    dataset: DatasetDefinition,
    cache: CacheBackend,
    raw_path: Path,
    meta_path: Path,
    existing_meta: dict[str, Any],
    url: str,
    force: bool,
    refresh: bool,
) -> FetchResult:
    parsed = urlparse(url)
    source_path = Path(unquote(parsed.path)).expanduser().resolve()
    if not source_path.exists():
        raise FileNotFoundError(f"Local source path does not exist: {source_path}")

    raw_path.parent.mkdir(parents=True, exist_ok=True)
    source_stat = source_path.stat()
    source_mtime_ns = int(source_stat.st_mtime_ns)
    source_size = int(source_stat.st_size)

    if raw_path.exists() and not force:
        cached_mtime_ns = int(existing_meta.get("source_mtime_ns", -1))
        cached_size = int(existing_meta.get("source_size", -1))
        if cached_mtime_ns == source_mtime_ns and cached_size == source_size:
            checksum = _ensure_sha256(
                raw_path,
                existing_meta,
                cache,
                meta_path,
                dataset=dataset,
            )
            return FetchResult(
                dataset_id=dataset.dataset_id,
                version=dataset.version,
                raw_path=raw_path,
                metadata_path=meta_path,
                source_url=url,
                cache_hit=True,
                refreshed=refresh,
                bytes_downloaded=0,
                sha256=checksum,
            )

    tmp_path = raw_path.with_suffix(raw_path.suffix + ".tmp")
    _, checksum = _copy_file_to_path(source_path, tmp_path)
    os.replace(tmp_path, raw_path)

    meta = {
        "dataset_id": dataset.dataset_id,
        "version": dataset.version,
        "source_type": "file",
        "source_url": url,
        "fetched_at": _utcnow_iso(),
        "refreshed": refresh,
        "sha256": checksum,
        "source_mtime_ns": source_mtime_ns,
        "source_size": source_size,
    }
    _write_raw_metadata(cache, meta_path, dataset=dataset, meta=meta)

    return FetchResult(
        dataset_id=dataset.dataset_id,
        version=dataset.version,
        raw_path=raw_path,
        metadata_path=meta_path,
        source_url=url,
        cache_hit=False,
        refreshed=refresh,
        bytes_downloaded=source_size,
        sha256=checksum,
    )


def _fetch_http_url(
    *,
    dataset: DatasetDefinition,
    cache: CacheBackend,
    raw_path: Path,
    meta_path: Path,
    existing_meta: dict[str, Any],
    url: str,
    force: bool,
    refresh: bool,
    timeout_seconds: float,
    session: requests.Session,
) -> FetchResult:
    headers = {"User-Agent": _DEFAULT_USER_AGENT}
    if refresh and not force:
        headers.update(_conditional_headers(existing_meta))

    with session.get(
        url, stream=True, timeout=timeout_seconds, headers=headers
    ) as response:
        if response.status_code == requests.codes.not_modified and raw_path.exists():
            checksum = _ensure_sha256(
                raw_path,
                existing_meta,
                cache,
                meta_path,
                dataset=dataset,
            )
            meta = dict(existing_meta)
            meta["source_url"] = url
            meta["refreshed_at"] = _utcnow_iso()
            _write_raw_metadata(cache, meta_path, dataset=dataset, meta=meta)
            return FetchResult(
                dataset_id=dataset.dataset_id,
                version=dataset.version,
                raw_path=raw_path,
                metadata_path=meta_path,
                source_url=url,
                cache_hit=True,
                refreshed=True,
                bytes_downloaded=0,
                sha256=checksum,
            )

        response.raise_for_status()
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = raw_path.with_suffix(raw_path.suffix + ".tmp")

        bytes_downloaded, checksum = _write_bytes_to_path(
            dest_path=tmp_path,
            chunks=response.iter_content(chunk_size=_CHUNK_SIZE),
        )

        os.replace(tmp_path, raw_path)

        meta = {
            "dataset_id": dataset.dataset_id,
            "version": dataset.version,
            "source_type": "http",
            "source_url": url,
            "fetched_at": _utcnow_iso(),
            "refreshed": refresh,
            "status_code": response.status_code,
            "etag": response.headers.get("ETag"),
            "last_modified": response.headers.get("Last-Modified"),
            "content_length": response.headers.get("Content-Length"),
            "sha256": checksum,
        }
        _write_raw_metadata(cache, meta_path, dataset=dataset, meta=meta)

        return FetchResult(
            dataset_id=dataset.dataset_id,
            version=dataset.version,
            raw_path=raw_path,
            metadata_path=meta_path,
            source_url=url,
            cache_hit=False,
            refreshed=refresh,
            bytes_downloaded=bytes_downloaded,
            sha256=checksum,
        )


def _fetch_api_dataset(
    *,
    dataset: DatasetDefinition,
    cache: CacheBackend,
    raw_path: Path,
    meta_path: Path,
    existing_meta: dict[str, Any],
    force: bool,
    refresh: bool,
    timeout_seconds: float,
    session: requests.Session,
) -> FetchResult:
    api = dataset.api
    if api is None:
        raise ValueError("API configuration is required for API dataset fetch.")

    request_signature = api.request_signature()
    if raw_path.exists() and not force and not refresh:
        existing_signature = existing_meta.get("api_request_signature")
        if (
            isinstance(existing_signature, dict)
            and existing_signature == request_signature
        ):
            checksum = _ensure_sha256(
                raw_path,
                existing_meta,
                cache,
                meta_path,
                dataset=dataset,
            )
            return FetchResult(
                dataset_id=dataset.dataset_id,
                version=dataset.version,
                raw_path=raw_path,
                metadata_path=meta_path,
                source_url=api.endpoint,
                cache_hit=True,
                refreshed=False,
                bytes_downloaded=0,
                sha256=checksum,
            )

    raw_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = raw_path.with_suffix(raw_path.suffix + ".tmp")
    base_headers = {"User-Agent": _DEFAULT_USER_AGENT, **api.headers}

    page_url = api.endpoint
    initial_params = dict(api.params)
    if api.page_size_param is not None and api.page_size is not None:
        initial_params.setdefault(api.page_size_param, api.page_size)
    if api.pagination == "chembl":
        initial_params.setdefault("limit", api.page_size or 1000)
        initial_params.setdefault("offset", 0)

    first_page_etag: str | None = None
    first_page_last_modified: str | None = None
    rows_written = 0
    pages_fetched = 0
    bytes_downloaded = 0
    digest = hashlib.sha256()

    with tmp_path.open("wb") as handle:
        next_params: dict[str, Any] | None = initial_params
        while True:
            headers = dict(base_headers)
            if pages_fetched == 0 and refresh and not force:
                headers.update(_conditional_headers(existing_meta))

            with session.get(
                page_url,
                params=next_params,
                timeout=timeout_seconds,
                headers=headers,
            ) as response:
                if (
                    response.status_code == requests.codes.not_modified
                    and raw_path.exists()
                    and pages_fetched == 0
                ):
                    checksum = _ensure_sha256(
                        raw_path,
                        existing_meta,
                        cache,
                        meta_path,
                        dataset=dataset,
                    )
                    meta = dict(existing_meta)
                    meta["source_url"] = api.endpoint
                    meta["refreshed_at"] = _utcnow_iso()
                    _write_raw_metadata(cache, meta_path, dataset=dataset, meta=meta)
                    return FetchResult(
                        dataset_id=dataset.dataset_id,
                        version=dataset.version,
                        raw_path=raw_path,
                        metadata_path=meta_path,
                        source_url=api.endpoint,
                        cache_hit=True,
                        refreshed=True,
                        bytes_downloaded=0,
                        sha256=checksum,
                    )

                response.raise_for_status()
                bytes_downloaded += len(response.content)

                if pages_fetched == 0:
                    first_page_etag = response.headers.get("ETag")
                    first_page_last_modified = response.headers.get("Last-Modified")

                payload = response.json()
                items = _extract_api_items(payload, api)

                for item in items:
                    if api.max_rows is not None and rows_written >= api.max_rows:
                        break
                    line = json.dumps(item, sort_keys=True).encode("utf-8") + b"\n"
                    handle.write(line)
                    digest.update(line)
                    rows_written += 1

                pages_fetched += 1

                if api.max_rows is not None and rows_written >= api.max_rows:
                    break
                if api.max_pages is not None and pages_fetched >= api.max_pages:
                    break

                next_url = _resolve_next_page_url(
                    api=api,
                    payload=payload,
                    current_url=response.url,
                    link_header=response.headers.get("Link"),
                )

            if not next_url:
                break

            page_url = next_url
            next_params = None

    os.replace(tmp_path, raw_path)
    checksum = digest.hexdigest()
    meta = {
        "dataset_id": dataset.dataset_id,
        "version": dataset.version,
        "source_type": "api",
        "source_url": api.endpoint,
        "fetched_at": _utcnow_iso(),
        "refreshed": refresh,
        "api_request_signature": request_signature,
        "api_rows": rows_written,
        "api_pages": pages_fetched,
        "api_pagination": api.pagination,
        "first_page_etag": first_page_etag,
        "first_page_last_modified": first_page_last_modified,
        "bytes_downloaded": bytes_downloaded,
        "sha256": checksum,
    }
    _write_raw_metadata(cache, meta_path, dataset=dataset, meta=meta)

    return FetchResult(
        dataset_id=dataset.dataset_id,
        version=dataset.version,
        raw_path=raw_path,
        metadata_path=meta_path,
        source_url=api.endpoint,
        cache_hit=False,
        refreshed=refresh,
        bytes_downloaded=bytes_downloaded,
        sha256=checksum,
    )


def _extract_api_items(payload: Any, api: ApiDatasetConfig) -> list[Any]:
    if api.items_path == "":
        if isinstance(payload, list):
            return payload
        raise ValueError("API payload must be a list when items_path is empty.")

    value: Any = payload
    for segment in api.items_path.split("."):
        if not isinstance(value, dict):
            raise ValueError(
                f"Cannot resolve API items_path '{api.items_path}'. Segment '{segment}' "
                "was not a mapping."
            )
        value = value.get(segment)

    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError(
            f"API items_path '{api.items_path}' did not resolve to a list."
        )
    return value


def _resolve_next_page_url(
    *,
    api: ApiDatasetConfig,
    payload: Any,
    current_url: str,
    link_header: str | None,
) -> str | None:
    if api.pagination == "none":
        return None

    if api.pagination == "chembl":
        raw_next = _nested_get(payload, "page_meta.next")
        if not isinstance(raw_next, str) or not raw_next:
            return None
        return urljoin(current_url, raw_next)

    if api.pagination == "link_header":
        return _parse_next_link_header(link_header)

    raise ValueError(f"Unsupported API pagination mode: {api.pagination}")


def _nested_get(payload: Any, path: str) -> Any:
    current = payload
    for segment in path.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(segment)
    return current


def _parse_next_link_header(link_header: str | None) -> str | None:
    if not link_header:
        return None

    for part in link_header.split(","):
        section = part.strip()
        if 'rel="next"' not in section:
            continue
        if not section.startswith("<") or ">" not in section:
            continue
        return section[1 : section.find(">")]
    return None
