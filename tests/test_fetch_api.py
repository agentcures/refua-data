from __future__ import annotations

import json
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from refua_data.cache import DataCache
from refua_data.catalog import DatasetCatalog
from refua_data.models import ApiDatasetConfig, DatasetDefinition
from refua_data.pipeline import DatasetManager


class _ApiHandler(BaseHTTPRequestHandler):
    requests_seen: list[str] = []

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        query = parse_qs(parsed.query)
        self.__class__.requests_seen.append(self.path)

        if parsed.path == "/chembl/activity.json":
            self._serve_chembl_page(query)
            return

        if parsed.path == "/uniprot/search":
            self._serve_uniprot_page(query)
            return

        self.send_response(404)
        self.end_headers()

    def _serve_chembl_page(self, query: dict[str, list[str]]) -> None:
        offset = int(query.get("offset", ["0"])[0])
        limit = int(query.get("limit", ["2"])[0])

        if offset == 0:
            payload = {
                "activities": [{"id": 1}, {"id": 2}],
                "page_meta": {
                    "next": f"/chembl/activity.json?offset={offset + limit}&limit={limit}",
                },
            }
        else:
            payload = {
                "activities": [{"id": 3}],
                "page_meta": {"next": None},
            }

        self._send_json(payload)

    def _serve_uniprot_page(self, query: dict[str, list[str]]) -> None:
        cursor = query.get("cursor", [""])[0]
        payload = {"results": [{"accession": "P00001"}, {"accession": "P00002"}]}
        link_header = None
        if cursor == "":
            host, port = self.server.server_address
            link_header = (
                f'<http://{host}:{port}/uniprot/search?cursor=next&size=2>; rel="next"'
            )
        else:
            payload = {"results": [{"accession": "P00003"}]}

        self._send_json(payload, link_header=link_header)

    def _send_json(
        self, payload: dict[str, object], *, link_header: str | None = None
    ) -> None:
        encoded = json.dumps(payload).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(encoded)))
        if link_header is not None:
            self.send_header("Link", link_header)
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, format: str, *args: object) -> None:  # noqa: A003
        return


@contextmanager
def _api_server() -> Iterator[str]:
    _ApiHandler.requests_seen = []
    server = ThreadingHTTPServer(("127.0.0.1", 0), _ApiHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        yield f"http://{host}:{port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_fetch_chembl_api_with_pagination_and_cache(tmp_path: Path) -> None:
    with _api_server() as base_url:
        dataset = DatasetDefinition(
            dataset_id="chembl_toy",
            name="Chembl Toy",
            description="Toy chembl api dataset",
            source="unit-test",
            homepage="https://example.test",
            license_name="test",
            license_url=None,
            file_format="jsonl",
            category="test",
            api=ApiDatasetConfig(
                endpoint=f"{base_url}/chembl/activity.json",
                pagination="chembl",
                items_path="activities",
                page_size_param="limit",
                page_size=2,
                max_pages=10,
                max_rows=10,
            ),
        )

        manager = DatasetManager(
            catalog=DatasetCatalog.from_entries([dataset]),
            cache=DataCache(tmp_path / "cache"),
        )

        first = manager.fetch("chembl_toy")
        request_count_after_first = len(_ApiHandler.requests_seen)
        second = manager.fetch("chembl_toy")

        assert first.cache_hit is False
        assert second.cache_hit is True
        assert request_count_after_first == 2
        assert len(_ApiHandler.requests_seen) == request_count_after_first

        lines = first.raw_path.read_text(encoding="utf-8").strip().splitlines()
        assert len(lines) == 3


def test_fetch_uniprot_api_link_header_pagination(tmp_path: Path) -> None:
    with _api_server() as base_url:
        dataset = DatasetDefinition(
            dataset_id="uniprot_toy",
            name="UniProt Toy",
            description="Toy uniprot api dataset",
            source="unit-test",
            homepage="https://example.test",
            license_name="test",
            license_url=None,
            file_format="jsonl",
            category="test",
            api=ApiDatasetConfig(
                endpoint=f"{base_url}/uniprot/search",
                pagination="link_header",
                items_path="results",
                page_size_param="size",
                page_size=2,
                max_pages=10,
                max_rows=10,
            ),
        )

        manager = DatasetManager(
            catalog=DatasetCatalog.from_entries([dataset]),
            cache=DataCache(tmp_path / "cache"),
        )

        fetched = manager.fetch("uniprot_toy")
        assert fetched.cache_hit is False

        lines = fetched.raw_path.read_text(encoding="utf-8").strip().splitlines()
        assert len(lines) == 3
        assert len(_ApiHandler.requests_seen) == 2
