from __future__ import annotations

import json
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from refua_data.cache import DataCache
from refua_data.catalog import DatasetCatalog
from refua_data.models import ApiDatasetConfig, DatasetDefinition
from refua_data.pipeline import DatasetManager


class _ValidationHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        if self.path.startswith("/ok.csv"):
            payload = b"smiles,label\nCCO,1\n"
            self.send_response(200)
            self.send_header("Content-Type", "text/csv")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return

        if self.path.startswith("/missing.csv"):
            self.send_response(404)
            self.end_headers()
            return

        if self.path.startswith("/api/search"):
            payload = json.dumps({"results": [{"id": "P1"}]}).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return

        self.send_response(404)
        self.end_headers()

    def log_message(self, format: str, *args: object) -> None:  # noqa: A003
        return


@contextmanager
def _server() -> Iterator[str]:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _ValidationHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        yield f"http://{host}:{port}"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_validate_sources_for_file_http_and_api(tmp_path: Path) -> None:
    local_file = tmp_path / "local.csv"
    local_file.write_text("smiles,label\nCCC,0\n", encoding="utf-8")

    with _server() as base:
        datasets = [
            DatasetDefinition(
                dataset_id="local_file",
                name="Local",
                description="local",
                source="unit",
                homepage="https://example.test",
                license_name="test",
                license_url=None,
                file_format="csv",
                category="test",
                urls=(local_file.as_uri(),),
            ),
            DatasetDefinition(
                dataset_id="http_ok",
                name="HTTP OK",
                description="http ok",
                source="unit",
                homepage="https://example.test",
                license_name="test",
                license_url=None,
                file_format="csv",
                category="test",
                urls=(f"{base}/ok.csv",),
            ),
            DatasetDefinition(
                dataset_id="http_bad",
                name="HTTP BAD",
                description="http bad",
                source="unit",
                homepage="https://example.test",
                license_name="test",
                license_url=None,
                file_format="csv",
                category="test",
                urls=(f"{base}/missing.csv",),
            ),
            DatasetDefinition(
                dataset_id="http_mirror",
                name="HTTP Mirror",
                description="http mirror",
                source="unit",
                homepage="https://example.test",
                license_name="test",
                license_url=None,
                file_format="csv",
                category="test",
                urls=(f"{base}/missing.csv", f"{base}/ok.csv"),
            ),
            DatasetDefinition(
                dataset_id="http_concat",
                name="HTTP Concat",
                description="http concat",
                source="unit",
                homepage="https://example.test",
                license_name="test",
                license_url=None,
                file_format="csv",
                category="test",
                urls=(f"{base}/ok.csv", f"{base}/missing.csv"),
                url_mode="concat",
            ),
            DatasetDefinition(
                dataset_id="api_ok",
                name="API OK",
                description="api ok",
                source="unit",
                homepage="https://example.test",
                license_name="test",
                license_url=None,
                file_format="jsonl",
                category="test",
                api=ApiDatasetConfig(
                    endpoint=f"{base}/api/search",
                    params={"format": "json"},
                    items_path="results",
                    pagination="none",
                ),
            ),
        ]

        manager = DatasetManager(
            catalog=DatasetCatalog.from_entries(datasets),
            cache=DataCache(tmp_path / "cache"),
        )

        results = manager.validate_sources(timeout_seconds=5)
        by_id = {result.dataset_id: result for result in results}

        assert by_id["local_file"].ok is True
        assert by_id["local_file"].source_type == "file"

        assert by_id["http_ok"].ok is True
        assert by_id["http_ok"].source_type == "http"

        assert by_id["http_bad"].ok is False
        assert by_id["http_bad"].status_code == 404

        assert by_id["http_mirror"].ok is True
        assert by_id["http_mirror"].source_type == "http"
        assert by_id["http_mirror"].source == f"{base}/ok.csv"
        fallback_failures = by_id["http_mirror"].details.get("fallback_failures")
        assert isinstance(fallback_failures, list)
        assert len(fallback_failures) == 1
        assert fallback_failures[0].get("status_code") == 404

        assert by_id["http_concat"].ok is False
        assert by_id["http_concat"].details.get("failed_count") == 1
        assert by_id["http_concat"].details.get("url_mode") == "concat"

        assert by_id["api_ok"].ok is True
        assert by_id["api_ok"].source_type == "api"
        assert by_id["api_ok"].details.get("sample_items") == 1
