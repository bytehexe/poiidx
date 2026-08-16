import hashlib
import http.server
import pathlib
import socket
import threading
from collections.abc import Generator
from typing import Any

import pytest  # type: ignore[import-not-found]

from poiidx.pbf import Pbf

BODY = bytes(range(256)) * 400  # ~100 KiB, several 8 KiB chunks
BODY_MD5 = hashlib.md5(BODY, usedforsecurity=False).hexdigest()


class _PbfHandler(http.server.BaseHTTPRequestHandler):
    """Serves a PBF body in a mode chosen per test by the fixture."""

    protocol_version = "HTTP/1.1"

    def do_GET(self) -> None:  # noqa: N802
        server: Any = self.server
        if self.path.endswith(".md5"):
            self._serve_md5(server)
            return

        if server.mode == "truncate_no_length":
            # Close-delimited framing: the client cannot detect the short body.
            self.protocol_version = "HTTP/1.0"
            self.send_response(200)
            self.end_headers()
            self._write_half_and_hang_up()
            return

        self.send_response(200)
        self.send_header("Content-Length", str(len(BODY)))
        self.end_headers()
        if server.mode == "truncate_with_length":
            self._write_half_and_hang_up()
        else:
            self.wfile.write(BODY)

    def _write_half_and_hang_up(self) -> None:
        """Send half the body, then FIN, so the client sees EOF rather than stalling."""
        self.wfile.write(BODY[: len(BODY) // 2])
        self.wfile.flush()
        self.connection.shutdown(socket.SHUT_WR)
        self.close_connection = True

    def _serve_md5(self, server: Any) -> None:
        if server.md5_body is None:
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        data = server.md5_body.encode()
        self.send_response(200)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args: Any) -> None:
        pass


@pytest.fixture
def pbf_server() -> Generator[Any, None, None]:
    server: Any = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _PbfHandler)
    server.mode = "ok"
    server.md5_body = f"{BODY_MD5}  region-latest.osm.pbf\n"
    server.url = f"http://127.0.0.1:{server.server_address[1]}/region-latest.osm.pbf"
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield server
    server.shutdown()
    server.server_close()
    thread.join(timeout=5)


def test_successful_download_stores_the_complete_file(
    pbf_server: Any, tmp_path: pathlib.Path
) -> None:
    result = Pbf(tmp_path).get_pbf_filename("region", pbf_server.url)

    assert result.read_bytes() == BODY


def test_interrupted_download_leaves_no_cached_file(
    pbf_server: Any, tmp_path: pathlib.Path
) -> None:
    """A connection dropped mid-body must not poison the cache with a stub."""
    pbf_server.mode = "truncate_with_length"

    with pytest.raises(Exception):  # noqa: B017
        Pbf(tmp_path).get_pbf_filename("region", pbf_server.url)

    assert not (tmp_path / "region.pbf").exists()


def test_interrupted_download_leaves_no_partial_files(
    pbf_server: Any, tmp_path: pathlib.Path
) -> None:
    pbf_server.mode = "truncate_with_length"

    with pytest.raises(Exception):  # noqa: B017
        Pbf(tmp_path).get_pbf_filename("region", pbf_server.url)

    assert list(tmp_path.iterdir()) == []


def test_silently_truncated_body_is_rejected_by_checksum(
    pbf_server: Any, tmp_path: pathlib.Path
) -> None:
    """Close-delimited responses hide short bodies; only the md5 catches them."""
    pbf_server.mode = "truncate_no_length"

    with pytest.raises(ValueError, match="checksum"):
        Pbf(tmp_path).get_pbf_filename("region", pbf_server.url)

    assert not (tmp_path / "region.pbf").exists()


def test_checksum_mismatch_is_rejected(pbf_server: Any, tmp_path: pathlib.Path) -> None:
    pbf_server.md5_body = f"{'0' * 32}  region-latest.osm.pbf\n"

    with pytest.raises(ValueError, match="checksum"):
        Pbf(tmp_path).get_pbf_filename("region", pbf_server.url)

    assert not (tmp_path / "region.pbf").exists()


def test_missing_checksum_file_does_not_fail_the_download(
    pbf_server: Any, tmp_path: pathlib.Path
) -> None:
    """Geofabrik may not publish an .md5; that must not break ingestion."""
    pbf_server.md5_body = None

    result = Pbf(tmp_path).get_pbf_filename("region", pbf_server.url)

    assert result.read_bytes() == BODY


def test_cached_file_is_not_downloaded_again(tmp_path: pathlib.Path) -> None:
    cached = tmp_path / "region.pbf"
    cached.write_bytes(BODY)

    result = Pbf(tmp_path).get_pbf_filename(
        "region", "http://127.0.0.1:1/unreachable.osm.pbf"
    )

    assert result == cached
