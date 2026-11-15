#!/usr/bin/env python3
"""Strict replay server for recorded Socrata (SODA 2.1 + Discovery API) responses.

Each upstream host is served under a path prefix named after the host::

    http://127.0.0.1:<port>/data.cityofnewyork.us/resource/erm2-nwe9.json?...
    http://127.0.0.1:<port>/api.us.socrata.com/api/catalog/v1?...

The worker is pointed at it with ``socrata`` secrets whose ``base_url`` names the
prefix, so results still report the real portal::

    CREATE SECRET (TYPE socrata, SCOPE 'https://data.cityofnewyork.us',
                   BASE_URL 'http://127.0.0.1:<port>/data.cityofnewyork.us');

**Strict.** A request is answered only if its path *and its exact query string*
(``$select``, ``$where``, ``$order``, ``$limit`` ... after sorting) were recorded.
Anything else gets ``404 UnrecordedRequest``, which fails the SQL test. That is how
the tests assert pushdown: if the worker stops pushing a filter into SoQL (or quotes
a literal differently), its request no longer matches.

``private.example.org`` is hand-written and answers only with ``X-App-Token: test-token``.

Switches (environment):

* ``MOCK_RECORD=1``  record misses from the live host once. Never set in CI.
* ``MOCK_FLAKY=1``   answer the first request for each URL with 429 + ``Retry-After: 0``.
* ``MOCK_MISS_LOG``  append every unrecorded request to this file.
* ``MOCK_HIT_LOG``   append the fixture file of every answered request (to prune unused ones).

Usage::

    python3 test/mock_api/server.py            # prints PORT:<n> once listening
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
import threading
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

FIXTURES = Path(__file__).resolve().parent.parent / "fixtures"
RECORDABLE = {
    "data.cityofnewyork.us",
    "data.cityofchicago.org",
    "opendata.rdw.nl",
    "api.us.socrata.com",
    "api.eu.socrata.com",
}
PRIVATE_HOST = "private.example.org"
PRIVATE_TOKEN = "test-token"
RECORD = os.environ.get("MOCK_RECORD") == "1"
FLAKY = os.environ.get("MOCK_FLAKY") == "1"
MISS_LOG = os.environ.get("MOCK_MISS_LOG")
HIT_LOG = os.environ.get("MOCK_HIT_LOG")

_seen: set[str] = set()
_lock = threading.Lock()


def canonical(path: str, query: str) -> str:
    """Path plus the query parameters sorted, so parameter order does not matter."""
    pairs = sorted(urllib.parse.parse_qsl(query, keep_blank_values=True))
    return path + ("?" + urllib.parse.urlencode(pairs) if pairs else "")


def fixture_file(alias: str, key: str) -> Path:
    """Where the response for ``key`` is stored."""
    digest = hashlib.sha1(key.encode()).hexdigest()[:16]
    return FIXTURES / alias / f"{digest}.json"


def load(alias: str, key: str) -> dict[str, Any] | None:
    """The recorded response, or None."""
    f = fixture_file(alias, key)
    if not f.exists():
        return None
    data: dict[str, Any] = json.loads(f.read_text(encoding="utf-8"))
    if data.get("request") != key:  # hash collision guard
        return None
    return data


KEEP_HEADERS = ("x-soda2-fields", "x-soda2-types")


def record(alias: str, key: str) -> dict[str, Any]:
    """Fetch ``key`` from the live host once and store it."""
    url = f"https://{alias}{key}"
    req = urllib.request.Request(url, headers={"User-Agent": "vgi-socrata-fixture-recorder", "Accept": "*/*"})
    headers: dict[str, str] = {}
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            status = resp.status
            ctype = resp.headers.get("Content-Type", "application/json")
            headers = {k.lower(): v for k, v in resp.headers.items() if k.lower() in KEEP_HEADERS}
            body = resp.read().decode()
    except urllib.error.HTTPError as err:
        status, ctype, body = err.code, "application/json", err.read().decode()
    data = {"request": key, "status": status, "content_type": ctype, "headers": headers, "body": body}
    f = fixture_file(alias, key)
    f.parent.mkdir(parents=True, exist_ok=True)
    f.write_text(json.dumps(data, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"recorded {alias}{key} -> {f.name}", file=sys.stderr)
    return data


class Handler(BaseHTTPRequestHandler):
    """Serve recorded responses; refuse anything unrecorded."""

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        """Log requests to stderr only."""
        sys.stderr.write("mock: " + (format % args) + "\n")

    def _send(
        self, status: int, body: str, ctype: str = "application/json", headers: dict[str, str] | None = None
    ) -> None:
        raw = body.encode()
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(raw)))
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self) -> None:
        """Answer one GET."""
        parsed = urllib.parse.urlsplit(self.path)
        if parsed.path == "/health":
            self._send(200, '{"status": "ok"}')
            return
        parts = parsed.path.split("/", 2)
        alias = parts[1] if len(parts) > 1 else ""
        rest = "/" + parts[2] if len(parts) > 2 else "/"
        key = canonical(rest, parsed.query)
        if FLAKY:
            with _lock:
                first = key not in _seen
                _seen.add(key)
            if first:
                self._send(
                    429, '{"message": "Too many requests", "errorCode": "throttled"}', headers={"Retry-After": "0"}
                )
                return
        if alias == PRIVATE_HOST and self.headers.get("X-App-Token") != PRIVATE_TOKEN:
            self._send(
                403, '{"message": "You must be logged in or use an app token", "errorCode": "authentication_required"}'
            )
            return
        data = load(alias, key)
        if data is None and RECORD and alias in RECORDABLE:
            data = record(alias, key)
        if data is None:
            print(f"mock: UNRECORDED {alias}{key}", file=sys.stderr)
            if MISS_LOG:
                with open(MISS_LOG, "a", encoding="utf-8") as log:
                    log.write(f"{alias}{key}\n")
            msg = json.dumps({"errorCode": "UnrecordedRequest", "message": f"no fixture for {alias}{key}"})
            self._send(404, msg)
            return
        if HIT_LOG:
            with open(HIT_LOG, "a", encoding="utf-8") as log:
                log.write(f"{fixture_file(alias, key).relative_to(FIXTURES)}\n")
        self._send(
            int(data["status"]), data["body"], data.get("content_type") or "application/json", data.get("headers")
        )


def main() -> None:
    """Listen on a free port (or --port N) and print PORT:<n>."""
    port = 0
    if "--port" in sys.argv:
        port = int(sys.argv[sys.argv.index("--port") + 1])
    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    print(f"PORT:{server.server_address[1]}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
