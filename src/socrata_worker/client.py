"""HTTP plumbing for Socrata (Tyler Data & Insights) portals and the Discovery API.

* ``normalize_portal`` turns ``data.cityofnewyork.us``, a pasted dataset page URL
  (``https://data.cityofnewyork.us/Social-Services/.../erm2-nwe9``) or an API URL
  into the portal's canonical base URL, ``https://data.cityofnewyork.us``.
* ``SocrataClient.get`` performs one GET with timeouts and retries 429 (throttled)
  and 5xx responses with backoff that honours ``Retry-After``.

A client has two URLs: ``portal`` is the logical identity (what results report and
what secrets are scoped to) and ``base_url`` is where requests are actually sent.
They differ only when a ``socrata`` secret sets ``base_url`` (a mirror, a proxy or
a local fixture server).

The app token (if any) is sent as the ``X-App-Token`` header, never in the URL, so
it cannot leak into logs or error messages.
"""

from __future__ import annotations

import contextlib
import json
import logging
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any

log = logging.getLogger(__name__)

TIMEOUT_SECONDS = 60.0
MAX_ATTEMPTS = 6
MAX_RETRY_DELAY = 60.0
USER_AGENT = "vgi-socrata-worker/0.1 (+https://query.farm)"

DISCOVERY_HOSTS = {"us": "https://api.us.socrata.com", "eu": "https://api.eu.socrata.com"}

_HOST_RE = re.compile(r"^[A-Za-z0-9.-]+(:\d+)?(/.*)?$")
FOUR_BY_FOUR_RE = re.compile(r"^[a-z0-9]{4}-[a-z0-9]{4}$")


class SocrataError(RuntimeError):
    """The portal returned an error, or a request could not be made."""


class HttpStatusError(SocrataError):
    """A request failed with an HTTP status (kept so callers can react to 404 / 400)."""

    def __init__(self, message: str, status: int, body: str) -> None:
        """Remember the status and the response body."""
        super().__init__(message)
        self.status = status
        self.body = body


def normalize_portal(value: str) -> str:
    """Return the canonical base URL of a Socrata portal.

    ``data.cityofnewyork.us`` becomes ``https://data.cityofnewyork.us``. Socrata
    portals always live at the root of their host, so any path (a dataset page,
    ``/resource/...``, ``/api/...``) is dropped.

    Args:
        value: A domain, or an http(s) URL.

    Returns:
        ``scheme://host[:port]`` with a lower-case host and no trailing slash.

    Raises:
        SocrataError: The value is empty or not a domain/URL.

    """
    text = (value or "").strip()
    if not text:
        raise SocrataError("portal is empty: pass a domain such as 'data.cityofnewyork.us' or an https:// URL")
    if "://" not in text:
        if not _HOST_RE.match(text):
            raise SocrataError(f"portal {value!r} is not a domain or URL, e.g. 'data.cityofnewyork.us'")
        text = "https://" + text
    parsed = urllib.parse.urlsplit(text)
    if parsed.scheme not in ("http", "https") or not parsed.netloc or "@" in parsed.netloc:
        raise SocrataError(f"portal {value!r} must be an http(s) URL or a domain")
    return f"{parsed.scheme}://{parsed.netloc.lower()}"


def portal_domain(portal: str) -> str:
    """Return the host (domain) of a normalized portal URL, e.g. ``data.cityofnewyork.us``."""
    return urllib.parse.urlsplit(portal).netloc


def normalize_base_url(value: str) -> str:
    """Return a ``base_url`` secret value without a trailing slash (any path prefix is kept)."""
    text = (value or "").strip().rstrip("/")
    parsed = urllib.parse.urlsplit(text)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise SocrataError(f"socrata secret base_url {value!r} must be an http(s) URL")
    return text


def require_four_by_four(value: str) -> str:
    """Validate a Socrata dataset id (a "four-by-four" such as ``erm2-nwe9``)."""
    v = (value or "").strip().lower()
    if not FOUR_BY_FOUR_RE.match(v):
        raise SocrataError(
            f"dataset_id {value!r} is not a Socrata four-by-four id (expected xxxx-xxxx, e.g. 'erm2-nwe9')"
        )
    return v


def dataset_page_url(portal: str, dataset_id: str) -> str:
    """Return the portal's human-facing page for a dataset."""
    return f"{portal}/d/{dataset_id}"


def resource_url(portal: str, dataset_id: str) -> str:
    """Return the SODA 2.1 resource endpoint of a dataset."""
    return f"{portal}/resource/{dataset_id}.json"


@dataclass
class Response:
    """A decoded JSON body plus the response headers (lower-cased names)."""

    body: Any
    headers: dict[str, str]


@dataclass
class SocrataClient:
    """A small client bound to one host and (optionally) one app token.

    Attributes:
        portal: Logical base URL (identity; secrets are scoped to it).
        base_url: Where requests go (``portal`` unless a secret overrides it).
        app_token: Socrata app token, sent as ``X-App-Token``.

    """

    portal: str
    base_url: str
    app_token: str | None = None

    def url(self, path: str, params: dict[str, Any] | None = None) -> str:
        """Build ``<base_url><path>?<params>`` (None/empty values dropped, keys sorted)."""
        pairs: list[tuple[str, str]] = []
        for k, v in (params or {}).items():
            for item in v if isinstance(v, (list, tuple)) else [v]:
                if item is not None and item != "":
                    pairs.append((k, str(item)))
        query = urllib.parse.urlencode(sorted(pairs))
        return f"{self.base_url}{path}" + (f"?{query}" if query else "")

    def _headers(self) -> dict[str, str]:
        headers = {"Accept": "application/json", "User-Agent": USER_AGENT}
        if self.app_token:
            headers["X-App-Token"] = self.app_token
        return headers

    def get(self, path: str, params: dict[str, Any] | None = None) -> Response:
        """GET a JSON document, retrying throttling (429) and server errors with backoff.

        Args:
            path: Path on the host, e.g. ``/resource/erm2-nwe9.json``.
            params: Query parameters.

        Returns:
            The decoded body and the response headers.

        Raises:
            HttpStatusError: The host answered with a non-retryable status, or kept throttling.
            SocrataError: The request could not be made, or the body is not JSON.
            AssertionError: Unreachable (the retry loop always returns or raises).

        """
        url = self.url(path, params)
        shown = self.portal + path
        log.debug("GET %s", url)
        for attempt in range(1, MAX_ATTEMPTS + 1):
            request = urllib.request.Request(url, headers=self._headers())
            try:
                with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS) as response:
                    raw = response.read()
                    headers = {k.lower(): v for k, v in response.headers.items()}
                try:
                    return Response(json.loads(raw or b"null"), headers)
                except ValueError as err:
                    raise SocrataError(f"GET {shown} returned a response that is not JSON") from err
            except urllib.error.HTTPError as err:
                retryable = err.code == 429 or err.code >= 500
                if not retryable or attempt == MAX_ATTEMPTS:
                    body = ""
                    with contextlib.suppress(Exception):
                        body = err.read(4000).decode(errors="replace")
                    raise HttpStatusError(_describe_http_error(shown, err.code, body), err.code, body) from err
                retry_after = err.headers.get("Retry-After") if err.headers else None
                delay = retry_delay(retry_after, attempt)
                log.info("HTTP %s from %s; retrying in %.1fs", err.code, shown, delay)
                time.sleep(delay)
            except (urllib.error.URLError, TimeoutError, ConnectionError) as err:
                if attempt == MAX_ATTEMPTS:
                    reason = getattr(err, "reason", err)
                    raise SocrataError(f"GET {shown} failed: {reason}") from err
                time.sleep(min(0.5 * 2**attempt, MAX_RETRY_DELAY))
        raise AssertionError("unreachable")


def retry_delay(retry_after: str | None, attempt: int) -> float:
    """Seconds to wait before retry ``attempt``: ``Retry-After`` if given, else exponential backoff."""
    if retry_after:
        try:
            return min(max(float(retry_after), 0.0), MAX_RETRY_DELAY)
        except ValueError:
            pass
    return float(min(0.5 * 2**attempt, MAX_RETRY_DELAY))


def _describe_http_error(shown: str, status: int, body: str) -> str:
    """Build a readable error: HTTP status plus Socrata's ``message`` / ``errorCode`` when present."""
    detail = body.strip()
    try:
        parsed = json.loads(detail)
        if isinstance(parsed, dict):
            message = parsed.get("message") or parsed.get("error")
            code = parsed.get("errorCode") or parsed.get("code")
            if message:
                detail = f"{code}: {message}" if code else str(message)
    except ValueError:
        pass
    if len(detail) > 600:
        detail = detail[:600] + "..."
    hint = ""
    if status in (401, 403):
        hint = " (private dataset, or the portal needs an app token: CREATE SECRET (TYPE socrata, APP_TOKEN '...'))"
    elif status == 404:
        hint = " (unknown portal or dataset_id, or the dataset is not public)"
    elif status == 429:
        hint = " (throttled: create a socrata secret with an app token to raise the limit)"
    return f"GET {shown} failed with HTTP {status}: {detail}{hint}"


class TtlCache:
    """A tiny per-process cache for bind-time metadata lookups (binds can repeat)."""

    def __init__(self, ttl: float = 300.0, max_items: int = 64) -> None:
        """Create an empty cache."""
        self._items: OrderedDict[str, tuple[float, Any]] = OrderedDict()
        self._ttl = ttl
        self._max = max_items
        self._lock = threading.Lock()

    def get(self, key: str) -> Any:
        """Return the cached value, or None if absent or expired."""
        with self._lock:
            hit = self._items.get(key)
            if hit is None or time.monotonic() - hit[0] > self._ttl:
                return None
            return hit[1]

    def put(self, key: str, value: Any) -> None:
        """Cache ``value`` under ``key``."""
        with self._lock:
            self._items[key] = (time.monotonic(), value)
            self._items.move_to_end(key)
            while len(self._items) > self._max:
                self._items.popitem(last=False)
