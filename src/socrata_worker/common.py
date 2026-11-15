"""Shared plumbing: portals, scoped ``socrata`` secrets and client construction.

Every function takes the portal as its first argument: a domain such as
``data.cityofnewyork.us`` or a URL (a pasted dataset page URL is trimmed).

A ``socrata`` secret applies to the hosts in its ``SCOPE`` (``https://<domain>``):

* ``app_token``  sent as ``X-App-Token`` to raise the anonymous throttling limit;
* ``base_url``   send that host's requests somewhere else (a caching proxy, a
  mirror, or a local fixture server) while results still name the real portal.

The Discovery API hosts (``https://api.us.socrata.com``, ``https://api.eu.socrata.com``)
are hosts like any other, so a secret scoped to one of them applies to catalog
searches. An unscoped secret is ignored, so a token is never sent to a host the
user did not name.
"""

from __future__ import annotations

import logging
from typing import Any

from .client import SocrataClient, normalize_base_url, normalize_portal

log = logging.getLogger(__name__)

SECRET_TYPE = "socrata"
SECRET_TOKEN_FIELD = "app_token"
SECRET_BASE_URL_FIELD = "base_url"

PORTAL_DOC = (
    "The Socrata (Tyler Data & Insights) portal: a domain such as 'data.cityofnewyork.us', "
    "'data.cityofchicago.org' or 'opendata.rdw.nl', or an https:// URL (a pasted dataset page URL is "
    "trimmed to the portal root)."
)
PORTAL_PATTERN = r"^(https?://)?[A-Za-z0-9.-]+"
DATASET_ID_DOC = (
    "The dataset's four-by-four id, as returned by datasets() and shown at the end of its page URL, e.g. 'erm2-nwe9'."
)
DATASET_ID_PATTERN = r"^[A-Za-z0-9]{4}-[A-Za-z0-9]{4}$"


def _str(value: Any) -> str:
    if value is None:
        return ""
    if hasattr(value, "as_py"):
        value = value.as_py()
    return "" if value is None else str(value)


def scope_match_length(scope: str, host_url: str) -> int:
    """Length of the longest scope prefix covering ``host_url`` at a path boundary, else -1."""
    best = -1
    for prefix in scope.split("\n"):
        p = prefix.strip().rstrip("/")
        if not p:
            continue
        if "://" not in p:
            p = "https://" + p
        p = p.lower()
        if (host_url == p or host_url.startswith(p + "/")) and len(p) > best:
            best = len(p)
    return best


def best_secret(candidates: list[dict[str, Any]], host_url: str) -> dict[str, str]:
    """Return the fields of the best-matching *scoped* ``socrata`` secret for ``host_url`` ({} if none).

    Args:
        candidates: Secret dicts (field name to value).
        host_url: The normalized host URL (``https://<domain>``).

    Returns:
        ``app_token`` / ``base_url`` strings (only the non-empty ones).

    """
    best: tuple[int, dict[str, str]] | None = None
    unscoped = False
    for fields in candidates:
        values = {k: _str(fields.get(k)) for k in (SECRET_TOKEN_FIELD, SECRET_BASE_URL_FIELD)}
        values = {k: v for k, v in values.items() if v}
        if not values:
            continue
        scope = _str(fields.get("scope"))
        if not scope:
            unscoped = True
            continue
        n = scope_match_length(scope, host_url)
        if n >= 0 and (best is None or n > best[0]):
            best = (n, values)
    if best is None and unscoped:
        log.warning("ignoring a socrata secret without SCOPE; scope it to 'https://<domain>' to use it")
    return best[1] if best else {}


def make_client(host_url: str, fields: dict[str, str]) -> SocrataClient:
    """Return a client for ``host_url`` configured by a secret's fields."""
    base = fields.get(SECRET_BASE_URL_FIELD)
    return SocrataClient(
        portal=host_url,
        base_url=normalize_base_url(base) if base else host_url,
        app_token=fields.get(SECRET_TOKEN_FIELD) or None,
    )


def request_secrets(params: Any, *host_urls: str) -> bool:
    """Ask DuckDB for the ``socrata`` secrets scoped to each host (bind time).

    Args:
        params: Bind parameters.
        *host_urls: The normalized host URLs the call will contact.

    Returns:
        True once secrets are resolved (the bind may proceed), False while the
        framework still has to round-trip a secret-scope request to DuckDB.

    """
    for url in host_urls:
        params.secrets.get(SECRET_TYPE, scope=url)
    return bool(params.secrets.all_resolved)


def bind_client(params: Any, host_url: str) -> SocrataClient:
    """Return a client for bind-time calls (after :func:`request_secrets` returned True)."""
    secret = params.secrets.get(SECRET_TYPE, scope=host_url)
    return make_client(host_url, best_secret([secret] if secret else [], host_url))


def process_client(params: Any, host_url: str) -> SocrataClient:
    """Return a client for ``host_url`` with its scoped secret, if any (process time)."""
    secrets = params.secrets
    candidates = list(secrets.of_type(SECRET_TYPE)) if secrets else []
    return make_client(host_url, best_secret(candidates, host_url))


def portal_arg(params: Any) -> str:
    """Return the normalized portal URL from the ``portal`` argument."""
    return normalize_portal(str(params.args.portal))


def strip_html(text: str | None) -> str | None:
    """Plain text from an HTML description (tags removed, whitespace collapsed)."""
    if text is None:
        return None
    from html.parser import HTMLParser

    parts: list[str] = []

    class _P(HTMLParser):
        def handle_data(self, data: str) -> None:
            parts.append(data)

        def handle_starttag(self, tag: str, attrs: Any) -> None:
            if tag in ("p", "br", "li", "div", "h1", "h2", "h3", "h4", "tr"):
                parts.append("\n")

    p = _P(convert_charrefs=True)
    p.feed(text)
    p.close()
    lines = [" ".join(line.split()) for line in "".join(parts).split("\n")]
    return "\n".join(line for line in lines if line) or None
