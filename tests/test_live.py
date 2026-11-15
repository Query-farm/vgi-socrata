"""Optional live smoke test against real portals. Skipped unless SOCRATA_LIVE=1.

It checks that SODA 2.1 and the Discovery API still answer the way the recorded
fixtures assume (view metadata types, keyset paging on :id, X-SODA2 type headers,
US/EU Discovery routing). Keyless and tiny: a handful of requests, a few rows each.
"""

from __future__ import annotations

import os

import pytest

from socrata_worker.client import DISCOVERY_HOSTS, HttpStatusError, SocrataClient, normalize_portal
from socrata_worker.dataset import fetch_view, schema_for_view

pytestmark = pytest.mark.skipif(os.environ.get("SOCRATA_LIVE") != "1", reason="set SOCRATA_LIVE=1")


def _client(host: str) -> SocrataClient:
    url = normalize_portal(host)
    return SocrataClient(portal=url, base_url=url)


def test_live_view_types_and_keyset() -> None:
    client = _client("data.cityofchicago.org")
    schema = schema_for_view(fetch_view(client, "x8fc-8rcq"))
    assert {"branch_", "website", "location", "soda_row_id"} <= set(schema.names)
    first = client.get("/resource/x8fc-8rcq.json", {"$select": "`branch_`,`:id`", "$order": "`:id`", "$limit": 2})
    assert len(first.body) == 2
    last = first.body[-1][":id"]
    nxt = client.get(
        "/resource/x8fc-8rcq.json",
        {"$select": "`branch_`,`:id`", "$order": "`:id`", "$limit": 2, "$where": f"`:id` > '{last}'"},
    )
    assert nxt.body and nxt.body[0][":id"] != last


def test_live_soql_type_headers() -> None:
    resp = _client("data.cityofchicago.org").get(
        "/resource/x8fc-8rcq.json", {"$query": "SELECT zip, count(*) AS n GROUP BY zip LIMIT 1"}
    )
    assert "x-soda2-types" in resp.headers


def test_live_discovery_regions() -> None:
    eu = SocrataClient(portal=DISCOVERY_HOSTS["eu"], base_url=DISCOVERY_HOSTS["eu"])
    body = eu.get("/api/catalog/v1", {"domains": "opendata.rdw.nl", "search_context": "opendata.rdw.nl", "limit": 1})
    assert body.body["results"]
    us = SocrataClient(portal=DISCOVERY_HOSTS["us"], base_url=DISCOVERY_HOSTS["us"])
    with pytest.raises(HttpStatusError, match="Domain not found"):
        us.get("/api/catalog/v1", {"domains": "opendata.rdw.nl", "limit": 1})
