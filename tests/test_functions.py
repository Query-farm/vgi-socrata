"""Unit tests. The SQL-level tests live in test/sql and run through DuckDB."""

from __future__ import annotations

import datetime as dt
from decimal import Decimal
from typing import Any

import pyarrow as pa
import pytest
from vgi.client import Client
from vgi.table_filter_pushdown import (
    AndFilter,
    ComparisonOp,
    ConstantFilter,
    InFilter,
    IsNotNullFilter,
    IsNullFilter,
    OrFilter,
)

from socrata_worker.client import (
    SocrataClient,
    SocrataError,
    normalize_portal,
    require_four_by_four,
    retry_delay,
)
from socrata_worker.common import best_secret, make_client, scope_match_length, strip_html
from socrata_worker.dataset import schema_for_view
from socrata_worker.discovery import column_rows, dataset_row
from socrata_worker.soql import (
    JSON_TYPE,
    LOCATION_TYPE,
    NUMBER,
    PHONE_TYPE,
    TIMESTAMP,
    TIMESTAMPTZ,
    URL_TYPE,
    combine_where,
    like_escape,
    literal,
    string_literal,
    to_arrow_batch,
    to_number,
    translate_filter,
)
from socrata_worker.soql_query import effective_query, has_limit, result_schema

SCHEMA = ["main"]
COLS = {
    "name": ("name", "text"),
    "n": ("n", "number"),
    "x": ("x", "double"),
    "d": ("d", "calendar_date"),
    "ts": ("ts", "fixed_timestamp"),
    "ok": ("ok", "checkbox"),
    "geo": ("geo", "point"),
    "soda_row_id": (":id", "row_identifier"),
}


def _const(col: str, op: ComparisonOp, value: Any) -> ConstantFilter:
    return ConstantFilter(column_name=col, column_index=0, op=op, value=pa.scalar(value))


# ---------------------------------------------------------------- SoQL text


def test_string_literal_doubles_quotes_and_backslashes() -> None:
    assert string_literal("O'Brien") == "'O''Brien'"
    assert string_literal("a\\b") == "'a\\\\b'"


def test_like_escape() -> None:
    assert like_escape("100%_x\\") == "100\\%\\_x\\\\"


def test_literals_by_column_type() -> None:
    assert literal(3, "number") == "3"
    assert literal(Decimal("10.5000000000"), "number") == "10.5"
    assert literal(Decimal("100.0000000000"), "money") == "100"
    assert literal(2.5, "double") == "2.5"
    assert literal(float("nan"), "double") is None
    assert literal(True, "checkbox") == "true"
    assert literal(True, "number") is None
    assert literal("3", "number") is None
    assert literal(3, "text") is None
    assert literal(dt.datetime(2024, 1, 2, 3, 4, 5), "calendar_date") == "'2024-01-02T03:04:05.000'"
    aware = dt.datetime(2024, 1, 2, 3, 4, 5, tzinfo=dt.timezone(dt.timedelta(hours=2)))
    assert literal(aware, "fixed_timestamp") == "'2024-01-02T01:04:05.000Z'"
    assert literal(dt.date(2024, 1, 2), "floating_timestamp") == "'2024-01-02T00:00:00.000'"
    # Socrata keeps milliseconds: an exact sub-millisecond bound cannot be expressed.
    assert literal(dt.datetime(2024, 1, 1, 0, 0, 0, 500), "calendar_date") is None


def test_comparisons() -> None:
    assert translate_filter(_const("name", ComparisonOp.EQ, "BRONX"), COLS) == "`name` = 'BRONX'"
    assert translate_filter(_const("name", ComparisonOp.NE, "x"), COLS) == "`name` != 'x'"
    assert translate_filter(_const("n", ComparisonOp.GE, 10), COLS) == "`n` >= 10"
    assert translate_filter(_const("x", ComparisonOp.LT, 0.5), COLS) == "`x` < 0.5"
    assert translate_filter(_const("ok", ComparisonOp.EQ, False), COLS) == "`ok` = false"
    got = translate_filter(_const("d", ComparisonOp.LT, dt.datetime(2020, 1, 1)), COLS)
    assert got == "`d` < '2020-01-01T00:00:00.000'"


def test_not_pushed() -> None:
    assert translate_filter(_const("name", ComparisonOp.GT, "M"), COLS) is None  # text collation
    assert translate_filter(_const("ok", ComparisonOp.GT, False), COLS) is None
    assert translate_filter(_const("unknown", ComparisonOp.EQ, "x"), COLS) is None
    assert translate_filter(_const("geo", ComparisonOp.EQ, "x"), COLS) is None


def test_in_and_null() -> None:
    f = InFilter(column_name="name", column_index=0, values=pa.array(["a", "b'", None]))
    assert translate_filter(f, COLS) == "`name` IN ('a', 'b''')"
    assert translate_filter(IsNullFilter(column_name="geo", column_index=0), COLS) == "`geo` IS NULL"
    assert translate_filter(IsNotNullFilter(column_name="n", column_index=0), COLS) == "`n` IS NOT NULL"


def test_row_id_maps_to_system_field() -> None:
    got = translate_filter(_const("soda_row_id", ComparisonOp.EQ, "row-ab"), COLS)
    assert got == "`:id` = 'row-ab'"


def test_like_prefix_range_becomes_starts_with() -> None:
    f = AndFilter(
        column_name="name",
        column_index=0,
        children=(_const("name", ComparisonOp.GE, "Noise"), _const("name", ComparisonOp.LT, "Noisf")),
    )
    assert translate_filter(f, COLS) == "starts_with(`name`, 'Noise')"


def test_and_keeps_translatable_parts_or_needs_all() -> None:
    good = _const("n", ComparisonOp.GT, 1)
    bad = _const("name", ComparisonOp.GT, "x")
    assert translate_filter(AndFilter(column_name="", column_index=0, children=(good, bad)), COLS) == "(`n` > 1)"
    assert translate_filter(OrFilter(column_name="", column_index=0, children=(good, bad)), COLS) is None
    both = OrFilter(column_name="", column_index=0, children=(good, _const("n", ComparisonOp.LT, -1)))
    assert translate_filter(both, COLS) == "(`n` > 1) OR (`n` < -1)"


def test_combine_where() -> None:
    assert combine_where([None, "", "a = 1"]) == "a = 1"
    assert combine_where(["a = 1", "b = 2"]) == "(a = 1) AND (b = 2)"
    assert combine_where([None]) is None


# ---------------------------------------------------------------- types and values

VIEW = {
    "id": "abcd-1234",
    "columns": [
        {"fieldName": "name", "name": "Name", "dataTypeName": "text", "description": "The  name"},
        {"fieldName": "n", "dataTypeName": "number"},
        {"fieldName": "d", "dataTypeName": "calendar_date"},
        {"fieldName": "ts", "dataTypeName": "fixed_timestamp"},
        {"fieldName": "ok", "dataTypeName": "checkbox"},
        {"fieldName": "pt", "dataTypeName": "point"},
        {"fieldName": "loc", "dataTypeName": "location"},
        {"fieldName": "web", "dataTypeName": "url"},
        {"fieldName": "tel", "dataTypeName": "phone"},
        {"fieldName": ":@computed_region_x", "dataTypeName": "number"},
    ],
}


def test_schema_for_view_maps_types() -> None:
    schema = schema_for_view(VIEW)
    assert schema.names == ["name", "n", "d", "ts", "ok", "pt", "loc", "web", "tel", "soda_row_id"]
    types = [f.type for f in schema]
    assert types == [
        pa.string(),
        NUMBER,
        TIMESTAMP,
        TIMESTAMPTZ,
        pa.bool_(),
        JSON_TYPE,
        LOCATION_TYPE,
        URL_TYPE,
        PHONE_TYPE,
        pa.string(),
    ]
    assert (schema.field("name").metadata or {})[b"comment"] == b"Name: The name (Socrata type text)"


def test_schema_for_view_needs_columns() -> None:
    with pytest.raises(SocrataError):
        schema_for_view({"id": "x", "columns": [{"fieldName": ":id"}]})


def test_to_arrow_batch_converts_values() -> None:
    schema = schema_for_view(VIEW)
    rows: list[dict[str, Any]] = [
        {
            "name": "A",
            "n": "12.5",
            "d": "2024-01-01T00:00:20.000",
            "ts": "2024-01-01T10:00:00.000Z",
            "ok": True,
            "pt": {"type": "Point", "coordinates": [-73.9, 40.8]},
            "loc": {"latitude": "41.8", "longitude": "-87.6", "human_address": '{"address": "1 Main", "city": ""}'},
            "web": {"url": "https://x", "description": "X"},
            "tel": {"phone_number": "555"},
            ":id": "row-1",
        },
        {"n": "1e30", "ok": "false"},
    ]
    out = to_arrow_batch(rows, schema).to_pylist()
    assert out[0]["n"] == Decimal("12.5000000000")
    assert out[0]["d"] == dt.datetime(2024, 1, 1, 0, 0, 20)
    assert out[0]["ts"] == dt.datetime(2024, 1, 1, 10, tzinfo=dt.UTC)
    assert out[0]["pt"] == '{"type": "Point", "coordinates": [-73.9, 40.8]}'
    assert out[0]["loc"] == {
        "latitude": 41.8,
        "longitude": -87.6,
        "address": "1 Main",
        "city": None,
        "state": None,
        "zip": None,
    }
    assert out[0]["web"] == {"url": "https://x", "description": "X"}
    assert out[0]["tel"] == {"phone_number": "555", "phone_type": None}
    assert out[0]["soda_row_id"] == "row-1"
    assert out[1]["n"] is None  # does not fit DECIMAL(28,10)
    assert out[1]["ok"] is False
    assert out[1]["name"] is None


def test_to_number_limits() -> None:
    assert to_number("999999999999999999") == Decimal("999999999999999999.0000000000")
    assert to_number("1000000000000000000") is None
    assert to_number("x") is None
    assert to_number(True) is None


# ---------------------------------------------------------------- portals, secrets, client


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("data.cityofnewyork.us", "https://data.cityofnewyork.us"),
        ("Data.CityOfNewYork.us/", "https://data.cityofnewyork.us"),
        (
            "https://data.cityofnewyork.us/Social-Services/311-Service-Requests/erm2-nwe9",
            "https://data.cityofnewyork.us",
        ),
        ("https://data.cityofchicago.org/resource/x8fc-8rcq.json?$limit=1", "https://data.cityofchicago.org"),
        ("http://localhost:8080/x", "http://localhost:8080"),
    ],
)
def test_normalize_portal(value: str, expected: str) -> None:
    assert normalize_portal(value) == expected


@pytest.mark.parametrize("value", ["", "not a domain!", "ftp://x.org", "https://user@x.org"])
def test_normalize_portal_rejects_garbage(value: str) -> None:
    with pytest.raises(SocrataError):
        normalize_portal(value)


def test_four_by_four() -> None:
    assert require_four_by_four(" ERM2-NWE9 ") == "erm2-nwe9"
    with pytest.raises(SocrataError, match="four-by-four"):
        require_four_by_four("erm2_nwe9")


def test_secret_scope_matching() -> None:
    portal = "https://data.cityofnewyork.us"
    assert scope_match_length("https://data.cityofnewyork.us", portal) == len(portal)
    assert scope_match_length("data.cityofnewyork.us", portal) == len(portal)
    assert scope_match_length("https://data.cityofnewyork.usx", portal) == -1
    assert scope_match_length("https://other.org\nhttps://data.cityofnewyork.us/", portal) == len(portal)
    secrets = [
        {"app_token": "unscoped"},
        {"app_token": "other", "scope": "https://data.cityofchicago.org"},
        {"app_token": "nyc", "scope": "https://data.cityofnewyork.us", "base_url": "http://127.0.0.1:1/nyc/"},
    ]
    assert best_secret(secrets, portal) == {"app_token": "nyc", "base_url": "http://127.0.0.1:1/nyc/"}
    assert best_secret(secrets[:2], portal) == {}
    client = make_client(portal, best_secret(secrets, portal))
    assert client.portal == portal
    assert client.base_url == "http://127.0.0.1:1/nyc"
    assert client.app_token == "nyc"


def test_url_sorts_params_and_repeats_lists() -> None:
    c = SocrataClient(portal="https://x.org", base_url="http://127.0.0.1:9/x.org")
    url = c.url("/api/catalog/v1", {"q": "a b", "ids": ["b", "a"], "x": None, "limit": 5})
    assert url == "http://127.0.0.1:9/x.org/api/catalog/v1?ids=a&ids=b&limit=5&q=a+b"


def test_retry_delay() -> None:
    assert retry_delay("3", 1) == 3.0
    assert retry_delay("garbage", 2) == 2.0
    assert retry_delay(None, 10) == 60.0


def test_strip_html() -> None:
    assert strip_html("<b>NOTE:</b> one<br>two &amp; three") == "NOTE: one\ntwo & three"
    assert strip_html(None) is None


# ---------------------------------------------------------------- catalog rows and soql()


def test_dataset_row() -> None:
    raw = {
        "resource": {
            "id": "erm2-nwe9",
            "name": "311",
            "description": "<p>Hi</p>",
            "attribution": "311",
            "type": "dataset",
            "updatedAt": "2026-11-15T02:58:11.000Z",
            "page_views": {"page_views_last_month": 5},
            "columns_field_name": ["a", ":@computed_region_x"],
        },
        "classification": {"domain_category": "Social", "domain_tags": ["t"], "domain_metadata": [{"key": "k"}]},
        "metadata": {"domain": "data.cityofnewyork.us"},
        "permalink": "https://data.cityofnewyork.us/d/erm2-nwe9",
    }
    row = dataset_row(raw)
    assert row is not None
    assert row["portal"] == "https://data.cityofnewyork.us"
    assert row["description"] == "Hi"
    assert row["modified"] == dt.datetime(2026, 11, 15, 2, 58, 11, tzinfo=dt.UTC)
    assert row["column_names"] == ["a"]
    assert row["domain_metadata"] == [{"key": "k", "value": None}]
    assert row["api_url"] == "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
    assert dataset_row({"resource": {}}) is None


def test_column_rows_flag_system_columns() -> None:
    rows = column_rows("https://x.org", "abcd-1234", VIEW)
    assert rows[1]["duckdb_type"] == "DECIMAL(28,10)"
    assert rows[-1]["is_system"] is True
    assert rows[-1]["duckdb_type"] is None


def test_soql_limit_handling() -> None:
    assert has_limit("SELECT a LIMIT 5")
    assert not has_limit("SELECT a WHERE b = 'limit 5'")
    assert effective_query("SELECT a;", 100) == "SELECT a LIMIT 101"
    assert effective_query("select a limit 3", 100) == "select a limit 3"
    with pytest.raises(SocrataError):
        effective_query("  ", 10)


def test_soql_result_schema() -> None:
    schema = result_schema(["borough", "count"], ["text", "number"])
    assert [f.type for f in schema] == [pa.string(), NUMBER]
    with pytest.raises(SocrataError):
        result_schema([], [])


# ---------------------------------------------------------------- through the worker


def _scalar(client: Client, name: str, values: list[str | None]) -> list[Any]:
    batch = pa.RecordBatch.from_arrays([pa.array(values, type=pa.string())], names=["v"])
    out = list(client.scalar_function(function_name=name, schema_path=SCHEMA, input=iter([batch])))
    return [v for b in out for v in b.column(0).to_pylist()]


def test_scalars_through_worker(client: Client) -> None:
    assert _scalar(client, "soql_quote", ["O'Brien", None, "a\\b"]) == ["'O''Brien'", None, "'a\\\\b'"]
    assert _scalar(client, "soql_like_escape", ["50%", None]) == ["50\\%", None]


# Table functions are not called through vgi.client.Client here: each asks DuckDB for a
# portal-scoped secret at bind time, and the Python Client does not answer secret-scope
# requests. test/sql covers them end to end through DuckDB against the replay server.
