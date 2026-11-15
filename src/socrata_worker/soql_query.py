"""``soql(portal, dataset_id, query)``: run a full SoQL query on the portal.

Aggregations (``count(*)``, ``sum``, ``avg``, ``GROUP BY``, ``date_trunc_ym`` ...)
run server-side, so a summary of a 40-million-row dataset costs one request. The
query runs at bind time so the result columns can be typed: the portal reports
each result column's name and Socrata type in the ``X-SODA2-Fields`` /
``X-SODA2-Types`` response headers, which map to DuckDB types exactly as in
``dataset()``.

When the query has no ``LIMIT``, ``LIMIT <max_rows + 1>`` is appended and a result
larger than ``max_rows`` is an error rather than a silent truncation.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Annotated, Any

import pyarrow as pa
from vgi.arguments import Arg
from vgi.invocation import BindResponse
from vgi.metadata import FunctionExample
from vgi.table_function import (
    BindParams,
    OutputCollector,
    ProcessParams,
    TableFunctionGenerator,
    init_single_worker,
)
from vgi_rpc import ArrowSerializableDataclass

from .client import SocrataClient, SocrataError, TtlCache, require_four_by_four
from .common import (
    DATASET_ID_DOC,
    DATASET_ID_PATTERN,
    PORTAL_DOC,
    PORTAL_PATTERN,
    bind_client,
    portal_arg,
    process_client,
    request_secrets,
)
from .meta import object_tags
from .soql import SOCRATA_FIELD_KEY, SOCRATA_TYPE_KEY, arrow_type_for, to_arrow_batch

DEFAULT_MAX_ROWS = 10_000
_RESULTS = TtlCache(ttl=300.0, max_items=16)
_LIMIT_RE = re.compile(r"\blimit\s+\d+", re.I)
_STRING_RE = re.compile(r"'(?:[^']|'')*'")


def has_limit(query: str) -> bool:
    """Return True when the SoQL text has a LIMIT clause outside string literals."""
    return bool(_LIMIT_RE.search(_STRING_RE.sub("''", query)))


def effective_query(query: str, max_rows: int) -> str:
    """Return the query sent to the portal: the user's, plus ``LIMIT max_rows+1`` when it has none."""
    q = (query or "").strip().rstrip(";").strip()
    if not q:
        raise SocrataError("query is empty: pass a SoQL query such as 'SELECT borough, count(*) GROUP BY borough'")
    return q if has_limit(q) else f"{q} LIMIT {max_rows + 1}"


@dataclass(slots=True, frozen=True, kw_only=True)
class SoqlArgs:
    """Arguments for ``soql(portal, dataset_id, query, max_rows :=)``."""

    portal: Annotated[str, Arg(0, doc=PORTAL_DOC, pattern=PORTAL_PATTERN)]
    dataset_id: Annotated[str, Arg(1, doc=DATASET_ID_DOC, pattern=DATASET_ID_PATTERN)]
    query: Annotated[
        str,
        Arg(
            2,
            doc=(
                "A SoQL query without FROM, e.g. 'SELECT borough, count(*) AS n WHERE created_date >= "
                "\\'2024-01-01\\' GROUP BY borough ORDER BY n DESC'."
            ),
        ),
    ]
    max_rows: Annotated[
        int,
        Arg(
            "max_rows",
            default=DEFAULT_MAX_ROWS,
            doc="When the query has no LIMIT: fail if it returns more than this many rows (default 10,000).",
            ge=1,
            le=1_000_000,
        ),
    ]


@dataclass(kw_only=True)
class SoqlState(ArrowSerializableDataclass):
    """A single batch."""

    done: bool = False


def run_query(client: SocrataClient, a: SoqlArgs) -> tuple[list[dict[str, Any]], list[str], list[str]]:
    """Run the query; return rows, result field names and their Socrata types."""
    path = f"/resource/{require_four_by_four(a.dataset_id)}.json"
    q = effective_query(a.query, int(a.max_rows))
    resp = client.get(path, {"$query": q})
    if not isinstance(resp.body, list):
        raise SocrataError(f"unexpected response from {client.portal}{path}: expected a JSON array of rows")
    rows = [r for r in resp.body if isinstance(r, dict)]
    if not has_limit(a.query) and len(rows) > int(a.max_rows):
        raise SocrataError(
            f"the query returned more than max_rows={a.max_rows} rows; add a LIMIT, aggregate further, "
            "or raise max_rows :="
        )
    names = _header_list(resp.headers.get("x-soda2-fields"))
    types = _header_list(resp.headers.get("x-soda2-types"))
    if not names or len(names) != len(types):
        names, types = [], []
        for r in rows:
            for k in r:
                if k not in names:
                    names.append(k)
        types = ["text"] * len(names)
    return rows, names, types


def _header_list(value: str | None) -> list[str]:
    if not value:
        return []
    try:
        parsed = json.loads(value)
    except ValueError:
        return []
    return [str(v) for v in parsed] if isinstance(parsed, list) else []


def result_schema(names: list[str], types: list[str]) -> pa.Schema:
    """Typed schema of a SoQL result."""
    if not names:
        raise SocrataError("the query returned no columns and no rows; cannot determine the result columns")
    fields = []
    for n, t in zip(names, types, strict=True):
        meta = {
            b"comment": f"SoQL result column {n} (Socrata type {t})".encode(),
            SOCRATA_TYPE_KEY: t.lower().encode(),
            SOCRATA_FIELD_KEY: n.encode(),
        }
        fields.append(pa.field(n, arrow_type_for(t), metadata=meta))
    return pa.schema(fields)


def _cache_key(client: SocrataClient, a: SoqlArgs) -> str:
    return json.dumps([client.portal, client.base_url, bool(client.app_token), a.dataset_id, a.query, a.max_rows])


@init_single_worker
class Soql(TableFunctionGenerator[SoqlArgs, SoqlState]):
    """Run a SoQL query (aggregates, GROUP BY) on the portal and return typed rows."""

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "soql"
        description = "Run a server-side SoQL query (aggregates, GROUP BY) on a Socrata dataset"
        tags = {
            **object_tags(
                title="Server-Side SoQL Query",
                category="data",
                keywords="socrata, soql, aggregate, group by, count, sum, server-side, analytics, $query",
                doc_llm=(
                    "Run a full SoQL query on the portal and return its rows, typed. Arguments: the portal, the "
                    "dataset_id (four-by-four) and a SoQL query, which is like a SQL query without a FROM clause, "
                    "for example:\n\n```sql\nSELECT borough, count(*) AS n WHERE created_date >= "
                    "'2024-01-01T00:00:00' GROUP BY borough\n```\n\n"
                    "SoQL string literals use single quotes; timestamps compare with text literals such as "
                    "'2024-01-01T00:00:00'; useful functions include count, sum, avg, min, max, date_trunc_ym, "
                    "upper, starts_with and within_circle. Result columns are named by alias (an unaliased count "
                    "is 'count') and typed from the portal's own type report (numbers `DECIMAL(28,10)`, timestamps "
                    "`TIMESTAMP`, text `VARCHAR`). A query without a limit may return at most max_rows (default "
                    "10,000) rows, otherwise it fails. Use it to summarize huge datasets in one request instead of "
                    "reading every row with dataset()."
                ),
                doc_md=(
                    "## soql(portal, dataset_id, query, max_rows :=)\n\n"
                    "A server-side SoQL query, e.g. NYC 311 requests per borough over 40 million rows in one "
                    "request. Column types come from the portal (`X-SODA2-Types`)."
                ),
            ),
            "vgi.result_dynamic_columns_md": (
                "One column per SELECT expression, named by its alias (or the portal's generated name, e.g. "
                "'count' for count(*)), typed from the portal's report:\n\n"
                "| Name | Type | Description |\n"
                "|---|---|---|\n"
                "| <text expression> | VARCHAR | Text results and group keys. |\n"
                "| <count / sum / avg / number expression> | DECIMAL(28,10) | Numeric results. |\n"
                "| <timestamp expression, e.g. date_trunc_ym(created_date)> | TIMESTAMP | Floating timestamps. |\n"
                "| <checkbox expression> | BOOLEAN | Booleans. |\n"
                "| <point / polygon expression> | JSON | GeoJSON geometry. |\n\n"
                "For the NYC 311 per-borough example:\n\n"
                "| Name | Type | Description |\n"
                "|---|---|---|\n"
                "| borough | VARCHAR | The borough (group key). |\n"
                "| requests | DECIMAL(28,10) | count(*) per borough. |\n\n"
                "For the Chicago arrests example:\n\n"
                "| Name | Type | Description |\n"
                "|---|---|---|\n"
                "| primary_type | VARCHAR | Crime type (group key). |\n"
                "| arrests | DECIMAL(28,10) | count(*) of arrests. |"
            ),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT borough, requests::BIGINT AS requests FROM socrata.main.soql('data.cityofnewyork.us', "
                    "'erm2-nwe9', 'SELECT borough, count(*) AS requests WHERE created_date >= ''2024-01-01T00:00:00'' "
                    "AND created_date < ''2024-01-02T00:00:00'' GROUP BY borough ORDER BY borough') ORDER BY borough"
                ),
                description="NYC 311 requests per borough on 1 January 2024, counted by the portal",
            ),
            FunctionExample(
                sql=(
                    "SELECT primary_type, arrests::BIGINT AS arrests FROM socrata.main.soql("
                    "'data.cityofchicago.org', 'ijzp-q8t2', 'SELECT primary_type, count(*) AS arrests "
                    "WHERE arrest = true AND date >= ''2024-01-01T00:00:00'' AND date < ''2024-01-02T00:00:00'' "
                    "GROUP BY primary_type ORDER BY arrests DESC, primary_type LIMIT 5') "
                    "ORDER BY arrests DESC, primary_type"
                ),
                description="The five crime types with the most arrests in Chicago on 1 January 2024",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[SoqlArgs]) -> BindResponse:
        """Run the query and type its result columns."""
        portal = portal_arg(params)
        if not request_secrets(params, portal):
            return BindResponse(output_schema=pa.schema([pa.field("pending", pa.string())]))
        client = bind_client(params, portal)
        rows, names, types = run_query(client, params.args)
        _RESULTS.put(_cache_key(client, params.args), rows)
        return BindResponse(output_schema=result_schema(names, types))

    @classmethod
    def initial_state(cls, params: ProcessParams[SoqlArgs]) -> SoqlState:
        """Not yet emitted."""
        return SoqlState()

    @classmethod
    def process(cls, params: ProcessParams[SoqlArgs], state: SoqlState, out: OutputCollector) -> None:
        """Emit the (cached or re-run) result in one batch."""
        if state.done:
            out.finish()
            return
        client = process_client(params, portal_arg(params))
        rows = _RESULTS.get(_cache_key(client, params.args))
        if rows is None:
            rows = run_query(client, params.args)[0]
        out.emit(to_arrow_batch(rows, params.output_schema))
        state.done = True
