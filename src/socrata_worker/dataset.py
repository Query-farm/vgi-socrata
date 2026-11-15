"""``dataset(portal, dataset_id, ...)``: the rows of one Socrata dataset, with SoQL pushdown.

The output columns are the dataset's columns from ``/api/views/<id>.json``, typed
(see :func:`socrata_worker.soql.arrow_type_for`), plus ``soda_row_id`` (the
system ``:id``).

How a scan reads (chosen on the first ``process`` call and frozen in the state):

* ``count``   no columns are needed at all: one ``$select=count(*)`` request. (DuckDB
  usually still projects one column for ``count(*)``, which then reads that column.)
* ``topn``    DuckDB pushed ``ORDER BY <number/timestamp column> LIMIT n``
  (n <= 50,000): ``$order=<col> <dir>, :id`` with ``$limit``/``$offset`` up to n rows,
  non-null values first (or last, per ``NULLS FIRST``) via two ``$where`` phases.
* ``keyset``  everything else: ``$order=:id`` pages of ``page_rows``, each continuing
  with ``$where=... AND :id > '<last :id>'``. The cursor (last ``:id``) lives in the
  scan state, so a scan resumes in another process (HTTP transport) without the
  deep ``$offset`` skip-scans that make portals time out.
"""

from __future__ import annotations

from dataclasses import dataclass, field
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
from .doc_variants import EXAMPLE_VARIANTS_MD
from .meta import object_tags
from .soql import (
    PUSHABLE_RANGE,
    ROW_ID_COLUMN,
    ROW_ID_FIELD,
    SOCRATA_FIELD_KEY,
    SOCRATA_TYPE_KEY,
    arrow_type_for,
    combine_where,
    field_socrata_type,
    field_soql_name,
    ident,
    string_literal,
    to_arrow_batch,
    where_clauses,
)

_VIEW_CACHE = TtlCache()

DEFAULT_PAGE_ROWS = 1000
MAX_PAGE_ROWS = 50_000
TOPN_MAX = 50_000
COUNT_BATCH = 50_000


def fetch_view(client: SocrataClient, dataset_id: str) -> dict[str, Any]:
    """Dataset metadata from ``/api/views/<id>.json``, cached briefly per process."""
    four = require_four_by_four(dataset_id)
    key = f"{client.portal}|{client.base_url}|{bool(client.app_token)}|{four}"
    hit = _VIEW_CACHE.get(key)
    if hit is not None:
        return dict(hit)
    body = client.get(f"/api/views/{four}.json").body
    if not isinstance(body, dict) or not isinstance(body.get("columns"), list):
        raise SocrataError(f"unexpected metadata for dataset {dataset_id!r} on {client.portal}")
    _VIEW_CACHE.put(key, body)
    return dict(body)


def is_user_column(c: dict[str, Any]) -> bool:
    """Return True for a regular data column (not a system or computed-region column)."""
    name = str(c.get("fieldName") or "")
    return bool(name) and not name.startswith(":")


def column_comment(c: dict[str, Any]) -> str:
    """Column comment: display name, description and Socrata type."""
    label = str(c.get("name") or c.get("fieldName") or "")
    desc = " ".join(str(c.get("description") or "").split())
    text = label if not desc else f"{label}: {desc}"
    return f"{text} (Socrata type {c.get('dataTypeName')})"


def schema_for_view(view: dict[str, Any]) -> pa.Schema:
    """Return the DuckDB schema of a dataset's rows (its columns plus ``soda_row_id``)."""
    out: list[pa.Field[Any]] = []
    seen: set[str] = set()
    for c in view.get("columns") or []:
        if not is_user_column(c):
            continue
        name = str(c["fieldName"])
        if name in seen or name == ROW_ID_COLUMN:
            continue
        seen.add(name)
        stype = str(c.get("dataTypeName") or "text").lower()
        meta = {b"comment": column_comment(c).encode(), SOCRATA_TYPE_KEY: stype.encode()}
        out.append(pa.field(name, arrow_type_for(stype), nullable=True, metadata=meta))
    if not out:
        raise SocrataError(f"dataset {view.get('id')!r} has no columns (is it a tabular dataset?)")
    out.append(
        pa.field(
            ROW_ID_COLUMN,
            pa.string(),
            nullable=True,
            metadata={
                b"comment": b"Socrata system row id (:id); stable per row, used as the paging cursor.",
                SOCRATA_TYPE_KEY: b"row_identifier",
                SOCRATA_FIELD_KEY: ROW_ID_FIELD.encode(),
            },
        )
    )
    return pa.schema(out)


def pushable_columns(schema: pa.Schema) -> dict[str, tuple[str, str]]:
    """Output column -> (SoQL field, Socrata type) for every column filters may be pushed on."""
    return {f.name: (field_soql_name(f), field_socrata_type(f)) for f in schema}


@dataclass(slots=True, frozen=True, kw_only=True)
class DatasetArgs:
    """Arguments for ``dataset(portal, dataset_id, soql_where :=, page_rows :=)``."""

    portal: Annotated[str, Arg(0, doc=PORTAL_DOC, pattern=PORTAL_PATTERN)]
    dataset_id: Annotated[str, Arg(1, doc=DATASET_ID_DOC, pattern=DATASET_ID_PATTERN)]
    soql_where: Annotated[
        str | None,
        Arg(
            "soql_where",
            default=None,
            doc=(
                "Extra raw SoQL condition ANDed into $where, for what SQL filters cannot express server-side, "
                "e.g. 'within_circle(location, 40.75, -73.98, 500)' or \"upper(descriptor) like '%NOISE%'\"."
            ),
        ),
    ]
    page_rows: Annotated[
        int,
        Arg(
            "page_rows",
            default=DEFAULT_PAGE_ROWS,
            doc="Rows per request (1 to 50,000; default 1,000). Each page is one keyset step ordered by :id.",
            ge=1,
            le=MAX_PAGE_ROWS,
        ),
    ]


@dataclass(kw_only=True)
class ScanState(ArrowSerializableDataclass):
    """Resumable cursor for a dataset scan. Holds the frozen query, never credentials."""

    mode: str = ""
    select: str = ""
    where: str = ""
    order: str = ""
    phases: list[str] = field(default_factory=list)
    phase: int = 0
    offset: int = 0
    last_id: str = ""
    total: int = -1
    emitted: int = 0
    row_limit: int = -1


def _start(params: ProcessParams[DatasetArgs], state: ScanState, client: SocrataClient) -> None:
    """Freeze the SoQL query and choose the read mode."""
    a = params.args
    schema = params.output_schema
    filters = params.current_pushdown_filters
    where = combine_where([a.soql_where, *where_clauses(filters, pushable_columns(schema))]) or ""
    state.where = where
    path = f"/resource/{require_four_by_four(a.dataset_id)}.json"
    if not schema.names:
        body = client.get(path, {"$select": "count(*) AS n", "$where": where or None}).body
        state.total = int((body or [{}])[0].get("n") or 0) if isinstance(body, list) else 0
        state.mode = "count"
        return

    hint = _order_hint(params)
    select = [ident(field_soql_name(f)) for f in schema]
    if hint is not None:
        col, direction, nulls_first, limit = hint
        state.select = ",".join(select)
        state.order = f"{ident(col)} {direction},{ident(ROW_ID_FIELD)}"
        not_null = combine_where([where, f"{ident(col)} IS NOT NULL"]) or ""
        is_null = combine_where([where, f"{ident(col)} IS NULL"]) or ""
        state.phases = [is_null, not_null] if nulls_first else [not_null, is_null]
        state.row_limit = limit
        state.mode = "topn"
        return

    if ident(ROW_ID_FIELD) not in select:
        select.append(ident(ROW_ID_FIELD))
    state.select = ",".join(select)
    state.order = ident(ROW_ID_FIELD)
    state.mode = "keyset"


def _order_hint(params: ProcessParams[DatasetArgs]) -> tuple[str, str, bool, int] | None:
    """DuckDB's ORDER BY ... LIMIT n hint, when the portal can sort that column exactly."""
    init = params.init_call
    if init is None or not init.order_by_column_name or init.order_by_limit is None:
        return None
    limit = int(init.order_by_limit)
    if not 0 < limit <= TOPN_MAX:
        return None
    name = init.order_by_column_name
    schema = params.output_schema
    if name not in schema.names:
        return None
    f = schema.field(name)
    if field_socrata_type(f) not in PUSHABLE_RANGE:
        return None  # text collation on the portal may differ from DuckDB's
    direction = "DESC" if getattr(init.order_by_direction, "name", "ASC") == "DESC" else "ASC"
    nulls_first = getattr(init.order_by_null_order, "name", "NULLS_LAST") == "NULLS_FIRST"
    return field_soql_name(f), direction, nulls_first, limit


def run_scan(params: ProcessParams[DatasetArgs], state: ScanState, out: OutputCollector, client: SocrataClient) -> None:
    """Emit the next batch of a dataset scan, or finish it."""
    if not state.mode:
        _start(params, state, client)
    if state.mode == "done":
        out.finish()
        return
    path = f"/resource/{require_four_by_four(params.args.dataset_id)}.json"
    page = int(params.args.page_rows)

    if state.mode == "count":
        remaining = state.total - state.emitted
        if remaining <= 0:
            state.mode = "done"
            out.finish()
            return
        n = min(remaining, COUNT_BATCH)
        empty: Any = pa.array([{}] * n, type=pa.struct([]))
        out.emit(pa.RecordBatch.from_struct_array(empty))
        state.emitted += n
        return

    if state.mode == "keyset":
        cursor = f"{ident(ROW_ID_FIELD)} > {string_literal(state.last_id)}" if state.last_id else None
        rows = _get_rows(
            client,
            path,
            {
                "$select": state.select,
                "$where": combine_where([state.where, cursor]),
                "$order": state.order,
                "$limit": page,
            },
        )
        if len(rows) < page:
            state.mode = "done"
        if rows:
            last = rows[-1].get(ROW_ID_FIELD)
            if not last:
                raise SocrataError("the portal did not return :id for this dataset; keyset paging is impossible")
            state.last_id = str(last)
            state.emitted += len(rows)
            out.emit(to_arrow_batch(rows, params.output_schema))
            return
        out.finish()
        return

    # topn
    phases = state.phases
    while state.phase < len(phases) and state.emitted < state.row_limit:
        limit = min(page, state.row_limit - state.emitted)
        rows = _get_rows(
            client,
            path,
            {
                "$select": state.select,
                "$where": phases[state.phase] or None,
                "$order": state.order,
                "$limit": limit,
                "$offset": state.offset or None,
            },
        )
        state.offset += len(rows)
        state.emitted += len(rows)
        if len(rows) < limit:
            state.phase += 1
            state.offset = 0
        if rows:
            out.emit(to_arrow_batch(rows, params.output_schema))
            return
    state.mode = "done"
    out.finish()


def _get_rows(client: SocrataClient, path: str, query: dict[str, Any]) -> list[dict[str, Any]]:
    body = client.get(path, query).body
    if not isinstance(body, list):
        raise SocrataError(f"unexpected response from {client.portal}{path}: expected a JSON array of rows")
    return [r for r in body if isinstance(r, dict)]


@init_single_worker
class Dataset(TableFunctionGenerator[DatasetArgs, ScanState]):
    """Read a dataset's rows with $select / $where / $order pushdown and keyset paging."""

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "dataset"
        description = "Rows of one Socrata dataset, typed from its metadata, with SoQL pushdown"
        filter_pushdown = True
        auto_apply_filters = True
        projection_pushdown = True
        tags = {
            **object_tags(
                title="Dataset Rows",
                category="data",
                keywords="socrata, tyler data & insights, soda, soql, open data, rows, dataset, pushdown, 311",
                doc_llm=(
                    "Read the rows of one dataset of a Socrata (Tyler Data & Insights) portal, given the portal "
                    "domain and the dataset's four-by-four id (e.g. 'erm2-nwe9'; find it with datasets()). Columns "
                    "are the dataset's columns (see dataset_columns(portal, dataset_id)) plus soda_row_id, typed: "
                    "number/money/percent `DECIMAL(28,10)`, double `DOUBLE`, checkbox `BOOLEAN`, calendar_date "
                    "and floating_timestamp `TIMESTAMP` (local time, no zone), fixed_timestamp `TIMESTAMPTZ`, "
                    "point/line/polygon GeoJSON `JSON`, location `STRUCT(latitude, longitude, address, city, "
                    "state, zip)`, url `STRUCT(url, description)`, text and others `VARCHAR`. "
                    "Selected columns become SoQL `$select`; WHERE filters (= <> IN IS NULL starts_with on text; "
                    "ranges on numbers and timestamps; = on checkbox) become `$where`; ORDER BY <number or "
                    "timestamp column> LIMIT n (with no WHERE) is sorted and limited by the portal. "
                    "Rows are paged with a keyset cursor on :id (page_rows := rows per request, default 1000), so "
                    "filter first on big datasets. soql_where := adds a raw SoQL condition (within_circle, like, "
                    "cross-column OR). For server-side GROUP BY use soql()."
                ),
                doc_md=(
                    "## dataset(portal, dataset_id, soql_where :=, page_rows :=)\n\n"
                    "All rows of a dataset, with columns typed from its metadata.\n\n"
                    "**Pushdown.** Projections, filters and `ORDER BY ... LIMIT n` (without `WHERE`) on numeric and "
                    "timestamp columns run on the portal as SoQL. Paging uses a keyset cursor on the system `:id` "
                    "column (never deep `$offset`), carried in the scan state so long scans resume."
                ),
            ),
            "vgi.result_dynamic_columns_md": (
                "One column per dataset column, named by its field name as in dataset_columns(portal, dataset_id); "
                "the type follows the Socrata data type:\n\n"
                "| Name | Type | Description |\n"
                "|---|---|---|\n"
                "| <number / money / percent column> | DECIMAL(28,10) | Exact number; NULL if it needs more than "
                "18 integer digits. |\n"
                "| <double column> | DOUBLE | Floating-point number. |\n"
                "| <checkbox column> | BOOLEAN | Checkbox. |\n"
                "| <calendar_date / floating_timestamp column> | TIMESTAMP | Local (floating) date and time. |\n"
                "| <fixed_timestamp column> | TIMESTAMPTZ | Absolute timestamp, UTC. |\n"
                "| <point / line / polygon / multi* column> | JSON | GeoJSON geometry (use ST_GeomFromGeoJSON). |\n"
                "| <location column> | STRUCT(latitude DOUBLE, longitude DOUBLE, address VARCHAR, city VARCHAR, "
                "state VARCHAR, zip VARCHAR) | Legacy location. |\n"
                "| <url column> | STRUCT(url VARCHAR, description VARCHAR) | Link. |\n"
                "| <phone column> | STRUCT(phone_number VARCHAR, phone_type VARCHAR) | Phone number. |\n"
                "| <text column> | VARCHAR | Text and any other type. |\n"
                "| soda_row_id | VARCHAR | System row id (:id), the paging cursor. |" + EXAMPLE_VARIANTS_MD
            ),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT complaint_type, count(*) AS requests "
                    "FROM socrata.main.dataset('data.cityofnewyork.us', 'erm2-nwe9') "
                    "WHERE created_date >= TIMESTAMP '2024-01-01 00:00:00' "
                    "AND created_date < TIMESTAMP '2024-01-01 00:10:00' AND borough = 'BROOKLYN' "
                    "GROUP BY complaint_type ORDER BY requests DESC, complaint_type"
                ),
                description="NYC 311 complaint types in Brooklyn in the first ten minutes of 2024 (filters run on "
                "the portal)",
            ),
            FunctionExample(
                sql=(
                    "SELECT branch_, website.url AS website, location.latitude, location.longitude "
                    "FROM socrata.main.dataset('data.cityofchicago.org', 'x8fc-8rcq') "
                    "WHERE starts_with(branch_, 'Ch') ORDER BY branch_"
                ),
                description="Chicago public library branches whose name starts with 'Ch', with website and coordinates",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[DatasetArgs]) -> BindResponse:
        """Fetch the dataset metadata and map its columns to typed DuckDB columns."""
        portal = portal_arg(params)
        if not request_secrets(params, portal):
            return BindResponse(output_schema=pa.schema([pa.field("pending", pa.string())]))
        view = fetch_view(bind_client(params, portal), params.args.dataset_id)
        return BindResponse(output_schema=schema_for_view(view))

    @classmethod
    def initial_state(cls, params: ProcessParams[DatasetArgs]) -> ScanState:
        """Return a fresh, unstarted scan."""
        return ScanState()

    @classmethod
    def process(cls, params: ProcessParams[DatasetArgs], state: ScanState, out: OutputCollector) -> None:
        """Emit the next page of rows."""
        run_scan(params, state, out, process_client(params, portal_arg(params)))
