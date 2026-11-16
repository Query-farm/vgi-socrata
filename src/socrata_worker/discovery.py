"""Catalog functions: ``datasets``, ``search_datasets`` (Discovery API) and ``dataset_columns``.

The Socrata Discovery API indexes every public asset of every portal, split by
region: ``https://api.us.socrata.com`` (US portals) and ``https://api.eu.socrata.com``
(European portals such as opendata.rdw.nl). ``datasets(portal)`` finds the right
region automatically (it asks the US index first and falls back to the EU one when
the US index does not know the domain); ``search_datasets`` searches one region's
federated index across all portals.
"""

from __future__ import annotations

import datetime as dt
import logging
from dataclasses import dataclass
from typing import Annotated, Any, ClassVar

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

from .client import (
    DISCOVERY_HOSTS,
    HttpStatusError,
    SocrataClient,
    TtlCache,
    dataset_page_url,
    portal_domain,
    require_four_by_four,
    resource_url,
)
from .common import (
    DATASET_ID_DOC,
    DATASET_ID_PATTERN,
    PORTAL_DOC,
    PORTAL_PATTERN,
    portal_arg,
    process_client,
    request_secrets,
    strip_html,
)
from .dataset import fetch_view, is_user_column
from .meta import column, object_tags, result_columns
from .soql import TIMESTAMPTZ, arrow_type_for, duckdb_type_name

log = logging.getLogger(__name__)

LIST = pa.list_(pa.string())
KV_LIST = pa.list_(pa.struct([pa.field("key", pa.string()), pa.field("value", pa.string())]))

PAGE = 100
ASSET_TYPES = ("dataset", "map", "chart", "filter", "file", "href", "story", "measure", "all")
MAX_OFFSET = 10_000  # the Discovery API's offset + limit ceiling

_REGION_CACHE = TtlCache(ttl=3600.0, max_items=256)

DATASET_COLUMNS: list[pa.Field[Any]] = [
    column(
        "portal",
        pa.string(),
        "Base URL of the portal the dataset belongs to, e.g. https://data.cityofnewyork.us.",
        nullable=False,
    ),
    column(
        "dataset_id",
        pa.string(),
        "Four-by-four dataset id, the key for dataset(), dataset_columns() and soql().",
        nullable=False,
    ),
    column("title", pa.string(), "Dataset title."),
    column("description", pa.string(), "Dataset description as plain text (HTML tags removed)."),
    column("asset_type", pa.string(), "Asset type: dataset, map, chart, filter (a saved view), file, href, ..."),
    column("category", pa.string(), "The portal's category for the dataset, e.g. 'Social Services'."),
    column("tags", LIST, "The portal's tags for the dataset."),
    column("publisher", pa.string(), "Attribution: the agency or organization that publishes the data."),
    column("owner", pa.string(), "Display name of the portal account that owns the dataset."),
    column("license", pa.string(), "License name, when the portal records one."),
    column("provenance", pa.string(), "'official' or 'community'."),
    column("created", TIMESTAMPTZ, "When the dataset was created on the portal (UTC)."),
    column("modified", TIMESTAMPTZ, "When the dataset (data or metadata) last changed (UTC)."),
    column("data_updated", TIMESTAMPTZ, "When the rows last changed (UTC)."),
    column("metadata_updated", TIMESTAMPTZ, "When the metadata last changed (UTC)."),
    column("page_views_last_month", pa.int64(), "Page views in the last month."),
    column("page_views_total", pa.int64(), "Page views since publication."),
    column("download_count", pa.int64(), "Number of downloads, when reported."),
    column("column_names", LIST, "Field names of the dataset's columns (system columns excluded)."),
    column("domain_metadata", KV_LIST, "Portal-specific metadata fields as (key, value) pairs."),
    column("url", pa.string(), "The dataset's page on the portal: <portal>/d/<dataset_id>.", nullable=False),
    column("api_url", pa.string(), "SODA endpoint: <portal>/resource/<dataset_id>.json.", nullable=False),
]
OUTPUT_SCHEMA = pa.schema(DATASET_COLUMNS)


def _ts(value: Any) -> dt.datetime | None:
    if not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.UTC)
    return parsed.astimezone(dt.UTC)


def _int(value: Any) -> int | None:
    try:
        return None if value is None else int(value)
    except TypeError, ValueError:
        return None


def dataset_row(raw: dict[str, Any]) -> dict[str, Any] | None:
    """Map one Discovery API result to an output row (None if it has no domain/id)."""
    res = raw.get("resource") or {}
    cls = raw.get("classification") or {}
    meta = raw.get("metadata") or {}
    domain = str(meta.get("domain") or "")
    dataset_id = str(res.get("id") or "")
    if not domain or not dataset_id:
        return None
    portal = f"https://{domain}"
    views = res.get("page_views") or {}
    tags = cls.get("domain_tags") or cls.get("tags") or []
    category = cls.get("domain_category") or next(iter(cls.get("categories") or []), None)
    names = [str(n) for n in res.get("columns_field_name") or [] if not str(n).startswith(":")]
    kv = [
        {"key": str(m.get("key")), "value": None if m.get("value") is None else str(m.get("value"))}
        for m in cls.get("domain_metadata") or []
        if isinstance(m, dict)
    ]
    return {
        "portal": portal,
        "dataset_id": dataset_id,
        "title": res.get("name"),
        "description": strip_html(res.get("description")),
        "asset_type": res.get("type"),
        "category": category,
        "tags": [str(t) for t in tags],
        "publisher": res.get("attribution"),
        "owner": (raw.get("owner") or {}).get("display_name"),
        "license": meta.get("license"),
        "provenance": res.get("provenance"),
        "created": _ts(res.get("createdAt")),
        "modified": _ts(res.get("updatedAt")),
        "data_updated": _ts(res.get("data_updated_at")),
        "metadata_updated": _ts(res.get("metadata_updated_at")),
        "page_views_last_month": _int(views.get("page_views_last_month")),
        "page_views_total": _int(views.get("page_views_total")),
        "download_count": _int(res.get("download_count")),
        "column_names": names,
        "domain_metadata": kv,
        "url": raw.get("permalink") or dataset_page_url(portal, dataset_id),
        "api_url": resource_url(portal, dataset_id),
    }


@dataclass(kw_only=True)
class CatalogState(ArrowSerializableDataclass):
    """Offset cursor of a Discovery API listing (plus the resolved region)."""

    region: str = ""
    probing: bool = False
    offset: int = 0
    done: bool = False


def _pushed_ids(params: ProcessParams[Any]) -> list[str]:
    """Dataset ids from ``dataset_id = 'x'`` / ``IN (...)`` filters (pushed as ``ids=``)."""
    for flt in getattr(params.current_pushdown_filters, "filters", None) or ():
        if getattr(flt, "column_name", None) != "dataset_id":
            continue
        kind = type(flt).__name__
        if kind == "ConstantFilter" and getattr(flt.op, "value", "") == "eq":
            v = flt.value.as_py()
            return [v] if isinstance(v, str) else []
        if kind == "InFilter":
            return [v for v in flt.values.to_pylist() if isinstance(v, str)]
    return []


def _catalog_query(a: Any, domain: str | None, ids: list[str], offset: int) -> dict[str, Any]:
    asset = (a.asset_type or "").strip().lower()
    return {
        "domains": domain,
        "search_context": domain,
        "q": a.q,
        "categories": a.category,
        "tags": a.tag,
        "only": None if asset in ("", "all") else asset,
        "ids": ids or None,
        "limit": PAGE,
        "offset": offset or None,
    }


def _is_unknown_domain(err: HttpStatusError) -> bool:
    return err.status in (400, 404) and "domain not found" in err.body.lower()


def known_region(domain: str, wanted: str) -> tuple[str, bool]:
    """Return ``(region, probing)``: the requested or cached region, else ``('us', True)``.

    ``probing`` means the region is a guess: the first page is asked of the US
    index and, if it does not know the domain, of the EU index.
    """
    if wanted in DISCOVERY_HOSTS:
        return wanted, False
    hit = _REGION_CACHE.get(domain)
    if hit:
        return str(hit), False
    return "us", True


def catalog_step(
    params: ProcessParams[Any], state: CatalogState, out: OutputCollector, domain: str | None, max_rows: int
) -> None:
    """Emit the next page of a Discovery listing, or finish."""
    if state.done:
        out.finish()
        return
    client: SocrataClient = process_client(params, DISCOVERY_HOSTS[state.region])
    ids = _pushed_ids(params)
    remaining = max_rows - state.offset
    if remaining <= 0 or state.offset >= MAX_OFFSET:
        if state.offset >= MAX_OFFSET:
            log.warning(
                "Discovery API listings stop at %d results; narrow the search with q := or category :=", MAX_OFFSET
            )
        state.done = True
        out.finish()
        return
    query = _catalog_query(params.args, domain, ids, state.offset)
    query["limit"] = min(PAGE, remaining)
    try:
        body = client.get("/api/catalog/v1", query).body or {}
    except HttpStatusError as err:
        if not (state.probing and domain and _is_unknown_domain(err)):
            raise
        log.info("the US Discovery index does not know %s; asking the EU index", domain)
        state.region, state.probing = "eu", False
        client = process_client(params, DISCOVERY_HOSTS["eu"])
        try:
            body = client.get("/api/catalog/v1", query).body or {}
        except HttpStatusError as err2:
            if _is_unknown_domain(err2):
                raise HttpStatusError(
                    f"the Socrata Discovery API (US and EU) does not know the domain {domain!r}; "
                    "is it a Socrata portal?",
                    404,
                    err2.body,
                ) from err2
            raise
    if state.probing and domain:
        _REGION_CACHE.put(domain, state.region)
        state.probing = False
    results = [r for r in body.get("results") or [] if isinstance(r, dict)]
    state.offset += len(results)
    if len(results) < int(query["limit"]):
        state.done = True
    rows = [row for row in (dataset_row(r) for r in results) if row is not None]
    if rows:
        out.emit(_batch(rows, params))
        return
    state.done = True
    out.finish()


def _batch(rows: list[dict[str, Any]], params: ProcessParams[Any]) -> pa.RecordBatch:
    schema = params.output_schema
    return pa.RecordBatch.from_pylist([{n: r.get(n) for n in schema.names} for r in rows], schema=schema)


CATALOG_DOC_COLUMNS = (
    "portal, dataset_id, title, description (plain text), asset_type, category, tags, publisher (attribution), "
    "owner, license, provenance, created, modified, data_updated, metadata_updated, page_views_last_month, "
    "page_views_total, download_count, column_names, domain_metadata, url, api_url"
)


@dataclass(slots=True, frozen=True, kw_only=True)
class DatasetsArgs:
    """Arguments for ``datasets(portal, q :=, category :=, tag :=, asset_type :=, region :=, max_rows :=)``."""

    portal: Annotated[str, Arg(0, doc=PORTAL_DOC, pattern=PORTAL_PATTERN)]
    q: Annotated[str | None, Arg("q", default=None, doc="Full-text search over titles, descriptions, columns, tags.")]
    category: Annotated[
        str | None, Arg("category", default=None, doc="Only datasets in this portal category, e.g. 'Transportation'.")
    ]
    tag: Annotated[str | None, Arg("tag", default=None, doc="Only datasets with this portal tag, e.g. '311'.")]
    asset_type: Annotated[
        str,
        Arg(
            "asset_type",
            default="dataset",
            doc="Asset type to list: 'dataset' (default), 'map', 'chart', 'filter', 'file', 'href', or 'all'.",
            choices=ASSET_TYPES,
        ),
    ]
    region: Annotated[
        str,
        Arg(
            "region",
            default="auto",
            doc="Discovery API region: 'us', 'eu', or 'auto' (default: whichever index knows the portal).",
            choices=("auto", "us", "eu"),
        ),
    ]
    max_rows: Annotated[
        int,
        Arg("max_rows", default=MAX_OFFSET, doc="Stop after this many datasets (at most 10,000).", ge=1, le=MAX_OFFSET),
    ]


@init_single_worker
class Datasets(TableFunctionGenerator[DatasetsArgs, CatalogState]):
    """List a portal's datasets from the Socrata Discovery API."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = OUTPUT_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "datasets"
        description = "Datasets published on a Socrata portal, from the Discovery API (US and EU)"
        filter_pushdown = True
        auto_apply_filters = True
        tags = {
            **object_tags(
                title="Portal Dataset Catalog",
                category="catalog",
                keywords="socrata, tyler data & insights, discovery api, open data, catalog, datasets, search, 4x4",
                doc_llm=(
                    "List the public datasets of one Socrata (Tyler Data & Insights) open-data portal, one row per "
                    f"dataset: {CATALOG_DOC_COLUMNS}. `portal` is a domain such as 'data.cityofnewyork.us'. "
                    "Optional named arguments: q := full-text search, category := / tag := exact portal category "
                    "or tag, asset_type := 'dataset' (default) | 'map' | 'chart' | 'filter' | 'all', region := "
                    "'auto' (default) | 'us' | 'eu', max_rows := cap (10,000 max). WHERE dataset_id = / IN is "
                    "pushed to the API; other filters run in DuckDB. Use dataset_id (a four-by-four such as "
                    "'erm2-nwe9') with dataset(), dataset_columns() and soql()."
                ),
                doc_md=(
                    "## datasets(portal, q :=, category :=, tag :=, asset_type :=, region :=, max_rows :=)\n\n"
                    "One row per dataset on the portal, from the Socrata Discovery API. US and EU portals are "
                    "served by different indexes; `region := 'auto'` finds the right one.\n\n"
                    "The columns `portal`, `dataset_id`, `title`, `description`, `publisher`, `modified` and `url` "
                    "match the other open-data workers (CKAN, Opendatasoft) so catalogs can be combined with "
                    "`UNION ALL BY NAME`."
                ),
            ),
            "vgi.result_columns_schema": result_columns(DATASET_COLUMNS),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT dataset_id, title, page_views_last_month FROM socrata.main.datasets("
                    "'data.cityofnewyork.us', q := '311 service requests') "
                    "ORDER BY page_views_last_month DESC, dataset_id LIMIT 5"
                ),
                description="The five most viewed NYC datasets about 311 service requests",
            ),
            FunctionExample(
                sql=(
                    "SELECT category, count(*) AS datasets FROM socrata.main.datasets('opendata.rdw.nl') "
                    "GROUP BY category ORDER BY datasets DESC, category"
                ),
                description="Datasets per category on the Dutch vehicle authority (RDW) portal, an EU portal",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[DatasetsArgs]) -> BindResponse:
        """Validate the portal and request the Discovery hosts' scoped secrets."""
        portal_arg(params)
        request_secrets(params, *DISCOVERY_HOSTS.values())
        return BindResponse(output_schema=OUTPUT_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[DatasetsArgs]) -> CatalogState:
        """Return a fresh listing."""
        return CatalogState()

    @classmethod
    def process(cls, params: ProcessParams[DatasetsArgs], state: CatalogState, out: OutputCollector) -> None:
        """Emit the next page of datasets."""
        domain = portal_domain(portal_arg(params))
        if not state.region:
            state.region, state.probing = known_region(domain, params.args.region)
        catalog_step(params, state, out, domain, int(params.args.max_rows))


@dataclass(slots=True, frozen=True, kw_only=True)
class SearchArgs:
    """Arguments for ``search_datasets(q, category :=, tag :=, asset_type :=, region :=, max_rows :=)``."""

    q: Annotated[str, Arg(0, doc="Full-text search across every portal, e.g. 'police use of force'.")]
    category: Annotated[str | None, Arg("category", default=None, doc="Only datasets in this category.")]
    tag: Annotated[str | None, Arg("tag", default=None, doc="Only datasets with this tag.")]
    asset_type: Annotated[
        str,
        Arg(
            "asset_type",
            default="dataset",
            doc="Asset type: 'dataset' (default), 'map', 'chart', ..., or 'all'.",
            choices=ASSET_TYPES,
        ),
    ]
    region: Annotated[
        str,
        Arg(
            "region",
            default="us",
            doc="Which federated index to search: 'us' (default, US portals) or 'eu' (European portals).",
            choices=("us", "eu"),
        ),
    ]
    max_rows: Annotated[
        int,
        Arg(
            "max_rows",
            default=100,
            doc="Stop after this many results (default 100, at most 10,000).",
            ge=1,
            le=MAX_OFFSET,
        ),
    ]


@init_single_worker
class SearchDatasets(TableFunctionGenerator[SearchArgs, CatalogState]):
    """Search every Socrata portal of one region through the Discovery API."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = OUTPUT_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "search_datasets"
        description = "Search datasets across every Socrata portal of a region (Discovery API)"
        filter_pushdown = True
        auto_apply_filters = True
        tags = {
            **object_tags(
                title="Search All Portals",
                category="catalog",
                keywords="socrata, discovery api, federated search, open data, all portals, datasets, find 4x4",
                doc_llm=(
                    "Search the Socrata Discovery API's federated index of every public portal in one region "
                    "('us', the default, or 'eu') for q, ranked by relevance, one row per dataset with the same "
                    f"columns as datasets(): {CATALOG_DOC_COLUMNS}. The portal column says which portal each "
                    "result is on; pass it with dataset_id to dataset(). Optional: category :=, tag :=, "
                    "asset_type :=, max_rows := (default 100)."
                ),
                doc_md=(
                    "## search_datasets(q, category :=, tag :=, asset_type :=, region :=, max_rows :=)\n\n"
                    "Search **every** US (or EU) Socrata portal at once, e.g. which cities publish police "
                    "use-of-force data."
                ),
            ),
            "vgi.result_columns_schema": result_columns(DATASET_COLUMNS),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT portal, dataset_id, title FROM socrata.main.search_datasets('police use of force', "
                    "max_rows := 10) ORDER BY portal, dataset_id"
                ),
                description="Ten police use-of-force datasets from any US portal",
            ),
            FunctionExample(
                sql=(
                    "SELECT portal, count(*) AS datasets FROM socrata.main.search_datasets('voertuigen', "
                    "region := 'eu', max_rows := 50) GROUP BY portal ORDER BY datasets DESC, portal"
                ),
                description="Which European portals publish datasets about vehicles ('voertuigen')",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[SearchArgs]) -> BindResponse:
        """Request the Discovery hosts' scoped secrets."""
        request_secrets(params, *DISCOVERY_HOSTS.values())
        return BindResponse(output_schema=OUTPUT_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[SearchArgs]) -> CatalogState:
        """Return a fresh search."""
        return CatalogState(region=params.args.region)

    @classmethod
    def process(cls, params: ProcessParams[SearchArgs], state: CatalogState, out: OutputCollector) -> None:
        """Emit the next page of results."""
        catalog_step(params, state, out, None, int(params.args.max_rows))


# ---------------------------------------------------------------- dataset_columns

COLUMN_COLUMNS: list[pa.Field[Any]] = [
    column("portal", pa.string(), "Base URL of the portal.", nullable=False),
    column("dataset_id", pa.string(), "Four-by-four dataset id.", nullable=False),
    column("position", pa.int32(), "1-based position of the column in the dataset.", nullable=False),
    column("field_name", pa.string(), "Field name: the column name in dataset() and in SoQL.", nullable=False),
    column("display_name", pa.string(), "Human-readable column name."),
    column("description", pa.string(), "Column description, when the publisher wrote one."),
    column(
        "data_type",
        pa.string(),
        "Socrata data type: text, number, calendar_date, checkbox, point, location, url, ...",
        nullable=False,
    ),
    column("duckdb_type", pa.string(), "The DuckDB type of the column in dataset() (NULL for system columns)."),
    column(
        "is_system",
        pa.bool_(),
        "True for system and computed-region columns (field names starting with ':'), not returned by dataset().",
        nullable=False,
    ),
]
COLUMNS_SCHEMA = pa.schema(COLUMN_COLUMNS)


def column_rows(portal: str, dataset_id: str, view: dict[str, Any]) -> list[dict[str, Any]]:
    """One row per column of a dataset's view metadata."""
    rows = []
    for i, c in enumerate(view.get("columns") or [], start=1):
        name = str(c.get("fieldName") or "")
        if not name:
            continue
        stype = str(c.get("dataTypeName") or "text").lower()
        user = is_user_column(c)
        rows.append(
            {
                "portal": portal,
                "dataset_id": dataset_id,
                "position": int(c.get("position") or i),
                "field_name": name,
                "display_name": c.get("name"),
                "description": " ".join(str(c.get("description") or "").split()) or None,
                "data_type": stype,
                "duckdb_type": duckdb_type_name(arrow_type_for(stype)) if user else None,
                "is_system": not user,
            }
        )
    return rows


@dataclass(kw_only=True)
class OnceState(ArrowSerializableDataclass):
    """A single batch."""

    done: bool = False


@dataclass(slots=True, frozen=True, kw_only=True)
class ColumnsArgs:
    """Arguments for ``dataset_columns(portal, dataset_id)``."""

    portal: Annotated[str, Arg(0, doc=PORTAL_DOC, pattern=PORTAL_PATTERN)]
    dataset_id: Annotated[str, Arg(1, doc=DATASET_ID_DOC, pattern=DATASET_ID_PATTERN)]


@init_single_worker
class DatasetColumns(TableFunctionGenerator[ColumnsArgs, OnceState]):
    """List a dataset's columns with their Socrata and DuckDB types."""

    FIXED_SCHEMA: ClassVar[pa.Schema] = COLUMNS_SCHEMA

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "dataset_columns"
        description = "Columns of a Socrata dataset with their Socrata and DuckDB types"
        tags = {
            **object_tags(
                title="Dataset Columns",
                category="catalog",
                keywords="socrata, columns, schema, field names, data types, metadata, describe",
                doc_llm=(
                    "List the columns of one Socrata dataset (from /api/views/<id>.json): portal, dataset_id, "
                    "position, field_name (the name to use in dataset() and SoQL), display_name, description, "
                    "data_type (Socrata type), duckdb_type (the column's type in dataset()), is_system (true for "
                    "':'-prefixed system and computed-region columns that dataset() does not return). Use it "
                    "before dataset() or soql() to learn field names: display names often differ (e.g. "
                    "'Problem (formerly Complaint Type)' is field complaint_type)."
                ),
                doc_md=(
                    "## dataset_columns(portal, dataset_id)\n\n"
                    "The dataset's columns, their field names and the DuckDB type each one gets in `dataset()`."
                ),
            ),
            "vgi.result_columns_schema": result_columns(COLUMN_COLUMNS),
        }
        examples = [
            FunctionExample(
                sql=(
                    "SELECT field_name, data_type, duckdb_type FROM socrata.main.dataset_columns("
                    "'data.cityofnewyork.us', 'erm2-nwe9') WHERE NOT is_system ORDER BY position"
                ),
                description="Field names and types of NYC 311 service requests",
            ),
            FunctionExample(
                sql=(
                    "SELECT data_type, count(*) AS columns FROM socrata.main.dataset_columns("
                    "'data.cityofchicago.org', 'ijzp-q8t2') GROUP BY data_type ORDER BY columns DESC, data_type"
                ),
                description="How many columns of each Socrata type the Chicago crimes dataset has",
            ),
        ]

    @classmethod
    def on_bind(cls, params: BindParams[ColumnsArgs]) -> BindResponse:
        """Validate arguments and request the portal's scoped secret."""
        require_four_by_four(params.args.dataset_id)
        request_secrets(params, portal_arg(params))
        return BindResponse(output_schema=COLUMNS_SCHEMA)

    @classmethod
    def initial_state(cls, params: ProcessParams[ColumnsArgs]) -> OnceState:
        """Not yet emitted."""
        return OnceState()

    @classmethod
    def process(cls, params: ProcessParams[ColumnsArgs], state: OnceState, out: OutputCollector) -> None:
        """Emit every column in one batch."""
        if state.done:
            out.finish()
            return
        portal = portal_arg(params)
        dataset_id = require_four_by_four(params.args.dataset_id)
        view = fetch_view(process_client(params, portal), dataset_id)
        rows = column_rows(portal, dataset_id, view)
        schema = params.output_schema
        out.emit(pa.RecordBatch.from_pylist([{n: r[n] for n in schema.names} for r in rows], schema=schema))
        state.done = True
