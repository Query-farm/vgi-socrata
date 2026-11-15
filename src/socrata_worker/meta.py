"""Discovery metadata: the one file to edit when you rename or extend the worker.

DuckDB, coding agents and the ``vgi-lint`` metadata checker all read these
tags. Keep every object documented and ``make lint`` stays at 100.

* Catalog-level tags (title, license, author, links, long description) live in
  ``CATALOG_TAGS`` below.
* Every function and table carries ``object_tags(...)``: a title, a category
  from ``SCHEMA_CATEGORIES``, search keywords, and LLM- and human-facing docs.
"""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa

CATALOG_NAME = "socrata"

SOURCE_URL = "https://github.com/Query-farm/vgi-socrata"

# Categories group functions in docs and agent tools. Every object's
# ``vgi.category`` must name one of these.
SCHEMA_CATEGORIES = [
    {
        "name": "catalog",
        "title": "Catalog & Schema",
        "description": "Find datasets on one portal or across all portals, and list a dataset's columns.",
    },
    {
        "name": "data",
        "title": "Dataset Data",
        "description": "Read dataset rows with SoQL pushdown, or run server-side SoQL aggregations.",
    },
    {
        "name": "soql",
        "title": "SoQL Helpers",
        "description": "Quote and escape values for hand-written SoQL conditions and queries.",
    },
]


def keywords_array(keywords: str) -> str:
    """Turn ``"a, b, c"`` into the JSON array string ``vgi.keywords`` requires."""
    return json.dumps([k.strip() for k in keywords.split(",") if k.strip()])


def object_tags(*, title: str, category: str, keywords: str, doc_llm: str, doc_md: str) -> dict[str, str]:
    """Build the standard per-object discovery tags."""
    if category not in {c["name"] for c in SCHEMA_CATEGORIES}:
        raise ValueError(f"unknown category {category!r}; add it to SCHEMA_CATEGORIES")
    return {
        "vgi.title": title,
        "vgi.category": category,
        "vgi.keywords": keywords_array(keywords),
        "vgi.doc_llm": doc_llm,
        "vgi.doc_md": doc_md,
    }


def column(name: str, type_: pa.DataType, comment: str, *, nullable: bool = True) -> pa.Field[Any]:
    """Build an Arrow field whose ``comment`` shows up in DuckDB's ``DESCRIBE`` and agent tools."""
    return pa.field(name, type_, nullable=nullable, metadata={b"comment": comment.encode()})


def result_columns(fields: list[pa.Field[Any]]) -> str:
    """Return the ``vgi.result_columns_schema`` JSON for a fixed output schema."""
    from .soql import duckdb_type_name

    return json.dumps(
        [
            {
                "name": f.name,
                "type": duckdb_type_name(f.type),
                "description": (f.metadata or {}).get(b"comment", b"").decode(),
            }
            for f in fields
        ]
    )


CATALOG_COMMENT = (
    "Any Socrata (Tyler Data & Insights) open-data portal as SQL: search portal catalogs and read typed "
    "dataset rows, with filters, projections and aggregates pushed down as SoQL. No key needed for public data."
)

CATALOG_DOC_LLM = (
    "Query any Socrata (Tyler Data & Insights) open-data portal through its SODA 2.1 API: thousands of US "
    "state, county and city portals such as data.cityofnewyork.us, data.cityofchicago.org, data.wa.gov and "
    "data.cdc.gov, and European ones such as opendata.rdw.nl. Every dataset has a four-by-four id such as "
    "'erm2-nwe9' (the end of its page URL). Every function takes the portal (a domain such as "
    "'data.cityofnewyork.us', or a URL) as its first argument, and most take the dataset id. Typical flow: "
    "find datasets on a portal with datasets(portal, q := ...) (or across every portal with "
    "search_datasets(q)), inspect a dataset's field names and types with dataset_columns(portal, id), then "
    "read rows with dataset(portal, id) and ordinary SQL: WHERE filters, selected columns, count(*) and "
    "ORDER BY <number or timestamp> LIMIT n are translated into SoQL so only matching rows travel, and rows "
    "are paged with a keyset cursor on the system :id column. For GROUP BY summaries of big datasets send a "
    "SoQL query to the portal with soql(portal, id, 'SELECT ... GROUP BY ...'). Column types are real DuckDB "
    "types: numbers `DECIMAL(28,10)`, calendar dates `TIMESTAMP`, checkboxes `BOOLEAN`, points GeoJSON `JSON`, "
    "locations and URLs `STRUCT`. Public data needs no credentials; an app token raises the throttling "
    "limit and is given as a secret scoped to the portal:\n\n"
    "```sql\nCREATE SECRET (TYPE socrata, APP_TOKEN '...', SCOPE 'https://data.cityofnewyork.us');\n```\n\n"
    "Catalog columns portal, dataset_id, title, description, publisher, modified and url match the CKAN and "
    "Opendatasoft workers so catalogs from several portal platforms can be combined with UNION ALL BY NAME."
)

CATALOG_DOC_MD = (
    "# Socrata open-data portals in DuckDB\n\n"
    "Query **any Socrata (Tyler Data & Insights) open-data portal** with SQL: New York City, Chicago, "
    "Washington State, the CDC and thousands more US and European government portals. No key is needed for "
    "public data.\n\n"
    "Filters, projections, `count(*)` and `ORDER BY ... LIMIT` run on the portal as SoQL, rows are paged "
    "with a resumable keyset cursor, and aggregations can run server-side. Columns get real DuckDB types, "
    "including timestamps, decimals, booleans, GeoJSON points and location structs."
)

SCHEMA_DOC_LLM = (
    "The `main` schema holds every function. Catalog: datasets(portal, q :=, category :=, tag :=) lists one "
    "portal's datasets from the Discovery API; search_datasets(q, region :=) searches every US (or EU) "
    "portal; dataset_columns(portal, dataset_id) lists field names and types. Data: dataset(portal, "
    "dataset_id, soql_where :=, page_rows :=) reads typed rows with SoQL pushdown; soql(portal, dataset_id, "
    "query) runs a full SoQL SELECT/GROUP BY on the portal. SoQL helpers: soql_quote(value) and "
    "soql_like_escape(text) build safe hand-written SoQL. `portal` is a domain such as "
    "'data.cityofnewyork.us'; dataset ids are four-by-fours such as 'erm2-nwe9'. Look up field names before "
    "filtering (display names differ from field names); filter in SQL so the portal does the work."
)

SCHEMA_DOC_MD = (
    "## main\n\n"
    "Everything this worker offers lives in one schema, grouped in three categories.\n\n"
    "**Catalog** functions search one portal's datasets or every portal at once through the Socrata "
    "Discovery API, and list a dataset's columns with the DuckDB type each one gets.\n\n"
    "**Data** functions read a dataset's rows with filters, projections and Top-N sorts pushed to the "
    "portal as SoQL, or run a SoQL aggregate on the portal and return only the summary.\n\n"
    "**SoQL helpers** quote values for hand-written SoQL."
)

AGENT_TEST_TASKS = [
    {
        "name": "nyc_311_dataset_id",
        "prompt": (
            "Using the Socrata worker, what is the dataset id (four-by-four) of the NYC '311 Service Requests "
            "from 2020 to Present' dataset on data.cityofnewyork.us?"
        ),
    },
    {
        "name": "nyc_311_brooklyn_first_minutes",
        "prompt": (
            "In NYC 311 service requests (dataset erm2-nwe9 on data.cityofnewyork.us), how many requests from "
            "the borough BROOKLYN were created in the first five minutes of 2024 (created_date from "
            "2024-01-01 00:00:00 inclusive to 00:05:00 exclusive)?"
        ),
    },
    {
        "name": "chicago_library_count",
        "prompt": (
            "How many Chicago Public Library branches are listed in the dataset x8fc-8rcq on data.cityofchicago.org?"
        ),
    },
    {
        "name": "nyc_311_created_date_type",
        "prompt": (
            "What Socrata data type does the column created_date have in dataset erm2-nwe9 on data.cityofnewyork.us?"
        ),
    },
    {
        "name": "quote_literal",
        "prompt": "Quote the text O'Brien as a SoQL string literal with the Socrata worker.",
    },
]

CATALOG_TAGS = {
    "vgi.title": "Socrata Open Data",
    "vgi.subtitle": "Any Socrata (Tyler Data & Insights) portal's catalog and datasets as SQL, with SoQL pushdown.",
    "vgi.doc_llm": CATALOG_DOC_LLM,
    "vgi.doc_md": CATALOG_DOC_MD,
    "vgi.source_url": SOURCE_URL,
    "vgi.icon_url": "https://query.farm/media-kit/products/vgi/vgi-logo.png",
    "vgi.license": "0BSD",
    "vgi.author": "Query.Farm",
    "vgi.copyright": "Copyright 2026 Query Farm LLC - https://query.farm",
    "vgi.keywords": keywords_array(
        "socrata, tyler data & insights, soda, soql, open data, government data, city data, 311, catalog, duckdb"
    ),
    "vgi.support_contact": f"{SOURCE_URL}/issues",
    "vgi.support_policy_url": f"{SOURCE_URL}#support",
    # Public analyst tasks for `vgi-lint simulate`; private graders live in vgi-agent-tests.yaml.
    "vgi.agent_test_tasks": json.dumps(AGENT_TEST_TASKS),
}
