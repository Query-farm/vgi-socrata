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
    "data.cdc.gov, and European ones such as opendata.rdw.nl. Key concepts: a portal is identified by its "
    "domain (or URL); every dataset has a four-by-four id such as 'erm2-nwe9' (the end of its page URL); "
    "columns have a field name (used in SQL and SoQL) that often differs from the display name. Reach for "
    "it to find public government datasets (on one portal or across all portals of a region), learn their "
    "columns, and read or summarize their rows. Ordinary SQL filters, column selection and ORDER BY "
    "<number or timestamp> LIMIT n run on the portal as SoQL, so only matching rows travel; rows are paged "
    "with a resumable keyset cursor on the system :id column. Big GROUP BY summaries should be sent to the "
    "portal as a SoQL query instead of downloading rows. Column types are real DuckDB types: numbers "
    "`DECIMAL(28,10)`, calendar dates `TIMESTAMP`, checkboxes `BOOLEAN`, points GeoJSON `JSON`, locations "
    "and URLs `STRUCT`. Public data needs no credentials; an app token raises the throttling limit and is "
    "given as a secret scoped to the portal:\n\n"
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
    "Everything for Socrata portals lives in this one schema, in three groups: catalog discovery (the "
    "Discovery API's dataset listings for one portal or a federated search, and a dataset's column "
    "metadata), data access (typed rows with SoQL pushdown, and full server-side SoQL queries for "
    "aggregates), and helpers that quote values for hand-written SoQL. Every portal-facing function takes "
    "the portal domain first (e.g. 'data.cityofnewyork.us') and, for a dataset, its four-by-four id. "
    "Look up field names before filtering (display names differ from field names); filter in SQL so the "
    "portal does the work; for GROUP BY over millions of rows let the portal aggregate."
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
        "name": "nyc_311_borough_counts",
        "prompt": (
            "Count NYC 311 service requests (dataset erm2-nwe9 on data.cityofnewyork.us) per borough for "
            "1 January 2024 (created_date from 2024-01-01T00:00:00 inclusive to 2024-01-02T00:00:00 exclusive), "
            "letting the portal do the counting. How many came from BROOKLYN?"
        ),
    },
    {
        "name": "use_of_force_portals",
        "prompt": (
            "Search every US Socrata portal for 'police use of force' datasets (first 10 results). Which "
            "portal domains appear?"
        ),
    },
    {
        "name": "like_escape_pattern",
        "prompt": "Escape the text 100%_done so it matches literally inside a SoQL like pattern.",
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
