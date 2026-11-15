"""Assemble the Socrata functions into the ``socrata`` catalog and run the worker.

Run it:

    vgi-socrata-worker                      # stdio: what DuckDB launches by default
    vgi-socrata-worker --http --port 8000   # HTTP: for Docker / hosted deploys

Attach it from DuckDB:

    ATTACH 'socrata' (TYPE vgi, LOCATION '/abs/path/to/.venv/bin/vgi-socrata-worker');
"""

from __future__ import annotations

import json
from typing import Any

import pyarrow as pa
from vgi import Worker
from vgi.catalog import Catalog, Schema, SecretTypeSpec

from .common import SECRET_BASE_URL_FIELD, SECRET_TOKEN_FIELD, SECRET_TYPE
from .dataset import Dataset
from .discovery import DatasetColumns, Datasets, SearchDatasets
from .helpers import SoqlLikeEscape, SoqlQuote
from .meta import (
    CATALOG_COMMENT,
    CATALOG_NAME,
    CATALOG_TAGS,
    SCHEMA_CATEGORIES,
    SCHEMA_DOC_LLM,
    SCHEMA_DOC_MD,
    SOURCE_URL,
    keywords_array,
)
from .soql_query import Soql

FUNCTIONS: list[type[Any]] = [Datasets, SearchDatasets, DatasetColumns, Dataset, Soql, SoqlQuote, SoqlLikeEscape]

# CREATE SECRET (TYPE socrata, APP_TOKEN '...', SCOPE 'https://data.cityofnewyork.us')
SOCRATA_SECRET_TYPE = SecretTypeSpec(
    name=SECRET_TYPE,
    description="Socrata app token (and optional base_url) for the portal hosts in the secret's SCOPE.",
    schema=pa.schema(
        [
            pa.field(
                SECRET_TOKEN_FIELD,
                pa.string(),
                metadata={
                    "redact": "true",
                    "vgi_doc": "Socrata app token, sent as X-App-Token; raises the anonymous throttling limit.",
                },
            ),
            pa.field(
                SECRET_BASE_URL_FIELD,
                pa.string(),
                metadata={
                    "vgi_doc": (
                        "Send this host's requests to another base URL (a caching proxy, mirror or local fixture "
                        "server) while results keep naming the real portal."
                    ),
                },
            ),
        ],
        metadata={"vgi_doc_url": f"{SOURCE_URL}#app-tokens-and-secrets"},
    ),
)


def _stamp_example_queries(functions: list[type[Any]]) -> list[dict[str, str]]:
    """Publish each function's examples as ``vgi.example_queries``; return them all for the schema."""
    by_name: dict[str, list[dict[str, str]]] = {}
    for fn in functions:
        bucket = by_name.setdefault(fn.Meta.name, [])
        for ex in getattr(fn.Meta, "examples", []):
            entry = {"description": ex.description, "sql": ex.sql}
            if entry not in bucket:
                bucket.append(entry)
    for fn in functions:
        fn.Meta.tags = {**fn.Meta.tags, "vgi.example_queries": json.dumps(by_name[fn.Meta.name])}
    return [entry for entries in by_name.values() for entry in entries]


_ALL_EXAMPLES = _stamp_example_queries(FUNCTIONS)


class SocrataWorker(Worker):
    """The worker process: one ``socrata`` catalog, one ``main`` schema."""

    catalog = Catalog(
        name=CATALOG_NAME,
        comment=CATALOG_COMMENT,
        source_url=SOURCE_URL,
        tags=CATALOG_TAGS,
        schemas=[
            Schema(
                path=["main"],
                comment="Socrata catalog discovery, dataset rows and server-side SoQL.",
                functions=FUNCTIONS,
                tags={
                    "vgi.title": "Socrata Functions",
                    "domain": "open-data",
                    "vgi.keywords": keywords_array("socrata, datasets, soql, soda, discovery api, open data"),
                    "vgi.doc_llm": SCHEMA_DOC_LLM,
                    "vgi.doc_md": SCHEMA_DOC_MD,
                    "vgi.categories": json.dumps(SCHEMA_CATEGORIES),
                    "vgi.example_queries": json.dumps(_ALL_EXAMPLES),
                },
            )
        ],
    )

    secret_types = [SOCRATA_SECRET_TYPE]


def main() -> None:
    """Console-script entry point: parse ``--http`` / ``--unix`` / logging flags and serve."""
    SocrataWorker.main()


if __name__ == "__main__":
    main()
