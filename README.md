<p align="center">
  <img src="https://query.farm/media-kit/products/vgi/vgi-logo.png" alt="Vector Gateway Interface (VGI)" width="320">
</p>

# Query Any Socrata Open-Data Portal in DuckDB

> **vgi-socrata** · a [Query.Farm](https://query.farm) VGI worker

[![CI](https://github.com/Query-farm/vgi-socrata/actions/workflows/ci.yml/badge.svg)](https://github.com/Query-farm/vgi-socrata/actions/workflows/ci.yml)
[![vgi worker: query.farm](https://query.farm/media-kit/shields/vgi-worker.svg)](https://query.farm)

Thousands of US state, county and city agencies, and a growing number of European ones, publish
their open data on **Socrata** (now *Tyler Data & Insights*): `data.cityofnewyork.us`,
`data.cityofchicago.org`, `data.wa.gov`, `data.cdc.gov`, `opendata.rdw.nl` and many more. This
[VGI](https://query.farm/vgi) worker turns any of them into SQL: find datasets through the Discovery
API, read rows with real column types, and let the portal do the filtering, sorting and aggregating
through its own query language, **SoQL**. No key is needed for public data.

```sql
-- NYC 311 complaints in Brooklyn in the first ten minutes of 2024. The filter and the columns are
-- sent to the portal as SoQL, so 40 million rows become one small request.
SELECT complaint_type, count(*) AS requests
FROM socrata.main.dataset('data.cityofnewyork.us', 'erm2-nwe9')
WHERE created_date >= TIMESTAMP '2024-01-01 00:00:00' AND created_date < TIMESTAMP '2024-01-01 00:10:00'
  AND borough = 'BROOKLYN'
GROUP BY complaint_type ORDER BY requests DESC;
```

## Functions

| Function | Returns |
|---|---|
| `datasets(portal, q :=, category :=, tag :=, asset_type :=, region :=, max_rows :=)` | One portal's catalog from the Discovery API: `portal, dataset_id, title, description, asset_type, category, tags, publisher, owner, license, provenance, created, modified, data_updated, metadata_updated, page_views_last_month, page_views_total, download_count, column_names, domain_metadata, url, api_url` |
| `search_datasets(q, category :=, tag :=, asset_type :=, region := 'us', max_rows := 100)` | The same columns, searched across **every** portal of a region (`us` or `eu`) |
| `dataset_columns(portal, dataset_id)` | A dataset's columns: `position, field_name, display_name, description, data_type, duckdb_type, is_system` |
| `dataset(portal, dataset_id, soql_where :=, page_rows := 1000)` | The dataset's rows, one column per field, typed (below), plus `soda_row_id` |
| `soql(portal, dataset_id, query, max_rows := 10000)` | The typed result of a server-side SoQL query (aggregates, `GROUP BY`) |
| `soql_quote(text)` | `text` as a SoQL string literal, for hand-written `soql_where :=` / `soql()` |
| `soql_like_escape(text)` | `text` with `%`, `_` and `\` escaped for a SoQL `like` pattern |

`portal` is a domain (`data.cityofnewyork.us`) or a URL; a pasted dataset page URL is trimmed to
the portal root. `dataset_id` is the dataset's **four-by-four** (`erm2-nwe9`), the last part of
its page URL. Socrata portals live in two Discovery indexes, US and EU; `datasets()` finds the
right one by itself (`region := 'auto'`).

**Catalog columns line up with the other open-data workers.** `portal, dataset_id, title,
description, publisher, modified, url` have the same names and types as in
[vgi-ckan](https://github.com/Query-farm/vgi-ckan) and
[vgi-opendatasoft](https://github.com/Query-farm/vgi-opendatasoft), so catalogs can be combined:

```sql
SELECT portal, dataset_id, title, modified FROM socrata.main.datasets('data.cityofchicago.org')
UNION ALL BY NAME
SELECT portal, dataset_id, title, modified FROM socrata.main.datasets('data.cityofnewyork.us');
```

### Column types

| Socrata type | DuckDB type |
|---|---|
| `text` | `VARCHAR` |
| `number`, `money`, `percent` | `DECIMAL(28,10)` (NULL if a value needs more than 18 integer digits) |
| `double` | `DOUBLE` |
| `checkbox` | `BOOLEAN` |
| `calendar_date`, `floating_timestamp` | `TIMESTAMP` (local time, no zone, as published) |
| `fixed_timestamp` | `TIMESTAMPTZ` |
| `point`, `multipoint`, `line`, `multiline`, `polygon`, `multipolygon` | GeoJSON `JSON` (`ST_GeomFromGeoJSON`) |
| `location` (legacy) | `STRUCT(latitude DOUBLE, longitude DOUBLE, address VARCHAR, city VARCHAR, state VARCHAR, zip VARCHAR)` |
| `url` | `STRUCT(url VARCHAR, description VARCHAR)` |
| `phone` | `STRUCT(phone_number VARCHAR, phone_type VARCHAR)` |
| system `:id` | `soda_row_id VARCHAR` |
| anything else | `VARCHAR` |

System and computed-region columns (field names starting with `:`, e.g. `:@computed_region_...`)
are not returned by `dataset()`; `dataset_columns()` lists them with `is_system = true`.

## Pushdown: what runs on the portal

`dataset()` turns what DuckDB hands the scan into a SODA 2.1 request:

| SQL | SoQL |
|---|---|
| selected columns | `$select` |
| `col = v`, `col <> v` | `` `col` = 'v' `` / `!=` (text: exact, case-sensitive) |
| `<`, `<=`, `>`, `>=`, `BETWEEN` | on number, money, percent, double and timestamp columns |
| `col IN (...)` | `` `col` IN (...) `` |
| `IS [NOT] NULL` | `` `col` IS [NOT] NULL `` (any column) |
| `starts_with(col, 'p')`, `col LIKE 'p%'` | `` starts_with(`col`, 'p') `` |
| `ORDER BY <number/timestamp> LIMIT n` (no `WHERE`) | `$order=<col> <dir>,:id` + `$limit=n` |

Literals are quoted the SoQL way (`'O''Brien'`, backslashes doubled), timestamps as
`'2024-01-01T00:00:00.000'` (floating) or `'...Z'` (fixed). Anything that cannot be expressed
exactly is left to DuckDB, which re-checks every pushed filter anyway, so pushdown never changes a
result. DuckDB never hands a scan `LIKE '%x%'`, `ILIKE` or an `OR` across columns: write those
server-side with `soql_where :=`:

```sql
SELECT descriptor, count(*) FROM socrata.main.dataset('data.cityofnewyork.us', 'erm2-nwe9',
  soql_where := 'upper(descriptor) like ' || socrata.main.soql_quote('%NOISE%')
             || ' AND created_date >= ''2024-01-01T00:00:00'' AND created_date < ''2024-01-01T01:00:00''')
GROUP BY ALL ORDER BY 2 DESC;
```

**Paging.** Rows are read in pages of `page_rows` (default 1,000) with a **keyset cursor on the
system `:id` column**: `$order=:id` and `$where=... AND :id > '<last id>'`, never a deep
`$offset` (which makes portals time out). The cursor lives in the scan state, so a long scan
resumes in another worker process over the HTTP transport.

**Aggregates on the portal.** For `GROUP BY` over millions of rows, send SoQL with `soql()`; the
result columns are typed from the portal's own `X-SODA2-Types` header:

```sql
SELECT borough, requests::BIGINT AS requests FROM socrata.main.soql('data.cityofnewyork.us', 'erm2-nwe9',
  'SELECT borough, count(*) AS requests WHERE created_date >= ''2024-01-01T00:00:00''
   AND created_date < ''2024-01-02T00:00:00'' GROUP BY borough ORDER BY borough');
```

Without a `LIMIT`, `soql()` appends `LIMIT max_rows + 1` and fails if more than `max_rows` rows come
back, rather than truncating silently.

## Install

Python 3.14+ and [uv](https://docs.astral.sh/uv/). Run the worker straight from a release tag
(workers are not published to PyPI):

```sql
INSTALL vgi FROM community;
LOAD vgi;
ATTACH 'socrata' (TYPE vgi,
  LOCATION 'uvx --from git+https://github.com/Query-farm/vgi-socrata@v0.1.2 vgi-socrata-worker');
```

From a checkout: `make build`, then `LOCATION '/abs/path/to/vgi-socrata/.venv/bin/vgi-socrata-worker'`.
Hosted: `vgi-socrata-worker --http --port 8000` (or the Dockerfile), then
`ATTACH 'socrata' (TYPE vgi, LOCATION 'https://your-host')`.

## Examples

```sql
-- Find the dataset id
SELECT dataset_id, title, page_views_last_month
FROM socrata.main.datasets('data.cityofnewyork.us', q := '311 service requests')
ORDER BY page_views_last_month DESC LIMIT 5;

-- Which cities publish police use-of-force data?
SELECT portal, dataset_id, title FROM socrata.main.search_datasets('police use of force', max_rows := 25);

-- Field names and types (display names differ from field names)
SELECT field_name, display_name, data_type, duckdb_type
FROM socrata.main.dataset_columns('data.cityofnewyork.us', 'erm2-nwe9') WHERE NOT is_system ORDER BY position;

-- The ten most recent Chicago crimes (sorted and limited by the portal)
SELECT id, date, primary_type, arrest FROM socrata.main.dataset('data.cityofchicago.org', 'ijzp-q8t2')
ORDER BY date DESC LIMIT 10;

-- Points into the spatial extension
INSTALL spatial; LOAD spatial;
SELECT unique_key, ST_GeomFromGeoJSON(location) AS geom
FROM socrata.main.dataset('data.cityofnewyork.us', 'erm2-nwe9')
WHERE created_date >= TIMESTAMP '2024-01-01' AND created_date < TIMESTAMP '2024-01-01 00:10:00';
```

## App tokens and secrets

Anonymous requests share a throttled pool. A free
[app token](https://dev.socrata.com/docs/app-tokens.html) raises the limit; store it as a DuckDB
secret **scoped to the portal**. It is sent as `X-App-Token` only to hosts whose URL starts with the
secret's `SCOPE`; an unscoped `socrata` secret is ignored, so a token is never sent to a host you did
not name. Tokens never appear in URLs, logs or scan state, and `duckdb_secrets()` redacts them.

```sql
CREATE SECRET nyc (TYPE socrata, APP_TOKEN 'your-token', SCOPE 'https://data.cityofnewyork.us');
-- Catalog searches go to the Discovery API hosts:
CREATE SECRET disc (TYPE socrata, APP_TOKEN 'your-token', SCOPE 'https://api.us.socrata.com');
```

A secret can also set `BASE_URL` to send a host's requests elsewhere (a caching proxy, a mirror, or
the test fixture server) while results keep naming the real portal.

Requests time out after 60 s; HTTP 429 (throttled) and 5xx are retried up to 6 times with backoff
that honours `Retry-After`. TLS uses the system trust store plus the `certifi` CA bundle.

## Testing

```bash
make check                      # ruff, mypy, pydoclint, pytest, SQL tests over stdio/launch/http, vgi-lint
make test-sql TRANSPORT=stdio   # one SQL lane
make lint-explain LINT_ARGS=    # vgi-lint with the examples executed against the live portals
SOCRATA_LIVE=1 uv run pytest tests/test_live.py   # optional live smoke test (a few tiny requests)
```

The SQL tests (`test/sql/*.test`) run through the real `vgi` extension against
`test/mock_api/server.py`, a **strict replay server** for responses recorded once from
`data.cityofnewyork.us`, `data.cityofchicago.org`, `opendata.rdw.nl` and the US/EU Discovery API
(stored in `test/fixtures/<host>/`). The tests point the worker at it with `socrata` secrets whose
`BASE_URL` names a per-host prefix. A request is answered only if its path and its exact SoQL
parameters (`$select`, `$where`, `$order`, `$limit`, `$query`, ...) were recorded, so every query in
the suite also asserts what was pushed down: a lost filter or a differently quoted literal is an
unrecorded request and fails the test. The mock also throttles each URL once (429) to exercise
retries, and serves a hand-made token-only portal (`test/mock_api/make_private.py`) with the column
types the recorded portals lack. To record after changing a query:
`MOCK_RECORD=1 make test-sql TRANSPORT=stdio` (keep requests tiny; respect the portals' terms).

## Support

Open an issue on [Query-farm/vgi-socrata](https://github.com/Query-farm/vgi-socrata/issues).
For VGI itself, see [query.farm](https://query.farm).

## Authorship & License

Written by [Query.Farm](https://query.farm). Released under the
[BSD Zero Clause License](LICENSE) (0BSD).

Copyright 2026 Query Farm LLC - https://query.farm

Data you read through this worker is published by each portal under its own terms. Socrata and Tyler
Data & Insights are trademarks of Tyler Technologies; this project is not affiliated with them.
