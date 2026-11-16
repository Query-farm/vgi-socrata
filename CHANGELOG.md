# Changelog

## 0.1.2 (2026-11-16)

- Bump vgi-python to 0.52.2.
- Requires Python 3.14 (CI, Docker image and tooling moved to 3.14).

## 0.1.1

- Fix: every query whose filter is pushed down to the worker (a `WHERE` clause, or
  `ORDER BY … LIMIT`) failed with "No DuckDB-compatible engine is installed". The SDK binds
  and evaluates pushed predicates with a local engine, which was only in the dev
  dependency group; the runtime dependency is now `vgi-python[http,haybarn]`.

## 0.1.0

First release: any Socrata (Tyler Data & Insights) open-data portal as SQL through SODA 2.1 and the
Discovery API.

- `datasets`, `search_datasets`: one portal's catalog or a federated search across every US or EU
  portal (Discovery API; the region of a portal is found automatically); `dataset_id` pushdown.
- `dataset_columns`: a dataset's columns with Socrata and DuckDB types.
- `dataset`: typed rows (DECIMAL(28,10)/DOUBLE/BOOLEAN/TIMESTAMP/TIMESTAMPTZ/GeoJSON JSON/location,
  url and phone STRUCTs/VARCHAR) with `$select`/`$where`/Top-N pushdown into SoQL and a resumable
  keyset cursor on `:id`.
- `soql`: full server-side SoQL queries (aggregates, GROUP BY) typed from `X-SODA2-Types`;
  `soql_quote` / `soql_like_escape` scalars.
- Optional `socrata` secret (APP_TOKEN, BASE_URL) scoped to a portal; 429/5xx retried honouring Retry-After.
- Strict replay fixtures recorded from data.cityofnewyork.us, data.cityofchicago.org, opendata.rdw.nl and
  the US/EU Discovery API.

Generated from vgi-starter-python v1.3.0.
