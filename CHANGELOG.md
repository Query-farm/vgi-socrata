# Changelog

## 0.1.0 (unreleased)

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
