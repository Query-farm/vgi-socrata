r"""SoQL translation and Socrata column typing.

* ``arrow_type_for`` maps a Socrata ``dataTypeName`` to a DuckDB/Arrow type.
* ``to_arrow_batch`` converts decoded SODA JSON rows into a typed Arrow batch.
* ``where_clauses`` translates DuckDB's pushed-down filters into SoQL ``$where``
  clauses. Only translations that can never drop a matching row are made; the SDK
  re-applies every pushed predicate exactly (``auto_apply_filters``), so a clause
  that matches a superset is safe, and an untranslated filter is simply left to DuckDB.

What translates (verified against live portals, see README "Pushdown"):

=============================  ==================================================
SQL                            SoQL ``$where``
=============================  ==================================================
``col = v`` / ``<>``           ```col` = 'v'`` / ``!=`` (text: exact, case-sensitive)
``<, <=, >, >=``               number, money, percent, double and timestamp columns
                               (text collation on the portal may differ: not pushed)
``col IN (...)``               ```col` IN (...)``
``IS [NOT] NULL``              ```col` IS [NOT] NULL`` (any column type)
``starts_with`` / ``LIKE 'p%'``  ``starts_with(`col`, 'p')`` (case-sensitive)
AND / OR                       AND of translatable parts; OR only if all parts translate
=============================  ==================================================

Timestamp literals: ``calendar_date`` / ``floating_timestamp`` columns are floating
(no zone) and take ``'2024-01-01T00:00:00.000'``; ``fixed_timestamp`` columns take
UTC ``'2024-01-01T00:00:00.000Z'``. A value with sub-millisecond precision is not
pushed (Socrata stores milliseconds).
"""

from __future__ import annotations

import datetime as dt
import json
import math
import re
from collections.abc import Iterable, Sequence
from decimal import Decimal, InvalidOperation
from typing import Any

import pyarrow as pa

TIMESTAMP = pa.timestamp("us")
TIMESTAMPTZ = pa.timestamp("us", tz="UTC")
NUMBER = pa.decimal128(28, 10)
JSON_TYPE: pa.DataType = pa.json_(pa.string())

LOCATION_TYPE = pa.struct(
    [
        pa.field("latitude", pa.float64()),
        pa.field("longitude", pa.float64()),
        pa.field("address", pa.string()),
        pa.field("city", pa.string()),
        pa.field("state", pa.string()),
        pa.field("zip", pa.string()),
    ]
)
URL_TYPE = pa.struct([pa.field("url", pa.string()), pa.field("description", pa.string())])
PHONE_TYPE = pa.struct([pa.field("phone_number", pa.string()), pa.field("phone_type", pa.string())])

# Field metadata key carrying the Socrata dataTypeName of a column.
SOCRATA_TYPE_KEY = b"socrata_type"
# Field metadata key carrying the SoQL field name (``:id`` for soda_row_id).
SOCRATA_FIELD_KEY = b"socrata_field"

ROW_ID_COLUMN = "soda_row_id"
ROW_ID_FIELD = ":id"

GEO_TYPES = {"point", "multipoint", "line", "multiline", "polygon", "multipolygon", "geometry"}
NUMERIC_TYPES = {"number", "money", "percent"}
FLOATING_TS = {"calendar_date", "floating_timestamp"}
FIXED_TS = {"fixed_timestamp", "date"}
PUSHABLE_RANGE = NUMERIC_TYPES | {"double"} | FLOATING_TS | FIXED_TS
PUSHABLE_EQ = PUSHABLE_RANGE | {"text", "checkbox", "row_identifier"}

# Largest |value| a DECIMAL(28,10) holds: 18 integer digits.
_NUMBER_LIMIT = Decimal(10) ** 18
_NUMBER_QUANTUM = Decimal(1).scaleb(-10)

_TYPE_MAP: dict[str, pa.DataType] = {
    "text": pa.string(),
    "number": NUMBER,
    "money": NUMBER,
    "percent": NUMBER,
    "double": pa.float64(),
    "checkbox": pa.bool_(),
    "calendar_date": TIMESTAMP,
    "floating_timestamp": TIMESTAMP,
    "fixed_timestamp": TIMESTAMPTZ,
    "date": TIMESTAMPTZ,
    "location": LOCATION_TYPE,
    "url": URL_TYPE,
    "phone": PHONE_TYPE,
    "json": JSON_TYPE,
    "document": JSON_TYPE,
    "row_identifier": pa.string(),
}


def arrow_type_for(socrata_type: str) -> pa.DataType:
    """Return the Arrow (DuckDB) type for a Socrata ``dataTypeName``.

    Args:
        socrata_type: The column's ``dataTypeName`` from ``/api/views/<id>.json``.

    Returns:
        DECIMAL(28,10) / DOUBLE / BOOLEAN / TIMESTAMP / TIMESTAMPTZ / STRUCT / JSON / VARCHAR.

    """
    t = (socrata_type or "text").lower()
    if t in GEO_TYPES:
        return JSON_TYPE
    return _TYPE_MAP.get(t, pa.string())


def duckdb_type_name(t: pa.DataType) -> str:
    """Return a DuckDB type name for documentation columns."""
    simple = {
        pa.int64(): "BIGINT",
        pa.int32(): "INTEGER",
        pa.float64(): "DOUBLE",
        pa.bool_(): "BOOLEAN",
        TIMESTAMP: "TIMESTAMP",
        TIMESTAMPTZ: "TIMESTAMPTZ",
        NUMBER: "DECIMAL(28,10)",
    }
    for k, v in simple.items():
        if t == k:
            return v
    if t == JSON_TYPE:
        return "JSON"
    if t == LOCATION_TYPE:
        return "STRUCT(latitude DOUBLE, longitude DOUBLE, address VARCHAR, city VARCHAR, state VARCHAR, zip VARCHAR)"
    if t == URL_TYPE:
        return "STRUCT(url VARCHAR, description VARCHAR)"
    if t == PHONE_TYPE:
        return "STRUCT(phone_number VARCHAR, phone_type VARCHAR)"
    if pa.types.is_list(t):
        return "VARCHAR[]"
    return "VARCHAR"


# ---------------------------------------------------------------- values


def parse_timestamp(value: Any, *, aware: bool) -> dt.datetime | None:
    """Parse a SODA timestamp (``2024-01-01T00:00:00.000``, optional ``Z``/offset).

    Args:
        value: The JSON value.
        aware: Return a UTC-aware datetime (fixed_timestamp) instead of a naive one.

    Returns:
        The datetime, or None for empty/unparseable values.

    """
    if value is None or value == "":
        return None
    text = str(value).strip().replace("Z", "+00:00")
    if len(text) == 10:
        text += "T00:00:00"
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError:
        return None
    if aware:
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.UTC)
        return parsed.astimezone(dt.UTC)
    return parsed.replace(tzinfo=None) if parsed.tzinfo is None else parsed.astimezone(dt.UTC).replace(tzinfo=None)


def to_number(value: Any) -> Decimal | None:
    """SODA ``number`` text -> Decimal(28,10); NULL when it does not fit (more than 18 integer digits)."""
    if value is None or value == "" or isinstance(value, bool):
        return None
    try:
        d = Decimal(str(value))
    except InvalidOperation:
        return None
    if not d.is_finite() or abs(d) >= _NUMBER_LIMIT:
        return None
    return d.quantize(_NUMBER_QUANTUM)


def _to_float(value: Any) -> float | None:
    if value is None or value == "" or isinstance(value, bool):
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def _to_bool(value: Any) -> bool | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in ("true", "t", "1", "yes"):
        return True
    if text in ("false", "f", "0", "no"):
        return False
    return None


def _to_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False)


def _to_json(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        try:
            json.loads(value)
            return value
        except ValueError:
            return json.dumps(value, ensure_ascii=False)
    return json.dumps(value, ensure_ascii=False)


def _to_location(value: Any) -> dict[str, Any] | None:
    """Legacy ``location``: ``{latitude, longitude, human_address: '<json text>'}``."""
    if not isinstance(value, dict):
        return None
    human: Any = value.get("human_address")
    if isinstance(human, str):
        try:
            human = json.loads(human)
        except ValueError:
            human = {"address": human}
    if not isinstance(human, dict):
        human = {}
    lat, lon = _to_float(value.get("latitude")), _to_float(value.get("longitude"))
    coords = value.get("coordinates")
    if lat is None and isinstance(coords, list) and len(coords) == 2:  # GeoJSON-shaped location
        lon, lat = _to_float(coords[0]), _to_float(coords[1])
    return {
        "latitude": lat,
        "longitude": lon,
        "address": _to_text(human.get("address")) or None,
        "city": _to_text(human.get("city")) or None,
        "state": _to_text(human.get("state")) or None,
        "zip": _to_text(human.get("zip")) or None,
    }


def _to_url(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, str):
        return {"url": value, "description": None}
    if isinstance(value, dict):
        return {"url": _to_text(value.get("url")), "description": _to_text(value.get("description"))}
    return None


def _to_phone(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, str):
        return {"phone_number": value, "phone_type": None}
    if isinstance(value, dict):
        return {
            "phone_number": _to_text(value.get("phone_number")),
            "phone_type": _to_text(value.get("phone_type")),
        }
    return None


def field_socrata_type(f: pa.Field[Any]) -> str:
    """Return the Socrata type stored in a field's metadata (``text`` when absent)."""
    meta = f.metadata or {}
    return meta[SOCRATA_TYPE_KEY].decode() if SOCRATA_TYPE_KEY in meta else "text"


def field_soql_name(f: pa.Field[Any]) -> str:
    """Return the SoQL field name of an output column (``:id`` for ``soda_row_id``)."""
    meta = f.metadata or {}
    return meta[SOCRATA_FIELD_KEY].decode() if SOCRATA_FIELD_KEY in meta else f.name


def convert_value(value: Any, f: pa.Field[Any]) -> Any:
    """Convert one decoded JSON value to the Python value Arrow expects for ``f``."""
    t = f.type
    if t == NUMBER:
        return to_number(value)
    if t == pa.float64():
        return _to_float(value)
    if t == pa.int64():
        d = to_number(value)
        return None if d is None or d != d.to_integral_value() else int(d)
    if t == pa.bool_():
        return _to_bool(value)
    if t == TIMESTAMP:
        return parse_timestamp(value, aware=False)
    if t == TIMESTAMPTZ:
        return parse_timestamp(value, aware=True)
    if t == LOCATION_TYPE:
        return _to_location(value)
    if t == URL_TYPE:
        return _to_url(value)
    if t == PHONE_TYPE:
        return _to_phone(value)
    if t == JSON_TYPE:
        return _to_json(value)
    return _to_text(value)


def to_arrow_batch(rows: Sequence[dict[str, Any]], schema: pa.Schema) -> pa.RecordBatch:
    """Build a typed batch from decoded SODA rows.

    Args:
        rows: Decoded JSON objects, keyed by SoQL field name.
        schema: The (projected) output schema.

    Returns:
        A record batch with exactly ``schema``.

    """
    arrays = []
    for f in schema:
        key = field_soql_name(f)
        values = [convert_value(r.get(key), f) for r in rows]
        storage_type = f.type.storage_type if isinstance(f.type, pa.ExtensionType) else f.type
        arr = pa.array(values, type=storage_type)
        if isinstance(f.type, pa.ExtensionType):
            arr = pa.ExtensionArray.from_storage(f.type, arr)
        arrays.append(arr)
    return pa.RecordBatch.from_arrays(arrays, schema=schema)


# ---------------------------------------------------------------- SoQL text


def ident(name: str) -> str:
    """Quote a field name as a SoQL identifier (backticks)."""
    return "`" + name.replace("`", "") + "`"


def string_literal(value: str) -> str:
    r"""Return a SoQL string literal: single quotes, embedded quotes doubled, backslashes doubled.

    SoQL string literals treat ``\`` as an escape character (``'a\b'`` is the
    two-character text ``ab``), so a literal backslash must be written ``\\``.
    """
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


def like_escape(pattern: str) -> str:
    r"""Escape ``%``, ``_`` and ``\`` so text matches literally inside a SoQL ``like`` pattern."""
    return pattern.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _timestamp_literal(value: dt.datetime, socrata_type: str) -> str | None:
    if value.microsecond % 1000:
        return None  # Socrata stores milliseconds; an exact sub-ms bound cannot be expressed
    if socrata_type in FIXED_TS:
        aware = value if value.tzinfo else value.replace(tzinfo=dt.UTC)
        utc = aware.astimezone(dt.UTC).replace(tzinfo=None)
        return "'" + utc.isoformat(timespec="milliseconds") + "Z'"
    naive = value if value.tzinfo is None else value.astimezone(dt.UTC).replace(tzinfo=None)
    return "'" + naive.isoformat(timespec="milliseconds") + "'"


def literal(value: Any, socrata_type: str) -> str | None:
    """Render a Python filter value as a SoQL literal for a column, or None if it cannot be pushed exactly."""
    if value is None:
        return None
    if isinstance(value, bool):
        return ("true" if value else "false") if socrata_type == "checkbox" else None
    if isinstance(value, (int, float, Decimal)):
        if socrata_type not in NUMERIC_TYPES and socrata_type != "double":
            return None
        if isinstance(value, float):
            return repr(value) if math.isfinite(value) else None
        if isinstance(value, Decimal):
            if not value.is_finite():
                return None
            text = format(value.normalize(), "f")
            return text
        return str(value)
    if isinstance(value, dt.datetime):
        if socrata_type not in FLOATING_TS | FIXED_TS:
            return None
        return _timestamp_literal(value, socrata_type)
    if isinstance(value, dt.date):
        if socrata_type not in FLOATING_TS | FIXED_TS:
            return None
        return _timestamp_literal(dt.datetime(value.year, value.month, value.day), socrata_type)
    if isinstance(value, str):
        return string_literal(value) if socrata_type in ("text", "row_identifier") else None
    return None


_OPS = {"eq": "=", "ne": "!=", "gt": ">", "ge": ">=", "lt": "<", "le": "<="}


def _scalar_py(value: Any) -> Any:
    return value.as_py() if hasattr(value, "as_py") else value


def translate_filter(flt: Any, columns: dict[str, tuple[str, str]]) -> str | None:
    """Translate one pushed-down filter to SoQL, or None when it cannot be pushed safely.

    Args:
        flt: A filter object from ``vgi.table_filter_pushdown``.
        columns: Map of output column name to ``(SoQL field name, Socrata type)`` for
            the columns that may be pushed. Columns not listed are never pushed.

    Returns:
        A SoQL boolean expression, or None.

    """
    kind = type(flt).__name__
    if kind == "AndFilter":
        prefix = _prefix_range(flt, columns)
        if prefix is not None:
            return prefix
        parts = [p for p in (translate_filter(c, columns) for c in flt.children) if p]
        return " AND ".join(f"({p})" for p in parts) if parts else None
    if kind == "OrFilter":
        maybe = [translate_filter(c, columns) for c in flt.children]
        if not maybe or any(p is None for p in maybe):
            return None
        return " OR ".join(f"({p})" for p in maybe)
    name = getattr(flt, "column_name", None)
    if name not in columns:
        return None
    field, socrata_type = columns[name]
    col = ident(field)
    if kind == "IsNullFilter":
        return f"{col} IS NULL"
    if kind == "IsNotNullFilter":
        return f"{col} IS NOT NULL"
    if socrata_type not in PUSHABLE_EQ:
        return None
    if kind == "ConstantFilter":
        op = getattr(flt.op, "value", str(flt.op))
        if op not in _OPS:
            return None
        if op not in ("eq", "ne") and socrata_type not in PUSHABLE_RANGE:
            return None
        lit = literal(_scalar_py(flt.value), socrata_type)
        return None if lit is None else f"{col} {_OPS[op]} {lit}"
    if kind == "InFilter":
        lits = []
        for v in flt.values.to_pylist():
            if v is None:
                continue
            lit = literal(v, socrata_type)
            if lit is None:
                return None
            lits.append(lit)
        return f"{col} IN ({', '.join(lits)})" if lits else None
    if kind in ("ExpressionFilter", "V2ExpressionFilter"):
        prefix = _prefix_of(flt)
        if prefix and socrata_type == "text":
            return f"starts_with({col}, {string_literal(prefix)})"
    return None


def _prefix_range(flt: Any, columns: dict[str, tuple[str, str]]) -> str | None:
    """Recognize DuckDB's rewrite of ``LIKE 'p%'``: ``col >= 'p' AND col < 'q'`` (q = p with last char + 1)."""
    children = list(getattr(flt, "children", ()))
    if len(children) != 2 or any(type(c).__name__ != "ConstantFilter" for c in children):
        return None
    names = {c.column_name for c in children}
    if len(names) != 1:
        return None
    name = names.pop()
    if name not in columns or columns[name][1] != "text":
        return None
    by_op = {getattr(c.op, "value", str(c.op)): _scalar_py(c.value) for c in children}
    low, high = by_op.get("ge"), by_op.get("lt")
    if not isinstance(low, str) or not isinstance(high, str) or not low:
        return None
    if high != low[:-1] + chr(ord(low[-1]) + 1):
        return None
    return f"starts_with({ident(columns[name][0])}, {string_literal(low)})"


_PREFIX_RE = re.compile(r"""^\s*(?:prefix|starts_with)\s*\(\s*"?[^,]*"?\s*,\s*'((?:[^']|'')*)'\s*\)\s*$""", re.I)


def _prefix_of(flt: Any) -> str | None:
    """Extract the literal prefix of a ``starts_with(col, 'p')``/``prefix`` expression filter."""
    expr = getattr(flt, "expr", None)
    if expr is None or not hasattr(expr, "to_sql"):
        return None
    try:
        sql = expr.to_sql(flt.column_name)
    except Exception:
        return None
    m = _PREFIX_RE.match(sql)
    if not m:
        return None
    prefix = m.group(1).replace("''", "'")
    return prefix or None


def where_clauses(filters: Any, columns: dict[str, tuple[str, str]]) -> list[str]:
    """Translate the top-level (AND-ed) pushed filters; untranslatable ones are skipped."""
    out: list[str] = []
    for flt in getattr(filters, "filters", None) or ():
        clause = translate_filter(flt, columns)
        if clause:
            out.append(clause)
    return out


def combine_where(parts: Iterable[str | None]) -> str | None:
    """AND together non-empty SoQL clauses (each parenthesized when there are several)."""
    kept = [p.strip() for p in parts if p and p.strip()]
    if not kept:
        return None
    if len(kept) == 1:
        return kept[0]
    return " AND ".join(f"({p})" for p in kept)
