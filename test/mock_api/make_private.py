#!/usr/bin/env python3
"""Write the hand-made ``private.example.org`` fixtures (a token-only portal).

It exercises column types the recorded portals do not have (fixed_timestamp,
money, percent, double, phone, polygon) and the app-token header. Run it after
changing the requests the worker makes for these queries::

    python3 test/mock_api/make_private.py
"""

from __future__ import annotations

import json
import sys
import urllib.parse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from server import FIXTURES, canonical, fixture_file

ALIAS = "private.example.org"
VIEW = {
    "id": "abcd-1234",
    "name": "Permits (private test dataset)",
    "columns": [
        {"fieldName": "permit", "name": "Permit", "dataTypeName": "text", "position": 1},
        {"fieldName": "issued_at", "name": "Issued At", "dataTypeName": "fixed_timestamp", "position": 2},
        {"fieldName": "fee", "name": "Fee", "dataTypeName": "money", "position": 3},
        {"fieldName": "share", "name": "Share", "dataTypeName": "percent", "position": 4},
        {"fieldName": "score", "name": "Score", "dataTypeName": "double", "position": 5},
        {"fieldName": "paid", "name": "Paid", "dataTypeName": "checkbox", "position": 6},
        {"fieldName": "contact", "name": "Contact", "dataTypeName": "phone", "position": 7},
        {"fieldName": "area", "name": "Area", "dataTypeName": "polygon", "position": 8},
        {"fieldName": ":@computed_region_x", "name": "Region", "dataTypeName": "number", "position": 9},
    ],
}
ROWS = [
    {
        "permit": "P-1",
        "issued_at": "2024-01-01T10:30:00.000Z",
        "fee": "125.50",
        "share": "12.5",
        "score": 0.25,
        "paid": True,
        "contact": {"phone_number": "555-0100", "phone_type": "Office"},
        "area": {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 0]]]},
        ":id": "row-aaaa-0001",
    },
    {
        "permit": "P-2",
        "issued_at": "2024-02-01T00:00:00.000Z",
        "fee": "99999999999999999999",
        "paid": False,
        ":id": "row-aaaa-0002",
    },
]
COLS = ["permit", "issued_at", "fee", "share", "score", "paid", "contact", "area", ":id"]


def put(path: str, params: dict[str, str], body: object) -> None:
    query = urllib.parse.urlencode(sorted(params.items()))
    key = canonical(path, query)
    data = {"request": key, "status": 200, "content_type": "application/json", "headers": {}, "body": json.dumps(body)}
    f = fixture_file(ALIAS, key)
    f.parent.mkdir(parents=True, exist_ok=True)
    f.write_text(json.dumps(data, indent=1) + "\n", encoding="utf-8")
    print(f"{key} -> {f.relative_to(FIXTURES)}", file=sys.stderr)


def main() -> None:
    """Write every private fixture."""
    put("/api/views/abcd-1234.json", {}, VIEW)
    select = ",".join(f"`{c}`" for c in COLS)
    put("/resource/abcd-1234.json", {"$select": select, "$order": "`:id`", "$limit": "1000"}, ROWS)
    put(
        "/resource/abcd-1234.json",
        {
            "$select": "`issued_at`,`permit`,`:id`",
            "$order": "`:id`",
            "$limit": "1000",
            "$where": "`issued_at` >= '2024-01-15T00:00:00.000Z'",
        },
        [{"issued_at": ROWS[1]["issued_at"], "permit": "P-2", ":id": "row-aaaa-0002"}],
    )


if __name__ == "__main__":
    main()
