"""SoQL quoting scalars: ``soql_quote(value)`` and ``soql_like_escape(pattern)``.

Use them to build ``soql_where :=`` conditions and ``soql()`` queries from data
safely. Both run locally; no network.
"""

from __future__ import annotations

import json
from typing import Annotated

import pyarrow as pa
from vgi.arguments import Param, Returns
from vgi.metadata import FunctionExample
from vgi.scalar_function import ScalarFunction

from .meta import object_tags
from .soql import like_escape, string_literal


class SoqlQuote(ScalarFunction):
    """``soql_quote(value)``: a SoQL single-quoted string literal."""

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "soql_quote"
        description = "Quote text as a SoQL string literal for soql_where := and soql() queries"
        tags = {
            **object_tags(
                title="Quote SoQL Literal",
                category="soql",
                keywords="soql, quote, escape, literal, string, where, injection-safe",
                doc_llm=(
                    "Return `value` as a SoQL string literal: wrapped in single quotes with embedded single quotes "
                    "doubled and backslashes doubled (SoQL literals treat backslash as an escape), ready to "
                    "concatenate into a "
                    "`soql_where :=` condition of dataset() or a soql() query, "
                    "e.g. 'borough = ' || soql_quote(b). NULL in, NULL out. Runs locally without contacting any "
                    "portal. SQL WHERE clauses on dataset() do not need it: those are translated and quoted "
                    "automatically."
                ),
                doc_md=(
                    "## soql_quote(value)\n\n"
                    "Quote text for use inside raw SoQL.\n\n"
                    "`soql_quote('O''Brien')` returns `'O''Brien'`."
                ),
            ),
            "vgi.executable_examples": json.dumps(
                [
                    {
                        "description": "Quote a value containing an apostrophe",
                        "sql": "SELECT socrata.main.soql_quote('O''Brien') AS literal",
                        "expected_result": [{"literal": "'O''Brien'"}],
                    },
                    {
                        "description": "Build a SoQL condition for each value of a column",
                        "sql": (
                            "SELECT 'borough = ' || socrata.main.soql_quote(b) AS condition "
                            "FROM (VALUES ('BRONX'), ('QUEENS')) t(b) ORDER BY b"
                        ),
                        "expected_result": [
                            {"condition": "borough = 'BRONX'"},
                            {"condition": "borough = 'QUEENS'"},
                        ],
                    },
                ]
            ),
        }
        examples = [
            FunctionExample(
                sql="SELECT socrata.main.soql_quote('O''Brien') AS literal",
                description="Quote a value containing an apostrophe for a SoQL condition",
            ),
            FunctionExample(
                sql=(
                    "SELECT count(*) AS requests FROM socrata.main.dataset('data.cityofnewyork.us', 'erm2-nwe9', "
                    "soql_where := 'agency = ' || socrata.main.soql_quote('NYPD') || "
                    "' AND created_date >= ''2024-01-01T00:00:00'' AND created_date < ''2024-01-01T00:10:00''')"
                ),
                description="Use a quoted literal inside a raw SoQL condition",
            ),
        ]

    @classmethod
    def compute(
        cls,
        value: Annotated[pa.StringArray, Param(doc="Text to quote; NULL in, NULL out.")],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Quote every value."""
        return pa.array(
            [None if v is None else string_literal(v) for v in value.to_pylist()],
            type=pa.string(),
        )


class SoqlLikeEscape(ScalarFunction):
    """``soql_like_escape(text)``: escape LIKE wildcards so text matches literally."""

    class Meta:
        """Function metadata surfaced to DuckDB, agents and vgi-lint."""

        name = "soql_like_escape"
        description = "Escape %, _ and backslash so text matches literally inside a SoQL like pattern"
        tags = {
            **object_tags(
                title="Escape SoQL LIKE Pattern",
                category="soql",
                keywords="soql, like, escape, wildcard, pattern, percent, underscore",
                doc_llm=(
                    "Escape the LIKE wildcards `%` and `_` (and the escape character backslash) in `text` with a "
                    "backslash, so it matches literally inside a SoQL `like` pattern. Combine with soql_quote: "
                    "'descriptor like ' || soql_quote('%' || soql_like_escape(t) || '%'). NULL in, NULL out. "
                    "Runs locally. DuckDB never pushes LIKE/ILIKE into dataset() scans, so a server-side "
                    "substring match must be written as soql_where := or inside soql()."
                ),
                doc_md=("## soql_like_escape(text)\n\n`soql_like_escape('100%_done')` returns `100\\%\\_done`."),
            ),
            "vgi.executable_examples": json.dumps(
                [
                    {
                        "description": "Escape the wildcards in a value",
                        "sql": "SELECT socrata.main.soql_like_escape('100%_done') AS pattern",
                        "expected_result": [{"pattern": "100\\%\\_done"}],
                    },
                    {
                        "description": "Build a quoted contains-pattern for a SoQL like",
                        "sql": (
                            "SELECT socrata.main.soql_quote('%' || socrata.main.soql_like_escape('50%') || '%') "
                            "AS pattern"
                        ),
                        "expected_result": [{"pattern": "'%50\\\\%%'"}],
                    },
                ]
            ),
        }
        examples = [
            FunctionExample(
                sql="SELECT socrata.main.soql_like_escape('100%_done') AS pattern",
                description="Escape LIKE wildcards in a literal value",
            ),
            FunctionExample(
                sql=(
                    "SELECT descriptor, count(*) AS requests FROM socrata.main.dataset('data.cityofnewyork.us', "
                    "'erm2-nwe9', soql_where := 'upper(descriptor) like ' || socrata.main.soql_quote('%' || "
                    "socrata.main.soql_like_escape('NOISE') || '%') || ' AND created_date >= ''2024-01-01T00:00:00'' "
                    "AND created_date < ''2024-01-01T01:00:00''') "
                    "GROUP BY descriptor ORDER BY requests DESC, descriptor"
                ),
                description="NYC 311 noise complaints in the first hour of 2024, matched on the portal",
            ),
        ]

    @classmethod
    def compute(
        cls,
        text: Annotated[pa.StringArray, Param(doc="Text to escape; NULL in, NULL out.")],
    ) -> Annotated[pa.StringArray, Returns()]:
        """Escape every value."""
        return pa.array([None if v is None else like_escape(v) for v in text.to_pylist()], type=pa.string())
