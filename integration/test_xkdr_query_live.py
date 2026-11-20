"""One live XKDR measurements request.

This file is outside ``tests/`` so the ordinary suite does not need a network
connection or an API key. It calls ``fetch_measurements`` once, prints the
response shape, and compares that shape with the query client's expectations.
The response is not written to ``data/raw``, ``data/processed``, or the query
cache. Request headers are not stored or printed.
"""

from __future__ import annotations

import io
import re
from collections.abc import Mapping
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ingestion.xkdr_query import (
    MeasurementQuery,
    MeasurementResult,
    QueryError,
    QueryResponse,
    UrllibQueryTransport,
    _AGGREGATE_COLUMNS,
    _PERIOD_COLUMNS,
    fetch_measurements,
)

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_QUERY = MeasurementQuery(
    start=date(2024, 2, 15),
    end=date(2024, 2, 15),
    source="cpcb_caaqm",
    parameters=("PM2.5",),
    aggregation="daily",
    response_format="parquet",
)
_SAMPLE_LIMIT = 3
_SENSITIVE_COLUMN = re.compile(r"authorization|api_key|token|secret|password|cookie|email", re.IGNORECASE)
_SECRET_TEXT = re.compile(r"Bearer\s+\S+|aqi_[A-Za-z0-9_-]+|AQI_API_KEY\s*=\s*\S+", re.IGNORECASE)


class _RecordingTransport:
    """Forward one GET and retain only the response fields this test prints."""

    def __init__(self) -> None:
        self._inner = UrllibQueryTransport()
        self.attempts = 0
        self.status: int | None = None
        self.content_type: str | None = None
        self.truncated: str | None = None
        self.reported_rows: str | None = None
        self.body = b""

    def send(
        self,
        url: str,
        headers: Mapping[str, str],
        timeout: float,
        max_bytes: int,
    ) -> QueryResponse:
        self.attempts += 1
        if self.attempts > 1:
            raise QueryError("The integration test refused a second measurements request.")
        response = self._inner.send(url, headers, timeout, max_bytes)
        self.status = response.status
        self.content_type = _one_header(response.headers, "content-type")
        self.truncated = _one_header(response.headers, "x-truncated")
        self.reported_rows = _one_header(response.headers, "x-row-count")
        self.body = response.body
        return response


@pytest.mark.integration
def test_one_daily_pm25_request_matches_the_client_contract() -> None:
    transport = _RecordingTransport()
    result: MeasurementResult | None = None
    failure: QueryError | None = None
    try:
        result = fetch_measurements(_QUERY, project_root=_PROJECT_ROOT, transport=transport)
    except QueryError as exc:
        failure = exc
    except Exception:
        failure = QueryError("unexpected client failure")

    table = result.table if result is not None else _read_optional_table(transport.body)
    _print_report(transport, result, table)
    comparison = _compare(transport, result, table)
    print("Schema comparison: " + comparison)
    if failure is not None:
        print("Failure category: " + _failure_category(str(failure)))
        print("Failure detail: " + _public(str(failure)))
        pytest.fail(_failure_category(str(failure)), pytrace=False)
    assert transport.attempts == 1
    assert comparison == "matches the query client"


def _print_report(
    transport: _RecordingTransport,
    result: MeasurementResult | None,
    table: pa.Table | None,
) -> None:
    returned = "unavailable" if table is None else str(table.num_rows)
    print("HTTP status: " + ("unavailable" if transport.status is None else str(transport.status)))
    print("Content type: " + _public(transport.content_type or "absent"))
    print("X-Truncated: " + _public(transport.truncated or "absent"))
    print("X-Row-Count: " + _public(transport.reported_rows or "absent"))
    print("Returned rows: " + returned)
    print("Schema: " + _schema_text(table))
    print("Date range: " + _date_range(table))
    print("Sample records:")
    for line in _sample_lines(table):
        print("  " + line)


def _compare(
    transport: _RecordingTransport,
    result: MeasurementResult | None,
    table: pa.Table | None,
) -> str:
    problems: list[str] = []
    if transport.truncated is None:
        problems.append("X-Truncated is absent")
    elif transport.truncated.strip().lower() not in {"false", "0", "no"}:
        problems.append("X-Truncated is not false")
    if transport.reported_rows is None:
        problems.append("X-Row-Count is absent")
    elif table is not None and transport.reported_rows.strip() != str(table.num_rows):
        problems.append("X-Row-Count does not match the Parquet row count")
    if table is None:
        problems.append("Parquet body is unreadable")
    else:
        names = set(table.column_names)
        missing = [column for column in _AGGREGATE_COLUMNS if column not in names]
        if missing:
            problems.append("missing aggregate columns: " + ", ".join(missing))
        if not any(column in names for column in _PERIOD_COLUMNS):
            problems.append("no recognised period column")
    if result is None and not problems:
        problems.append("the query client rejected a response that otherwise matches")
    if not problems:
        return "matches the query client"
    return "; ".join(problems)


def _failure_category(message: str) -> str:
    text = message.lower()
    if "unexpected client failure" in text:
        return "unexpected_client_failure"
    if "second measurements request" in text:
        return "repeated_request"
    if "aqi_api_key" in text:
        return "missing_api_key"
    if "timed out" in text:
        return "timeout"
    if "could not reach" in text:
        return "connection_error"
    if "redirect" in text:
        return "unexpected_redirect"
    if "truncated" in text:
        return "truncated_response"
    if "x-truncated" in text:
        return "invalid_truncation_header"
    if "x-row-count" in text and "does not match" in text:
        return "row_count_mismatch"
    if "x-row-count" in text:
        return "invalid_row_count_header"
    if "text body" in text:
        return "unexpected_content_type"
    if "empty parquet" in text:
        return "empty_body"
    if "could not be read" in text:
        return "malformed_parquet"
    if "missing columns" in text or "period column" in text:
        return "schema_mismatch"
    if "size limit" in text:
        return "response_too_large"
    if "http " in text:
        status = _http_status(message)
        return "http_" + status if status else "http_error"
    return "rejected_response"


def _http_status(message: str) -> str:
    match = re.search(r"HTTP (\d{3})", message)
    return match.group(1) if match else ""


def _schema_text(table: pa.Table | None) -> str:
    if table is None:
        return "unavailable"
    return ", ".join(f"{field.name}:{field.type}" for field in table.schema)


def _date_range(table: pa.Table | None) -> str:
    if table is None or table.num_rows == 0:
        return "unavailable"
    for name in _PERIOD_COLUMNS:
        if name not in table.column_names:
            continue
        values = [value for value in table.column(name).to_pylist() if value is not None]
        if not values:
            continue
        ordered = sorted(str(value) for value in values)
        return f"{ordered[0]} to {ordered[-1]}"
    return "no recognised period column"


def _sample_lines(table: pa.Table | None) -> list[str]:
    if table is None or table.num_rows == 0:
        return ["none"]
    columns = [name for name in table.column_names if _SENSITIVE_COLUMN.search(name) is None]
    lines: list[str] = []
    for row_index in range(min(_SAMPLE_LIMIT, table.num_rows)):
        fields = []
        for name in columns:
            value = _public(str(table.column(name)[row_index].as_py()))
            fields.append(f"{name}={value}")
        lines.append(", ".join(fields))
    return lines


def _read_optional_table(payload: bytes) -> pa.Table | None:
    if not payload:
        return None
    try:
        return pq.read_table(io.BytesIO(payload))
    except (OSError, pa.ArrowException, ValueError):
        return None


def _one_header(headers: Mapping[str, str], name: str) -> str | None:
    for key, value in headers.items():
        if key.lower() == name:
            return value
    return None


def _public(text: str) -> str:
    return _SECRET_TEXT.sub("[redacted]", text)
