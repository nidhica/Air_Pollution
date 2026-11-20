"""Server-side client for XKDR ``GET /v1/measurements``.

The client builds one measurements query and reads a Parquet body. It does
not download bulk files, does not start Spark, and does not write under
``data/raw`` or ``data/processed``. The Timeline asks this module only for
the most recent 7 days in Asia/Kolkata. The API key is not placed in
Streamlit state.

The API key is read with ``ingestion.acquire.read_api_key`` and is sent only
as an ``Authorization: Bearer`` header. It is never written into a URL,
exception message, or return value.

Public contract, from https://github.com/xKDR/Air-Quality-Database/blob/main/docs/PUBLIC_API.md :

- ``start`` and ``end`` are inclusive ``YYYY-MM-DD`` dates in Indian Standard Time.
- ``source`` is ``cpcb_caaqm`` or ``us_embassy``.
- ``parameter`` may be repeated. ``state`` and ``city`` are optional exact filters.
- ``station`` may be repeated.
- ``agg`` is ``raw``, ``hourly``, ``daily``, or ``monthly``.
- ``format=parquet`` is the only response body this client accepts.

A successful call requires HTTP 200, a readable Parquet body, ``X-Truncated: false``,
and an ``X-Row-Count`` that matches the table. Anything else is rejected.
Truncated, timed-out, and malformed bodies are not returned as partial results.

These points are taken from the public document and still need a live response
before the dashboard relies on them:

- Every 200 response includes ``X-Truncated`` and ``X-Row-Count``. A missing
  header is treated as an incomplete result.
- ``agg=raw`` rows include ``station_id``, ``parameter_name``, ``unit``,
  ``collected_at``, and ``value``. City, state, and coordinates are not in
  that payload and are not joined here.
- Aggregates include ``mean``, ``min``, ``max``, and ``n``. A live daily
  response uses ``period_start``. One of ``period_start``, ``collected_at``,
  ``period``, ``date``, ``month``, ``observation_date``, or ``timestamp``
  is required.
- The Parquet content type is not specified. A JSON or text type is rejected.
  Any other type is accepted only when the body parses as Parquet.
- A personal key is documented as uncapped. ``X-Truncated: true`` is still
  rejected. The documented server timeout is 90 seconds. This client stops
  at 60 seconds by default.
- One live daily request confirmed ``period_start`` with ``mean``, ``min``,
  ``max``, and ``n``. Hourly, monthly, and raw Parquet shapes are still
  unverified.
"""

from __future__ import annotations

import hashlib
import io
import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urlparse
from urllib.request import Request, urlopen

import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.acquire import AcquisitionError, read_api_key

_API_ORIGIN = "https://airquality.xkdr.org"
_MEASUREMENTS_PATH = "/v1/measurements"
_USER_AGENT = "AirPollutionBDA/1.0 (academic research)"
_SOURCES = frozenset({"cpcb_caaqm", "us_embassy"})
_AGGREGATIONS = frozenset({"raw", "hourly", "daily", "monthly"})
_RAW_COLUMNS = ("station_id", "parameter_name", "unit", "collected_at", "value")
_AGGREGATE_COLUMNS = ("mean", "min", "max", "n")
_PERIOD_COLUMNS = (
    "period_start",
    "collected_at",
    "period",
    "date",
    "month",
    "observation_date",
    "timestamp",
)
_DEFAULT_TIMEOUT_SECONDS = 60.0
_DEFAULT_MAX_BYTES = 512 * 1024 * 1024
_TRUE_VALUES = frozenset({"true", "1", "yes"})
_FALSE_VALUES = frozenset({"false", "0", "no"})


class QueryError(Exception):
    """A measurements query was rejected before a complete table was returned."""


class QueryResponse(Protocol):
    status: int
    headers: Mapping[str, str]
    body: bytes

    def close(self) -> None:
        """Release the connection."""


class QueryTransport(Protocol):
    def send(
        self,
        url: str,
        headers: Mapping[str, str],
        timeout: float,
        max_bytes: int,
    ) -> QueryResponse:
        """Perform one GET. Implementations must not log ``headers`` or ``url``."""


@dataclass(frozen=True)
class MeasurementQuery:
    """One ``/v1/measurements`` request. The API key is not a field."""

    start: date
    end: date
    source: str = "cpcb_caaqm"
    parameters: tuple[str, ...] = ("PM2.5", "PM10")
    aggregation: str = "raw"
    response_format: str = "parquet"
    state: str | None = None
    city: str | None = None
    stations: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        start = _as_date(self.start, "start")
        end = _as_date(self.end, "end")
        if end < start:
            raise QueryError("The end date is before the start date.")
        source = str(self.source).strip().lower()
        if source not in _SOURCES:
            raise QueryError("source must be cpcb_caaqm or us_embassy.")
        aggregation = str(self.aggregation).strip().lower()
        if aggregation not in _AGGREGATIONS:
            raise QueryError("aggregation must be raw, hourly, daily, or monthly.")
        response_format = str(self.response_format).strip().lower()
        if response_format != "parquet":
            raise QueryError("Only format=parquet is accepted.")
        parameters = _unique_text(self.parameters, "parameter")
        if not parameters:
            raise QueryError("At least one pollutant is required.")
        stations = _unique_text(self.stations, "station")
        object.__setattr__(self, "start", start)
        object.__setattr__(self, "end", end)
        object.__setattr__(self, "source", source)
        object.__setattr__(self, "parameters", parameters)
        object.__setattr__(self, "aggregation", aggregation)
        object.__setattr__(self, "response_format", response_format)
        object.__setattr__(self, "state", _optional_text(self.state))
        object.__setattr__(self, "city", _optional_text(self.city))
        object.__setattr__(self, "stations", stations)

    def normalized(self) -> dict[str, object]:
        """Stable parameters for a cache key. Location text is case-folded."""

        return {
            "aggregation": self.aggregation,
            "city": None if self.city is None else self.city.casefold(),
            "end": self.end.isoformat(),
            "format": self.response_format,
            "parameters": list(self.parameters),
            "source": self.source,
            "start": self.start.isoformat(),
            "state": None if self.state is None else self.state.casefold(),
            "stations": list(self.stations),
        }

    def cache_token(self) -> str:
        """Hash of the normalized parameters. The API key cannot be part of it."""

        payload = json.dumps(self.normalized(), sort_keys=True, separators=(",", ":")).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()


@dataclass(frozen=True)
class MeasurementResult:
    """A complete Parquet table. ``payload`` is the response body, unchanged."""

    query: MeasurementQuery
    payload: bytes
    table: pa.Table
    row_count: int
    reported_row_count: int

    def __repr__(self) -> str:
        return (
            f"MeasurementResult(rows={self.row_count}, aggregation={self.query.aggregation!r})"
        )


class UrllibQueryTransport:
    """Stdlib GET. Redirects that leave the XKDR host are refused."""

    def send(
        self,
        url: str,
        headers: Mapping[str, str],
        timeout: float,
        max_bytes: int,
    ) -> QueryResponse:
        request = Request(url, headers=dict(headers), method="GET")
        try:
            response = urlopen(request, timeout=timeout)
        except HTTPError as exc:
            try:
                body = exc.read(max_bytes + 1)
                headers = dict(exc.headers.items()) if exc.headers else {}
                status = int(exc.code)
            finally:
                exc.close()
            return _ClosedResponse(status, headers, body)
        except TimeoutError as exc:
            raise QueryError("XKDR measurements query timed out.") from exc
        except (URLError, OSError) as exc:
            raise QueryError("Could not reach the XKDR measurements API.") from exc
        if urlparse(url).netloc != urlparse(response.geturl()).netloc:
            response.close()
            raise QueryError("Refusing a redirect that leaves the XKDR host.")
        try:
            body = _read_limited(response, max_bytes)
            status = int(response.status)
            headers = dict(response.headers.items())
        finally:
            response.close()
        return _ClosedResponse(status, headers, body)


class _ClosedResponse:
    def __init__(self, status: int, headers: Mapping[str, str], body: bytes) -> None:
        self.status = status
        self.headers = headers
        self.body = body

    def close(self) -> None:
        return None


def measurement_headers(api_key: str) -> dict[str, str]:
    """Headers for one measurements request. The key stays inside Authorization."""

    token = api_key.strip()
    if not token:
        raise QueryError("AQI_API_KEY is empty.")
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.apache.parquet",
        "User-Agent": _USER_AGENT,
    }


def build_measurements_url(query: MeasurementQuery) -> str:
    """Return the measurements URL. The API key is not a query parameter."""

    pairs: list[tuple[str, str]] = [
        ("source", query.source),
        ("start", query.start.isoformat()),
        ("end", query.end.isoformat()),
        ("agg", query.aggregation),
        ("format", query.response_format),
    ]
    pairs.extend(("parameter", name) for name in query.parameters)
    if query.state is not None:
        pairs.append(("state", query.state))
    if query.city is not None:
        pairs.append(("city", query.city))
    pairs.extend(("station", station) for station in query.stations)
    return f"{_API_ORIGIN}{_MEASUREMENTS_PATH}?{urlencode(pairs)}"


def fetch_measurements(
    query: MeasurementQuery,
    *,
    project_root: Path,
    transport: QueryTransport | None = None,
    api_key: str | None = None,
    timeout: float = _DEFAULT_TIMEOUT_SECONDS,
    max_bytes: int = _DEFAULT_MAX_BYTES,
) -> MeasurementResult:
    """Fetch one complete measurements table.

    ``api_key`` overrides the environment for tests. A missing key raises
    ``QueryError``. The returned object does not contain the key.
    """

    if timeout <= 0:
        raise QueryError("The measurements timeout must be positive.")
    if max_bytes <= 0:
        raise QueryError("The measurements size limit must be positive.")
    key = _resolve_api_key(project_root, api_key)
    url = build_measurements_url(query)
    if key in url:
        raise QueryError("Refusing a measurements URL that contains the API key.")
    headers = measurement_headers(key)
    client = transport if transport is not None else UrllibQueryTransport()
    try:
        response = client.send(url, headers, timeout, max_bytes)
    except TimeoutError as exc:
        raise QueryError("XKDR measurements query timed out.") from exc
    except QueryError:
        raise
    except OSError as exc:
        raise QueryError("Could not reach the XKDR measurements API.") from exc
    try:
        return _complete_result(query, response, key, max_bytes)
    finally:
        response.close()


def _resolve_api_key(project_root: Path, api_key: str | None) -> str:
    if api_key is not None:
        token = api_key.strip()
        if not token:
            raise QueryError("AQI_API_KEY is empty.")
        return token
    try:
        return read_api_key(project_root)
    except AcquisitionError as exc:
        raise QueryError(_redact(str(exc), "")) from exc


def _complete_result(
    query: MeasurementQuery,
    response: QueryResponse,
    api_key: str,
    max_bytes: int,
) -> MeasurementResult:
    body = response.body
    if response.status != 200:
        detail = "" if len(body) > max_bytes else _error_detail(body, api_key)
        message = f"XKDR measurements query failed with HTTP {response.status}."
        if detail:
            message = f"{message} {detail}"
        raise QueryError(message)
    if len(body) > max_bytes:
        raise QueryError("XKDR measurements response exceeded the configured size limit.")
    content_type = _header(response.headers, "content-type") or ""
    media_type = content_type.split(";", 1)[0].strip().lower()
    if media_type.startswith("application/json") or media_type.startswith("text/"):
        raise QueryError("XKDR returned a text body for a Parquet measurements query.")
    _require_complete_headers(response.headers)
    if len(body) == 0:
        raise QueryError("XKDR returned an empty Parquet body.")
    table = _read_parquet(body)
    reported = _reported_row_count(response.headers)
    if reported != table.num_rows:
        raise QueryError(
            "XKDR X-Row-Count does not match the Parquet row count "
            f"({reported} reported, {table.num_rows} readable)."
        )
    _require_columns(query, table)
    return MeasurementResult(
        query=query,
        payload=body,
        table=table,
        row_count=table.num_rows,
        reported_row_count=reported,
    )


def _require_complete_headers(headers: Mapping[str, str]) -> None:
    truncated = _header(headers, "x-truncated")
    if truncated is None:
        raise QueryError("XKDR measurements response did not include X-Truncated.")
    token = truncated.strip().lower()
    if token in _TRUE_VALUES:
        raise QueryError("XKDR marked the measurements response as truncated. The incomplete result was rejected.")
    if token not in _FALSE_VALUES:
        raise QueryError("XKDR X-Truncated was not true or false.")
    if _header(headers, "x-row-count") is None:
        raise QueryError("XKDR measurements response did not include X-Row-Count.")


def _reported_row_count(headers: Mapping[str, str]) -> int:
    raw = _header(headers, "x-row-count")
    if raw is None:
        raise QueryError("XKDR measurements response did not include X-Row-Count.")
    try:
        count = int(raw.strip())
    except ValueError as exc:
        raise QueryError("XKDR X-Row-Count was not an integer.") from exc
    if count < 0:
        raise QueryError("XKDR X-Row-Count was negative.")
    return count


def _require_columns(query: MeasurementQuery, table: pa.Table) -> None:
    names = set(table.column_names)
    if query.aggregation == "raw":
        missing = [column for column in _RAW_COLUMNS if column not in names]
        if missing:
            raise QueryError("XKDR raw Parquet is missing columns: " + ", ".join(missing) + ".")
        return
    missing = [column for column in _AGGREGATE_COLUMNS if column not in names]
    if missing:
        raise QueryError("XKDR aggregate Parquet is missing columns: " + ", ".join(missing) + ".")
    if not any(column in names for column in _PERIOD_COLUMNS):
        found = ", ".join(table.column_names) or "none"
        raise QueryError("XKDR aggregate Parquet has no recognised period column. Columns: " + found + ".")


def _read_parquet(payload: bytes) -> pa.Table:
    try:
        return pq.read_table(io.BytesIO(payload))
    except (OSError, pa.ArrowException, ValueError) as exc:
        raise QueryError("XKDR Parquet body could not be read.") from exc


def _read_limited(response: object, max_bytes: int) -> bytes:
    chunks: list[bytes] = []
    total = 0
    while True:
        chunk = response.read(1024 * 1024)  # type: ignore[attr-defined]
        if not chunk:
            break
        total += len(chunk)
        if total > max_bytes:
            raise QueryError("XKDR measurements response exceeded the configured size limit.")
        chunks.append(chunk)
    return b"".join(chunks)


def _header(headers: Mapping[str, str], name: str) -> str | None:
    for key, value in headers.items():
        if key.lower() == name:
            return value
    return None


def _error_detail(body: bytes, secret: str) -> str:
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return ""
    if not isinstance(payload, dict):
        return ""
    parts: list[str] = []
    for field in ("error", "detail"):
        value = payload.get(field)
        if isinstance(value, str) and value.strip():
            parts.append(value.strip())
    return _redact(": ".join(parts), secret)[:300]


def _redact(message: str, secret: str) -> str:
    if secret and secret in message:
        return message.replace(secret, "[redacted]")
    return message


def _as_date(value: object, label: str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return date.fromisoformat(value)
        except ValueError as exc:
            raise QueryError(f"{label} must be YYYY-MM-DD.") from exc
    raise QueryError(f"{label} must be a date.")


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _unique_text(values: object, label: str) -> tuple[str, ...]:
    if isinstance(values, str):
        items = [values]
    else:
        try:
            items = list(values)  # type: ignore[arg-type]
        except TypeError as exc:
            raise QueryError(f"{label} must be a sequence of strings.") from exc
    cleaned: list[str] = []
    for item in items:
        text = str(item).strip()
        if text and text not in cleaned:
            cleaned.append(text)
    cleaned.sort()
    return tuple(cleaned)
