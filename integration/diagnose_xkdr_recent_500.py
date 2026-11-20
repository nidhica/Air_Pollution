"""One-pass diagnosis of XKDR HTTP 500 responses for recent dates.

Each query shape is sent once. Nothing is written under ``data/raw``,
``data/interim``, or ``data/processed``. The API key and the Authorization
header are never printed. This script is not part of the dashboard.
"""

from __future__ import annotations

import json
import re
import sys
from datetime import date
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT / "src"))

from ingestion.acquire import AcquisitionError, read_api_key  # noqa: E402
from ingestion.xkdr_query import (  # noqa: E402
    MeasurementQuery,
    UrllibQueryTransport,
    build_measurements_url,
    measurement_headers,
)

_ORIGIN = "https://airquality.xkdr.org"
_TIMEOUT_SECONDS = 60.0
_MAX_BYTES = 8 * 1024 * 1024
_TEXT_LIMIT = 500
_HEADER_NAMES = (
    "content-type",
    "content-length",
    "date",
    "server",
    "retry-after",
    "x-truncated",
    "x-row-count",
    "x-request-id",
    "cf-ray",
)
_SECRET_TEXT = re.compile(r"Bearer\s+\S+|aqi_[A-Za-z0-9_-]+", re.IGNORECASE)
_SENSITIVE_KEY = re.compile(r"authorization|api_key|token|secret|password|cookie|email", re.IGNORECASE)


def main() -> int:
    try:
        api_key = read_api_key(_ROOT)
    except AcquisitionError as exc:
        print("Key status: missing")
        print("Detail: " + _public(str(exc), ""))
        return 1
    if not api_key.strip():
        print("Key status: empty")
        return 1
    print("Key status: present")
    print("Requests: one each, no retry after HTTP 500")
    transport = UrllibQueryTransport()
    _meta(transport, api_key)
    print("")
    print("Daily measurements (source=cpcb_caaqm, format=parquet)")
    for label, start, end, parameters in _daily_cases():
        _measurements(transport, api_key, label, start, end, parameters, "daily")
    print("")
    print("Separate raw comparison (not used by the dashboard)")
    _measurements(
        transport,
        api_key,
        "raw 2026-11-18 PM2.5",
        date(2026, 11, 18),
        date(2026, 11, 18),
        ("PM2.5",),
        "raw",
    )
    return 0


def _daily_cases() -> tuple[tuple[str, date, date, tuple[str, ...]], ...]:
    recent_days = (date(2026, 11, 18), date(2026, 11, 17))
    week = (date(2026, 11, 12), date(2026, 11, 18))
    pollutants = (
        ("PM2.5", ("PM2.5",)),
        ("PM10", ("PM10",)),
        ("PM2.5+PM10", ("PM2.5", "PM10")),
    )
    cases: list[tuple[str, date, date, tuple[str, ...]]] = []
    for day in recent_days:
        for name, parameters in pollutants:
            cases.append((f"daily {day.isoformat()} {name}", day, day, parameters))
    start, end = week
    for name, parameters in pollutants:
        cases.append((f"daily {start.isoformat()} to {end.isoformat()} {name}", start, end, parameters))
    cases.append(("daily 2024-02-15 PM2.5 historical control", date(2024, 2, 15), date(2024, 2, 15), ("PM2.5",)))
    return tuple(cases)


def _meta(transport: UrllibQueryTransport, api_key: str) -> None:
    headers = measurement_headers(api_key)
    headers["Accept"] = "application/json"
    response = transport.send(f"{_ORIGIN}/v1/meta?format=json", headers, _TIMEOUT_SECONDS, _MAX_BYTES)
    print("")
    print("GET /v1/meta")
    _print_response(response.status, response.headers, response.body, api_key)


def _measurements(
    transport: UrllibQueryTransport,
    api_key: str,
    label: str,
    start: date,
    end: date,
    parameters: tuple[str, ...],
    aggregation: str,
) -> None:
    query = MeasurementQuery(
        start=start,
        end=end,
        source="cpcb_caaqm",
        parameters=parameters,
        aggregation=aggregation,
        response_format="parquet",
    )
    url = build_measurements_url(query)
    if api_key in url:
        print("")
        print(label)
        print("Refused: the measurements URL contained the API key.")
        return
    response = transport.send(url, measurement_headers(api_key), _TIMEOUT_SECONDS, _MAX_BYTES)
    print("")
    print(label)
    print(
        "Query: source=cpcb_caaqm"
        f" start={start.isoformat()} end={end.isoformat()}"
        f" agg={aggregation} format=parquet"
        f" parameter={','.join(parameters)}"
    )
    _print_response(response.status, response.headers, response.body, api_key)


def _print_response(status: int, headers: object, body: bytes, secret: str) -> None:
    print("HTTP status: " + str(status))
    lowered = {str(key).lower(): str(value) for key, value in dict(headers).items()}
    for name in _HEADER_NAMES:
        if name in lowered:
            print(f"{name}: " + _public(lowered[name], secret))
    content_type = lowered.get("content-type", "")
    print("Body: " + _body_text(content_type, body, secret))


def _body_text(content_type: str, body: bytes, secret: str) -> str:
    if not body:
        return "empty"
    media = content_type.split(";", 1)[0].strip().lower()
    if "parquet" in media or body.startswith(b"PAR1"):
        return _parquet_summary(body)
    if media.startswith("application/json") or body[:1] == b"{":
        text = body.decode("utf-8", errors="replace")
        summary = _json_summary(text, secret)
        if summary:
            return summary
    if media.startswith("text/") or body[:1] in {b"{", b"<"}:
        text = body[:_TEXT_LIMIT].decode("utf-8", errors="replace")
        return _public(text.replace("\n", " "), secret)
    return f"{len(body)} bytes, content type {content_type or 'absent'}"


def _json_summary(text: str, secret: str) -> str:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return ""
    if not isinstance(payload, dict):
        return _public(text[:_TEXT_LIMIT], secret)
    parts: list[str] = []
    for field in ("error", "detail"):
        value = payload.get(field)
        if isinstance(value, str) and value.strip():
            parts.append(f"{field}={_public(value.strip(), secret)}")
    coverage = _coverage_fields(payload, secret)
    if coverage:
        parts.append(coverage)
    if len(text) <= 2000:
        parts.append("json=" + _public(json.dumps(payload, separators=(",", ":")), secret))
    if parts:
        return "; ".join(parts)
    return "JSON object with keys: " + ", ".join(sorted(str(key) for key in payload))


def _coverage_fields(payload: dict[str, object], secret: str) -> str:
    found: list[str] = []

    def walk(value: object, path: str) -> None:
        if len(found) >= 12:
            return
        if isinstance(value, dict):
            for key, item in value.items():
                name = str(key)
                if _SENSITIVE_KEY.search(name):
                    continue
                walk(item, f"{path}.{name}" if path else name)
            return
        if isinstance(value, list):
            if value and not isinstance(value[0], (dict, list)):
                shown = ", ".join(_public(str(item), secret) for item in value[:8])
                found.append(f"{path}=[{shown}]")
            return
        if isinstance(value, (str, int, float, bool)) and path:
            leaf = path.rsplit(".", 1)[-1]
            if re.search(r"month|from|to|export|tier|rows|coverage|published", leaf, re.IGNORECASE):
                found.append(f"{path}={_public(str(value), secret)}")

    walk(payload, "")
    return "; ".join(found)


def _parquet_summary(body: bytes) -> str:
    import io

    import pyarrow.parquet as pq

    try:
        table = pq.read_table(io.BytesIO(body))
    except Exception:
        return f"unreadable parquet, {len(body)} bytes"
    names = ", ".join(table.column_names)
    return f"parquet rows={table.num_rows}; columns={names}"


def _public(text: str, secret: str) -> str:
    cleaned = text.replace(secret, "[redacted]") if secret else text
    return _SECRET_TEXT.sub("[redacted]", cleaned)


if __name__ == "__main__":
    raise SystemExit(main())
