"""Query-client and cache tests. The transport is fake and does not call XKDR."""

from __future__ import annotations

import hashlib
import io
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dashboard.query_cache import CacheError, QueryCache, default_cache_directory
from ingestion.xkdr_query import (
    MeasurementQuery,
    QueryError,
    build_measurements_url,
    fetch_measurements,
    measurement_headers,
)

_SECRET = "unit-test-secret-value"


class FakeResponse:
    def __init__(self, status: int, body: bytes, headers: dict[str, str] | None = None) -> None:
        self.status = status
        self.body = body
        self.headers = headers or {}
        self.closed = False

    def close(self) -> None:
        self.closed = True


class FakeTransport:
    def __init__(self, response: FakeResponse | Exception) -> None:
        self.response = response
        self.calls: list[tuple[str, dict[str, str], float, int]] = []

    def send(self, url: str, headers: dict[str, str], timeout: float, max_bytes: int) -> FakeResponse:
        self.calls.append((url, dict(headers), timeout, max_bytes))
        if isinstance(self.response, Exception):
            raise self.response
        return self.response


def test_authorization_header_is_bearer_and_stays_out_of_the_url(capsys: pytest.CaptureFixture[str]) -> None:
    query = _query()
    url = build_measurements_url(query)
    headers = measurement_headers(_SECRET)
    token = headers["Authorization"].removeprefix("Bearer ")
    assert headers["Authorization"].startswith("Bearer ")
    assert hashlib.sha256(token.encode("utf-8")).hexdigest() == hashlib.sha256(_SECRET.encode("utf-8")).hexdigest()
    assert token not in url
    assert "Authorization" not in url
    assert "parameter=PM10" in url and "parameter=PM2.5" in url
    captured = capsys.readouterr()
    assert captured.out == ""
    assert captured.err == ""


def test_successful_parquet_response_returns_the_table(capsys: pytest.CaptureFixture[str]) -> None:
    payload = _raw_payload()
    transport = FakeTransport(FakeResponse(200, payload, _headers(1)))
    result = fetch_measurements(_query(), project_root=Path("."), transport=transport, api_key=_SECRET)
    assert result.row_count == 1
    assert result.reported_row_count == 1
    assert result.table.column("value")[0].as_py() == 12.5
    assert result.payload == payload
    assert _SECRET not in repr(result)
    assert _SECRET not in transport.calls[0][0]
    sent = transport.calls[0][1]["Authorization"].removeprefix("Bearer ")
    assert hashlib.sha256(sent.encode("utf-8")).hexdigest() == hashlib.sha256(_SECRET.encode("utf-8")).hexdigest()
    captured = capsys.readouterr()
    assert _SECRET not in captured.out
    assert _SECRET not in captured.err


def test_truncated_response_is_rejected() -> None:
    headers = _headers(1)
    headers["X-Truncated"] = "true"
    transport = FakeTransport(FakeResponse(200, _raw_payload(), headers))
    with pytest.raises(QueryError, match="truncated"):
        fetch_measurements(_query(), project_root=Path("."), transport=transport, api_key=_SECRET)


def test_http_errors_timeouts_and_connection_failures_are_rejected() -> None:
    denied = json.dumps({"error": "invalid_api_key", "detail": f"rejected {_SECRET}"}).encode("utf-8")
    transport = FakeTransport(FakeResponse(401, denied, {"Content-Type": "application/json"}))
    with pytest.raises(QueryError, match="HTTP 401") as denied_error:
        fetch_measurements(_query(), project_root=Path("."), transport=transport, api_key=_SECRET)
    assert _SECRET not in str(denied_error.value)
    assert "[redacted]" in str(denied_error.value)

    transport = FakeTransport(FakeResponse(504, b"", {"Content-Type": "application/json"}))
    with pytest.raises(QueryError, match="HTTP 504"):
        fetch_measurements(_query(), project_root=Path("."), transport=transport, api_key=_SECRET)

    transport = FakeTransport(TimeoutError("timed out"))
    with pytest.raises(QueryError, match="timed out") as timeout_error:
        fetch_measurements(_query(), project_root=Path("."), transport=transport, api_key=_SECRET)
    assert _SECRET not in str(timeout_error.value)

    transport = FakeTransport(OSError("connection refused"))
    with pytest.raises(QueryError, match="Could not reach") as connection_error:
        fetch_measurements(_query(), project_root=Path("."), transport=transport, api_key=_SECRET)
    assert _SECRET not in str(connection_error.value)


def test_malformed_responses_are_rejected() -> None:
    cases = [
        FakeResponse(200, b"", _headers(0)),
        FakeResponse(200, b"not-parquet", _headers(0)),
        FakeResponse(200, _raw_payload(), {"X-Row-Count": "1", "Content-Type": "application/json"}),
        FakeResponse(200, _raw_payload(), {"X-Truncated": "false", "Content-Type": "application/octet-stream"}),
        FakeResponse(200, _raw_payload(), {"X-Truncated": "false", "X-Row-Count": "9"}),
        FakeResponse(200, _table_payload({"station_id": ["site_1"]}), _headers(1)),
        FakeResponse(200, _table_payload({"mean": [1.0], "min": [1.0], "max": [1.0]}), _headers(1)),
    ]
    for response in cases:
        transport = FakeTransport(response)
        with pytest.raises(QueryError):
            fetch_measurements(_query(), project_root=Path("."), transport=transport, api_key=_SECRET)


def test_daily_aggregate_with_period_start_keeps_the_api_columns() -> None:
    columns = {
        "station_id": ["site_103"],
        "parameter_name": ["PM2.5"],
        "unit": ["µg/m³"],
        "period_start": [datetime(2024, 2, 15)],
        "mean": [106.11],
        "min": [46.2],
        "max": [151.2],
        "n": [24],
    }
    transport = FakeTransport(FakeResponse(200, _table_payload(columns), _headers(1)))
    result = fetch_measurements(
        _query(aggregation="daily", parameters=("PM2.5",)),
        project_root=Path("."),
        transport=transport,
        api_key=_SECRET,
    )
    assert result.row_count == 1
    assert result.reported_row_count == 1
    assert result.table.column_names == list(columns)
    assert result.table.column("period_start")[0].as_py() == datetime(2024, 2, 15)
    assert result.table.column("mean")[0].as_py() == 106.11
    assert result.table.column("n")[0].as_py() == 24


def test_unknown_period_column_is_a_schema_mismatch() -> None:
    payload = _table_payload(
        {
            "period_end": [datetime(2024, 2, 15)],
            "mean": [10.0],
            "min": [1.0],
            "max": [12.0],
            "n": [4],
        }
    )
    transport = FakeTransport(FakeResponse(200, payload, _headers(1)))
    with pytest.raises(QueryError, match="no recognised period column") as error:
        fetch_measurements(
            _query(aggregation="daily"),
            project_root=Path("."),
            transport=transport,
            api_key=_SECRET,
        )
    assert "period_end" in str(error.value)
    assert "mean" in str(error.value)


def test_daily_aggregate_with_a_period_column_is_accepted() -> None:
    payload = _table_payload(
        {
            "collected_at": [datetime(2024, 2, 1)],
            "mean": [10.0],
            "min": [1.0],
            "max": [12.0],
            "n": [4],
        }
    )
    transport = FakeTransport(FakeResponse(200, payload, _headers(1)))
    query = _query(aggregation="daily")
    result = fetch_measurements(query, project_root=Path("."), transport=transport, api_key=_SECRET)
    assert result.row_count == 1
    assert result.table.column("n")[0].as_py() == 4


def test_cache_hit_miss_expiry_and_filter_isolation(tmp_path: Path) -> None:
    clock = _Clock(datetime(2026, 1, 1, tzinfo=timezone.utc))
    cache = QueryCache(tmp_path / "cache", ttl=timedelta(days=1), clock=clock)
    first = _query()
    second = _query(end=date(2024, 3, 1))
    other_pollutant = _query(parameters=("PM2.5",))
    assert cache.get(first) is None

    stored = cache.put(first, b"pm-both")
    assert cache.get(first) is not None
    assert cache.get(first).read_bytes() == b"pm-both"
    assert cache.get(second) is None
    assert cache.get(other_pollutant) is None
    assert stored.path.name == f"{first.cache_token()}.parquet"
    assert _SECRET not in stored.path.name
    assert "api_key" not in stored.path.with_suffix(".json").read_text(encoding="utf-8")

    same_city = MeasurementQuery(date(2024, 1, 1), date(2024, 1, 31), city="delhi")
    other_case = MeasurementQuery(date(2024, 1, 1), date(2024, 1, 31), city="Delhi")
    cache.put(same_city, b"city")
    assert cache.get(other_case).read_bytes() == b"city"
    assert same_city.cache_token() == other_case.cache_token()
    assert first.cache_token() != second.cache_token()

    clock.now += timedelta(days=2)
    assert cache.get(first) is None
    assert cache.purge_expired() >= 1
    assert cache.get(same_city) is None


def test_failed_cache_write_keeps_the_previous_entry(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    cache = QueryCache(tmp_path / "cache")
    query = _query()
    cache.put(query, b"original")

    def fail_replace(source: str, destination: str) -> None:
        raise OSError("disk full")

    monkeypatch.setattr("dashboard.query_cache.os.replace", fail_replace)
    with pytest.raises(CacheError, match="previous entry"):
        cache.put(query, b"replacement")
    assert cache.get(query).read_bytes() == b"original"
    assert not any("partial" in path.name for path in cache.directory.iterdir())


def test_cache_refuses_raw_and_processed_directories(tmp_path: Path) -> None:
    with pytest.raises(CacheError, match="data/raw or data/processed"):
        QueryCache(tmp_path / "data" / "raw" / "xkdr_query")
    with pytest.raises(CacheError, match="data/raw or data/processed"):
        QueryCache(tmp_path / "data" / "processed" / "measurements")
    allowed = default_cache_directory(tmp_path)
    assert allowed.parts[-3:] == ("data", "cache", "xkdr_query")


def test_invalidation_removes_one_query_only(tmp_path: Path) -> None:
    cache = QueryCache(tmp_path / "cache")
    first = _query()
    second = _query(parameters=("PM10",))
    cache.put(first, b"one")
    cache.put(second, b"two")
    cache.invalidate(first)
    assert cache.get(first) is None
    assert cache.get(second).read_bytes() == b"two"


def test_sources_and_cache_names_do_not_contain_a_key_or_the_test_secret() -> None:
    sources = [
        Path("src/ingestion/xkdr_query.py").read_text(encoding="utf-8"),
        Path("src/dashboard/query_cache.py").read_text(encoding="utf-8"),
    ]
    for source in sources:
        assert "aqi_" not in source
        assert _SECRET not in source
        assert "print(" not in source


def _query(**changes: object) -> MeasurementQuery:
    values: dict[str, object] = {
        "start": date(2024, 1, 1),
        "end": date(2024, 1, 31),
        "parameters": ("PM2.5", "PM10"),
        "aggregation": "raw",
    }
    values.update(changes)
    return MeasurementQuery(**values)  # type: ignore[arg-type]


def _headers(rows: int) -> dict[str, str]:
    return {
        "X-Truncated": "false",
        "X-Row-Count": str(rows),
        "Content-Type": "application/vnd.apache.parquet",
    }


def _raw_payload() -> bytes:
    return _table_payload(
        {
            "station_id": ["site_1"],
            "parameter_name": ["PM2.5"],
            "unit": ["µg/m³"],
            "collected_at": [datetime(2024, 1, 1, 0, 0)],
            "value": [12.5],
        }
    )


def _table_payload(columns: dict[str, list[object]]) -> bytes:
    sink = io.BytesIO()
    pq.write_table(pa.table(columns), sink)
    return sink.getvalue()


class _Clock:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now
