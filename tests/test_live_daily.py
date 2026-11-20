"""Timeline on-demand daily queries. The transport is fake and does not call XKDR."""

from __future__ import annotations

import io
import json
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dashboard.live_daily import (
    API_DAILY_COLUMNS,
    load_live_daily,
    station_daily_chart,
)
from dashboard.query_cache import QueryCache
from dashboard.timeline import gov_observations, make_catalog

_SECRET = "unit-test-secret-value"
_ROOT = Path(__file__).resolve().parents[1]


class _Response:
    def __init__(self, status: int, body: bytes, headers: dict[str, str]) -> None:
        self.status = status
        self.body = body
        self.headers = headers
        self.closed = False

    def close(self) -> None:
        self.closed = True


class _Transport:
    def __init__(self, response: _Response | Exception) -> None:
        self.response = response
        self.urls: list[str] = []

    def send(self, url: str, headers: dict[str, str], timeout: float, max_bytes: int) -> _Response:
        del headers, timeout, max_bytes
        self.urls.append(url)
        if isinstance(self.response, Exception):
            raise self.response
        return self.response


def test_api_success_keeps_the_daily_columns_and_weights_by_n(tmp_path: Path) -> None:
    payload = _payload(
        [
            _row("2024-02-15", station="site_103", mean=10.0, count=24),
            _row("2024-02-15", station="site_104", mean=20.0, count=8),
            _row("2024-02-16", station="site_103", mean=99.0, count=24),
        ]
    )
    transport = _Transport(_Response(200, payload, _headers(3)))
    result = _load(tmp_path, transport, start=date(2024, 2, 15), end=date(2024, 2, 15))
    assert result.origin == "api"
    assert len(transport.urls) == 1
    assert list(result.rows.columns) == list(API_DAILY_COLUMNS)
    assert set(result.rows["station_id"]) == {"site_103", "site_104"}
    assert set(pd.to_datetime(result.rows["period_start"]).dt.date) == {date(2024, 2, 15)}
    assert float(result.chart["mean_valid_value"].iloc[0]) == 12.5
    assert int(result.chart["observations"].iloc[0]) == 32
    assert list(pd.to_datetime(result.chart["observation_date"]).dt.date) == [date(2024, 2, 15)]
    assert _SECRET not in result.detail


def test_cache_hit_reuses_the_completed_response(tmp_path: Path) -> None:
    cache = QueryCache(tmp_path / "cache")
    assert cache.ttl == timedelta(days=7)
    transport = _Transport(_Response(200, _payload([_row("2024-02-15")]), _headers(1)))
    first = _load(tmp_path, transport, cache=cache)
    assert first.origin == "api"
    follow_up = _Transport(TimeoutError("timed out"))
    second = _load(tmp_path, follow_up, cache=cache)
    assert second.origin == "cache"
    assert follow_up.urls == []
    assert list(second.rows["station_id"]) == ["site_103"]
    stored = list((tmp_path / "cache").glob("*.json"))
    assert stored
    assert _SECRET not in stored[0].read_text(encoding="utf-8")
    assert "api_key" not in stored[0].read_text(encoding="utf-8")


def test_date_changes_request_a_different_cached_query(tmp_path: Path) -> None:
    transport = _Transport(_Response(200, b"", {}))
    transport.send = _routed_send(transport)  # type: ignore[method-assign]
    cache = QueryCache(tmp_path / "cache")
    first = _load(tmp_path, transport, cache=cache, start=date(2024, 2, 15), end=date(2024, 2, 15))
    second = _load(tmp_path, transport, cache=cache, start=date(2024, 2, 16), end=date(2024, 2, 16))
    assert len(transport.urls) == 2
    assert "start=2024-02-15" in transport.urls[0]
    assert "start=2024-02-16" in transport.urls[1]
    assert set(pd.to_datetime(first.rows["period_start"]).dt.date) == {date(2024, 2, 15)}
    assert set(pd.to_datetime(second.rows["period_start"]).dt.date) == {date(2024, 2, 16)}
    assert first.rows["mean"].iloc[0] != second.rows["mean"].iloc[0]


def test_pollutant_changes_do_not_reuse_the_other_pollutant(tmp_path: Path) -> None:
    payload = _payload(
        [
            _row("2024-02-15", pollutant="PM2.5", mean=11.0),
            _row("2024-02-15", pollutant="PM10", mean=80.0),
        ]
    )
    transport = _Transport(_Response(200, payload, _headers(2)))
    cache = QueryCache(tmp_path / "cache")
    pm25 = _load(tmp_path, transport, cache=cache, pollutant="PM2.5")
    pm10 = _load(tmp_path, transport, cache=cache, pollutant="PM10")
    assert len(transport.urls) == 2
    assert "parameter=PM2.5" in transport.urls[0]
    assert "parameter=PM10" not in transport.urls[0]
    assert "parameter=PM10" in transport.urls[1]
    assert set(pm25.rows["parameter_name"]) == {"PM2.5"}
    assert set(pm10.rows["parameter_name"]) == {"PM10"}
    assert float(pm25.rows["mean"].iloc[0]) == 11.0
    assert float(pm10.rows["mean"].iloc[0]) == 80.0


def test_timeout_does_not_substitute_prepared_days(tmp_path: Path) -> None:
    catalog = _catalog()
    transport = _Transport(TimeoutError("timed out"))
    result = _load(
        tmp_path,
        transport,
        catalog=catalog,
        start=date(2024, 2, 15),
        end=date(2024, 2, 17),
    )
    assert result.origin == "unavailable"
    assert len(transport.urls) == 1
    assert result.rows.empty
    assert result.chart.empty
    assert "not substituted" in result.detail
    assert "not filled" in result.detail
    assert "40.0" not in result.detail
    assert gov_observations(catalog, "2026-11-17", "2026-11-17")["avg_value"].tolist() == [9.0]


def test_truncated_response_is_not_shown_or_cached(tmp_path: Path) -> None:
    payload = _payload([_row("2024-02-15", station="site_truncated", mean=5.0)])
    headers = _headers(1)
    headers["X-Truncated"] = "true"
    transport = _Transport(_Response(200, payload, headers))
    result = _load(tmp_path, transport, catalog=make_catalog())
    assert result.origin == "unavailable"
    assert result.rows.empty
    assert result.chart.empty
    assert "truncated" in result.detail.lower()
    assert "site_truncated" not in result.detail
    assert list((tmp_path / "cache").glob("*.parquet")) == []
    assert "unavailable" in result.detail.lower()


def test_uncovered_dates_stay_empty_when_live_retrieval_fails(tmp_path: Path) -> None:
    catalog = _catalog()
    transport = _Transport(TimeoutError("timed out"))
    result = _load(
        tmp_path,
        transport,
        catalog=catalog,
        start=date(2024, 6, 1),
        end=date(2024, 6, 1),
    )
    assert result.origin == "unavailable"
    assert result.rows.empty
    assert result.chart.empty
    assert "unavailable" in result.detail.lower()
    assert "not filled" in result.detail
    assert "data.gov.in" in result.detail
    assert "9.0" not in result.detail


def test_data_gov_rows_stay_out_of_the_xkdr_daily_result(tmp_path: Path) -> None:
    catalog = _catalog()
    before = gov_observations(catalog, "2026-11-17", "2026-11-17").copy()
    transport = _Transport(_Response(200, _payload([_row("2024-02-15")]), _headers(1)))
    result = _load(tmp_path, transport, catalog=catalog)
    after = gov_observations(catalog, "2026-11-17", "2026-11-17")
    assert before.equals(after)
    assert "avg_value" not in result.rows.columns
    assert "pollutant_id" not in result.rows.columns
    assert set(result.rows["parameter_name"]) == {"PM2.5"}
    source = (_ROOT / "src" / "dashboard" / "live_daily.py").read_text(encoding="utf-8")
    assert "gov_observations" not in source
    assert "avg_value" not in source


def test_place_filters_are_sent_and_unmatched_stations_are_dropped(tmp_path: Path) -> None:
    payload = _payload([_row("2024-02-15", station="site_other", mean=70.0)])
    transport = _Transport(_Response(200, payload, _headers(1)))
    result = _load(tmp_path, transport, state="Delhi", city="Delhi", station="site_9")
    url = transport.urls[0]
    assert "state=Delhi" in url
    assert "city=Delhi" in url
    assert "station=site_9" in url
    assert result.rows.empty
    assert "did not match" in result.detail
    assert "site_other" not in result.detail
    assert _SECRET not in url


def test_failures_and_cache_do_not_expose_the_api_key(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    denied = json.dumps({"error": "invalid_api_key", "detail": f"rejected {_SECRET}"}).encode("utf-8")
    transport = _Transport(_Response(401, denied, {"Content-Type": "application/json"}))
    result = _load(tmp_path, transport, api_key=_SECRET)
    assert result.origin == "unavailable"
    assert _SECRET not in result.detail
    assert "Bearer" not in result.detail
    captured = capsys.readouterr()
    assert _SECRET not in captured.out
    assert _SECRET not in captured.err
    page = (_ROOT / "src" / "dashboard" / "timeline_page.py").read_text(encoding="utf-8")
    live = (_ROOT / "src" / "dashboard" / "live_daily.py").read_text(encoding="utf-8")
    assert "api_key=" not in page
    assert "AQI_API_KEY" not in page
    assert "Authorization" not in page
    assert ".env" not in page
    assert "st.session_state" not in live
    assert "print(" not in live
    assert "QueryCache(default_cache_directory(project_root))" in live
    assert "ttl=" not in live
    assert "plan_sources" in page
    assert "resolve_timeline_sources" in page
    assert "uses_on_demand_daily" not in page


def test_chart_does_not_invent_a_day_without_a_positive_count() -> None:
    frame = pd.DataFrame([_row("2024-02-15", mean=10.0, count=0)])
    assert station_daily_chart(frame).empty


def test_processed_output_is_not_rewritten(tmp_path: Path) -> None:
    prepared = tmp_path / "data" / "processed" / "measurements" / "data.parquet"
    prepared.parent.mkdir(parents=True)
    prepared.write_bytes(b"prepared-sentinel")
    transport = _Transport(_Response(200, _payload([_row("2024-02-15")]), _headers(1)))
    _load(tmp_path, transport)
    assert prepared.read_bytes() == b"prepared-sentinel"
    assert not (tmp_path / "data" / "raw").exists()


def _load(tmp_path: Path, transport: _Transport, catalog: object | None = None, **changes: object):
    cache = changes.pop("cache", None)
    values: dict[str, object] = {
        "project_root": tmp_path,
        "catalog": catalog if catalog is not None else make_catalog(),
        "start": date(2024, 2, 15),
        "end": date(2024, 2, 15),
        "transport": transport,
        "cache": cache if cache is not None else QueryCache(tmp_path / "cache"),
        "api_key": _SECRET,
    }
    values.update(changes)
    return load_live_daily(**values)  # type: ignore[arg-type]


def _routed_send(transport: _Transport):
    def send(url: str, headers: dict[str, str], timeout: float, max_bytes: int) -> _Response:
        del headers, timeout, max_bytes
        transport.urls.append(url)
        query = parse_qs(urlparse(url).query)
        day = query["start"][0]
        pollutants = query.get("parameter", ["PM2.5"])
        rows = [
            _row(day, pollutant=name, mean=10.0 if day.endswith("15") else 30.0)
            for name in pollutants
        ]
        return _Response(200, _payload(rows), _headers(len(rows)))

    return send


def _catalog():
    measurements = pd.DataFrame(
        [
            {
                "station_id": "site_1",
                "station_name": "Site 1",
                "state_name": "Delhi",
                "city_name": "Delhi",
                "parameter_name": "PM2.5",
                "collected_at": pd.Timestamp("2024-02-15 08:00"),
                "value": 40.0,
                "unit": "µg/m³",
                "latitude": 28.6,
                "longitude": 77.2,
            }
        ]
    )
    daily = pd.DataFrame(
        [
            {
                "city_name": "Delhi",
                "state_name": "Delhi",
                "observation_date": pd.Timestamp("2024-02-15"),
                "parameter_name": "PM2.5",
                "observations": 1,
                "valid_observations": 1,
                "eligible_observations": 1,
                "mean_valid_value": 40.0,
                "spike_count": 0,
            }
        ]
    )
    gov = pd.DataFrame(
        [
            {
                "state": "Uttar Pradesh",
                "city": "Noida",
                "station": "Station N",
                "pollutant_id": "PM2.5",
                "observed_at": pd.Timestamp("2026-11-17 10:00"),
                "avg_value": 9.0,
                "min_value": 1.0,
                "max_value": 12.0,
                "reported_unit": "not documented by the API",
                "value_kind": "api-reported pollutant value",
            }
        ]
    )
    return make_catalog(xkdr_daily=daily, xkdr_measurements=measurements, gov_observations=gov)


def _row(
    day: str,
    *,
    pollutant: str = "PM2.5",
    station: str = "site_103",
    mean: float = 10.0,
    count: int = 24,
) -> dict[str, object]:
    return {
        "station_id": station,
        "parameter_name": pollutant,
        "unit": "µg/m³",
        "period_start": datetime.fromisoformat(day),
        "mean": mean,
        "min": 1.0,
        "max": mean + 5,
        "n": count,
    }


def _payload(rows: list[dict[str, object]]) -> bytes:
    columns = {name: [row[name] for row in rows] for name in API_DAILY_COLUMNS}
    sink = io.BytesIO()
    pq.write_table(pa.table(columns), sink)
    return sink.getvalue()


def _headers(rows: int) -> dict[str, str]:
    return {
        "X-Truncated": "false",
        "X-Row-Count": str(rows),
        "Content-Type": "application/vnd.apache.parquet",
    }
