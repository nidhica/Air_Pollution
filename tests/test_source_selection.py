"""Source selection for prepared history and the recent XKDR window.

These tests use a fake transport. They do not call the network or Spark.
"""

from __future__ import annotations

import io
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dashboard.live_daily import (
    HISTORICAL_LABEL,
    LIVE_LABEL,
    availability_messages,
    kolkata_today,
    period_start_days,
    plan_sources,
    present_live_section,
    resolve_timeline_sources,
)
from dashboard.xkdr_archive import ArchiveInfo
from dashboard.query_cache import QueryCache
from dashboard.timeline import gov_observations, make_catalog, preset_bounds, xkdr_daily_series

_TODAY = date(2026, 11, 18)
_LIVE_START = date(2026, 11, 12)
_ARCHIVE_END = date(2026, 11, 30)


class _Response:
    def __init__(self, status: int, body: bytes, headers: dict[str, str]) -> None:
        self.status = status
        self.body = body
        self.headers = headers

    def close(self) -> None:
        return None


class _Recording:
    def __init__(self, response: _Response | Exception | None = None) -> None:
        self.response = response
        self.urls: list[str] = []

    def send(self, url: str, headers: dict[str, str], timeout: float, max_bytes: int) -> _Response:
        del headers, timeout, max_bytes
        self.urls.append(url)
        if isinstance(self.response, Exception):
            raise self.response
        if self.response is not None:
            return self.response
        query = parse_qs(urlparse(url).query)
        day = query["start"][0]
        body = _payload([_row(day)])
        return _Response(200, body, _headers(1))


def test_historical_selection_does_not_call_the_api(tmp_path: Path) -> None:
    transport = _Recording()
    resolved = _resolve(tmp_path, transport, date(2024, 2, 1), date(2024, 2, 15))
    assert resolved.live is None
    assert transport.urls == []
    assert resolved.plan.mode == "unprepared"
    assert resolved.plan.historical_start is None
    assert resolved.plan.unprepared == ((date(2024, 2, 1), date(2024, 2, 15)),)
    assert resolved.plan.unpublished_start is None


def test_today_only_selection_calls_the_api(tmp_path: Path) -> None:
    transport = _Recording()
    resolved = _resolve(tmp_path, transport, _TODAY, _TODAY)
    assert resolved.plan.mode == "recent"
    assert len(transport.urls) == 1
    assert _bounds(transport.urls[0]) == (_TODAY, _TODAY)
    assert resolved.live is not None
    assert resolved.live.origin == "api"


def test_yesterday_only_selection_calls_the_api(tmp_path: Path) -> None:
    yesterday = _TODAY - timedelta(days=1)
    transport = _Recording()
    resolved = _resolve(tmp_path, transport, yesterday, yesterday)
    assert resolved.plan.mode == "recent"
    assert _bounds(transport.urls[0]) == (yesterday, yesterday)


def test_seven_day_cutoff_keeps_history_and_live_apart() -> None:
    day_before = _LIVE_START - timedelta(days=1)
    historical = plan_sources(
        date(2026, 11, 1),
        day_before,
        today=_TODAY,
        archive_end=_ARCHIVE_END,
        prepared_start=date(2026, 11, 1),
        prepared_end=day_before,
    )
    assert historical.mode == "historical"
    assert historical.live_query_start is None
    assert historical.historical_end == day_before

    recent = plan_sources(_LIVE_START, _TODAY, today=_TODAY, archive_end=_ARCHIVE_END)
    assert recent.mode == "recent"
    assert recent.historical_start is None
    assert (recent.live_query_start, recent.live_query_end) == (_LIVE_START, _TODAY)

    mixed = plan_sources(
        day_before,
        _TODAY,
        today=_TODAY,
        archive_end=_ARCHIVE_END,
        prepared_start=day_before,
        prepared_end=day_before,
    )
    assert mixed.mode == "mixed"
    assert mixed.historical_end == day_before
    assert mixed.live_query_start == _LIVE_START
    assert mixed.historical_end + timedelta(days=1) == mixed.live_query_start


def test_mixed_selection_queries_only_the_recent_dates(tmp_path: Path) -> None:
    transport = _Recording()
    resolved = _resolve(tmp_path, transport, date(2026, 10, 1), _TODAY)
    assert resolved.plan.mode == "mixed"
    assert len(transport.urls) == 1
    assert _bounds(transport.urls[0]) == (_LIVE_START, _TODAY)
    assert "start=2026-10-01" not in transport.urls[0]
    assert resolved.plan.unprepared == ((date(2026, 10, 1), _LIVE_START - timedelta(days=1)),)


def test_last_7_days_preset_is_entirely_live(tmp_path: Path) -> None:
    start, end = preset_bounds("Last 7 days", _TODAY)
    transport = _Recording()
    resolved = _resolve(tmp_path, transport, start, end)
    assert (start, end) == (_LIVE_START, _TODAY)
    assert resolved.plan.mode == "recent"
    assert resolved.plan.historical_start is None
    assert _bounds(transport.urls[0]) == (_LIVE_START, _TODAY)


def test_last_7_and_last_30_days_stop_at_the_published_archive(tmp_path: Path) -> None:
    for preset in ("Last 7 days", "Last 30 days"):
        start, end = preset_bounds(preset, _TODAY)
        transport = _Recording()
        resolved = _resolve(tmp_path / preset, transport, start, end, archive_end=date(2026, 3, 31))
        assert transport.urls == []
        assert resolved.live is None
        assert resolved.plan.live_query_start is None
        assert (resolved.plan.unpublished_start, resolved.plan.unpublished_end) == (start, end)
        text = " ".join(availability_messages(resolved.plan, resolved.archive))
        assert "XKDR has not published months after 2026-03" in text
        assert "not substituted" in text
        assert "No XKDR observations are available" in text


def test_last_30_days_preset_queries_only_the_recent_slice(tmp_path: Path) -> None:
    _assert_preset_keeps_history_off_the_api(tmp_path, "Last 30 days")


def test_last_90_days_preset_queries_only_the_recent_slice(tmp_path: Path) -> None:
    _assert_preset_keeps_history_off_the_api(tmp_path, "Last 90 days")


def test_last_1_year_preset_queries_only_the_recent_slice(tmp_path: Path) -> None:
    _assert_preset_keeps_history_off_the_api(tmp_path, "Last 1 year")


def test_custom_range_uses_the_same_split(tmp_path: Path) -> None:
    transport = _Recording()
    resolved = _resolve(tmp_path, transport, date(2026, 1, 1), _TODAY)
    assert resolved.plan.mode == "mixed"
    assert _bounds(transport.urls[0]) == (_LIVE_START, _TODAY)
    assert resolved.plan.unprepared[0] == (date(2026, 1, 1), _LIVE_START - timedelta(days=1))


def test_future_dates_are_excluded_from_the_api_request(tmp_path: Path) -> None:
    transport = _Recording()
    resolved = _resolve(tmp_path, transport, _TODAY - timedelta(days=1), _TODAY + timedelta(days=4))
    assert _bounds(transport.urls[0]) == (_TODAY - timedelta(days=1), _TODAY)
    assert resolved.plan.future_start == _TODAY + timedelta(days=1)
    assert resolved.plan.future_end == _TODAY + timedelta(days=4)
    future = _resolve(tmp_path, _Recording(), _TODAY + timedelta(days=1), _TODAY + timedelta(days=3))
    assert future.plan.mode == "future"
    assert future.live is None


def test_api_failure_keeps_historical_results_and_marks_recent_unavailable(tmp_path: Path) -> None:
    catalog = _history_catalog(date(2024, 2, 15))
    transport = _Recording(TimeoutError("timed out"))
    resolved = _resolve(tmp_path, transport, date(2024, 2, 1), _TODAY, catalog=catalog)
    assert resolved.live is not None
    assert resolved.live.origin == "unavailable"
    assert resolved.live.rows.empty
    assert resolved.live.chart.empty
    assert "unavailable" in resolved.live.detail.lower()
    assert "not substituted" in resolved.live.detail
    historical = xkdr_daily_series(
        catalog,
        resolved.plan.historical_start,
        resolved.plan.historical_end,
    )
    assert set(pd.to_datetime(historical["observation_date"]).dt.date) == {date(2024, 2, 15)}
    assert float(historical["mean_valid_value"].iloc[0]) == 40.0


def test_truncated_response_is_not_displayed_as_complete(tmp_path: Path) -> None:
    body = _payload([_row(_LIVE_START.isoformat(), station="site_truncated")])
    headers = _headers(1)
    headers["X-Truncated"] = "true"
    transport = _Recording(_Response(200, body, headers))
    catalog = _history_catalog(date(2024, 2, 15))
    resolved = _resolve(tmp_path, transport, date(2024, 2, 1), _TODAY, catalog=catalog)
    assert resolved.live is not None
    assert resolved.live.origin == "unavailable"
    assert resolved.live.rows.empty
    assert "truncated" in resolved.live.detail.lower()
    assert "site_truncated" not in resolved.live.detail
    assert list((tmp_path / "cache").glob("*.parquet")) == []
    historical = xkdr_daily_series(catalog, resolved.plan.historical_start, resolved.plan.historical_end)
    assert not historical.empty


def test_cache_is_reused_for_an_identical_recent_query(tmp_path: Path) -> None:
    cache = QueryCache(tmp_path / "cache")
    first_transport = _Recording()
    first = _resolve(tmp_path, first_transport, _TODAY, _TODAY, cache=cache)
    second_transport = _Recording(TimeoutError("timed out"))
    second = _resolve(tmp_path, second_transport, _TODAY, _TODAY, cache=cache)
    assert first.live is not None and first.live.origin == "api"
    assert second.live is not None and second.live.origin == "cache"
    assert second_transport.urls == []
    assert len(first_transport.urls) == 1


def test_boundary_dates_are_not_duplicated(tmp_path: Path) -> None:
    boundary = _LIVE_START - timedelta(days=1)
    catalog = _history_catalog(boundary)
    body = _payload(
        [
            _row(boundary.isoformat(), station="site_old", mean=40.0),
            _row(_LIVE_START.isoformat(), station="site_new", mean=12.0),
        ]
    )
    transport = _Recording(_Response(200, body, _headers(2)))
    resolved = _resolve(tmp_path, transport, boundary, _LIVE_START, catalog=catalog)
    assert _bounds(transport.urls[0]) == (_LIVE_START, _LIVE_START)
    live_days = set(period_start_days(resolved.live.rows["period_start"]).dt.date)
    historical = xkdr_daily_series(catalog, resolved.plan.historical_start, resolved.plan.historical_end)
    historical_days = set(pd.to_datetime(historical["observation_date"]).dt.date)
    assert live_days == {_LIVE_START}
    assert historical_days == {boundary}
    assert live_days.isdisjoint(historical_days)


def test_month_year_and_leap_day_boundaries() -> None:
    leap_today = date(2024, 3, 1)
    leap = plan_sources(
        date(2024, 2, 23),
        leap_today,
        today=leap_today,
        archive_end=date(2024, 3, 31),
        prepared_start=date(2024, 2, 23),
        prepared_end=date(2024, 2, 23),
    )
    assert leap.live_start == date(2024, 2, 24)
    assert leap.historical_end == date(2024, 2, 23)
    assert leap.live_query_start == date(2024, 2, 24)
    assert date(2024, 2, 29) >= leap.live_query_start
    assert date(2024, 2, 29) <= leap.live_query_end

    before_leap = plan_sources(
        date(2024, 2, 1),
        date(2024, 2, 28),
        today=date(2024, 3, 6),
        archive_end=date(2024, 3, 31),
        prepared_start=date(2024, 2, 1),
        prepared_end=date(2024, 2, 28),
    )
    assert before_leap.mode == "historical"
    assert before_leap.historical_end == date(2024, 2, 28)
    on_leap = plan_sources(
        date(2024, 2, 29),
        date(2024, 2, 29),
        today=date(2024, 3, 6),
        archive_end=date(2024, 3, 31),
    )
    assert on_leap.mode == "recent"
    assert on_leap.live_query_start == date(2024, 2, 29)

    year = plan_sources(
        date(2025, 12, 1),
        date(2026, 1, 3),
        today=date(2026, 1, 3),
        archive_end=date(2026, 1, 31),
        prepared_start=date(2025, 12, 1),
        prepared_end=date(2026, 1, 3),
    )
    assert year.historical_end == date(2025, 12, 27)
    assert year.live_query_start == date(2025, 12, 28)
    assert year.live_query_end == date(2026, 1, 3)
    assert year.historical_end < year.live_query_start


def test_today_follows_asia_kolkata() -> None:
    late_utc = datetime(2026, 11, 18, 20, 0, tzinfo=timezone.utc)
    early_utc = datetime(2026, 11, 18, 18, 0, tzinfo=timezone.utc)
    assert kolkata_today(late_utc) == date(2026, 11, 19)
    assert kolkata_today(early_utc) == date(2026, 11, 18)
    plan = plan_sources(date(2026, 11, 1), date(2026, 11, 19), today=kolkata_today(late_utc))
    assert plan.today == date(2026, 11, 19)
    assert plan.live_end == date(2026, 11, 19)
    assert plan.live_start == date(2026, 11, 13)
    with pytest.raises(ValueError, match="timezone"):
        kolkata_today(datetime(2026, 11, 18, 20, 0))


def test_naive_period_start_keeps_its_wall_clock_date() -> None:
    naive = pd.Series([datetime(2026, 11, 18, 23, 0)])
    aware = pd.Series([pd.Timestamp("2026-11-18 20:00", tz="UTC")])
    assert period_start_days(naive).dt.date.tolist() == [date(2026, 11, 18)]
    assert period_start_days(aware).dt.date.tolist() == [date(2026, 11, 19)]


def test_last_7_days_success_empty_and_failure_drive_the_section(tmp_path: Path) -> None:
    start, end = preset_bounds("Last 7 days", _TODAY)
    success = _Recording(
        _Response(
            200,
            _payload(
                [
                    _row(start.isoformat(), pollutant="PM2.5", mean=11.0),
                    _row(start.isoformat(), pollutant="PM10", mean=40.0),
                ]
            ),
            _headers(2),
        )
    )
    ready = _resolve(tmp_path, success, start, end)
    ready_view = present_live_section(ready.live, start, end)
    assert ready.plan.mode == "recent"
    assert len(success.urls) == 1
    assert ready_view.status == "ready"
    assert ready_view.show_chart is True
    assert ready_view.show_metrics is True
    assert set(ready.live.chart["parameter_name"]) == {"PM2.5", "PM10"}
    assert "Requested 2026-11-12 to 2026-11-18" in ready_view.message

    empty = _Recording(_Response(200, _empty_payload(), _headers(0)))
    empty_result = _resolve(tmp_path / "empty", empty, start, end)
    empty_view = present_live_section(empty_result.live, start, end)
    assert empty_view.status == "empty"
    assert empty_view.show_chart is False
    assert "No XKDR daily observations were returned" in empty_view.message

    failed = _Recording(TimeoutError("timed out"))
    failed_result = _resolve(tmp_path / "failed", failed, start, end)
    failed_view = present_live_section(failed_result.live, start, end)
    assert failed_view.status == "failed"
    assert failed_view.show_chart is False
    assert failed_view.show_metrics is False
    assert "No daily chart is shown" in failed_view.message
    assert "not substituted" in failed_view.message
    assert failed_result.live.rows.empty


def test_last_30_days_keeps_history_and_reports_a_failed_recent_query(tmp_path: Path) -> None:
    start, end = preset_bounds("Last 30 days", _TODAY)
    catalog = _history_catalog(date(2024, 2, 15))
    transport = _Recording(_Response(500, b'{"error":"internal_error","detail":"Internal Server Error"}', {"Content-Type": "application/json"}))
    resolved = _resolve(tmp_path, transport, start, end, catalog=catalog)
    view = present_live_section(resolved.live, resolved.plan.live_query_start, resolved.plan.live_query_end)
    assert resolved.plan.mode == "mixed"
    assert _bounds(transport.urls[0]) == (_LIVE_START, _TODAY)
    assert view.status == "failed"
    assert "HTTP 500" in view.message
    assert "No daily chart is shown" in view.message
    assert resolved.plan.historical_start is None
    assert resolved.plan.unprepared == ((start, _LIVE_START - timedelta(days=1)),)
    assert resolved.live.rows.empty
    historical = xkdr_daily_series(catalog, date(2024, 2, 15), date(2024, 2, 15))
    assert set(pd.to_datetime(historical["observation_date"]).dt.date) == {date(2024, 2, 15)}
    page = Path(__file__).resolve().parents[1].joinpath("src", "dashboard", "timeline_page.py").read_text(encoding="utf-8")
    assert "st.header(ON_DEMAND_HEADING)" in page
    assert "st.header(HISTORICAL_LABEL)" in page
    assert "st.error(view.message)" in page


def test_data_gov_rows_remain_separate(tmp_path: Path) -> None:
    catalog = _history_catalog(date(2024, 2, 15))
    before = gov_observations(catalog, "2026-11-17", "2026-11-17").copy()
    resolved = _resolve(tmp_path, _Recording(), _TODAY, _TODAY, catalog=catalog)
    after = gov_observations(catalog, "2026-11-17", "2026-11-17")
    assert before.equals(after)
    assert resolved.live is not None
    assert "avg_value" not in resolved.live.rows.columns
    source = Path(__file__).resolve().parents[1].joinpath("src", "dashboard", "live_daily.py").read_text(encoding="utf-8")
    assert "gov_observations" not in source
    assert HISTORICAL_LABEL == "Historical observations — prepared CPCB dataset"
    assert LIVE_LABEL == "Recent observations — live XKDR API"


def _assert_preset_keeps_history_off_the_api(tmp_path: Path, preset: str) -> None:
    start, end = preset_bounds(preset, _TODAY)
    transport = _Recording()
    resolved = _resolve(tmp_path, transport, start, end)
    assert resolved.plan.mode == "mixed"
    assert resolved.plan.unprepared == ((start, _LIVE_START - timedelta(days=1)),)
    assert _bounds(transport.urls[0]) == (_LIVE_START, _TODAY)
    assert f"start={start.isoformat()}" not in transport.urls[0]


def _resolve(
    tmp_path: Path,
    transport: _Recording,
    start: date,
    end: date,
    catalog: object | None = None,
    cache: QueryCache | None = None,
    archive_end: date | None = _ARCHIVE_END,
):
    archive = None if archive_end is None else _archive(archive_end)
    return resolve_timeline_sources(
        project_root=tmp_path,
        catalog=catalog if catalog is not None else make_catalog(),
        start=start,
        end=end,
        today=_TODAY,
        transport=transport,
        cache=cache if cache is not None else QueryCache(tmp_path / "cache"),
        api_key="unit-test-secret-value",
        archive=archive,
    )


def _archive(archive_end: date) -> ArchiveInfo:
    return ArchiveInfo(
        last_month=f"{archive_end.year:04d}-{archive_end.month:02d}",
        archive_end=archive_end,
        available=True,
        origin="api",
        detail=f"Published archive ends {archive_end.isoformat()}.",
    )


def _bounds(url: str) -> tuple[date, date]:
    query = parse_qs(urlparse(url).query)
    return date.fromisoformat(query["start"][0]), date.fromisoformat(query["end"][0])


def _history_catalog(day: date):
    stamp = pd.Timestamp(day)
    measurements = pd.DataFrame(
        [
            {
                "station_id": "site_1",
                "station_name": "Site 1",
                "state_name": "Delhi",
                "city_name": "Delhi",
                "parameter_name": "PM2.5",
                "collected_at": datetime(day.year, day.month, day.day, 8, 0),
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
                "observation_date": stamp,
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


def _row(day: str, *, station: str = "site_103", mean: float = 10.0, pollutant: str = "PM2.5") -> dict[str, object]:
    return {
        "station_id": station,
        "parameter_name": pollutant,
        "unit": "µg/m³",
        "period_start": datetime.fromisoformat(day),
        "mean": mean,
        "min": 1.0,
        "max": mean + 1,
        "n": 24,
    }


def _payload(rows: list[dict[str, object]]) -> bytes:
    columns = {
        "station_id": [row["station_id"] for row in rows],
        "parameter_name": [row["parameter_name"] for row in rows],
        "unit": [row["unit"] for row in rows],
        "period_start": [row["period_start"] for row in rows],
        "mean": [row["mean"] for row in rows],
        "min": [row["min"] for row in rows],
        "max": [row["max"] for row in rows],
        "n": [row["n"] for row in rows],
    }
    sink = io.BytesIO()
    pq.write_table(pa.table(columns), sink)
    return sink.getvalue()


def _empty_payload() -> bytes:
    table = pa.table(
        {
            "station_id": pa.array([], type=pa.string()),
            "parameter_name": pa.array([], type=pa.string()),
            "unit": pa.array([], type=pa.string()),
            "period_start": pa.array([], type=pa.timestamp("us")),
            "mean": pa.array([], type=pa.float64()),
            "min": pa.array([], type=pa.float64()),
            "max": pa.array([], type=pa.float64()),
            "n": pa.array([], type=pa.int64()),
        }
    )
    sink = io.BytesIO()
    pq.write_table(table, sink)
    return sink.getvalue()


def _headers(rows: int) -> dict[str, str]:
    return {
        "X-Truncated": "false",
        "X-Row-Count": str(rows),
        "Content-Type": "application/vnd.apache.parquet",
    }
