"""Archive metadata and the date split it controls.

These tests use a fake transport. They do not call the network or Spark.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pytest

from dashboard.live_daily import load_live_daily, plan_sources, resolve_timeline_sources
from dashboard.query_cache import QueryCache
from dashboard.timeline import make_catalog
from dashboard.xkdr_archive import ArchiveError, ArchiveInfo, load_archive, parse_last_month
from ingestion.xkdr_query import QueryError

_SECRET = "unit-test-secret-value"
_MARCH = date(2026, 3, 31)


class _Response:
    def __init__(self, status: int, body: bytes, headers: dict[str, str] | None = None) -> None:
        self.status = status
        self.body = body
        self.headers = headers or {"Content-Type": "application/json"}

    def close(self) -> None:
        return None


class _MetaTransport:
    def __init__(self, response: _Response | Exception) -> None:
        self.response = response
        self.urls: list[str] = []

    def send(self, url: str, headers: dict[str, str], timeout: float, max_bytes: int) -> _Response:
        del headers, timeout, max_bytes
        self.urls.append(url)
        if _SECRET in url:
            raise QueryError("The archive URL contained the API key.")
        if isinstance(self.response, Exception):
            raise self.response
        return self.response


def test_last_month_uses_the_real_calendar_end() -> None:
    march, march_end, exported = parse_last_month(_meta_payload("2026-03"))
    assert (march, march_end, exported) == ("2026-03", date(2026, 3, 31), "2026-09-30T11:25:11+00:00")
    leap, leap_end, _exported = parse_last_month(_meta_payload("2024-02"))
    assert (leap, leap_end) == ("2024-02", date(2024, 2, 29))
    plain, plain_end, _exported = parse_last_month(_meta_payload("2025-02"))
    assert (plain, plain_end) == ("2025-02", date(2025, 2, 28))
    year, year_end, _exported = parse_last_month(_meta_payload("2025-12"))
    assert (year, year_end) == ("2025-12", date(2025, 12, 31))


def test_missing_last_month_is_not_cached(tmp_path: Path) -> None:
    transport = _MetaTransport(_Response(200, b'{"measurements":{"rows":1}}'))
    info = _load(tmp_path, transport)
    assert info.available is False
    assert info.archive_end is None
    assert info.origin == "unavailable"
    assert "last_month" in info.detail
    assert not (tmp_path / "meta.json").exists()
    assert transport.urls == ["https://airquality.xkdr.org/v1/meta?format=json"]


def test_metadata_failure_does_not_issue_a_daily_query(tmp_path: Path) -> None:
    body = json.dumps({"error": "internal_error", "detail": f"rejected {_SECRET}"}).encode("utf-8")
    transport = _MetaTransport(_Response(500, body))
    info = _load(tmp_path, transport)
    assert info.available is False
    assert info.last_month is None
    assert "HTTP 500" in info.detail
    assert _SECRET not in info.detail
    resolved = resolve_timeline_sources(
        project_root=tmp_path,
        catalog=make_catalog(),
        start=date(2026, 11, 12),
        end=date(2026, 11, 18),
        today=date(2026, 11, 18),
        transport=transport,
        cache=QueryCache(tmp_path / "queries"),
        api_key=_SECRET,
        archive=info,
    )
    assert resolved.live is None
    assert resolved.plan.live_query_start is None
    assert resolved.plan.mode == "unconfirmed"
    assert len(transport.urls) == 1
    assert "/v1/measurements" not in transport.urls[0]


def test_fresh_metadata_is_cached_and_reused(tmp_path: Path) -> None:
    now = datetime(2026, 11, 18, 12, tzinfo=timezone.utc)
    transport = _MetaTransport(_Response(200, json.dumps(_meta_payload("2026-03")).encode("utf-8")))
    first = _load(tmp_path, transport, now)
    assert first.origin == "api"
    assert first.last_month == "2026-03"
    assert first.archive_end == _MARCH
    stored = (tmp_path / "meta.json").read_text(encoding="utf-8")
    assert _SECRET not in stored
    assert "Bearer" not in stored
    failing = _MetaTransport(TimeoutError("timed out"))
    second = _load(tmp_path, failing, now + timedelta(hours=1))
    assert second.origin == "cache"
    assert second.last_month == "2026-03"
    assert failing.urls == []


def test_metadata_failure_keeps_a_stored_archive_month(tmp_path: Path) -> None:
    now = datetime(2026, 11, 18, 12, tzinfo=timezone.utc)
    _load(tmp_path, _MetaTransport(_Response(200, json.dumps(_meta_payload("2026-03")).encode("utf-8"))), now)
    later = now + timedelta(days=2)
    transport = _MetaTransport(_Response(500, b'{"error":"internal_error","detail":"Internal Server Error"}'))
    info = _load(tmp_path, transport, later)
    assert info.origin == "stale"
    assert info.archive_end == _MARCH
    assert "refresh failed" in info.detail
    assert len(transport.urls) == 1


def test_daily_query_stops_on_the_archive_boundary(tmp_path: Path) -> None:
    today = date(2026, 4, 3)
    transport = _MeasurementTransport()
    resolved = _resolve(tmp_path, transport, date(2026, 3, 30), today, today=today, archive_end=_MARCH)
    assert resolved.plan.live_query_start == date(2026, 3, 30)
    assert resolved.plan.live_query_end == _MARCH
    assert (resolved.plan.unpublished_start, resolved.plan.unpublished_end) == (date(2026, 4, 1), today)
    assert _bounds(transport.urls[0]) == (date(2026, 3, 30), _MARCH)
    assert "end=2026-04-03" not in transport.urls[0]


def test_daily_query_stops_on_a_year_boundary(tmp_path: Path) -> None:
    today = date(2026, 1, 3)
    archive_end = date(2025, 12, 31)
    transport = _MeasurementTransport()
    resolved = _resolve(
        tmp_path,
        transport,
        date(2025, 12, 28),
        today,
        today=today,
        archive_end=archive_end,
    )
    assert resolved.plan.live_query_end == archive_end
    assert resolved.plan.unpublished_start == date(2026, 1, 1)
    assert _bounds(transport.urls[0]) == (date(2025, 12, 28), archive_end)


def test_mixed_selection_keeps_prepared_published_and_unpublished_apart() -> None:
    plan = plan_sources(
        date(2024, 2, 1),
        date(2026, 11, 18),
        today=date(2026, 11, 18),
        archive_end=_MARCH,
        prepared_start=date(2024, 2, 1),
        prepared_end=date(2025, 9, 1),
    )
    assert plan.live_query_start is None
    assert (plan.historical_start, plan.historical_end) == (date(2024, 2, 1), date(2025, 9, 1))
    assert plan.unprepared == ((date(2025, 9, 2), _MARCH),)
    assert (plan.unpublished_start, plan.unpublished_end) == (date(2026, 4, 1), date(2026, 11, 18))
    assert plan.mode == "mixed"


def test_direct_daily_call_refuses_a_month_after_the_archive(tmp_path: Path) -> None:
    transport = _MeasurementTransport()
    result = load_live_daily(
        project_root=tmp_path,
        catalog=make_catalog(),
        start=date(2026, 4, 1),
        end=date(2026, 4, 1),
        transport=transport,
        cache=QueryCache(tmp_path / "queries"),
        api_key=_SECRET,
        archive_end=_MARCH,
    )
    assert transport.urls == []
    assert result.origin == "unpublished"
    assert result.rows.empty
    assert "2026-03-31" in result.detail


def test_archive_cache_cannot_be_placed_in_processed_data(tmp_path: Path) -> None:
    with pytest.raises(ArchiveError, match="processed"):
        load_archive(
            project_root=tmp_path,
            transport=_MetaTransport(_Response(200, b"{}")),
            api_key=_SECRET,
            cache_directory=tmp_path / "data" / "processed" / "xkdr_meta",
        )


def test_dashboard_does_not_download_bulk_months() -> None:
    root = Path(__file__).resolve().parents[1]
    page = (root / "src" / "dashboard" / "timeline_page.py").read_text(encoding="utf-8")
    live = (root / "src" / "dashboard" / "live_daily.py").read_text(encoding="utf-8")
    archive = (root / "src" / "dashboard" / "xkdr_archive.py").read_text(encoding="utf-8")
    for source in (page, live, archive):
        assert "download_bulk_file" not in source
        assert "acquire_dataset" not in source
    assert "availability_messages" in page
    assert "not download bulk files" in page


def _load(tmp_path: Path, transport: _MetaTransport, moment: datetime | None = None):
    clock = None if moment is None else (lambda moment=moment: moment)
    return load_archive(
        project_root=tmp_path,
        transport=transport,
        api_key=_SECRET,
        cache_directory=tmp_path,
        clock=clock,
    )


def _resolve(
    tmp_path: Path,
    transport: _MeasurementTransport,
    start: date,
    end: date,
    *,
    today: date,
    archive_end: date,
):
    archive = ArchiveInfo(
        last_month=f"{archive_end.year:04d}-{archive_end.month:02d}",
        archive_end=archive_end,
        available=True,
        origin="api",
        detail="test archive",
    )
    return resolve_timeline_sources(
        project_root=tmp_path,
        catalog=make_catalog(),
        start=start,
        end=end,
        today=today,
        transport=transport,
        cache=QueryCache(tmp_path / "queries"),
        api_key=_SECRET,
        archive=archive,
    )


def _meta_payload(last_month: str) -> dict[str, object]:
    return {
        "exported_at": "2026-09-30T11:25:11+00:00",
        "measurements": {"last_month": last_month, "first_month": "2009-01"},
    }


def _bounds(url: str) -> tuple[date, date]:
    from urllib.parse import parse_qs, urlparse

    query = parse_qs(urlparse(url).query)
    return date.fromisoformat(query["start"][0]), date.fromisoformat(query["end"][0])


class _MeasurementTransport:
    def __init__(self) -> None:
        self.urls: list[str] = []

    def send(self, url: str, headers: dict[str, str], timeout: float, max_bytes: int) -> _Response:
        del headers, timeout, max_bytes
        self.urls.append(url)
        return _Response(500, b'{"error":"internal_error","detail":"should not be parsed as success"}')
