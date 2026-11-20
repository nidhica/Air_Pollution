"""data.gov.in ingestion tests.

The transport is in-memory. These tests do not call the live API and do not
write under the repository data/raw directory.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from ingestion.data_gov import (
    ENDPOINT,
    DataGovError,
    fetch_snapshot,
    read_data_gov_api_key,
)
from tests.config_factory import make_config

_KEY = "test-key-should-not-be-stored"
_FIXTURE = Path(__file__).parent / "fixtures" / "data_gov_in_sample.json"


class _Response:
    def __init__(self, status: int, body: bytes) -> None:
        self.status = status
        self.headers: dict[str, str] = {}
        self.body = body


class ScriptedTransport:
    def __init__(self, pages: list[dict[str, object]], statuses: list[int] | None = None) -> None:
        self.pages = pages
        self.statuses = statuses or [200] * len(pages)
        self.calls = 0
        self.urls: list[str] = []

    def send(self, url: str, headers: dict[str, str], timeout: float) -> _Response:
        del headers, timeout
        self.urls.append(url)
        index = min(self.calls, len(self.pages) - 1)
        status = self.statuses[min(self.calls, len(self.statuses) - 1)]
        self.calls += 1
        return _Response(status, json.dumps(self.pages[index]).encode("utf-8"))


def _page(records: list[dict[str, object]], *, total: int, count: int | None = None, offset: int = 0) -> dict[str, object]:
    return {
        "status": "ok",
        "total": total,
        "count": len(records) if count is None else count,
        "offset": offset,
        "limit": len(records),
        "records": records,
    }


def _record(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "country": "India",
        "state": "Delhi",
        "city": "Delhi",
        "station": "Anand Vihar, Delhi - DPCC",
        "last_update": "17-11-2026 17:00:00",
        "latitude": "28.6508",
        "longitude": "77.3152",
        "pollutant_id": "PM2.5",
        "min_value": "40",
        "max_value": "80",
        "avg_value": "55",
    }
    row.update(overrides)
    return row


def _clock(second: int) -> datetime:
    return datetime(2026, 11, 17, 12, 0, second, tzinfo=timezone.utc)


def test_missing_api_key_names_the_variable(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DATA_GOV_IN_API_KEY", raising=False)
    with pytest.raises(DataGovError, match="DATA_GOV_IN_API_KEY"):
        read_data_gov_api_key(tmp_path)


def test_pagination_stores_every_page_and_keeps_the_xkdr_file(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    sentinel = config.paths.raw_root / "v1" / "measurements" / "year=2024" / "month=01" / "data.parquet"
    sentinel.parent.mkdir(parents=True)
    sentinel.write_bytes(b"xkdr-bytes")
    first = [_record(station="Station A"), _record(station="Station B", pollutant_id="PM10")]
    second = [_record(city="Mumbai", state="Maharashtra", station="Station C")]
    transport = ScriptedTransport(
        [
            _page(first, total=3, offset=0),
            _page(second, total=3, offset=2),
        ]
    )
    report = fetch_snapshot(
        config,
        transport=transport,
        api_key=_KEY,
        page_limit=2,
        clock=lambda: _clock(1),
    )
    assert transport.calls == 2
    assert report.pages == 2
    assert report.records_received == 3
    assert report.records_new == 3
    assert report.endpoint == ENDPOINT
    stored = pq.read_table(config.paths.interim_dir / "data_gov_in" / "records" / "data.parquet").to_pylist()
    assert {row["station"] for row in stored} == {"Station A", "Station B", "Station C"}
    raw_pages = list((config.paths.raw_root / "data_gov_in" / "snapshots").rglob("*.json"))
    assert raw_pages
    blob = "\n".join(path.read_text(encoding="utf-8") for path in raw_pages)
    assert _KEY not in blob
    assert sentinel.read_bytes() == b"xkdr-bytes"


def test_repeat_fetch_does_not_duplicate_and_a_new_hour_is_appended(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    original = [_record()]
    transport = ScriptedTransport([_page(original, total=1)])
    first = fetch_snapshot(config, transport=transport, api_key=_KEY, clock=lambda: _clock(2))
    second = fetch_snapshot(config, transport=transport, api_key=_KEY, clock=lambda: _clock(3))
    assert first.records_new == 1
    assert second.records_new == 0
    assert second.records_already_stored == 1
    later = [_record(avg_value="99"), _record(last_update="17-11-2026 18:00:00", avg_value="70")]
    later_transport = ScriptedTransport([_page(later, total=2)])
    third = fetch_snapshot(config, transport=later_transport, api_key=_KEY, clock=lambda: _clock(4))
    assert third.records_new == 1
    fourth = fetch_snapshot(config, transport=later_transport, api_key=_KEY, clock=lambda: _clock(5))
    assert fourth.records_new == 0
    assert fourth.records_stored == 2
    stored = pq.read_table(config.paths.interim_dir / "data_gov_in" / "records" / "data.parquet").to_pylist()
    assert len(stored) == 2
    older = next(row for row in stored if row["last_update"] == "17-11-2026 17:00:00")
    newer = next(row for row in stored if row["last_update"] == "17-11-2026 18:00:00")
    assert older["avg_value"] == "55"
    assert older["retrieved_at"] == "2026-11-17T12:00:02Z"
    assert newer["avg_value"] == "70"
    assert newer["retrieved_at"] == "2026-11-17T12:00:04Z"
    snapshots = list((config.paths.raw_root / "data_gov_in" / "snapshots").iterdir())
    assert len(snapshots) == 4


def test_missing_fields_and_sample_fixture_are_stored_without_inventing_values(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    sample = json.loads(_FIXTURE.read_text(encoding="utf-8"))
    incomplete = _record(latitude=None, min_value=None, station=None)
    del incomplete["latitude"]
    transport = ScriptedTransport([_page([*sample["records"], incomplete], total=3)])
    fetch_snapshot(config, transport=transport, api_key=_KEY, clock=lambda: _clock(5))
    stored = pq.read_table(config.paths.interim_dir / "data_gov_in" / "records" / "data.parquet").to_pylist()
    kadapa = next(row for row in stored if row["city"] == "Kadapa")
    assert kadapa["avg_value"] == "61"
    assert kadapa["pollutant_id"] == "PM2.5"
    blank = next(row for row in stored if row["station"] is None)
    assert blank["latitude"] is None
    assert blank["min_value"] is None
    assert "0" != blank["min_value"]


def test_http_401_does_not_retry_and_the_key_is_removed_from_the_message(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    transport = ScriptedTransport(
        [{"message": f"rejected {_KEY}"}],
        statuses=[401],
    )
    with pytest.raises(DataGovError, match="HTTP 401") as raised:
        fetch_snapshot(config, transport=transport, api_key=_KEY, clock=lambda: _clock(6))
    assert transport.calls == 1
    assert _KEY not in str(raised.value)


def test_transient_http_500_is_retried(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    transport = ScriptedTransport(
        [_page([_record()], total=1), _page([_record()], total=1)],
        statuses=[500, 200],
    )
    waits: list[float] = []
    report = fetch_snapshot(
        config,
        transport=transport,
        api_key=_KEY,
        sleep=waits.append,
        clock=lambda: _clock(7),
    )
    assert transport.calls == 2
    assert waits == [0.5]
    assert report.records_new == 1


def test_api_key_can_be_read_from_an_untracked_env_file(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    # Fixture only. This is not a data.gov.in credential.
    env_file_fixture = "dummy-env-file"
    monkeypatch.delenv("DATA_GOV_IN_API_KEY", raising=False)
    (tmp_path / ".env").write_text("DATA_GOV_IN_API_KEY=" + env_file_fixture + "\n", encoding="utf-8")
    assert read_data_gov_api_key(tmp_path) == env_file_fixture
