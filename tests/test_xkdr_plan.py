"""XKDR acquisition planning. The transport is fake, so these tests do not call XKDR."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ingestion.acquire import AcquisitionError, BulkFile, download_bulk_file
from ingestion.xkdr_plan import (
    duplicate_timestamp_count,
    listed_measurement_keys,
    parse_month_token,
    plan_acquisition,
)
from tests.config_factory import make_config
from tests.test_acquire import FakeResponse, FakeTransport

_KEY = "plan-test-key-do-not-log"
_LIST = {
    "files": [
        {"key": "v1/stations.parquet", "size": 10},
        {"key": "v1/parameters.parquet", "size": 4},
        {"key": "v1/measurements/year=2024/month=01/data.parquet", "size_bytes": 100, "row_count": 5},
        {"key": "v1/measurements/year=2024/month=02/data.parquet", "size_bytes": 50, "row_count": 2},
        {"key": "v1/us_embassy.parquet"},
    ]
}


def test_missing_key_writes_a_manifest_without_calling_the_network(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("AQI_API_KEY", raising=False)
    called = False

    def handler(method: str, url: str, headers: dict[str, str]) -> FakeResponse:
        nonlocal called
        called = True
        return FakeResponse(200, b"{}")

    manifest = plan_acquisition(
        make_config(tmp_path, date_start="2024-01-01", date_end="2024-02-29"),
        transport=FakeTransport(handler),
    )
    assert called is False
    assert manifest["api_key_configured"] is False
    assert manifest["remote_listing"] == "not_requested"
    february = _month(manifest, 2024, 2)
    assert february["remote_availability"] == "unknown"
    assert february["download_status"] == "not_requested"
    assert february["download_status"] != "unavailable_remotely"
    text = (tmp_path / "data" / "interim" / "xkdr_acquisition_manifest.json").read_text(encoding="utf-8")
    assert "AQI_API_KEY is not configured" in text


def test_dry_run_records_remote_size_and_does_not_download_months(tmp_path: Path) -> None:
    transport = FakeTransport(lambda method, url, headers: _listing(url))
    _write_month(tmp_path, 2024, 1)
    manifest = plan_acquisition(
        make_config(tmp_path, date_start="2024-01-01", date_end="2024-02-29"),
        transport=transport,
        api_key=_KEY,
    )
    assert manifest["mode"] == "dry_run"
    assert all(not call[1].endswith("month=02/data.parquet") for call in transport.calls)
    january = _month(manifest, 2024, 1)
    february = _month(manifest, 2024, 2)
    assert january["download_status"] == "already_downloaded"
    assert january["validation_status"] == "validated"
    assert february["remote_availability"] == "available"
    assert february["download_status"] == "download_pending"
    assert february["remote_file_bytes"] == 50
    assert _KEY not in json.dumps(manifest)


def test_explicit_month_selection_ignores_embassy_objects() -> None:
    keys = listed_measurement_keys(_LIST, (parse_month_token("2024-02"),))
    assert keys == ("v1/measurements/year=2024/month=02/data.parquet",)
    assert "v1/us_embassy.parquet" not in keys


def test_download_requires_an_explicit_month(tmp_path: Path) -> None:
    with pytest.raises(AcquisitionError, match="Refusing to download"):
        plan_acquisition(
            make_config(tmp_path, date_start="2024-01-01", date_end="2024-02-29"),
            download=True,
        )


def test_download_skips_a_validated_file_and_fetches_only_the_requested_month(tmp_path: Path) -> None:
    body = _parquet_bytes()
    transport = FakeTransport(lambda method, url, headers: _route(url, body))
    _write_month(tmp_path, 2024, 1)
    manifest = plan_acquisition(
        make_config(tmp_path, date_start="2024-01-01", date_end="2024-02-29"),
        transport=transport,
        api_key=_KEY,
        months=(parse_month_token("2024-02"),),
        download=True,
    )
    object_calls = [url for _method, url, _headers in transport.calls if url.endswith("data.parquet")]
    assert object_calls == ["https://airquality.xkdr.org/v1/files/v1/measurements/year=2024/month=02/data.parquet"]
    saved = tmp_path / "data" / "raw" / "v1" / "measurements" / "year=2024" / "month=02" / "data.parquet"
    assert saved.read_bytes() == body
    assert not saved.with_name("data.parquet.partial").exists()
    assert _month(manifest, 2024, 2)["download_status"] == "downloaded"
    assert _month(manifest, 2024, 2)["duplicate_timestamp_keys"] == 0
    assert _month(manifest, 2024, 1)["download_status"] == "already_downloaded"


def test_corrupt_parquet_is_not_published(tmp_path: Path) -> None:
    def handler(method: str, url: str, headers: dict[str, str]) -> FakeResponse:
        if url.endswith("/v1/files"):
            return FakeResponse(200, _json(_LIST))
        return FakeResponse(200, b"not-parquet", {"Content-Length": "11"})

    manifest = plan_acquisition(
        make_config(tmp_path, date_start="2024-02-01", date_end="2024-02-29"),
        transport=FakeTransport(handler),
        api_key=_KEY,
        months=(parse_month_token("2024-02"),),
        download=True,
    )
    destination = tmp_path / "data" / "raw" / "v1" / "measurements" / "year=2024" / "month=02" / "data.parquet"
    assert not destination.exists()
    assert not destination.with_name("data.parquet.partial").exists()
    assert _month(manifest, 2024, 2)["download_status"] == "failed"


def test_retry_then_resume_uses_the_partial_file(tmp_path: Path) -> None:
    body = _parquet_bytes()
    partial = tmp_path / "data" / "raw" / "v1" / "measurements" / "year=2024" / "month=02" / "data.parquet.partial"
    partial.parent.mkdir(parents=True)
    partial.write_bytes(body[:4])
    calls = {"count": 0}

    def handler(method: str, url: str, headers: dict[str, str]) -> FakeResponse:
        if url.endswith("/v1/files"):
            return FakeResponse(200, _json(_LIST))
        calls["count"] += 1
        if calls["count"] == 1:
            raise AcquisitionError("Could not reach the XKDR API: timed out")
        assert headers["Range"] == "bytes=4-"
        return FakeResponse(206, body[4:], {"Content-Range": f"bytes 4-{len(body) - 1}/{len(body)}"})

    result = download_bulk_file(
        FakeTransport(handler),
        "https://airquality.xkdr.org",
        _KEY,
        tmp_path / "data" / "raw",
        BulkFile("v1/measurements/year=2024/month=02/data.parquet", 2024, 2),
        retries=2,
        validate_parquet=True,
    )
    assert result.status == "resumed"
    assert calls["count"] == 2
    assert Path(result.path).read_bytes() == body
    assert not partial.exists()


def test_duplicate_timestamp_keys_are_counted_and_the_file_is_kept(tmp_path: Path) -> None:
    path = tmp_path / "data.parquet"
    _write_parquet(path, duplicate=True)
    assert duplicate_timestamp_count(path) == 1
    assert path.is_file()


def test_plan_module_does_not_touch_data_gov_automation() -> None:
    source = Path("src/ingestion/xkdr_plan.py").read_text(encoding="utf-8")
    assert "data_gov" not in source
    assert "AEROSCOPE_DataGov" not in source
    assert "cpcb_caaqm" in Path("config/settings.yaml").read_text(encoding="utf-8")


def _month(manifest: dict, year: int, month: int) -> dict:
    found = [item for item in manifest["months"] if item["year"] == year and item["month"] == month]
    assert len(found) == 1
    return found[0]


def _listing(url: str) -> FakeResponse:
    assert url.endswith("/v1/files")
    return FakeResponse(200, _json(_LIST))


def _route(url: str, body: bytes) -> FakeResponse:
    if url.endswith("/v1/files"):
        return FakeResponse(200, _json(_LIST))
    return FakeResponse(200, body, {"Content-Length": str(len(body))})


def _write_month(root: Path, year: int, month: int) -> None:
    path = root / "data" / "raw" / "v1" / "measurements" / f"year={year}" / f"month={month:02d}" / "data.parquet"
    _write_parquet(path, duplicate=False)


def _write_parquet(path: Path, *, duplicate: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    stamp = datetime(2024, 2, 1, 0, 0)
    rows = 2 if duplicate else 1
    table = pa.table(
        {
            "station_id": ["site_1"] * rows,
            "parameter_name": ["PM2.5"] * rows,
            "collected_at": [stamp] * rows,
            "value": [1.0] * rows,
            "source": ["cpcb_caaqm"] * rows,
            "unit": ["µg/m³"] * rows,
        }
    )
    pq.write_table(table, path)


def _parquet_bytes() -> bytes:
    path_holder: list[bytes] = []
    # Build the bytes through pyarrow's file writer via a temporary buffer.
    sink = pa.BufferOutputStream()
    stamp = datetime(2024, 2, 1, 0, 0)
    table = pa.table(
        {
            "station_id": ["site_1"],
            "parameter_name": ["PM2.5"],
            "collected_at": [stamp],
            "value": [1.0],
            "source": ["cpcb_caaqm"],
            "unit": ["µg/m³"],
        }
    )
    pq.write_table(table, sink)
    path_holder.append(sink.getvalue().to_pybytes())
    return path_holder[0]


def _json(payload: dict) -> bytes:
    return json.dumps(payload).encode("utf-8")
