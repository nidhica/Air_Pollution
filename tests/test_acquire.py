"""Acquisition tests. The transport is fake, so these tests do not call XKDR."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from urllib.request import Request

from ingestion.acquire import (
    AcquisitionError,
    acquire_dataset,
    api_origin,
    endpoint_url,
    format_diagnostic,
    parse_file_list,
    probe_api,
    request_headers,
    select_bulk_files,
)
from tests.config_factory import make_config

_KEY = "test-key-do-not-log"
_LIST = {
    "files": [
        {"key": "v1/parameters.parquet"},
        {"key": "v1/measurements/year=2024/month=12/data.parquet"},
        {"key": "v1/stations.parquet", "note": "extra fields are ignored"},
        {"key": "v1/measurements/year=2024/month=11/data.parquet"},
        {"key": "v1/other.parquet"},
    ]
}


class FakeResponse:
    def __init__(self, status: int, body: bytes, headers: dict[str, str] | None = None) -> None:
        self.status = status
        self.headers = headers or {}
        self._body = body

    def iter_chunks(self):
        if self._body:
            yield self._body

    def close(self) -> None:
        return None


class FakeTransport:
    def __init__(self, handler) -> None:
        self.handler = handler
        self.calls: list[tuple[str, str, dict[str, str]]] = []

    def send(self, method: str, url: str, headers: dict[str, str]) -> FakeResponse:
        self.calls.append((method, url, dict(headers)))
        return self.handler(method, url, headers)


def test_source_does_not_contain_an_api_key() -> None:
    source = Path("src/ingestion/acquire.py").read_text(encoding="utf-8")
    assert "aqi_" not in source


def test_selects_documented_keys_inside_the_date_window() -> None:
    plan = select_bulk_files(
        parse_file_list(_LIST),
        date_start=date(2024, 11, 30),
        date_end=date(2024, 11, 30),
    )
    assert [item.key for item in plan.selected] == [
        "v1/stations.parquet",
        "v1/parameters.parquet",
        "v1/measurements/year=2024/month=11/data.parquet",
    ]
    assert plan.ignored_keys == ("v1/other.parquet",)


def test_full_archive_keeps_every_measurement_month() -> None:
    plan = select_bulk_files(parse_file_list(_LIST), date_start=None, date_end=None)
    assert [item.key for item in plan.selected if item.year is not None] == [
        "v1/measurements/year=2024/month=11/data.parquet",
        "v1/measurements/year=2024/month=12/data.parquet",
    ]


def test_rejects_an_unsafe_or_unrecognised_measurement_key() -> None:
    with pytest.raises(AcquisitionError, match="Refusing"):
        parse_file_list({"files": [{"key": "v1/../../secret.parquet"}]})
    with pytest.raises(AcquisitionError, match="Unrecognised"):
        parse_file_list({"files": [{"key": "v1/measurements/year=2024/month=13/data.parquet"}]})


def test_download_preserves_bytes_and_skips_a_completed_file(tmp_path: Path) -> None:
    bodies = {
        "v1/stations.parquet": b"stations-bytes",
        "v1/parameters.parquet": b"parameters-bytes",
        "v1/measurements/year=2024/month=11/data.parquet": b"month-bytes",
    }
    transport = FakeTransport(lambda method, url, headers: _route(_LIST, bodies, url))
    config = make_config(tmp_path, date_start="2024-11-01", date_end="2024-11-30")

    first = acquire_dataset(config, transport=transport, api_key=_KEY)
    assert first.ok
    for key, body in bodies.items():
        assert (tmp_path / "data" / "raw" / key).read_bytes() == body
    assert all(call[2]["Authorization"] == f"Bearer {_KEY}" for call in transport.calls)
    assert all(call[2]["User-Agent"] == "AirPollutionBDA/1.0 (academic research)" for call in transport.calls)
    assert _KEY not in "\n".join(item.message for item in first.files)

    calls_after_first = len(transport.calls)
    second = acquire_dataset(config, transport=transport, api_key=_KEY)
    assert second.ok
    assert all(item.status == "skipped" for item in second.files)
    assert len(transport.calls) == calls_after_first + 1


def test_resume_appends_a_partial_file(tmp_path: Path) -> None:
    config = make_config(tmp_path, date_start="2024-11-01", date_end="2024-11-30")
    partial = tmp_path / "data" / "raw" / "v1" / "measurements" / "year=2024" / "month=11" / "data.parquet.partial"
    partial.parent.mkdir(parents=True)
    partial.write_bytes(b"abc")

    def handler(method: str, url: str, headers: dict[str, str]) -> FakeResponse:
        if url.endswith("/v1/files"):
            return FakeResponse(200, _json(_LIST))
        if url.endswith("data.parquet"):
            assert headers["Range"] == "bytes=3-"
            return FakeResponse(206, b"def", {"Content-Range": "bytes 3-5/6"})
        body = b"meta"
        return FakeResponse(200, body, {"Content-Length": str(len(body))})

    report = acquire_dataset(config, transport=FakeTransport(handler), api_key=_KEY)
    measurement = next(item for item in report.files if item.key.endswith("data.parquet"))
    assert measurement.status == "resumed"
    assert Path(measurement.path).read_bytes() == b"abcdef"
    assert not partial.exists()


def test_incomplete_body_is_not_published(tmp_path: Path) -> None:
    config = make_config(tmp_path, date_start="2024-11-01", date_end="2024-11-30")

    def handler(method: str, url: str, headers: dict[str, str]) -> FakeResponse:
        if url.endswith("/v1/files"):
            return FakeResponse(200, _json(_LIST))
        if url.endswith("stations.parquet"):
            return FakeResponse(200, b"ok", {"Content-Length": "2"})
        if url.endswith("parameters.parquet"):
            return FakeResponse(200, b"ok", {"Content-Length": "2"})
        return FakeResponse(200, b"abc", {"Content-Length": "6"})

    report = acquire_dataset(config, transport=FakeTransport(handler), api_key=_KEY)
    measurement = next(item for item in report.files if item.key.endswith("data.parquet"))
    assert measurement.status == "failed"
    assert not Path(measurement.path).exists()
    assert Path(measurement.path + ".partial").read_bytes() == b"abc"
    assert not report.ok


def test_rejected_key_is_not_echoed(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    transport = FakeTransport(lambda method, url, headers: FakeResponse(401, _KEY.encode("utf-8")))
    with pytest.raises(AcquisitionError, match="HTTP 401") as raised:
        acquire_dataset(config, transport=transport, api_key=_KEY)
    assert _KEY not in str(raised.value)


def test_configured_endpoints_use_the_xkdr_origin() -> None:
    from utils.config_loader import load_config

    origin = api_origin(load_config().dataset.documentation_url)
    assert origin == "https://airquality.xkdr.org"
    assert endpoint_url(origin, "/v1/meta") == "https://airquality.xkdr.org/v1/meta"
    assert endpoint_url(origin, "/v1/files") == "https://airquality.xkdr.org/v1/files"


def test_application_user_agent_is_not_a_browser() -> None:
    headers = request_headers(_KEY, accept="application/json")
    prepared = Request("https://airquality.xkdr.org/v1/files", headers=headers, method="GET")
    bare = Request("https://airquality.xkdr.org/v1/files", method="GET")
    assert prepared.get_header("User-agent") == "AirPollutionBDA/1.0 (academic research)"
    assert prepared.has_header("User-agent")
    assert not bare.has_header("User-agent")
    assert "Mozilla" not in headers["User-Agent"]
    assert prepared.get_header("Authorization") == f"Bearer {_KEY}"


def test_probe_reports_status_content_type_and_ray_without_the_key(tmp_path: Path) -> None:
    def handler(method: str, url: str, headers: dict[str, str]) -> FakeResponse:
        assert headers["Authorization"] == f"Bearer {_KEY}"
        assert headers["User-Agent"] == "AirPollutionBDA/1.0 (academic research)"
        if url.endswith("/v1/meta"):
            return FakeResponse(
                200,
                b'{"published":true}',
                {"Content-Type": "application/json", "CF-Ray": "meta123-MAA"},
            )
        return FakeResponse(
            403,
            f'{{"detail":"blocked {_KEY}"}}'.encode("utf-8"),
            {"Content-Type": "application/json", "CF-Ray": "files999-MAA"},
        )

    diagnostics = probe_api(make_config(tmp_path), transport=FakeTransport(handler), api_key=_KEY)
    text = "\n".join(format_diagnostic(item) for item in diagnostics)
    assert "GET https://airquality.xkdr.org/v1/meta -> HTTP 200" in text
    assert "content-type=application/json" in text
    assert "cf-ray=meta123-MAA" in text
    assert "GET https://airquality.xkdr.org/v1/files -> HTTP 403" in text
    assert "cf-ray=files999-MAA" in text
    assert _KEY not in text
    assert "[redacted]" in text


def test_missing_key_does_not_call_the_network(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("AQI_API_KEY", raising=False)
    called = False

    def handler(method: str, url: str, headers: dict[str, str]) -> FakeResponse:
        nonlocal called
        called = True
        return FakeResponse(200, b"{}")

    with pytest.raises(AcquisitionError, match="AQI_API_KEY"):
        acquire_dataset(make_config(tmp_path), transport=FakeTransport(handler))
    assert called is False


def _route(payload: dict, bodies: dict[str, bytes], url: str) -> FakeResponse:
    if url.endswith("/v1/files"):
        return FakeResponse(200, _json(payload))
    for key, body in bodies.items():
        if url.endswith(key):
            return FakeResponse(200, body, {"Content-Length": str(len(body))})
    raise AssertionError(url)


def _json(payload: dict) -> bytes:
    import json

    return json.dumps(payload).encode("utf-8")
