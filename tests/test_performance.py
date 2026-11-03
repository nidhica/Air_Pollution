"""Performance-report tests.

These tests do not start Spark and do not read the January measurements.
They check timing records, redaction, and missing measurements.
"""

from __future__ import annotations

from datetime import date
from zoneinfo import ZoneInfo

import pytest

from performance.report import (
    assemble_report,
    machine_profile,
    make_stage,
    redact,
    render_markdown,
    rows_per_second,
    write_report,
)
from performance.run import PerformanceError, _counts_match, _inspect, _month_count, _time_call, run_evaluation
from tests.config_factory import make_config


def test_throughput_is_unavailable_without_both_inputs() -> None:
    rate, reason = rows_per_second(None, 2.0)
    assert rate is None
    assert reason == "the row count was not recorded"
    rate, reason = rows_per_second(10, None)
    assert rate is None
    assert reason == "the wall-clock time was not recorded"
    rate, reason = rows_per_second(10, 2.0)
    assert rate == pytest.approx(5.0)
    assert reason is None


def test_stage_record_keeps_missing_metrics_null() -> None:
    stage = make_stage(
        name="inspection",
        started_at=None,
        ended_at=None,
        wall_clock_seconds=None,
        success=False,
        input_rows=None,
        output_rows=4,
        input_bytes=None,
        output_bytes=None,
    )

    assert stage["input_rows_per_second"] is None
    assert stage["output_rows_per_second"] is None
    fields = {item["field"] for item in stage["unavailable"]}
    assert "input_rows_per_second" in fields
    assert "output_rows_per_second" in fields
    assert stage["wall_clock_seconds"] is None


def test_error_text_does_not_keep_a_secret() -> None:
    zone = ZoneInfo("Asia/Kolkata")

    def fail() -> None:
        raise RuntimeError("download failed for token-secret")

    _result, success, error, started, ended, elapsed = _time_call(fail, zone, ("token-secret",))

    assert success is False
    assert error is not None
    assert "token-secret" not in error
    assert "[redacted]" in error
    assert started
    assert ended
    assert elapsed >= 0
    assert "token-secret" not in redact("AQI_API_KEY=token-secret", ("token-secret",))


def test_missing_spark_session_does_not_invent_a_runtime(tmp_path) -> None:
    config = make_config(tmp_path)
    stage = _inspect(config, None, tmp_path / "sandbox", ZoneInfo("Asia/Kolkata"), ())

    assert stage["success"] is False
    assert stage["wall_clock_seconds"] is None
    assert stage["input_rows"] is None
    assert any(item["field"] == "wall_clock_seconds" for item in stage["unavailable"])


def test_sandbox_cannot_replace_saved_outputs(tmp_path) -> None:
    config = make_config(tmp_path)

    with pytest.raises(PerformanceError, match="Refusing to write performance outputs"):
        run_evaluation(config, spark=None, sandbox=tmp_path / "data" / "processed" / "timing")


def test_report_labels_a_local_run_and_renders_unavailable_values(tmp_path) -> None:
    stage = make_stage(
        name="acquisition",
        started_at="2026-11-02T22:00:00+05:30",
        ended_at="2026-11-02T22:00:01+05:30",
        wall_clock_seconds=1.0,
        success=True,
        input_rows=None,
        output_rows=None,
        input_bytes=100,
        output_bytes=None,
        unavailable=[
            {
                "field": "network_download_seconds",
                "reason": "The original download duration was not recorded.",
            }
        ],
        detail={"network_download_timed": False, "files": [{"key": "v1/stations.parquet", "status": "skipped", "bytes": 10}]},
    )
    document = assemble_report(
        generated_at="2026-11-02T22:00:01+05:30",
        timezone_name="Asia/Kolkata",
        machine=machine_profile(spark_version=None, spark_master=None, java_description=None),
        dataset={
            "date_start": "2024-01-01",
            "date_end": "2024-01-31",
            "months": 1,
            "measurement_files": 1,
            "input_observations": None,
            "output_observations": None,
            "candidate_spikes": None,
            "eligible_observations": None,
            "saved_reports": {},
            "counts_match_saved_reports": None,
            "unavailable": [{"field": "input_observations", "reason": "not recorded"}],
        },
        stages=[stage],
        session_startup_seconds=None,
        canonical_outputs_unchanged=True,
    )
    text = render_markdown(document)

    assert document["distributed_cluster"] is False
    assert document["speedup_measured"] is False
    assert "not a distributed cluster benchmark" in text
    assert "does not report a speedup" in text
    assert "unavailable" in text
    assert "network_download_seconds" in text
    assert "AQI_API_KEY" not in text
    assert _month_count(date(2024, 1, 1), date(2024, 1, 31)) == 1
    assert _counts_match(None, None, None, None, {}) is None

    json_path = tmp_path / "performance_report.json"
    markdown_path = tmp_path / "reports" / "performance.md"
    write_report(document, json_path, markdown_path)
    assert json_path.is_file()
    assert "unavailable" in markdown_path.read_text(encoding="utf-8")
