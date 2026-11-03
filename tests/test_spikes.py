"""Spike-detection tests.

Spark tests build synthetic Parquet fixtures under pytest's temporary
directory. Those fixtures are not January 2024 measurements and are not
written to data/raw. Counts in this module describe the fixtures only.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from analytics.spikes.detect import TIE_POLICY, SpikeDetectionError, detect_spikes
from tests.config_factory import make_config
from utils.config_loader import ConfigError, load_config
from utils.spark_session import build_spark_session, stop_spark

_START = datetime(2024, 1, 1, 0, 0)


@pytest.fixture(scope="module")
def spark():
    base = load_config()
    tuned = replace(base, spark=replace(base.spark, ui_enabled=False, log_level="ERROR"))
    session = build_spark_session(tuned, force_new=True)
    yield session
    stop_spark(session)


def test_tie_policy_does_not_invent_an_order() -> None:
    assert "no other column is used to break the tie" in TIE_POLICY
    assert "omitted from every baseline" in TIE_POLICY


def test_missing_cleaned_file_fails_before_spark(tmp_path: Path) -> None:
    with pytest.raises(SpikeDetectionError, match="Cleaned measurements not found"):
        detect_spikes(make_config(tmp_path), spark=None)  # type: ignore[arg-type]


def test_minimum_baseline_must_support_a_sample_deviation(tmp_path: Path) -> None:
    source = Path(__file__).resolve().parents[1] / "config" / "settings.yaml"
    text = source.read_text(encoding="utf-8").replace("minimum_baseline: 12", "minimum_baseline: 1")
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "settings.yaml").write_text(text, encoding="utf-8")

    with pytest.raises(ConfigError, match="at least 2"):
        load_config(config_dir / "settings.yaml")


@pytest.mark.spark
def test_insufficient_history_is_not_a_spike(tmp_path: Path, spark) -> None:
    _write(tmp_path, _rows("s1", "PM2.5", [10.0] * 11 + [1000.0]))
    report = detect_spikes(make_config(tmp_path), spark)
    current = _row(spark, report, value=1000.0)

    assert report.document["counts"]["total_records"] == 12
    assert current["baseline_count"] == 11
    assert current["spike_threshold"] is None
    assert current["is_spike"] is False
    assert report.document["counts"]["spike_records"] == 0


@pytest.mark.spark
def test_zero_standard_deviation_uses_a_strict_threshold(tmp_path: Path, spark) -> None:
    rows = _rows("flat", "PM2.5", [10.0] * 12 + [10.0])
    rows.extend(_rows("rise", "PM2.5", [10.0] * 12 + [50.0]))
    _write(tmp_path, rows)
    report = detect_spikes(make_config(tmp_path), spark)
    frame = spark.read.parquet(str(report.annotated_file))

    flat = frame.where((frame.station_id == "flat") & (frame.value == 10.0)).orderBy("collected_at").collect()
    rise = frame.where((frame.station_id == "rise") & (frame.value == 50.0)).collect()[0]
    current = flat[-1]

    assert current["baseline_count"] == 12
    assert current["rolling_stddev"] == 0.0
    assert current["spike_threshold"] == 10.0
    assert current["is_spike"] is False
    assert rise["baseline_count"] == 12
    assert rise["rolling_stddev"] == 0.0
    assert rise["is_spike"] is True
    assert frame.count() == len(rows)


@pytest.mark.spark
def test_ordinary_observation_and_spike_use_only_the_previous_window(tmp_path: Path, spark) -> None:
    baseline = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0]
    _write(
        tmp_path,
        _rows("ordinary", "PM2.5", baseline + [6.0]) + _rows("spike", "PM2.5", baseline + [100.0]),
    )
    report = detect_spikes(make_config(tmp_path), spark)
    ordinary = _row(spark, report, station_id="ordinary", value=6.0)
    spike = _row(spark, report, station_id="spike", value=100.0)

    assert ordinary["baseline_count"] == 12
    assert ordinary["value"] < ordinary["spike_threshold"]
    assert ordinary["is_spike"] is False
    assert spike["baseline_count"] == 12
    assert spike["value"] > spike["spike_threshold"]
    assert spike["is_spike"] is True
    assert ordinary["rolling_mean"] == pytest.approx(spike["rolling_mean"])
    assert report.document["counts"]["spike_records"] == 1
    assert report.document["counts"]["spike_percentage_of_observations"] == pytest.approx(100 / 26)


@pytest.mark.spark
def test_invalid_measurements_are_kept_and_excluded_from_the_baseline(tmp_path: Path, spark) -> None:
    values = [10.0] * 12 + [-5.0, None, 999.0, float("nan"), 10.0]
    statuses = ["valid"] * 12 + ["negative", "missing", "non_numeric", "non_finite", "valid"]
    source = _write(tmp_path, _rows("s1", "PM2.5", values, statuses))
    before = source.read_bytes()
    report = detect_spikes(make_config(tmp_path), spark)
    frame = spark.read.parquet(str(report.annotated_file))
    final = frame.where(frame.measurement_status == "valid").orderBy("collected_at").collect()[-1]
    invalid = frame.where(~frame.is_valid_measurement).collect()

    assert source.read_bytes() == before
    assert frame.count() == len(values)
    assert len(invalid) == 4
    assert all(row["is_spike"] is False for row in invalid)
    assert final["baseline_count"] == 12
    assert final["rolling_mean"] == pytest.approx(10.0)
    assert final["rolling_stddev"] == 0.0
    assert final["is_spike"] is False


@pytest.mark.spark
def test_stations_and_pollutants_have_separate_baselines(tmp_path: Path, spark) -> None:
    rows = _rows("A", "PM2.5", [1.0] * 12 + [100.0])
    rows.extend(_rows("B", "PM2.5", [100.0] * 12 + [100.0]))
    rows.extend(_rows("A", "PM10", [5.0] * 12 + [5.0]))
    _write(tmp_path, rows)
    report = detect_spikes(make_config(tmp_path), spark)
    frame = spark.read.parquet(str(report.annotated_file))
    low_station = _last(frame, "A", "PM2.5")
    high_station = _last(frame, "B", "PM2.5")
    pm10 = _last(frame, "A", "PM10")

    assert low_station["rolling_mean"] == pytest.approx(1.0)
    assert low_station["is_spike"] is True
    assert high_station["rolling_mean"] == pytest.approx(100.0)
    assert high_station["is_spike"] is False
    assert pm10["rolling_mean"] == pytest.approx(5.0)
    assert pm10["is_spike"] is False


@pytest.mark.spark
def test_timestamp_ties_are_not_ordered_or_used_as_baseline(tmp_path: Path, spark) -> None:
    rows = _rows("s1", "PM2.5", [10.0] * 12)
    tie_time = _START + timedelta(hours=12)
    later = tie_time + timedelta(hours=1)
    rows.extend(
        [
            _point("s1", "PM2.5", tie_time, 10.0),
            _point("s1", "PM2.5", tie_time, 100.0),
            _point("s1", "PM2.5", later, 10.0),
        ]
    )
    _write(tmp_path, rows)
    report = detect_spikes(make_config(tmp_path), spark)
    frame = spark.read.parquet(str(report.annotated_file))
    ties = frame.where(frame.timestamp_tie).collect()
    following = (
        frame.where((frame.station_id == "s1") & ~frame.timestamp_tie)
        .orderBy("collected_at")
        .collect()[-1]
    )
    ordering = report.document["ordering"]

    assert frame.count() == len(rows)
    assert {row["value"] for row in ties} == {10.0, 100.0}
    assert all(row["is_spike"] is False for row in ties)
    assert all(row["baseline_count"] is None for row in ties)
    assert following["baseline_count"] == 12
    assert following["rolling_mean"] == pytest.approx(10.0)
    assert following["is_spike"] is False
    assert ordering["duplicate_key_rows"] == 2
    assert ordering["duplicate_key_groups"] == 1
    assert ordering["unique_within_station_and_parameter"] is False


@pytest.mark.spark
def test_window_and_minimum_are_configurable(tmp_path: Path, spark) -> None:
    _write(tmp_path, _rows("s1", "PM2.5", [2.0, 2.0, 100.0]))
    config = make_config(tmp_path)
    default_report = detect_spikes(config, spark, report_path=tmp_path / "default.json")
    short = replace(config, spikes=replace(config.spikes, baseline_window=4, minimum_baseline=2))
    short_report = detect_spikes(
        short,
        spark,
        report_path=tmp_path / "short.json",
        annotated_dir=tmp_path / "short-annotated",
        spike_dir=tmp_path / "short-spikes",
    )

    assert default_report.document["counts"]["spike_records"] == 0
    assert short_report.document["parameters"]["minimum_baseline"] == 2
    assert short_report.document["counts"]["spike_records"] == 1
    current = _row(spark, short_report, value=100.0)
    assert current["baseline_count"] == 2
    assert current["is_spike"] is True


def _rows(
    station_id: str,
    parameter_name: str,
    values: list[float | None],
    statuses: list[str] | None = None,
) -> list[dict[str, object]]:
    return [
        _point(
            station_id,
            parameter_name,
            _START + timedelta(hours=index),
            value,
            "valid" if statuses is None else statuses[index],
        )
        for index, value in enumerate(values)
    ]


def _point(
    station_id: str,
    parameter_name: str,
    collected_at: datetime,
    value: float | None,
    status: str = "valid",
) -> dict[str, object]:
    return {
        "station_id": station_id,
        "parameter_name": parameter_name,
        "collected_at": collected_at,
        "value": value,
        "measurement_status": status,
    }


def _write(root: Path, rows: list[dict[str, object]]) -> Path:
    directory = root / "data" / "processed" / "measurements"
    directory.mkdir(parents=True)
    destination = directory / "data.parquet"
    pq.write_table(
        pa.table(
            {
                "station_id": [row["station_id"] for row in rows],
                "parameter_name": [row["parameter_name"] for row in rows],
                "collected_at": pa.array(
                    [row["collected_at"] for row in rows],
                    type=pa.timestamp("us"),
                ),
                "value": pa.array([row["value"] for row in rows], type=pa.float64()),
                "measurement_status": [row["measurement_status"] for row in rows],
            }
        ),
        destination,
    )
    return destination


def _last(frame, station_id: str, parameter_name: str):
    matches = (
        frame.where((frame.station_id == station_id) & (frame.parameter_name == parameter_name))
        .orderBy("collected_at")
        .collect()
    )
    assert matches
    return matches[-1]


def _row(spark, report, **filters: object):
    frame = spark.read.parquet(str(report.annotated_file))
    for name, value in filters.items():
        frame = frame.where(frame[name] == value)
    matches = frame.collect()
    assert len(matches) == 1
    return matches[0]
