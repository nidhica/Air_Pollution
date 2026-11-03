"""Spatiotemporal aggregation tests.

Spark tests build synthetic annotated Parquet fixtures under pytest's
temporary directory. Those fixtures are not January 2024 measurements and
are not written to data/raw. Counts in this module describe the fixtures only.
"""

from __future__ import annotations

import csv
from dataclasses import replace
from datetime import datetime
from pathlib import Path

import pytest
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
)

from analytics.spatiotemporal.summarize import DENOMINATORS, AnalyticsError, analyze_dataset
from preprocessing.clean import _write_parquet
from tests.config_factory import make_config
from utils.config_loader import ConfigError, load_config
from utils.spark_session import build_spark_session, stop_spark


@pytest.fixture(scope="module")
def spark():
    base = load_config()
    tuned = replace(base, spark=replace(base.spark, ui_enabled=False, log_level="ERROR"))
    session = build_spark_session(tuned, force_new=True)
    yield session
    stop_spark(session)


def test_documented_denominators_keep_counts_and_rates_separate() -> None:
    assert "raw count" in DENOMINATORS["spike_count"]
    assert "eligible_observations" in DENOMINATORS["spike_rate_of_eligible"]
    assert "not a high spike count" in DENOMINATORS["consistently_elevated"]
    assert "not administrative boundaries" in DENOMINATORS["geography"]
    assert "Missing city" in DENOMINATORS["geography"]


def test_analytics_settings_default_when_the_section_is_absent(tmp_path: Path) -> None:
    config = make_config(tmp_path)

    assert config.analytics.elevated_minimum_valid_observations == 168
    assert config.analytics.elevated_percentile == 0.9
    assert config.analytics.unusual_sigmas == 2


def test_elevated_percentile_must_lie_between_zero_and_one(tmp_path: Path) -> None:
    source = Path(__file__).resolve().parents[1] / "config" / "settings.yaml"
    text = source.read_text(encoding="utf-8").replace("elevated_percentile: 0.9", "elevated_percentile: 1", 1)
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "settings.yaml").write_text(text, encoding="utf-8")

    with pytest.raises(ConfigError, match="greater than 0 and less than 1"):
        load_config(config_dir / "settings.yaml")


def test_missing_annotated_file_fails_before_spark(tmp_path: Path) -> None:
    with pytest.raises(AnalyticsError, match="Annotated measurements not found"):
        analyze_dataset(make_config(tmp_path), spark=None)  # type: ignore[arg-type]


@pytest.mark.spark
def test_rates_use_eligible_observations_and_means_ignore_invalid_values(tmp_path: Path, spark) -> None:
    config = _analysis_config(tmp_path)
    source = _write_annotated(tmp_path, spark, _fixture_rows(), config)
    before = source.read_bytes()

    report = analyze_dataset(config, spark)

    assert source.read_bytes() == before
    assert report.document["input_file_modified"] is False
    assert report.document["raw_files_modified"] is False
    counts = report.document["counts"]
    assert counts["total_records"] == 15
    assert counts["valid_measurements"] == 14
    assert counts["eligible_records"] == 14
    assert counts["spike_records"] == 3
    assert report.document["parameters"]["eligible_definition_disagreement"] == 0
    assert counts["spike_rate_of_eligible"] == pytest.approx(3 / 14)
    assert counts["spike_rate_of_observations"] == pytest.approx(3 / 15)

    daily = report.document["daily_by_pollutant"]
    first = _find(daily, observation_date="2024-01-01", parameter_name="PM2.5")
    assert first["observations"] == 3
    assert first["valid_observations"] == 2
    assert first["eligible_observations"] == 2
    assert first["mean_valid_value"] == pytest.approx(20)
    assert first["spike_count"] == 0
    assert first["spike_rate_of_observations"] == 0
    assert first["spike_rate_of_eligible"] == 0
    assert first["unusually_high_spike_count"] is False

    third = _find(daily, observation_date="2024-01-03", parameter_name="PM2.5")
    assert third["observations"] == 3
    assert third["mean_valid_value"] == pytest.approx(400)
    assert third["spike_count"] == 2
    assert third["eligible_observations"] == 3
    assert third["spike_rate_of_eligible"] == pytest.approx(2 / 3)
    assert third["spike_count_mean"] == pytest.approx(0.4)
    assert third["spike_count_stddev"] == pytest.approx(0.8**0.5)
    assert third["unusually_high_spike_count"] is True
    _assert_unusual_flags(daily, sigmas=1.0)

    pm10 = _find(daily, observation_date="2024-01-01", parameter_name="PM10")
    assert pm10["mean_valid_value"] == pytest.approx(50)
    assert pm10["spike_count"] == 1
    assert pm10["eligible_observations"] == 2
    assert pm10["spike_rate_of_eligible"] == pytest.approx(0.5)
    assert pm10["unusually_high_spike_count"] is False

    by_hour = report.document["hour_of_day"]
    hour_eight = _find(by_hour, observation_hour=8, parameter_name="PM2.5")
    assert hour_eight["observations"] == 6
    assert hour_eight["eligible_observations"] == 6
    assert hour_eight["mean_valid_value"] == pytest.approx(1142 / 6)
    assert hour_eight["spike_count"] == 2
    hour_ten = _find(by_hour, observation_hour=10, parameter_name="PM2.5")
    assert hour_ten["observations"] == 1
    assert hour_ten["valid_observations"] == 0
    assert hour_ten["mean_valid_value"] is None
    assert hour_ten["eligible_observations"] == 0
    assert hour_ten["spike_rate_of_eligible"] is None
    assert hour_ten["spike_rate_of_observations"] == 0

    rankings = report.document["station_rankings"]
    low = _find(rankings, station_id="low", parameter_name="PM2.5")
    high = _find(rankings, station_id="high", parameter_name="PM2.5")
    short = _find(rankings, station_id="short", parameter_name="PM2.5")
    assert low["eligible_observations"] == 2
    assert low["mean_valid_value"] == pytest.approx(20)
    assert low["spike_count"] == 0
    assert high["spike_count"] == 1
    assert high["eligible_observations"] == 2
    assert high["spike_rate_of_eligible"] == pytest.approx(0.5)
    assert short["spike_count"] == 1
    assert short["eligible_observations"] == 1
    assert short["spike_rate_of_eligible"] == pytest.approx(1)
    assert high["spike_count_rank"] == short["spike_count_rank"] == 1
    assert short["spike_rate_rank"] < high["spike_rate_rank"]
    assert low["spike_count_rank"] > high["spike_count_rank"]

    compared = _find(report.document["station_comparison"], station_id="low")
    assert compared["PM2_5_mean_valid_value"] == pytest.approx(20)
    assert compared["PM2_5_spike_count"] == 0
    assert compared["PM2_5_eligible_observations"] == 2
    assert compared["PM10_mean_valid_value"] == pytest.approx(50)
    assert compared["PM10_spike_count"] == 1
    assert compared["PM10_eligible_observations"] == 2
    assert compared["PM10_spike_rate_of_eligible"] == pytest.approx(0.5)
    absent = _find(report.document["station_comparison"], station_id="missing")
    assert absent["PM10_mean_valid_value"] is None
    assert absent["PM10_eligible_observations"] is None

    with (tmp_path / "data" / "processed" / "analytics" / "csv" / "daily_by_pollutant.csv").open(
        encoding="utf-8", newline=""
    ) as handle:
        header = next(csv.reader(handle))
    assert "eligible_observations" in header
    assert "spike_count" in header
    assert "spike_rate_of_eligible" in header


@pytest.mark.spark
def test_missing_and_conflicting_places_are_not_assigned_a_location(tmp_path: Path, spark) -> None:
    config = _analysis_config(tmp_path)
    _write_annotated(tmp_path, spark, _fixture_rows(), config)
    report = analyze_dataset(config, spark)
    document = report.document

    rankings = document["station_rankings"]
    missing = _find(rankings, station_id="missing", parameter_name="PM2.5")
    conflict = _find(rankings, station_id="conflict", parameter_name="PM2.5")
    split_name = _find(rankings, station_id="twocity", parameter_name="PM2.5")
    high = _find(rankings, station_id="high", parameter_name="PM2.5")
    low = _find(rankings, station_id="low", parameter_name="PM2.5")
    assert missing["city_name"] is None
    assert missing["state_name"] is None
    assert missing["latitude"] is None
    assert missing["longitude"] is None
    assert missing["city_conflict"] is False
    assert missing["coordinate_conflict"] is False
    assert conflict["city_name"] == "Split"
    assert conflict["latitude"] is None
    assert conflict["longitude"] is None
    assert conflict["coordinate_conflict"] is True
    assert split_name["city_name"] is None
    assert split_name["city_conflict"] is True
    assert split_name["state_name"] == "One"
    assert split_name["consistently_elevated"] is False
    assert high["consistently_elevated"] is True
    assert low["consistently_elevated"] is False
    assert short_is_not_elevated(rankings)
    assert high["elevated_threshold"] == pytest.approx(low["elevated_threshold"])
    assert high["comparison_series"] == 5
    pm10 = _find(rankings, station_id="low", parameter_name="PM10")
    assert pm10["comparison_series"] == 1
    assert pm10["consistently_elevated"] is False

    cities = {row["city_name"] for row in document["city_pollutant"]}
    assert cities == {"Alpha", "Beta", "Gamma", "Split"}
    assert "latitude" not in document["city_pollutant"][0]
    alpha = _find(document["city_pollutant"], city_name="Alpha", parameter_name="PM2.5")
    assert alpha["mean_valid_value"] == pytest.approx(20)
    assert alpha["observations"] == 3
    assert alpha["valid_observations"] == 2
    assert alpha["eligible_observations"] == 2
    assert alpha["stations"] == 1
    gamma = _find(document["city_pollutant"], city_name="Gamma", parameter_name="PM2.5")
    assert gamma["mean_valid_value"] == pytest.approx(400)
    assert gamma["stations"] == 2
    assert gamma["spike_count"] == 2
    split = _find(document["city_pollutant"], city_name="Split", parameter_name="PM2.5")
    assert split["stations_with_coordinates"] == 0

    state_one = _find(document["state_pollutant"], state_name="One", parameter_name="PM2.5")
    assert state_one["observations"] == 7
    assert state_one["valid_observations"] == 6
    assert state_one["eligible_observations"] == 6
    assert state_one["mean_valid_value"] == pytest.approx(15)
    assert state_one["stations"] == 3
    state_two = _find(document["state_pollutant"], state_name="Two", parameter_name="PM2.5")
    assert state_two["mean_valid_value"] == pytest.approx(243.2)
    assert state_two["spike_count"] == 2
    assert state_two["stations"] == 3

    coverage = document["coverage"]
    assert coverage["stations"] == 7
    assert coverage["stations_without_city"] == 1
    assert coverage["stations_with_city_conflict"] == 1
    assert coverage["stations_without_coordinates"] == 1
    assert coverage["stations_with_coordinate_conflict"] == 1
    assert coverage["rows_without_city"] == 1
    assert coverage["rows_without_latitude"] == 1
    assert coverage["rows_in_city_aggregates"] == 12
    assert coverage["rows_in_state_aggregates"] == 14
    assert "not administrative boundaries" in coverage["note"]


@pytest.mark.spark
def test_analytics_output_cannot_replace_raw_or_annotated_files(tmp_path: Path, spark) -> None:
    config = _analysis_config(tmp_path)
    _write_annotated(tmp_path, spark, _fixture_rows()[:1], config)

    with pytest.raises(AnalyticsError, match="Refusing to write analytics"):
        analyze_dataset(
            config,
            spark,
            output_dir=tmp_path / "data" / "raw" / "analytics",
        )


def _analysis_config(root: Path):
    config = make_config(root)
    return replace(
        config,
        analytics=replace(
            config.analytics,
            elevated_minimum_valid_observations=2,
            elevated_percentile=0.9,
            unusual_sigmas=1.0,
        ),
    )


def _fixture_rows() -> list[tuple[object, ...]]:
    return [
        _measurement("low", "PM2.5", datetime(2024, 1, 1, 8), 10, "Alpha", "One", 10.0, 20.0),
        _measurement("low", "PM2.5", datetime(2024, 1, 1, 9), 30, "Alpha", "One", 10.0, 20.0),
        _measurement(
            "low",
            "PM2.5",
            datetime(2024, 1, 1, 10),
            None,
            "Alpha",
            "One",
            10.0,
            20.0,
            valid=False,
        ),
        _measurement("low", "PM10", datetime(2024, 1, 1, 8), 40, "Alpha", "One", 10.0, 20.0, spike=True),
        _measurement("low", "PM10", datetime(2024, 1, 1, 11), 60, "Alpha", "One", 10.0, 20.0),
        _measurement("mid", "PM2.5", datetime(2024, 1, 2, 8), 20, "Beta", "One", 11.0, 21.0),
        _measurement("mid", "PM2.5", datetime(2024, 1, 2, 9), 20, "Beta", "One", 11.0, 21.0),
        _measurement("high", "PM2.5", datetime(2024, 1, 3, 8), 100, "Gamma", "Two", 12.0, 22.0, spike=True),
        _measurement("high", "PM2.5", datetime(2024, 1, 3, 9), 100, "Gamma", "Two", 12.0, 22.0),
        _measurement("short", "PM2.5", datetime(2024, 1, 3, 8), 1000, "Gamma", "Two", 13.0, 23.0, spike=True),
        _measurement("missing", "PM2.5", datetime(2024, 1, 4, 8), 5, None, None, None, None),
        _measurement("conflict", "PM2.5", datetime(2024, 1, 4, 8), 7, "Split", "Two", 1.0, 2.0),
        _measurement("conflict", "PM2.5", datetime(2024, 1, 4, 9), 9, "Split", "Two", 3.0, 4.0),
        _measurement("twocity", "PM2.5", datetime(2024, 1, 5, 12), 4, "Left", "One", 1.0, 1.0),
        _measurement("twocity", "PM2.5", datetime(2024, 1, 5, 13), 6, "Right", "One", 1.0, 1.0),
    ]


def _measurement(
    station_id: str,
    parameter_name: str,
    collected_at: datetime,
    value: float | None,
    city_name: str | None,
    state_name: str | None,
    latitude: float | None,
    longitude: float | None,
    *,
    valid: bool = True,
    spike: bool = False,
) -> tuple[object, ...]:
    return (
        station_id,
        parameter_name,
        collected_at,
        None if value is None else float(value),
        city_name,
        state_name,
        None if latitude is None else float(latitude),
        None if longitude is None else float(longitude),
        valid,
        True,
        12,
        1.0,
        1.0 if valid else None,
        spike if valid else False,
    )


def _write_annotated(root: Path, spark, rows: list[tuple[object, ...]], config) -> Path:
    schema = StructType(
        [
            StructField("station_id", StringType(), True),
            StructField("parameter_name", StringType(), True),
            StructField("collected_at", TimestampNTZType(), True),
            StructField("value", DoubleType(), True),
            StructField("city_name", StringType(), True),
            StructField("state_name", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("is_valid_measurement", BooleanType(), True),
            StructField("ordering_defined", BooleanType(), True),
            StructField("baseline_count", LongType(), True),
            StructField("rolling_stddev", DoubleType(), True),
            StructField("spike_threshold", DoubleType(), True),
            StructField("is_spike", BooleanType(), True),
        ]
    )
    frame = spark.createDataFrame(rows, schema)
    directory = root / "data" / "processed" / "spikes" / "annotated"
    return _write_parquet(frame, directory, config)


def _find(records: list[dict[str, object]], **wanted: object) -> dict[str, object]:
    matches = []
    for record in records:
        if all(_same(record[key], value) for key, value in wanted.items()):
            matches.append(record)
    assert len(matches) == 1, (wanted, records)
    return matches[0]


def _same(actual: object, expected: object) -> bool:
    if expected == "2024-01-01" or (isinstance(expected, str) and expected.startswith("2024-")):
        return str(actual).startswith(expected)
    return actual == expected


def _assert_unusual_flags(records: list[dict[str, object]], sigmas: float) -> None:
    for record in records:
        stddev = record["spike_count_stddev"]
        if stddev is None:
            assert record["unusually_high_spike_count"] is False
            continue
        threshold = record["spike_count_mean"] + sigmas * stddev
        assert record["unusually_high_spike_count"] is (record["spike_count"] > threshold)


def short_is_not_elevated(rankings: list[dict[str, object]]) -> bool:
    short = _find(rankings, station_id="short", parameter_name="PM2.5")
    return short["consistently_elevated"] is False and short["valid_observations"] == 1
