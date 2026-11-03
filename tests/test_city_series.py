"""City-series aggregation tests.

Spark tests use a synthetic annotated file under pytest's temporary directory.
The counts below describe that fixture. They are not January 2024 results.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from pathlib import Path

import pandas as pd
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

from analytics.spatiotemporal.city_series import CitySeriesError, build_city_series
from dashboard.cities import city_rows, pollutant_summaries
from preprocessing.clean import _write_parquet
from tests.config_factory import make_config
from utils.config_loader import load_config
from utils.spark_session import build_spark_session, stop_spark


@pytest.fixture(scope="module")
def spark():
    base = load_config()
    tuned = replace(base, spark=replace(base.spark, ui_enabled=False, log_level="ERROR"))
    session = build_spark_session(tuned, force_new=True)
    yield session
    stop_spark(session)


def test_city_series_refuses_to_replace_existing_analytics(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    source = tmp_path / "data" / "processed" / "spikes" / "annotated" / "data.parquet"
    source.parent.mkdir(parents=True)
    source.write_bytes(b"placeholder")

    with pytest.raises(CitySeriesError, match="Refusing to write city series"):
        build_city_series(config, spark=None, output_dir=tmp_path / "data" / "processed" / "analytics")


@pytest.mark.spark
def test_city_daily_and_hourly_keep_cities_separate(tmp_path: Path, spark) -> None:
    config = make_config(tmp_path)
    source = _write_annotated(tmp_path, spark, _fixture_rows(), config)
    before = source.read_bytes()

    report = build_city_series(config, spark)

    assert source.read_bytes() == before
    assert report.document["protected_outputs_unchanged"] is True
    assert report.document["method"]["spikes_rescored"] is False
    assert report.document["method"]["national_daily_used"] is False
    assert report.document["city_pollutant_match"] is None
    assert report.document["counts"]["stations_without_city"] == 1
    assert report.document["counts"]["stations_with_city_conflict"] == 1
    assert report.document["counts"]["cities"] == 2

    daily = pd.read_parquet(tmp_path / "data" / "processed" / "city_portal" / "city_daily" / "data.parquet")
    hourly = pd.read_parquet(tmp_path / "data" / "processed" / "city_portal" / "city_hourly" / "data.parquet")
    by_hour = pd.read_parquet(tmp_path / "data" / "processed" / "city_portal" / "city_hour_of_day" / "data.parquet")
    assert set(daily["city_name"]) == {"Delhi", "Patna"}
    assert "Left" not in set(daily["city_name"])
    assert daily["city_name"].notna().all()

    delhi_day = city_rows(daily, "Delhi", "Delhi")
    january_first = delhi_day.loc[
        (pd.to_datetime(delhi_day["observation_date"]).dt.date.astype(str) == "2024-01-01")
        & delhi_day["parameter_name"].eq("PM2.5")
    ].iloc[0]
    assert int(january_first["observations"]) == 3
    assert int(january_first["valid_observations"]) == 3
    assert int(january_first["spike_count"]) == 1
    assert int(january_first["eligible_observations"]) == 3
    assert january_first["mean_valid_value"] == pytest.approx(20)
    assert january_first["spike_rate_of_eligible"] == pytest.approx(1 / 3)
    assert int(january_first["stations"]) == 2
    assert int(january_first["stations_with_coordinates"]) == 1

    summaries = pollutant_summaries(delhi_day)
    assert summaries["PM2.5"]["observations"] == 4
    assert summaries["PM2.5"]["spike_count"] == 1
    assert summaries["PM2.5"]["mean_valid_value"] == pytest.approx(25)
    assert summaries["PM10"]["observations"] == 1
    assert summaries["PM10"]["spike_count"] == 1
    assert summaries["PM10"]["mean_valid_value"] == pytest.approx(50)

    patna = city_rows(daily, "Patna", "Bihar")
    assert int(patna["observations"].sum()) == 1
    assert int(patna["spike_count"].sum()) == 1
    assert set(patna["city_name"]) == {"Patna"}

    hour_eight = city_rows(hourly, "Delhi", "Delhi")
    hour_eight = hour_eight.loc[
        hour_eight["parameter_name"].eq("PM2.5") & hour_eight["observation_hour"].eq(8)
        & (pd.to_datetime(hour_eight["observation_date"]).dt.date.astype(str) == "2024-01-01")
    ].iloc[0]
    assert int(hour_eight["observations"]) == 2
    assert hour_eight["mean_valid_value"] == pytest.approx(15)
    assert int(hour_eight["spike_count"]) == 0

    pattern = city_rows(by_hour, "Delhi", "Delhi")
    pattern = pattern.loc[pattern["parameter_name"].eq("PM2.5") & pattern["observation_hour"].eq(8)].iloc[0]
    assert int(pattern["observations"]) == 3
    assert pattern["mean_valid_value"] == pytest.approx(70 / 3)
    assert int(pattern["spike_count"]) == 0
    assert set(city_rows(hourly, "Patna", "Bihar")["city_name"]) == {"Patna"}


def _fixture_rows() -> list[tuple[object, ...]]:
    return [
        _measurement("delhi-a", "PM2.5", datetime(2024, 1, 1, 8), 10, "Delhi", "Delhi", 28.6, 77.2, spike=False),
        _measurement("delhi-a", "PM2.5", datetime(2024, 1, 1, 9), 30, "Delhi", "Delhi", 28.6, 77.2, spike=True),
        _measurement("delhi-b", "PM2.5", datetime(2024, 1, 1, 8), 20, "Delhi", "Delhi", None, None, spike=False),
        _measurement("delhi-a", "PM2.5", datetime(2024, 1, 2, 8), 40, "Delhi", "Delhi", 28.6, 77.2, spike=False),
        _measurement("delhi-a", "PM10", datetime(2024, 1, 1, 8), 50, "Delhi", "Delhi", 28.6, 77.2, spike=True),
        _measurement("patna", "PM2.5", datetime(2024, 1, 1, 8), 100, "Patna", "Bihar", 25.6, 85.1, spike=True),
        _measurement("blank", "PM2.5", datetime(2024, 1, 1, 8), 5, None, None, None, None, spike=False),
        _measurement("conflict", "PM2.5", datetime(2024, 1, 1, 8), 7, "Left", "One", 1.0, 1.0, spike=False),
        _measurement("conflict", "PM2.5", datetime(2024, 1, 1, 9), 9, "Right", "One", 1.0, 1.0, spike=False),
    ]


def _measurement(
    station_id: str,
    parameter_name: str,
    collected_at: datetime,
    value: float,
    city_name: str | None,
    state_name: str | None,
    latitude: float | None,
    longitude: float | None,
    *,
    spike: bool,
) -> tuple[object, ...]:
    return (
        station_id,
        parameter_name,
        collected_at,
        float(value),
        city_name,
        state_name,
        None if latitude is None else float(latitude),
        None if longitude is None else float(longitude),
        True,
        True,
        12,
        1.0,
        1.0,
        spike,
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
    return _write_parquet(frame, root / "data" / "processed" / "spikes" / "annotated", config)
