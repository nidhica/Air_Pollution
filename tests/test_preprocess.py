"""Preprocessing tests.

Spark tests build synthetic Parquet fixtures under pytest's temporary
directory. Those fixtures are not XKDR measurements and are not written to
data/raw. Counts in this module describe the fixtures only.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from preprocessing.clean import (
    PreprocessingError,
    classify_numeric_value,
    locations_conflict,
    preprocess_dataset,
    recover_location,
)
from tests.config_factory import make_config
from utils.config_loader import load_config
from utils.spark_session import build_spark_session, stop_spark

_UNIT = "\u00b5g/m\u00b3"


@pytest.fixture(scope="module")
def spark():
    base = load_config()
    tuned = replace(base, spark=replace(base.spark, ui_enabled=False, log_level="ERROR"))
    session = build_spark_session(tuned, force_new=True)
    yield session
    stop_spark(session)


def test_numeric_status_keeps_zero_and_the_observed_maximum() -> None:
    assert classify_numeric_value(None) == "missing"
    assert classify_numeric_value(float("nan")) == "non_finite"
    assert classify_numeric_value(float("inf")) == "non_finite"
    assert classify_numeric_value(float("-inf")) == "non_finite"
    assert classify_numeric_value(-0.1) == "negative"
    assert classify_numeric_value(0) == "valid"
    assert classify_numeric_value(1000) == "valid"


def test_location_recovery_does_not_invent_or_overwrite() -> None:
    assert recover_location(None, "Delhi", ambiguous=False) == ("Delhi", "station_metadata")
    assert recover_location("  ", None, ambiguous=False) == (None, "missing")
    assert recover_location(None, "Delhi", ambiguous=True) == (None, "missing")
    assert recover_location("Mumbai", "Delhi", ambiguous=False) == ("Mumbai", "measurement")
    assert locations_conflict("Mumbai", "Delhi", ambiguous=False) is True
    assert locations_conflict("Mumbai", "Delhi", ambiguous=True) is False
    assert locations_conflict("Delhi", " Delhi ", ambiguous=False) is False


def test_missing_archive_fails_before_spark(tmp_path: Path) -> None:
    with pytest.raises(PreprocessingError, match="No measurement Parquet"):
        preprocess_dataset(make_config(tmp_path), spark=None)  # type: ignore[arg-type]


def test_missing_stations_file_fails_before_spark(tmp_path: Path) -> None:
    month = tmp_path / "data" / "raw" / "v1" / "measurements" / "year=2024" / "month=01"
    month.mkdir(parents=True)
    (month / "data.parquet").write_bytes(b"not-read")
    parameters = tmp_path / "data" / "raw" / "v1"
    (parameters / "parameters.parquet").write_bytes(b"not-read")

    with pytest.raises(PreprocessingError, match="Stations file not found"):
        preprocess_dataset(make_config(tmp_path), spark=None)  # type: ignore[arg-type]


@pytest.mark.spark
def test_cleaning_flags_invalid_values_and_fills_only_known_locations(tmp_path: Path, spark) -> None:
    paths = _write_quality_fixture(tmp_path)
    before = {path: path.read_bytes() for path in paths}
    report = preprocess_dataset(
        make_config(tmp_path, date_start="2024-01-01", date_end="2024-01-31"),
        spark,
    )
    document = report.document
    counts = document["counts"]

    assert report.output_dir == (tmp_path / "data" / "processed" / "measurements").resolve()
    assert counts["rows_in_downloaded_files"] == 16
    assert counts["rows_matching_source_and_pollutants"] == 14
    assert counts["rows_outside_date_window"] == 1
    assert counts["rows_removed"] == 2
    assert counts["rows_removed_by_reason"]["null_or_blank_station_id"] == 1
    assert counts["rows_written"] == 11
    assert counts["rows_read_back"] == 11
    assert document["invalid_measurements"] == {
        "missing": 1,
        "non_numeric": 0,
        "non_finite": 2,
        "negative": 1,
        "valid": 7,
        "retained_in_output": True,
    }
    assert document["duplicates"]["exact_duplicate_extras_removed"] == 1
    assert document["duplicates"]["key_conflict_rows_kept"] == 2
    location = document["location_enrichment"]
    assert location["city_filled_from_station"] == 1
    assert location["state_filled_from_station"] == 2
    assert location["city_still_missing"] == 2
    assert location["state_still_missing"] == 2
    assert location["location_conflicts"] == 1
    assert location["ambiguous_station_rows"] == 1
    assert location["rows_without_station_record"] == 0
    assert location["city_unrecoverable_because_station_value_is_null"] == 1
    assert location["station_name_mismatches"] == 1
    assert location["latitude_non_null"] == 9
    assert document["units"]["counts"] == {"match": 10, "unexpected": 1}
    assert document["pollutant_counts"] == {"PM10": 3, "PM2.5": 8}
    assert document["value_summary"]["min"] == -5.0
    assert document["value_summary"]["max"] == 40.0
    assert document["raw_files_modified"] is False
    assert {path: path.read_bytes() for path in paths} == before

    cleaned = spark.read.parquet(str(report.output_file))
    filled = cleaned.where(F_city_source(cleaned, "station_metadata") & (cleaned.parameter_name == "PM2.5") & (cleaned.value == 12.0)).collect()
    assert len(filled) == 1
    assert filled[0]["city_name"] == "Delhi"
    assert filled[0]["state_name"] == "Delhi"
    assert filled[0]["latitude"] == pytest.approx(28.6)
    conflict = cleaned.where(cleaned.location_conflict).collect()
    assert len(conflict) == 1
    assert conflict[0]["city_name"] == "Old Name"
    assert conflict[0]["state_name"] == "Goa"
    unresolved = cleaned.where(cleaned.station_id == "s4").collect()
    assert len(unresolved) == 1
    assert unresolved[0]["city_name"] is None
    assert unresolved[0]["latitude"] is None
    negative = cleaned.where(cleaned.measurement_status == "negative").collect()
    assert len(negative) == 1
    assert negative[0]["value"] == -5.0


@pytest.mark.spark
def test_source_and_pollutant_filters_are_configurable(tmp_path: Path, spark) -> None:
    _write_quality_fixture(tmp_path)
    config = make_config(tmp_path, date_start="2024-01-01", date_end="2024-01-31")
    pm10 = preprocess_dataset(
        replace(config, dataset=replace(config.dataset, pollutants=("PM10",))),
        spark,
        report_path=tmp_path / "pm10.json",
        output_dir=tmp_path / "pm10",
    )
    assert pm10.document["counts"]["rows_written"] == 3
    assert pm10.document["pollutant_counts"] == {"PM10": 3}
    assert pm10.document["duplicates"]["key_conflict_rows_kept"] == 2

    embassy = preprocess_dataset(
        replace(config, dataset=replace(config.dataset, source_filter="us_embassy")),
        spark,
        report_path=tmp_path / "embassy.json",
        output_dir=tmp_path / "embassy",
    )
    assert embassy.document["counts"]["rows_written"] == 1
    assert embassy.document["location_enrichment"]["rows_without_station_record"] == 1
    assert embassy.document["location_enrichment"]["city_measurement_value_kept"] == 1


@pytest.mark.spark
def test_non_numeric_text_is_flagged_and_kept(tmp_path: Path, spark) -> None:
    _write_string_values(tmp_path)
    report = preprocess_dataset(make_config(tmp_path), spark)
    document = report.document

    assert document["counts"]["rows_removed"] == 0
    assert document["counts"]["rows_written"] == 4
    assert document["invalid_measurements"]["non_numeric"] == 1
    assert document["invalid_measurements"]["missing"] == 1
    assert document["invalid_measurements"]["negative"] == 1
    assert document["invalid_measurements"]["valid"] == 1
    rows = {row["value_text"]: row for row in spark.read.parquet(str(report.output_file)).collect()}
    assert rows["abc"]["value"] is None
    assert rows["abc"]["measurement_status"] == "non_numeric"
    kept = spark.read.parquet(str(report.output_file)).where("measurement_status = 'valid'").collect()
    assert kept[0]["value"] == 12.5


@pytest.mark.spark
def test_unparseable_timestamps_are_counted_and_removed(tmp_path: Path, spark) -> None:
    _write_string_timestamps(tmp_path)
    report = preprocess_dataset(make_config(tmp_path), spark)
    document = report.document

    assert document["timestamp"]["input_type"] == "string"
    assert document["timestamp"]["unparseable_rows"] == 1
    assert document["timestamp"]["null_rows"] == 1
    assert document["counts"]["rows_removed_by_reason"]["null_or_unparseable_timestamp"] == 2
    assert document["counts"]["rows_written"] == 1
    remaining = spark.read.parquet(str(report.output_file)).collect()
    assert len(remaining) == 1
    assert str(remaining[0]["collected_at"]).startswith("2024-01-01 08:00:00")


def F_city_source(frame, source: str):
    return frame.city_name_source == source


def _write_quality_fixture(root: Path) -> list[Path]:
    collected_at = [
        datetime(2024, 1, 1, 8, 0),
        datetime(2024, 1, 1, 8, 0),
        datetime(2024, 1, 1, 9, 0),
        datetime(2024, 1, 1, 8, 0),
        datetime(2024, 1, 1, 8, 0),
        datetime(2024, 1, 1, 8, 0),
        datetime(2024, 1, 2, 8, 0),
        datetime(2024, 1, 2, 8, 0),
        datetime(2024, 1, 3, 8, 0),
        datetime(2024, 1, 1, 10, 0),
        datetime(2024, 1, 1, 8, 0),
        datetime(2024, 1, 1, 8, 0),
        datetime(2024, 2, 1, 8, 0),
        datetime(2024, 1, 1, 11, 0),
        datetime(2024, 1, 4, 8, 0),
        datetime(2024, 1, 4, 9, 0),
    ]
    table = pa.table(
        {
            "station_id": ["s1", "s1", "s1", "s1", "s2", "s3", "s1", "s1", "s1", None, "s1", "s9", "s1", "s4", "s1", "s1"],
            "station_name": ["Site A", "Site A", "Site A", "Site A", "Site B", "Wrong", "Site A", "Site A", "Site A", "Site A", "Site A", "Embassy", "Site A", "Site D", "Site A", "Site A"],
            "state_name": [None, None, "Delhi", None, None, "Goa", "Delhi", "Delhi", "Delhi", "Delhi", "Delhi", "Delhi", "Delhi", None, "Delhi", "Delhi"],
            "city_name": [None, None, "Delhi", "Delhi", None, "Old Name", "Delhi", "Delhi", "Delhi", "Delhi", "Delhi", "Embassy City", "Delhi", None, "Delhi", "Delhi"],
            "parameter_name": ["PM2.5", "PM2.5", "PM2.5", "PM10", "PM2.5", "PM2.5", "PM10", "PM10", "PM2.5", "PM2.5", "Ozone", "PM2.5", "PM2.5", "PM2.5", "PM2.5", "PM2.5"],
            "unit": [_UNIT, _UNIT, _UNIT, _UNIT, _UNIT, _UNIT, _UNIT, _UNIT, "ppb", _UNIT, _UNIT, _UNIT, _UNIT, _UNIT, _UNIT, _UNIT],
            "collected_at": pa.array(collected_at, type=pa.timestamp("us")),
            "value": pa.array(
                [12.0, 12.0, -5.0, None, 40.0, 7.0, 3.0, 4.0, 8.0, 1.0, 5.0, 1.0, 6.0, 9.0, float("inf"), float("nan")],
                type=pa.float64(),
            ),
            "source": ["cpcb_caaqm"] * 11 + ["us_embassy"] + ["cpcb_caaqm"] * 4,
        }
    )
    return _write_archive(root, table)


def _write_string_values(root: Path) -> None:
    table = pa.table(
        {
            "station_id": ["s1", "s1", "s1", "s1"],
            "parameter_name": ["PM2.5", "PM2.5", "PM2.5", "PM2.5"],
            "collected_at": pa.array([datetime(2024, 1, 1, 8, hour) for hour in range(4)], type=pa.timestamp("us")),
            "value": ["12.5", "abc", "  ", "-2"],
            "city_name": ["Delhi", "Delhi", "Delhi", "Delhi"],
            "source": ["cpcb_caaqm", "cpcb_caaqm", "cpcb_caaqm", "cpcb_caaqm"],
        }
    )
    _write_archive(
        root,
        table,
        stations=pa.table(
            {
                "station_id": ["s1"],
                "city_name": ["Delhi"],
                "source": ["cpcb_caaqm"],
            }
        ),
        parameters=pa.table({"parameter_name": ["PM2.5"], "unit": [_UNIT]}),
    )


def _write_string_timestamps(root: Path) -> None:
    table = pa.table(
        {
            "station_id": ["s1", "s1", "s1"],
            "parameter_name": ["PM2.5", "PM2.5", "PM2.5"],
            "collected_at": ["2024-01-01 08:00:00", "not-a-time", None],
            "value": [1.0, 2.0, 3.0],
            "city_name": ["Delhi", "Delhi", "Delhi"],
            "source": ["cpcb_caaqm", "cpcb_caaqm", "cpcb_caaqm"],
        }
    )
    _write_archive(root, table)


def _write_archive(
    root: Path,
    measurements: pa.Table,
    *,
    stations: pa.Table | None = None,
    parameters: pa.Table | None = None,
) -> list[Path]:
    month = root / "data" / "raw" / "v1" / "measurements" / "year=2024" / "month=01"
    month.mkdir(parents=True)
    measurement_path = month / "data.parquet"
    pq.write_table(measurements, measurement_path)
    metadata = root / "data" / "raw" / "v1"
    station_path = metadata / "stations.parquet"
    parameter_path = metadata / "parameters.parquet"
    pq.write_table(stations if stations is not None else _default_stations(), station_path)
    pq.write_table(parameters if parameters is not None else _default_parameters(), parameter_path)
    return [measurement_path, station_path, parameter_path]


def _default_stations() -> pa.Table:
    return pa.table(
        {
            "station_id": ["s1", "s2", "s3", "s4", "s4"],
            "station_name": ["Site A", "Site B", "Site C", "Site D", "Site D"],
            "state_name": ["Delhi", None, "Goa", "StateOne", "StateTwo"],
            "city_name": ["Delhi", None, "Panaji", "CityOne", "CityTwo"],
            "source": ["cpcb_caaqm", "cpcb_caaqm", "cpcb_caaqm", "cpcb_caaqm", "cpcb_caaqm"],
            "latitude": [28.6, None, 15.5, 1.0, 3.0],
            "longitude": [77.2, None, 73.8, 2.0, 4.0],
        }
    )


def _default_parameters() -> pa.Table:
    return pa.table(
        {
            "parameter_name": ["PM2.5", "PM10", "Ozone"],
            "unit": [_UNIT, _UNIT, "ppb"],
        }
    )
