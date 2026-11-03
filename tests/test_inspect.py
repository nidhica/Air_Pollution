"""Inspection tests.

The Parquet files here are synthetic fixtures under pytest's temporary
directory. They are not XKDR measurements and are not written to data/raw.
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from dataclasses import replace

from ingestion.inspect import InspectionError, inspect_dataset
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


def test_missing_archive_fails_before_spark(tmp_path: Path) -> None:
    with pytest.raises(InspectionError, match="No measurement Parquet"):
        inspect_dataset(make_config(tmp_path), spark=None)  # type: ignore[arg-type]


@pytest.mark.spark
def test_selection_counts_come_from_the_fixture(tmp_path: Path, spark) -> None:
    _write_fixture(tmp_path)
    report = inspect_dataset(make_config(tmp_path), spark)
    document = report.document

    assert report.path == tmp_path / "data" / "interim" / "inspection_report.json"
    assert report.path.is_file()
    assert document["measurement_row_count"] == 5
    assert document["selected_row_count"] == 3
    assert document["station_count"] == 1
    assert document["pollutant_counts"] == {"PM10": 1, "PM2.5": 2}
    assert document["missing_values"]["value"] == 1
    assert document["extra_exact_duplicate_rows"] == 1
    assert document["extra_key_duplicate_rows"] == 1
    assert document["distinct_sources"] == ["cpcb_caaqm"]
    assert document["distinct_parameters"] == ["PM10", "PM2.5"]
    assert document["selection_ok"] is True
    assert document["date_min"].startswith("2024-01-01")
    assert document["date_max"].startswith("2024-01-02")
    assert {column["name"] for column in document["columns"]} >= {
        "station_id",
        "parameter_name",
        "collected_at",
        "value",
        "city_name",
        "source",
        "year",
        "month",
    }
    assert document["stations_file"]["row_count"] == 2
    assert document["stations_file"]["distinct_sources"] == ["cpcb_caaqm", "us_embassy"]
    assert "distinct_sources" not in document["parameters_file"]


@pytest.mark.spark
def test_date_window_limits_the_selection(tmp_path: Path, spark) -> None:
    _write_fixture(tmp_path)
    report = inspect_dataset(
        make_config(tmp_path, date_start="2024-01-02", date_end="2024-01-02"),
        spark,
    )
    document = report.document
    assert document["selected_row_count"] == 1
    assert document["pollutant_counts"] == {"PM10": 1}
    assert document["pollutants_absent"] == ["PM2.5"]
    assert document["extra_exact_duplicate_rows"] == 0
    assert document["selection_ok"] is True


@pytest.mark.spark
def test_missing_configured_column_does_not_write_a_report(tmp_path: Path, spark) -> None:
    month = tmp_path / "data" / "raw" / "v1" / "measurements" / "year=2024" / "month=01"
    month.mkdir(parents=True)
    pq.write_table(pa.table({"station_id": ["s1"], "value": [1.0]}), month / "data.parquet")

    with pytest.raises(InspectionError, match="missing configured columns"):
        inspect_dataset(make_config(tmp_path), spark)
    assert not (tmp_path / "data" / "interim" / "inspection_report.json").exists()


def _write_fixture(root: Path) -> None:
    month = root / "data" / "raw" / "v1" / "measurements" / "year=2024" / "month=01"
    month.mkdir(parents=True)
    collected_at = pa.array(
        [
            datetime(2024, 1, 1, 8, 0),
            datetime(2024, 1, 1, 8, 0),
            datetime(2024, 1, 2, 8, 0),
            datetime(2024, 1, 1, 8, 0),
            datetime(2024, 1, 3, 8, 0),
        ],
        type=pa.timestamp("us"),
    )
    pq.write_table(
        pa.table(
            {
                "station_id": ["s1", "s1", "s1", "s2", "s1"],
                "parameter_name": ["PM2.5", "PM2.5", "PM10", "PM2.5", "NO2"],
                "collected_at": collected_at,
                "value": [10.0, 10.0, None, 30.0, 5.0],
                "city_name": ["Delhi", "Delhi", "Delhi", "Mumbai", "Delhi"],
                "source": ["cpcb_caaqm", "cpcb_caaqm", "cpcb_caaqm", "us_embassy", "cpcb_caaqm"],
            }
        ),
        month / "data.parquet",
    )
    metadata = root / "data" / "raw" / "v1"
    pq.write_table(
        pa.table({"station_id": ["s1", "s2"], "source": ["cpcb_caaqm", "us_embassy"]}),
        metadata / "stations.parquet",
    )
    pq.write_table(
        pa.table({"parameter_name": ["PM2.5", "PM10", "NO2"]}),
        metadata / "parameters.parquet",
    )
