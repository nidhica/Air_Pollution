"""Spark cleaning tests for data.gov.in snapshots.

Fixtures are temporary. They are not XKDR measurements and they are not
written to the repository data directory.
"""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from analytics.data_gov import DataGovPrepareError, prepare_data_gov
from ingestion.data_gov import normalized_records_file
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


@pytest.mark.spark
def test_prepare_parses_values_without_scoring_spikes_or_changing_xkdr(tmp_path: Path, spark) -> None:
    config = make_config(tmp_path)
    sentinel = config.paths.processed_dir / "measurements" / "data.parquet"
    sentinel.parent.mkdir(parents=True)
    sentinel.write_bytes(b"xkdr-processed")
    rows = [
        _row("17-11-2026 17:00:00", "55", "28.6508", "77.3152"),
        _row("17-11-2026 18:00:00", "70", "28.6508", "77.3152"),
        _row("not-a-time", "NA", "999", "77.3152"),
        _row("17-11-2026 17:00:00", "40", None, None, station="No coordinates"),
    ]
    destination = normalized_records_file(config)
    destination.parent.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist(rows), destination)
    report = prepare_data_gov(config, spark)
    assert report["spike_algorithm_applied"] is False
    assert report["aqi_field_created"] is False
    assert report["missing_values_filled_with_zero"] is False
    assert report["reported_unit"] == "not documented by the API"
    assert report["xkdr_files_modified"] is False
    assert report["counts"]["records"] == 4
    assert report["counts"]["parsed_timestamps"] == 3
    assert report["counts"]["unparsed_timestamps"] == 1
    assert report["counts"]["distinct_timestamps"] == 2
    observations = pq.read_table(config.paths.processed_dir / "data_gov_in" / "observations" / "data.parquet")
    assert "aqi" not in observations.column_names
    assert "is_spike" not in observations.column_names
    parsed = observations.to_pylist()
    good = next(row for row in parsed if row["last_update_text"] == "17-11-2026 18:00:00")
    assert good["avg_value"] == 70
    assert good["reported_unit"] == "not documented by the API"
    assert good["coordinate_status"] == "valid"
    broken = next(row for row in parsed if row["last_update_text"] == "not-a-time")
    assert broken["timestamp_status"] == "unparsed"
    assert broken["observed_at"] is None
    assert broken["avg_value"] is None
    assert broken["coordinate_status"] == "out of range"
    missing = next(row for row in parsed if row["station"] == "No coordinates")
    assert missing["coordinate_status"] == "missing"
    assert missing["latitude"] is None
    coverage = pq.read_table(config.paths.processed_dir / "data_gov_in" / "series_coverage" / "data.parquet").to_pylist()
    tracked = next(row for row in coverage if row["station"] == "Anand Vihar, Delhi - DPCC")
    assert tracked["timestamp_count"] == 2
    assert sentinel.read_bytes() == b"xkdr-processed"


def test_prepare_requires_the_normalized_file(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    with pytest.raises(DataGovPrepareError, match="fetch-data-gov"):
        prepare_data_gov(config, spark=None)


def _row(
    last_update: str,
    avg_value: str,
    latitude: str | None,
    longitude: str | None,
    station: str = "Anand Vihar, Delhi - DPCC",
) -> dict[str, str | None]:
    return {
        "country": "India",
        "state": "Delhi",
        "city": "Delhi",
        "station": station,
        "last_update": last_update,
        "latitude": latitude,
        "longitude": longitude,
        "pollutant_id": "PM2.5",
        "min_value": "40",
        "max_value": "80",
        "avg_value": avg_value,
        "retrieved_at": "2026-11-17T12:00:00Z",
    }
