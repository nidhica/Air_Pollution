"""Dashboard loading and filter tests.

Filter tests use synthetic tables. The repository test reads the saved
January analytics outputs when those files are present. Neither test writes
to data/raw or data/processed.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dashboard.filters import (
    comparison_columns,
    filter_date_range,
    filter_place,
    filter_pollutant,
    located_stations,
    period_summary,
    pool_hours,
    unlocated_stations,
)
from dashboard.loading import DashboardDataError, load_bundle, load_report

PROJECT_ROOT = Path(__file__).resolve().parents[1]
_REPORT = PROJECT_ROOT / "data" / "interim" / "analytics_report.json"


def test_filters_keep_pollutant_date_and_place_rules() -> None:
    frame = pd.DataFrame(
        {
            "station_id": ["s1", "s1", "s2", "s3"],
            "parameter_name": ["PM2.5", "PM10", "PM2.5", "PM2.5"],
            "observation_date": pd.to_datetime(["2024-01-01", "2024-01-01", "2024-01-02", "2024-01-31"]),
            "city_name": ["Delhi", "Delhi", None, "Patna"],
            "state_name": ["Delhi", "Delhi", None, "Bihar"],
            "latitude": [28.6, 28.6, None, 25.6],
            "longitude": [77.2, 77.2, None, 85.1],
            "observations": [2, 2, 1, 4],
            "valid_observations": [1, 3, 1, 4],
            "mean_valid_value": [10.0, 30.0, 5.0, 8.0],
            "spike_count": [1, 1, 0, 2],
            "eligible_observations": [2, 2, 0, 4],
        }
    )

    pm25 = filter_pollutant(frame, "PM2.5")
    assert set(pm25["parameter_name"]) == {"PM2.5"}
    assert len(pm25) == 3

    selected = filter_date_range(frame, "observation_date", "2024-01-01", "2024-01-02")
    assert set(pd.to_datetime(selected["observation_date"]).dt.date.astype(str)) == {"2024-01-01", "2024-01-02"}

    delhi = filter_place(frame, city="Delhi")
    assert set(delhi["station_id"]) == {"s1"}
    assert delhi["city_name"].notna().all()

    missing = unlocated_stations(filter_pollutant(frame, "PM2.5"))
    assert list(missing["station_id"]) == ["s2"]
    assert missing["city_name"].isna().all()
    assert missing["latitude"].isna().all()

    located = located_stations(frame)
    assert set(located["station_id"]) == {"s1", "s3"}
    assert located["latitude"].notna().all()
    assert located["longitude"].notna().all()

    summary = period_summary(filter_pollutant(frame, "PM2.5").iloc[:1])
    assert summary["mean_valid_value"] == pytest.approx(10)
    mixed = period_summary(filter_pollutant(frame, "PM2.5").iloc[[0]]._append(filter_pollutant(frame, "PM10").iloc[:1]))
    assert mixed["valid_observations"] == 4
    assert mixed["mean_valid_value"] == pytest.approx((10 * 1 + 30 * 3) / 4)
    assert mixed["spike_rate_of_eligible"] == pytest.approx(0.5)

    hours = frame.rename(columns={"observation_date": "observation_hour"}).copy()
    hours["observation_hour"] = [8, 8, 9, 8]
    pooled = pool_hours(filter_pollutant(hours, "PM2.5"))
    hour_eight = pooled.loc[pooled["observation_hour"].eq(8)].iloc[0]
    assert hour_eight["observations"] == 6
    assert hour_eight["spike_count"] == 3
    assert comparison_columns("PM2.5")[2] == "PM2_5_spike_count"


def test_loader_reads_a_fixture_and_does_not_replace_missing_places(tmp_path: Path) -> None:
    _write_fixture(tmp_path)
    report_path = tmp_path / "data" / "interim" / "analytics_report.json"
    before = report_path.read_bytes()

    bundle = load_bundle(tmp_path)

    assert report_path.read_bytes() == before
    assert bundle.report["counts"]["total_records"] == 4
    assert bundle.annotated_rows == 4
    assert bundle.spike_rows == 1
    assert len(bundle.spike_events) == 1
    assert list(bundle.spike_events.columns) == [
        "station_id",
        "parameter_name",
        "collected_at",
        "value",
        "unit",
        "city_name",
        "state_name",
        "latitude",
        "longitude",
        "baseline_count",
        "rolling_mean",
        "rolling_stddev",
        "spike_threshold",
    ]
    missing = unlocated_stations(bundle.stations)
    assert list(missing["station_id"]) == ["blank"]
    assert missing.iloc[0]["city_name"] is None or pd.isna(missing.iloc[0]["city_name"])
    assert located_stations(bundle.stations)["station_id"].tolist() == ["known"]


def test_missing_report_fails_before_a_table_is_read(tmp_path: Path) -> None:
    with pytest.raises(DashboardDataError, match="Analytics report not found"):
        load_report(tmp_path / "data" / "interim" / "analytics_report.json")


@pytest.mark.skipif(not _REPORT.is_file(), reason="January analytics outputs are not on disk")
def test_repository_bundle_matches_january_results() -> None:
    report_bytes = _REPORT.read_bytes()
    bundle = load_bundle(PROJECT_ROOT)

    assert _REPORT.read_bytes() == report_bytes
    counts = bundle.report["counts"]
    assert counts["total_records"] == 667280
    assert counts["valid_measurements"] == 667280
    assert counts["eligible_records"] == 655383
    assert counts["spike_records"] == 16058
    assert bundle.annotated_rows == 667280
    assert bundle.spike_rows == 16058
    assert len(bundle.daily) == 62
    assert len(bundle.hourly) == 1488
    assert len(bundle.stations) == 993
    assert bundle.stations["latitude"].isna().sum() == bundle.stations.loc[
        bundle.stations["latitude"].isna(), "station_id"
    ].shape[0]
    missing_ids = set(unlocated_stations(bundle.stations)["station_id"])
    assert len(missing_ids) == 45
    assert unlocated_stations(bundle.stations)["city_name"].isna().all()
    assert located_stations(bundle.stations)["latitude"].notna().all()
    totals = {row["parameter_name"]: row for row in bundle.report["pollutant_totals"]}
    assert totals["PM2.5"]["spike_count"] == 7787
    assert totals["PM10"]["spike_count"] == 8271
    assert totals["PM2.5"]["mean_valid_value"] == pytest.approx(84.94324110177112)
    assert totals["PM10"]["mean_valid_value"] == pytest.approx(155.4562315110614)


def _write_fixture(root: Path) -> None:
    report = {
        "generated_at": "2026-11-02T21:41:50+05:30",
        "timezone": "Asia/Kolkata",
        "denominators": {"geography": "not administrative boundaries"},
        "parameters": {
            "minimum_baseline": 12,
            "elevated_minimum_valid_observations": 168,
            "elevated_percentile": 0.9,
            "unusual_sigmas": 2,
            "eligible_rule": "non-null spike_threshold",
        },
        "counts": {
            "total_records": 4,
            "valid_measurements": 4,
            "eligible_records": 3,
            "spike_records": 1,
            "null_timestamps": 0,
            "spike_rate_of_observations": 0.25,
            "spike_rate_of_eligible": 1 / 3,
        },
        "coverage": {
            "stations": 2,
            "stations_without_city": 1,
            "stations_without_state": 1,
            "stations_without_coordinates": 1,
            "stations_with_coordinates": 1,
            "stations_with_city_conflict": 0,
            "stations_with_coordinate_conflict": 0,
            "rows_without_city": 1,
            "rows_without_state": 1,
            "rows_without_latitude": 1,
            "rows_without_longitude": 1,
            "eligible_rows_without_city": 1,
            "eligible_rows_without_coordinates": 1,
            "rows_in_city_aggregates": 3,
            "distinct_city_names": 1,
            "distinct_state_names": 1,
            "note": "Missing coordinates stay missing.",
        },
        "pollutant_totals": [
            {
                "parameter_name": "PM2.5",
                "observations": 4,
                "valid_observations": 4,
                "mean_valid_value": 10,
                "spike_count": 1,
                "eligible_observations": 3,
                "spike_rate_of_observations": 0.25,
                "spike_rate_of_eligible": 1 / 3,
            }
        ],
    }
    report_path = root / "data" / "interim" / "analytics_report.json"
    report_path.parent.mkdir(parents=True)
    report_path.write_text(json.dumps(report), encoding="utf-8")

    stations = pa.table(
        {
            "station_id": ["known", "blank"],
            "parameter_name": ["PM2.5", "PM2.5"],
            "city_name": ["Delhi", None],
            "state_name": ["Delhi", None],
            "latitude": [28.6, None],
            "longitude": [77.2, None],
            "city_conflict": [False, False],
            "state_conflict": [False, False],
            "coordinate_conflict": [False, False],
            "observations": [3, 1],
            "valid_observations": [3, 1],
            "mean_valid_value": [12.0, 4.0],
            "spike_count": [1, 0],
            "eligible_observations": [2, 1],
            "spike_rate_of_observations": [1 / 3, 0.0],
            "spike_rate_of_eligible": [0.5, 0.0],
            "spike_count_rank": [1, 2],
            "spike_rate_rank": [1, 2],
            "comparison_series": [2, 2],
            "elevated_threshold": [10.0, 10.0],
            "consistently_elevated": [True, False],
        }
    )
    _write_parquet(root / "data" / "processed" / "analytics" / "station_pollutant" / "data.parquet", stations)
    _write_parquet(root / "data" / "processed" / "analytics" / "elevated_stations" / "data.parquet", stations.slice(0, 1))
    daily = pa.table(
        {
            "observation_date": [pd.Timestamp("2024-01-01").date()],
            "parameter_name": ["PM2.5"],
            "observations": [4],
            "valid_observations": [4],
            "mean_valid_value": [10.0],
            "spike_count": [1],
            "eligible_observations": [3],
            "spike_rate_of_observations": [0.25],
            "spike_rate_of_eligible": [1 / 3],
            "unusually_high_spike_count": [False],
            "unusually_high_eligible_rate": [True],
        }
    )
    _write_parquet(root / "data" / "processed" / "analytics" / "daily_by_pollutant" / "data.parquet", daily)
    hourly = daily.append_column("observation_hour", pa.array([8]))
    _write_parquet(root / "data" / "processed" / "analytics" / "hourly_by_pollutant" / "data.parquet", hourly)
    hour_of_day = hourly.drop_columns(["observation_date", "unusually_high_spike_count", "unusually_high_eligible_rate"])
    _write_parquet(root / "data" / "processed" / "analytics" / "hour_of_day" / "data.parquet", hour_of_day)
    comparison = pa.table(
        {
            "station_id": ["known"],
            "city_name": ["Delhi"],
            "state_name": ["Delhi"],
            "latitude": [28.6],
            "longitude": [77.2],
            "PM2_5_mean_valid_value": [12.0],
            "PM2_5_valid_observations": [3],
            "PM2_5_spike_count": [1],
            "PM2_5_eligible_observations": [2],
            "PM2_5_spike_rate_of_eligible": [0.5],
        }
    )
    _write_parquet(root / "data" / "processed" / "analytics" / "station_comparison" / "data.parquet", comparison)
    cities = pa.table(
        {
            "city_name": ["Delhi"],
            "state_name": ["Delhi"],
            "parameter_name": ["PM2.5"],
            "stations": [1],
            "stations_with_coordinates": [1],
            "observations": [3],
            "valid_observations": [3],
            "mean_valid_value": [12.0],
            "spike_count": [1],
            "eligible_observations": [2],
            "spike_rate_of_observations": [1 / 3],
            "spike_rate_of_eligible": [0.5],
        }
    )
    _write_parquet(root / "data" / "processed" / "analytics" / "city_pollutant" / "data.parquet", cities)
    states = cities.drop_columns(["city_name"])
    _write_parquet(root / "data" / "processed" / "analytics" / "state_pollutant" / "data.parquet", states)
    events = pa.table(
        {
            "station_id": ["known"],
            "parameter_name": ["PM2.5"],
            "collected_at": [pd.Timestamp("2024-01-01 08:00:00")],
            "value": [40.0],
            "unit": ["µg/m³"],
            "city_name": ["Delhi"],
            "state_name": ["Delhi"],
            "latitude": [28.6],
            "longitude": [77.2],
            "baseline_count": [12],
            "rolling_mean": [10.0],
            "rolling_stddev": [1.0],
            "spike_threshold": [13.0],
        }
    )
    _write_parquet(root / "data" / "processed" / "spikes" / "spikes_only" / "data.parquet", events)
    annotated = pa.table({"station_id": ["known", "known", "known", "blank"]})
    _write_parquet(root / "data" / "processed" / "spikes" / "annotated" / "data.parquet", annotated)


def _write_parquet(path: Path, table: pa.Table) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)
