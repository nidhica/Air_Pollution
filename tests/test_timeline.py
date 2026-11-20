"""Timeline coverage, filtering, and unit-separation tests.

These tests use synthetic frames. They do not call data.gov.in and they do
not start Spark.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from dashboard.figures import snapshot_trend, time_series
from dashboard.timeline import (
    IncompatibleMeasurements,
    assert_same_measurement_scale,
    assess_range,
    filter_choices,
    gov_observations,
    gov_summary,
    load_timeline,
    make_catalog,
    prepared_fingerprint,
    preset_bounds,
    sources_can_share_scale,
    xkdr_daily_series,
    xkdr_hour_pattern,
    xkdr_spike_events,
    xkdr_station_ranking,
    xkdr_summary,
)


def test_presets_are_inclusive_and_custom_has_no_implied_dates() -> None:
    today = date(2026, 11, 17)
    assert preset_bounds("Last 7 days", today) == (date(2026, 11, 11), today)
    assert preset_bounds("Last 30 days", today) == (date(2026, 10, 19), today)
    assert preset_bounds("Last 90 days", today) == (date(2026, 8, 20), today)
    assert preset_bounds("Last 1 year", today) == (date(2025, 11, 17), today)
    assert preset_bounds("Custom", today) is None


def test_xkdr_dates_keep_the_selected_days_only() -> None:
    catalog = _catalog()
    series = xkdr_daily_series(catalog, "2025-08-01", "2025-08-02")
    assert set(pd.to_datetime(series["observation_date"]).dt.date) == {date(2025, 8, 1), date(2025, 8, 2)}
    assert int(series["observations"].sum()) == 4
    hours = xkdr_hour_pattern(catalog, "2025-08-01", "2025-08-02")
    assert list(hours["observation_hour"]) == [8]


def test_range_covered_only_by_xkdr_leaves_data_gov_empty() -> None:
    xkdr, gov = assess_range(_catalog(), "2025-08-01", "2025-08-31")
    assert xkdr.status == "partial"
    assert xkdr.days_with_observations == 2
    assert xkdr.days_selected == 31
    assert xkdr.observations == 4
    exact, _ = assess_range(_catalog(), "2025-08-01", "2025-08-02")
    assert exact.status == "full"
    assert gov.status == "none"
    assert gov.observations == 0
    assert gov_observations(_catalog(), "2025-08-01", "2025-08-31").empty


def test_range_covered_only_by_data_gov_leaves_xkdr_empty() -> None:
    xkdr, gov = assess_range(_catalog(), "2026-11-17", "2026-11-17")
    assert xkdr.status == "none"
    assert gov.status == "full"
    assert gov.observations == 3
    assert xkdr_daily_series(_catalog(), "2026-11-17", "2026-11-17").empty


def test_range_with_both_sources_keeps_each_observation_set() -> None:
    catalog = _catalog()
    xkdr, gov = assess_range(catalog, "2025-08-01", "2026-11-17")
    assert xkdr.status == "partial"
    assert gov.status == "partial"
    assert xkdr.observations == 4
    assert gov.observations == 3
    assert not sources_can_share_scale(xkdr, gov)


def test_empty_range_reports_the_stored_dates_without_rows() -> None:
    xkdr, gov = assess_range(_catalog(), "2024-01-01", "2024-01-31")
    assert xkdr.status == "none"
    assert gov.status == "none"
    assert xkdr.available_start == pd.Timestamp("2025-08-01")
    assert gov.available_start == pd.Timestamp("2026-11-17")
    assert xkdr_summary(_catalog(), "2024-01-01", "2024-01-31")["observations"] == 0


def test_partial_range_counts_only_the_days_that_exist() -> None:
    xkdr, gov = assess_range(_catalog(), "2025-08-02", "2025-08-10")
    assert xkdr.status == "partial"
    assert xkdr.days_selected == 9
    assert xkdr.days_with_observations == 1
    assert xkdr.overlap_start == pd.Timestamp("2025-08-02")
    assert gov.status == "none"


def test_different_units_cannot_be_averaged() -> None:
    with pytest.raises(IncompatibleMeasurements):
        assert_same_measurement_scale(
            "µg/m³",
            "not documented by the API",
            "concentration",
            "api-reported pollutant value",
        )
    xkdr, gov = assess_range(_catalog(), "2025-08-01", "2026-11-17")
    assert sources_can_share_scale(xkdr, gov) is False


def test_place_filters_follow_the_selected_dates() -> None:
    august = filter_choices(_catalog(), "2025-08-01", "2025-08-31")
    assert august["city"] == ["All", "Delhi", "Patna"]
    assert "Noida" not in august["city"]
    assert filter_choices(_catalog(), "2025-08-01", "2025-08-31", state="Bihar")["city"] == ["All", "Patna"]
    november = filter_choices(_catalog(), "2026-11-17", "2026-11-17")
    assert november["city"] == ["All", "Noida"]
    assert november["station"] == ["All", "Station N"]
    assert november["pollutant"] == ["All", "CO", "PM2.5"]


def test_metrics_use_only_the_selected_dates_and_station() -> None:
    catalog = _catalog()
    both_days = xkdr_summary(catalog, "2025-08-01", "2025-08-02", city="Delhi")
    one_station = xkdr_summary(catalog, "2025-08-01", "2025-08-02", city="Delhi", station="S1")
    assert both_days["observations"] == 3
    assert one_station["observations"] == 2
    assert one_station["stations"] == 1
    station_days = xkdr_daily_series(catalog, "2025-08-01", "2025-08-02", city="Delhi", station="S1")
    assert int(station_days["observations"].sum()) == 2
    assert int(one_station["spike_count"]) == 1
    assert one_station["source"].startswith("XKDR")
    later = gov_summary(
        gov_observations(catalog, "2026-11-17", "2026-11-17", station="Station N", pollutant="PM2.5"),
        unit="not documented by the API",
        start="2026-11-17",
        end="2026-11-17",
    )
    assert later["observations"] == 2
    assert later["stations"] == 1
    assert later["missing_values"] == 1


def test_existing_chart_functions_draw_the_filtered_series() -> None:
    series = xkdr_daily_series(_catalog(), "2025-08-01", "2025-08-02", pollutant="PM2.5")
    figure = time_series(series, y="mean_valid_value", title="Daily", y_title="µg/m³")
    assert len(figure.data) == 1
    assert list(figure.data[0].x) == [pd.Timestamp("2025-08-01"), pd.Timestamp("2025-08-02")]
    trend = gov_observations(_catalog(), "2026-11-17", "2026-11-17", station="Station N", pollutant="PM2.5")
    snapshot = snapshot_trend(trend, title="API")
    assert [trace.name for trace in snapshot.data] == ["Reported minimum", "Reported maximum", "Reported average"]


def test_new_snapshot_rows_appear_after_the_prepared_file_changes(tmp_path: Path) -> None:
    path = tmp_path / "data" / "processed" / "data_gov_in" / "observations" / "data.parquet"
    path.parent.mkdir(parents=True)
    first = _gov_frame().iloc[0:1]
    first.to_parquet(path, index=False)
    before = prepared_fingerprint(tmp_path)
    loaded = load_timeline(tmp_path)
    assert len(gov_observations(loaded, "2026-11-17", "2026-11-17")) == 1
    _gov_frame().to_parquet(path, index=False)
    after = prepared_fingerprint(tmp_path)
    assert before != after
    refreshed = load_timeline(tmp_path)
    assert len(gov_observations(refreshed, "2026-11-17", "2026-11-17")) == 3


def test_loading_a_catalog_does_not_require_ingestion_or_the_scheduler(tmp_path: Path) -> None:
    source = Path(__file__).resolve().parents[1].joinpath("src", "dashboard", "timeline.py").read_text(encoding="utf-8")
    assert "fetch_snapshot" not in source
    assert "Register-ScheduledTask" not in source
    assert "detect_spikes" not in source
    before = sorted(path.name for path in tmp_path.iterdir())
    catalog = load_timeline(tmp_path)
    assert catalog.xkdr.start is None
    assert catalog.gov.start is None
    assert sorted(path.name for path in tmp_path.iterdir()) == before


def test_station_ranking_counts_spikes_inside_the_window_only() -> None:
    ranking = xkdr_station_ranking(_catalog(), "2025-08-01", "2025-08-01", pollutant="PM2.5")
    assert list(ranking["station_id"]) == ["S2", "S1"]
    s1 = ranking.loc[ranking["station_id"].eq("S1")].iloc[0]
    assert int(s1["spike_count"]) == 1
    assert xkdr_spike_events(_catalog(), "2025-08-02", "2025-08-02").empty


def _catalog():
    return make_catalog(
        xkdr_daily=_daily(),
        xkdr_hourly=_hourly(),
        xkdr_measurements=_measurements(),
        xkdr_spikes=_spikes(),
        gov_observations=_gov_frame(),
    )


def _daily() -> pd.DataFrame:
    return pd.DataFrame(
        [
            _day("Delhi", "Delhi", "2025-08-01", "PM2.5", 2, 40),
            _day("Delhi", "Delhi", "2025-08-02", "PM2.5", 1, 50),
            _day("Patna", "Bihar", "2025-08-02", "PM10", 1, 80),
        ]
    )


def _day(city: str, state: str, day: str, pollutant: str, observations: int, mean: float) -> dict[str, object]:
    return {
        "city_name": city,
        "state_name": state,
        "observation_date": pd.Timestamp(day),
        "parameter_name": pollutant,
        "observations": observations,
        "valid_observations": observations,
        "mean_valid_value": mean,
        "spike_count": 1 if day == "2025-08-01" else 0,
        "eligible_observations": observations,
    }


def _hourly() -> pd.DataFrame:
    row = _day("Delhi", "Delhi", "2025-08-01", "PM2.5", 1, 40)
    row["observation_hour"] = 8
    return pd.DataFrame([row])


def _measurements() -> pd.DataFrame:
    return pd.DataFrame(
        [
            _measurement("S1", "Delhi", "Delhi", "2025-08-01 01:00:00", "PM2.5", 40),
            _measurement("S2", "Delhi", "Delhi", "2025-08-01 02:00:00", "PM2.5", 60),
            _measurement("S1", "Delhi", "Delhi", "2025-08-02 01:00:00", "PM2.5", 50),
            _measurement("S3", "Patna", "Bihar", "2025-08-02 03:00:00", "PM10", None),
        ]
    )


def _measurement(
    station: str,
    city: str,
    state: str,
    stamp: str,
    pollutant: str,
    value: float | None,
) -> dict[str, object]:
    return {
        "station_id": station,
        "station_name": station,
        "state_name": state,
        "city_name": city,
        "parameter_name": pollutant,
        "collected_at": pd.Timestamp(stamp),
        "value": value,
        "unit": "µg/m³",
        "latitude": 28.6,
        "longitude": 77.2,
    }


def _spikes() -> pd.DataFrame:
    row = _measurement("S1", "Delhi", "Delhi", "2025-08-01 01:00:00", "PM2.5", 40)
    return pd.DataFrame([row])


def _gov_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            _gov("2026-11-17 17:00:00", "PM2.5", 10, 20, 15),
            _gov("2026-11-17 20:00:00", "PM2.5", 12, 22, None),
            _gov("2026-11-17 20:00:00", "CO", 1, 2, 1.5),
        ]
    )


def _gov(
    stamp: str,
    pollutant: str,
    minimum: float,
    maximum: float,
    average: float | None,
) -> dict[str, object]:
    return {
        "state": "Uttar Pradesh",
        "city": "Noida",
        "station": "Station N",
        "pollutant_id": pollutant,
        "observed_at": pd.Timestamp(stamp),
        "latitude": 28.5,
        "longitude": 77.3,
        "coordinate_status": "valid",
        "min_value": minimum,
        "max_value": maximum,
        "avg_value": average,
        "reported_unit": "not documented by the API",
        "value_kind": "api-reported pollutant value",
    }
