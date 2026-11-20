"""Dashboard helpers for prepared data.gov.in snapshots."""

from __future__ import annotations

from datetime import datetime

import pandas as pd

from dashboard.figures import recent_avg_series, snapshot_trend
from dashboard.recent_data import (
    at_timestamp,
    filtered_observations,
    load_pipeline_status,
    pipeline_status_text,
    pollutant_choices,
    recent_period,
    recent_period_text,
    trend_frame,
    trend_unavailable_message,
)


def _frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "state": ["Delhi", "Delhi", "Delhi"],
            "city": ["Delhi", "Delhi", "Delhi"],
            "station": ["Anand Vihar", "Anand Vihar", "R K Puram"],
            "pollutant_id": ["PM2.5", "PM2.5", "PM2.5"],
            "observed_at": [
                datetime(2026, 11, 17, 17, 0),
                datetime(2026, 11, 17, 18, 0),
                datetime(2026, 11, 17, 17, 0),
            ],
            "avg_value": [55.0, 70.0, 40.0],
            "min_value": [40.0, 50.0, None],
            "max_value": [80.0, 90.0, None],
            "coordinate_status": ["valid", "valid", "missing"],
        }
    )


def _mixed() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "state": ["Delhi"] * 8,
            "city": ["Delhi"] * 8,
            "station": ["Anand Vihar", "R K Puram"] * 4,
            "pollutant_id": ["PM2.5", "PM2.5", "PM10", "PM10", "PM2.5", "PM2.5", "PM10", "PM10"],
            "observed_at": [
                datetime(2026, 11, 17, 17, 0),
                datetime(2026, 11, 17, 17, 0),
                datetime(2026, 11, 17, 17, 0),
                datetime(2026, 11, 17, 17, 0),
                datetime(2026, 11, 18, 13, 0),
                datetime(2026, 11, 18, 13, 0),
                datetime(2026, 11, 18, 13, 0),
                datetime(2026, 11, 18, 13, 0),
            ],
            "avg_value": [10.0, 30.0, 100.0, None, 20.0, 40.0, 80.0, 120.0],
        }
    )


def _select(frame: pd.DataFrame, *, station: str | None, pollutant: str | None) -> pd.DataFrame:
    rows = frame
    if station:
        rows = rows.loc[rows["station"].eq(station)]
    if pollutant:
        rows = rows.loc[rows["pollutant_id"].eq(pollutant)]
    return rows.copy()


def _chart(period: object, *, markers_only: bool):
    return recent_avg_series(
        period.points,
        title="Recent Air Quality Observations — data.gov.in",
        y_title="Average reported value (API unit not documented)",
        markers_only=markers_only,
    )


def test_trend_requires_two_timestamps_for_one_station() -> None:
    frame = _frame()
    selected = filtered_observations(frame, state="Delhi", city="Delhi", station="Anand Vihar", pollutant="PM2.5")
    trend = trend_frame(selected)
    assert trend is not None
    assert list(trend["observed_at"]) == [datetime(2026, 11, 17, 17, 0), datetime(2026, 11, 17, 18, 0)]
    assert list(trend["avg_value"]) == [55.0, 70.0]
    assert list(trend["min_value"]) == [40.0, 50.0]
    single = filtered_observations(frame, state="Delhi", city="Delhi", station="R K Puram", pollutant="PM2.5")
    assert trend_frame(single) is None
    assert "different last_update" in trend_unavailable_message(single, "PM2.5")
    assert "Another source update" in trend_unavailable_message(single, "PM2.5")
    mixed = filtered_observations(frame, state="Delhi", city="Delhi", station=None, pollutant="PM2.5")
    assert trend_frame(mixed) is None
    assert "Choose one station" in trend_unavailable_message(mixed, "PM2.5")


def test_trend_uses_distinct_api_timestamps_and_keeps_missing_values() -> None:
    repeated = pd.DataFrame(
        {
            "state": ["Delhi", "Delhi", "Delhi"],
            "city": ["Delhi", "Delhi", "Delhi"],
            "station": ["Anand Vihar", "Anand Vihar", "Anand Vihar"],
            "pollutant_id": ["PM2.5", "PM2.5", "PM2.5"],
            "observed_at": [
                datetime(2026, 11, 17, 17, 0),
                datetime(2026, 11, 17, 17, 0),
                datetime(2026, 11, 17, 18, 0),
            ],
            "retrieved_at": [
                "2026-11-17T12:00:00Z",
                "2026-11-17T13:00:00Z",
                "2026-11-17T14:00:00Z",
            ],
            "avg_value": [55.0, 99.0, 70.0],
            "min_value": [40.0, 40.0, None],
            "max_value": [80.0, 80.0, 90.0],
        }
    )
    trend = trend_frame(repeated)
    assert trend is not None
    assert len(trend) == 2
    assert list(trend["observed_at"]) == [datetime(2026, 11, 17, 17, 0), datetime(2026, 11, 17, 18, 0)]
    figure = snapshot_trend(trend, title="Anand Vihar · PM2.5")
    names = [trace.name for trace in figure.data]
    assert names == ["Reported minimum", "Reported maximum", "Reported average"]
    minimum = next(trace for trace in figure.data if trace.name == "Reported minimum")
    assert list(minimum.y) == [40.0, None]
    assert 0 not in list(minimum.y)
    assert figure.layout.xaxis.title.text == "API last_update"


def test_pipeline_status_reports_a_failed_prepare_without_hiding_the_last_success(tmp_path) -> None:
    assert load_pipeline_status(tmp_path) is None
    assert "does not call data.gov.in" in pipeline_status_text(None)
    path = tmp_path / "logs" / "data_gov_automation"
    path.mkdir(parents=True)
    (path / "status.json").write_text(
        """
        {
          "finished_at": "2026-11-17T19:00:00+05:30",
          "success": false,
          "fetch_ran": true,
          "prepare_ran": true,
          "fetch_exit_code": 0,
          "prepare_exit_code": 9,
          "last_success_at": "2026-11-17T18:00:00+05:30",
          "message": "prepare-data-gov failed."
        }
        """,
        encoding="utf-8",
    )
    status = load_pipeline_status(tmp_path)
    assert status is not None
    text = pipeline_status_text(status)
    assert "Fetch succeeded" in text
    assert "Preparation failed (exit 9)" in text
    assert "2026-11-17T18:00:00+05:30" in text
    assert "source observation time" in text
    assert "add no new observations" in text


def test_recent_chart_covers_filter_combinations_and_null_averages() -> None:
    frame = _mixed()
    everyone = _select(frame, station=None, pollutant=None)
    period = recent_period(everyone)
    assert period.kind == "trend"
    assert period.distinct_timestamps == 2
    assert period.observations == 7
    assert period.stations == 2
    pm25 = period.points.loc[period.points["pollutant_id"].eq("PM2.5")].sort_values("observed_at")
    assert list(pm25["avg_value"]) == [20.0, 30.0]
    assert list(pm25["station_count"]) == [2, 2]
    pm10 = period.points.loc[period.points["pollutant_id"].eq("PM10")].sort_values("observed_at")
    assert list(pm10["avg_value"]) == [100.0, 100.0]
    assert list(pm10["station_count"]) == [1, 2]
    assert 0 not in list(period.points["avg_value"])
    figure = _chart(period, markers_only=False)
    assert [trace.name for trace in figure.data] == ["PM10", "PM2.5"]
    assert figure.data[0].mode == "lines+markers"
    assert figure.layout.title.text == "Recent Air Quality Observations — data.gov.in"
    assert figure.layout.xaxis.title.text == "Observation timestamp"
    assert figure.layout.yaxis.title.text == "Average reported value (API unit not documented)"
    assert "CPCB" not in figure.layout.yaxis.title.text

    one_station = recent_period(_select(frame, station="Anand Vihar", pollutant=None))
    assert one_station.stations == 1
    assert sorted(one_station.points["pollutant_id"]) == ["PM10", "PM10", "PM2.5", "PM2.5"]
    assert list(one_station.points.loc[one_station.points["pollutant_id"].eq("PM2.5"), "avg_value"]) == [10.0, 20.0]

    one_pollutant = recent_period(_select(frame, station=None, pollutant="PM2.5"))
    assert list(one_pollutant.points["pollutant_id"]) == ["PM2.5", "PM2.5"]
    assert list(one_pollutant.points["station_count"]) == [2, 2]

    exact = recent_period(_select(frame, station="R K Puram", pollutant="PM10"))
    assert exact.kind == "snapshot"
    assert list(exact.points["avg_value"]) == [120.0]
    assert list(exact.points["station_count"]) == [1]
    point = _chart(exact, markers_only=True)
    assert point.data[0].mode == "markers"
    assert len(list(point.data[0].x)) == 1


def test_one_timestamp_is_a_point_and_empty_or_null_data_has_no_series() -> None:
    single = pd.DataFrame(
        {
            "state": ["Delhi", "Delhi"],
            "city": ["Delhi", "Delhi"],
            "station": ["Anand Vihar", "R K Puram"],
            "pollutant_id": ["PM2.5", "PM10"],
            "observed_at": [datetime(2026, 11, 19, 13, 0), datetime(2026, 11, 19, 13, 0)],
            "avg_value": [80.0, 120.0],
        }
    )
    period = recent_period(single)
    assert period.kind == "snapshot"
    assert period.distinct_timestamps == 1
    assert period.distinct_dates == 1
    figure = _chart(period, markers_only=True)
    assert [trace.name for trace in figure.data] == ["PM10", "PM2.5"]
    assert list(figure.data[0].y) == [120.0]
    text = recent_period_text(period, selected_days=7, unit="not documented by the API")
    assert "1 valid observations" not in text
    assert "2 valid observations" in text
    assert "1 distinct observation timestamps" in text
    assert "not CPCB CAAQM" in text
    blank = recent_period(single.iloc[0:0])
    assert blank.kind == "empty"
    assert _chart(blank, markers_only=False).data == ()
    assert "not filled" in recent_period_text(blank, selected_days=30, unit="not documented by the API")
    nulls = single.copy()
    nulls["avg_value"] = [None, None]
    assert recent_period(nulls).kind == "empty"
    assert "not replaced with zero" in recent_period_text(recent_period(nulls), selected_days=7, unit="not documented by the API")


def test_other_pollutants_are_excluded_from_the_recent_chart() -> None:
    frame = pd.DataFrame(
        {
            "state": ["Delhi"] * 8,
            "city": ["Delhi"] * 8,
            "station": ["Anand Vihar", "R K Puram"] * 4,
            "pollutant_id": ["PM2.5", "PM10", "CO", "NH3", "NO2", "OZONE", "SO2", "PM2.5"],
            "observed_at": [
                datetime(2026, 11, 17, 17, 0),
                datetime(2026, 11, 17, 17, 0),
                datetime(2026, 11, 17, 17, 0),
                datetime(2026, 11, 18, 13, 0),
                datetime(2026, 11, 18, 13, 0),
                datetime(2026, 11, 18, 13, 0),
                datetime(2026, 11, 19, 13, 0),
                datetime(2026, 11, 18, 13, 0),
            ],
            "avg_value": [10.0, 40.0, 1.0, 2.0, 3.0, 4.0, 5.0, None],
        }
    )
    original_ids = list(frame["pollutant_id"])
    period = recent_period(frame)
    assert list(frame["pollutant_id"]) == original_ids
    assert period.kind == "snapshot"
    assert period.observations == 2
    assert period.stations == 2
    assert period.distinct_timestamps == 1
    assert sorted(period.points["pollutant_id"]) == ["PM10", "PM2.5"]
    assert list(period.points["avg_value"]) == [40.0, 10.0]
    assert None not in list(period.points["avg_value"])
    figure = _chart(period, markers_only=True)
    assert [trace.name for trace in figure.data] == ["PM10", "PM2.5"]
    assert pollutant_choices(["CO", "NH3", "NO2", "OZONE", "SO2", "PM2.5", "PM10", "All"]) == [
        "All",
        "PM10",
        "PM2.5",
    ]
    gases = frame.loc[~frame["pollutant_id"].isin(["PM2.5", "PM10"])]
    assert recent_period(gases).kind == "empty"


def test_multiple_timestamps_on_one_date_are_a_trend() -> None:
    hours = _frame()
    period = recent_period(hours)
    assert period.kind == "trend"
    assert period.distinct_dates == 1
    assert period.distinct_timestamps == 2


def test_last_7_and_last_30_chart_only_stored_dates() -> None:
    from datetime import date

    from dashboard.timeline import preset_bounds

    today = date(2026, 11, 19)
    frame = pd.DataFrame(
        {
            "state": ["Delhi"] * 6,
            "city": ["Delhi"] * 6,
            "station": ["Anand Vihar", "R K Puram"] * 3,
            "pollutant_id": ["PM2.5"] * 6,
            "observed_at": [
                datetime(2026, 10, 1, 10, 0),
                datetime(2026, 10, 1, 10, 0),
                datetime(2026, 11, 17, 17, 0),
                datetime(2026, 11, 17, 17, 0),
                datetime(2026, 11, 18, 13, 0),
                datetime(2026, 11, 18, 13, 0),
            ],
            "avg_value": [1.0, 3.0, 55.0, 45.0, 70.0, 50.0],
        }
    )
    last_7 = _window(frame, preset_bounds("Last 7 days", today))
    last_30 = _window(frame, preset_bounds("Last 30 days", today))
    week = recent_period(last_7)
    month = recent_period(last_30)
    assert week.kind == "trend"
    assert month.kind == "trend"
    assert week.distinct_dates == 2
    assert month.distinct_dates == 2
    assert list(pd.to_datetime(week.points["observed_at"])) == [
        datetime(2026, 11, 17, 17, 0),
        datetime(2026, 11, 18, 13, 0),
    ]
    assert list(week.points["avg_value"]) == [50.0, 60.0]
    week_figure = recent_avg_series(week.points, title="Last 7 days", y_title="Mean of API avg_value")
    month_figure = recent_avg_series(month.points, title="Last 30 days", y_title="Mean of API avg_value")
    assert list(week_figure.data[0].x) == list(month_figure.data[0].x)
    assert datetime(2026, 10, 1, 10, 0) not in list(week_figure.data[0].x)
    assert week_figure.layout.yaxis.title.text == "Mean of API avg_value"
    assert "µg" not in week_figure.layout.yaxis.title.text
    week_text = recent_period_text(week, selected_days=7, unit="not documented by the API")
    assert "data.gov.in real-time observations" in week_text
    assert "not CPCB CAAQM" in week_text
    assert "not interpolated" in week_text
    assert "2 dates" in week_text


def _window(frame: pd.DataFrame, bounds: tuple[object, object]) -> pd.DataFrame:
    start, end = bounds
    stamps = pd.to_datetime(frame["observed_at"]).dt.normalize()
    return frame.loc[(stamps >= pd.Timestamp(start)) & (stamps <= pd.Timestamp(end))].copy()


def test_latest_timestamp_filter_does_not_replace_a_missing_average() -> None:
    frame = _frame()
    selected = filtered_observations(frame, state="Delhi", city="Delhi", station=None, pollutant="PM2.5")
    current = at_timestamp(selected, pd.Timestamp("2026-11-17 18:00:00"))
    assert list(current["station"]) == ["Anand Vihar"]
    assert current.iloc[0]["avg_value"] == 70
    earlier = at_timestamp(selected, pd.Timestamp("2026-11-17 17:00:00"))
    missing = earlier.loc[earlier["station"].eq("R K Puram")].iloc[0]
    assert missing["min_value"] is None or pd.isna(missing["min_value"])
