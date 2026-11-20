"""Coverage categories and time-series gap tests. No live API and no Spark."""

from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd

from dashboard.coverage import PARTIAL, PROCESSED, NOT_DOWNLOADED, DOWNLOADED_NOT_PROCESSED, classify_months
from dashboard.figures import NO_PREPARED_OBSERVATIONS, missing_observation_periods, time_series


def test_missing_months_are_not_called_unavailable() -> None:
    days = tuple(pd.date_range("2024-01-01", "2024-01-31", freq="D")) + tuple(
        pd.date_range("2025-08-01", "2025-08-31", freq="D")
    )
    days = days + (pd.Timestamp("2025-09-01"),)
    months = { (item.year, item.month): item for item in classify_months([(2024, 1), (2025, 8), (2025, 9), (2026, 3)], days) }
    assert months[(2024, 1)].status == PROCESSED
    assert months[(2024, 2)].status == NOT_DOWNLOADED
    assert months[(2025, 8)].status == PROCESSED
    assert months[(2025, 9)].status == PARTIAL
    assert months[(2025, 9)].days_with_observations == 1
    assert months[(2026, 3)].status == DOWNLOADED_NOT_PROCESSED
    remote = classify_months(
        [(2024, 1)],
        tuple(pd.date_range("2024-01-01", "2024-01-31", freq="D")),
        remote={(2024, 2): "available", (2024, 3): "unavailable"},
        partial=[(2024, 4)],
    )
    labelled = {(item.year, item.month): item.status for item in remote}
    assert labelled[(2024, 2)] == "available_remotely"
    assert labelled[(2024, 3)] == "unavailable_remotely"
    assert labelled[(2024, 4)] == "incomplete"


def test_daily_lines_break_across_a_month_with_no_observations() -> None:
    frame = pd.DataFrame(
        {
            "observation_date": pd.to_datetime(["2024-01-31", "2025-08-01"]),
            "parameter_name": ["PM2.5", "PM2.5"],
            "mean_valid_value": [10.0, 20.0],
            "eligible_observations": [1, 1],
        }
    )
    figure = time_series(frame, y="mean_valid_value", title="Daily", y_title="µg/m³")
    values = list(figure.data[0].y)
    assert values[0] == 10.0
    assert pd.isna(values[1])
    assert values[2] == 20.0
    assert figure.data[0].connectgaps is False
    assert list(figure.layout.shapes) == []


def test_missing_days_are_shaded_without_changing_the_line() -> None:
    frame = pd.DataFrame(
        {
            "observation_date": pd.to_datetime(["2024-02-29", "2025-08-01", "2025-09-01"]),
            "parameter_name": ["PM2.5", "PM2.5", "PM2.5"],
            "mean_valid_value": [12.0, 18.0, 21.0],
            "observations": [4, 5, 6],
            "eligible_observations": [4, 5, 6],
        }
    )
    figure = time_series(
        frame,
        y="mean_valid_value",
        title="Daily",
        y_title="µg/m³",
        coverage_window=("2024-01-01", "2025-09-30"),
    )
    values = list(figure.data[0].y)
    assert values[0] == 12.0
    assert pd.isna(values[1])
    assert values[2] == 18.0
    assert pd.isna(values[3])
    assert values[4] == 21.0
    assert figure.data[0].connectgaps is False
    assert _covers(figure, "2024-03-01")
    assert _covers(figure, "2025-07-31")
    assert _covers(figure, "2025-08-15")
    assert _covers(figure, "2025-09-02")
    assert _covers(figure, "2025-09-30")
    assert not _covers(figure, "2024-02-29")
    assert not _covers(figure, "2025-08-01")
    assert not _covers(figure, "2025-09-01")
    assert figure.layout.shapes[0].fillcolor == "rgba(142, 182, 255, 0.18)"
    assert figure.layout.shapes[0].layer == "below"
    labels = [item.text for item in figure.layout.annotations]
    assert labels.count("No prepared observations") == 4
    assert [trace.name for trace in figure.data].count("No prepared observations") == 1
    assert NO_PREPARED_OBSERVATIONS == "No prepared observations are available for this period."


def test_shading_follows_the_selected_pollutant_and_dates() -> None:
    frame = pd.DataFrame(
        {
            "observation_date": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-01", "2024-01-03"]),
            "parameter_name": ["PM2.5", "PM2.5", "PM10", "PM10"],
            "mean_valid_value": [10.0, 11.0, 20.0, 22.0],
        }
    )
    both = time_series(
        frame,
        y="mean_valid_value",
        title="Daily",
        y_title="µg/m³",
        coverage_window=("2024-01-01", "2024-01-04"),
    )
    assert not _covers(both, "2024-01-01")
    assert not _covers(both, "2024-01-02")
    assert not _covers(both, "2024-01-03")
    assert _covers(both, "2024-01-04")
    pm25 = frame.loc[frame["parameter_name"].eq("PM2.5")]
    selected = time_series(
        pm25,
        y="mean_valid_value",
        title="Daily",
        y_title="µg/m³",
        coverage_window=("2024-01-01", "2024-01-03"),
    )
    assert _covers(selected, "2024-01-03")
    assert not _covers(selected, "2024-01-01")
    assert not _covers(selected, "2024-01-02")
    assert list(selected.data[0].y) == [10.0, 11.0]
    assert selected.data[0].connectgaps is False
    pm10 = frame.loc[frame["parameter_name"].eq("PM10")]
    pm10_chart = time_series(
        pm10,
        y="mean_valid_value",
        title="Daily",
        y_title="µg/m³",
        coverage_window=("2024-01-01", "2024-01-03"),
    )
    assert _covers(pm10_chart, "2024-01-02")
    assert not _covers(pm10_chart, "2024-01-01")
    assert not _covers(pm10_chart, "2024-01-03")
    assert pd.isna(list(pm10_chart.data[0].y)[1])
    assert pm10_chart.data[0].connectgaps is False
    covered = time_series(
        pm25,
        y="mean_valid_value",
        title="Daily",
        y_title="µg/m³",
        coverage_window=("2024-01-01", "2024-01-02"),
    )
    assert list(covered.layout.shapes) == []
    assert [trace.name for trace in covered.data] == ["PM2.5"]
    periods = missing_observation_periods(
        ["2024-02-29", "2025-08-01"],
        "2025-09-30",
        "2024-01-01",
    )
    assert periods == [
        (pd.Timestamp("2024-01-01"), pd.Timestamp("2024-02-28")),
        (pd.Timestamp("2024-03-01"), pd.Timestamp("2025-07-31")),
        (pd.Timestamp("2025-08-02"), pd.Timestamp("2025-09-30")),
    ]


def test_timeline_page_explains_periods_without_prepared_observations() -> None:
    source = (
        __import__("pathlib").Path(__file__).resolve().parents[1]
        / "src"
        / "dashboard"
        / "timeline_page.py"
    ).read_text(encoding="utf-8")
    assert "coverage_window=(start, end)" in source
    assert "st.caption(NO_PREPARED_OBSERVATIONS)" in source


def _covers(figure, day: str) -> bool:
    moment = datetime.fromisoformat(day) + timedelta(hours=12)
    return any(
        pd.Timestamp(shape.x0) <= moment <= pd.Timestamp(shape.x1)
        for shape in figure.layout.shapes
    )
