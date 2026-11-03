"""City comparison tests.

The frames are synthetic. They are not January 2024 measurements, and they
do not use the national daily or hourly tables.
"""

from __future__ import annotations

import pandas as pd
import pytest

from dashboard.comparison import (
    comparison_insights,
    comparison_rows,
    daily_lines,
    dropdown_options,
    hourly_lines,
    place_label,
    restore_saved_value,
    selected_places,
    selection_error,
)


def test_two_and_three_cities_keep_distinct_city_state_pairs() -> None:
    labels = [
        "Delhi, Delhi",
        "Aurangabad, Bihar",
        "Aurangabad, Maharashtra",
        "Patna, Bihar",
    ]
    places = [
        ("Delhi", "Delhi"),
        ("Aurangabad", "Bihar"),
        ("Aurangabad", "Maharashtra"),
        ("Patna", "Bihar"),
    ]
    assert place_label("Aurangabad", "Bihar") != place_label("Aurangabad", "Maharashtra")
    two = selected_places(labels, places, ["Delhi, Delhi", "Patna, Bihar", "None"])
    three = selected_places(
        labels,
        places,
        ["Aurangabad, Bihar", "Aurangabad, Maharashtra", "Delhi, Delhi"],
    )
    assert selection_error(two) is None
    assert selection_error(three) is None
    assert two == [("Delhi", "Delhi"), ("Patna", "Bihar")]
    assert [place_label(*place) for place in three] == [
        "Aurangabad, Bihar",
        "Aurangabad, Maharashtra",
        "Delhi, Delhi",
    ]
    assert selection_error([("Delhi", "Delhi")]) == "Select at least two cities."
    assert selection_error([("Delhi", "Delhi"), ("Delhi", "Delhi")]) == (
        "Select each city and state combination once."
    )
    options = dropdown_options(labels, ["Delhi, Delhi", None, None], 1)
    assert "Delhi, Delhi" not in options
    assert "Aurangabad, Bihar" in options
    assert "Aurangabad, Maharashtra" in options


def test_pollutant_averages_spikes_and_rates_stay_separate() -> None:
    rows = comparison_rows(
        _daily(),
        [("Delhi", "Delhi"), ("Patna", "Bihar")],
        _stations(),
        _metrics(),
        "2024-01-01",
        "2024-01-02",
    )
    delhi, patna = rows
    assert delhi["stations"] == 2
    assert patna["stations"] == 1
    assert delhi["pm25_mean"] == pytest.approx(20)
    assert delhi["pm10_mean"] == pytest.approx(50)
    assert delhi["pm25_mean"] != pytest.approx((20 + 50) / 2)
    assert delhi["pm25_spike_count"] == 2
    assert delhi["pm25_eligible"] == 4
    assert delhi["pm25_rate"] == pytest.approx(0.5)
    assert delhi["pm10_spike_count"] == 2
    assert delhi["pm10_rate"] == pytest.approx(0.5)
    assert delhi["total_observations"] == 8
    assert patna["pm25_mean"] == pytest.approx(100)
    assert patna["pm25_observations"] == 1
    assert patna["pm25_spike_count"] == 0
    assert patna["pm25_rate"] == pytest.approx(0)
    assert patna["pm10_mean"] is None
    assert patna["pm10_observations"] is None
    assert patna["pm10_spike_count"] is None
    assert patna["pm10_rate"] is None
    assert patna["pm10_status"] == "missing"
    assert patna["total_observations"] == 1


def test_date_and_hour_filters_do_not_mix_cities_or_national_rows() -> None:
    places = [("Delhi", "Delhi"), ("Patna", "Bihar")]
    daily = daily_lines(_daily(), places, "PM2.5", "2024-01-01", "2024-01-01")
    assert set(daily["city_label"]) == {"Delhi, Delhi", "Patna, Bihar"}
    assert "All India" not in set(daily["city_label"])
    assert set(daily["parameter_name"]) == {"PM2.5"}
    assert set(pd.to_datetime(daily["observation_date"]).dt.date.astype(str)) == {"2024-01-01"}
    delhi = daily.loc[daily["city_label"].eq("Delhi, Delhi")].iloc[0]
    assert int(delhi["observations"]) == 2
    assert delhi["mean_valid_value"] == pytest.approx(10)

    hourly = hourly_lines(
        _hourly(),
        _national_hours(),
        places,
        "PM2.5",
        "2024-01-01",
        "2024-01-01",
        "2024-01-01",
        "2024-01-02",
    )
    assert set(hourly["city_label"]) == {"Delhi, Delhi", "Patna, Bihar"}
    assert "All India" not in set(hourly["city_label"])
    delhi_hour = hourly.loc[hourly["city_label"].eq("Delhi, Delhi") & hourly["observation_hour"].eq(8)].iloc[0]
    assert delhi_hour["mean_valid_value"] == pytest.approx(10)
    assert int(delhi_hour["observations"]) == 2
    patna_hour = hourly.loc[hourly["city_label"].eq("Patna, Bihar")].iloc[0]
    assert patna_hour["mean_valid_value"] == pytest.approx(100)
    assert int(patna_hour["observations"]) == 1


def test_saved_controls_return_only_when_the_widget_value_is_gone() -> None:
    state: dict[str, object] = {"compare_city_1": "Delhi, Delhi"}
    restore_saved_value(state, "compare_city_1", "Agra, Uttar Pradesh")
    restore_saved_value(state, "compare_city_2", "Patna, Bihar")
    restore_saved_value(state, "compare_dates", None)
    assert state["compare_city_1"] == "Delhi, Delhi"
    assert state["compare_city_2"] == "Patna, Bihar"
    assert "compare_dates" not in state


def test_insights_use_averages_rates_and_coverage_separately() -> None:
    rows = comparison_rows(
        _daily(),
        [("Delhi", "Delhi"), ("Patna", "Bihar"), ("Aurangabad", "Maharashtra")],
        _stations(),
        _metrics(),
        "2024-01-01",
        "2024-01-02",
    )
    text = " ".join(comparison_insights(rows))
    assert "Patna, Bihar has the highest PM2.5 average concentration: 100.00 µg/m³." in text
    assert "Delhi, Delhi has the highest PM10 average concentration: 50.00 µg/m³." in text
    assert "Delhi, Delhi has the highest PM2.5 eligible spike rate: 50.00%." in text
    assert "Aurangabad, Maharashtra has the highest monitoring station coverage: 3 stations." in text
    assert "Patna, Bihar and Aurangabad, Maharashtra have no saved value for this measure." in text
    assert "more polluted" not in text
    assert "cleaner" not in text
    assert "health" not in text.casefold()


def _daily() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "city_name": ["Delhi", "Delhi", "Delhi", "Patna", "Aurangabad", "All India"],
            "state_name": ["Delhi", "Delhi", "Delhi", "Bihar", "Maharashtra", None],
            "observation_date": pd.to_datetime(
                ["2024-01-01", "2024-01-02", "2024-01-01", "2024-01-01", "2024-01-01", "2024-01-01"]
            ),
            "parameter_name": ["PM2.5", "PM2.5", "PM10", "PM2.5", "PM2.5", "PM2.5"],
            "observations": [2, 2, 4, 1, 3, 999],
            "valid_observations": [2, 2, 4, 1, 3, 999],
            "mean_valid_value": [10.0, 30.0, 50.0, 100.0, 8.0, 1.0],
            "spike_count": [1, 1, 2, 0, 1, 50],
            "eligible_observations": [2, 2, 4, 1, 3, 999],
            "spike_rate_of_eligible": [0.5, 0.5, 0.5, 0.0, 1 / 3, 0.9],
        }
    )


def _hourly() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "city_name": ["Delhi", "Patna", "All India"],
            "state_name": ["Delhi", "Bihar", None],
            "observation_date": pd.to_datetime(["2024-01-01", "2024-01-01", "2024-01-01"]),
            "observation_hour": [8, 8, 8],
            "parameter_name": ["PM2.5", "PM2.5", "PM2.5"],
            "observations": [2, 1, 999],
            "valid_observations": [2, 1, 999],
            "mean_valid_value": [10.0, 100.0, 1.0],
            "spike_count": [1, 0, 40],
            "eligible_observations": [2, 1, 999],
            "spike_rate_of_eligible": [0.5, 0.0, 0.9],
        }
    )


def _national_hours() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "observation_hour": [8],
            "parameter_name": ["PM2.5"],
            "observations": [999],
            "valid_observations": [999],
            "mean_valid_value": [1.0],
            "spike_count": [40],
            "eligible_observations": [999],
            "spike_rate_of_eligible": [0.9],
        }
    )


def _stations() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "station_id": ["d1", "d2", "p1", "a1", "a2", "a3"],
            "parameter_name": ["PM2.5", "PM10", "PM2.5", "PM2.5", "PM2.5", "PM2.5"],
            "city_name": ["Delhi", "Delhi", "Patna", "Aurangabad", "Aurangabad", "Aurangabad"],
            "state_name": ["Delhi", "Delhi", "Bihar", "Maharashtra", "Maharashtra", "Maharashtra"],
            "city_conflict": [False, False, False, False, False, False],
        }
    )


def _metrics() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "city_name": ["Delhi", "Delhi", "Patna", "Aurangabad"],
            "state_name": ["Delhi", "Delhi", "Bihar", "Maharashtra"],
            "parameter_name": ["PM2.5", "PM10", "PM2.5", "PM2.5"],
        }
    )
