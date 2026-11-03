"""Portal login, city listing, and city-filter tests.

These tests use synthetic tables. They do not start Spark and they do not
write under data/raw or data/processed. The January check runs only when the
city series have already been built.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from dashboard.auth import begin_session, end_session, load_users, select_city, verify_credentials
from dashboard.cities import (
    build_city_cards,
    city_period,
    city_rows,
    city_station_ids,
    filter_station_ids,
    pollutant_summaries,
    search_cards,
    unassigned_summary,
)
from dashboard.filters import located_stations, unlocated_stations
from dashboard.loading import load_city_portal

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_login_session_accepts_the_demonstration_hash_and_logout_clears_it() -> None:
    users = load_users(PROJECT_ROOT / "config" / "portal_users.json")
    stored = (PROJECT_ROOT / "config" / "portal_users.json").read_text(encoding="utf-8")
    assert "aeroscope-demo" not in stored
    assert "password_hash" in stored
    assert verify_credentials("analyst@aeroscope.local", "aeroscope-demo", users)
    assert verify_credentials("Analyst@Aeroscope.local", "aeroscope-demo", users)
    assert not verify_credentials("analyst@aeroscope.local", "wrong-password", users)
    assert not verify_credentials("other@aeroscope.local", "aeroscope-demo", users)

    state: dict[str, object] = {"comparison_places": [("Delhi", "Delhi")], "compare_city_1": "Delhi, Delhi"}
    begin_session(state, "analyst@aeroscope.local")
    select_city(state, "Delhi", "Delhi")
    assert state["authenticated"] is True
    assert state["city_selected"] is True
    assert "comparison_places" not in state
    state["comparison_places"] = [("Delhi", "Delhi")]
    end_session(state)
    assert state["authenticated"] is False
    assert state["user_email"] is None
    assert state["city_selected"] is False
    assert state["city_name"] is None
    assert "comparison_places" not in state


def test_city_cards_list_recorded_cities_and_keep_missing_cities_separate() -> None:
    stations, metrics = _places()
    cards = build_city_cards(stations, metrics)

    assert [card["city_name"] for card in cards] == ["Delhi", "Patna"]
    delhi = cards[0]
    assert delhi["state_name"] == "Delhi"
    assert delhi["stations"] == 2
    assert delhi["stations_with_coordinates"] == 1
    assert delhi["pollutants"] == ["PM10", "PM2.5"]
    assert delhi["observations"] == 5
    assert "blank" not in {card["city_name"] for card in cards}
    assert "Left" not in {card["city_name"] for card in cards}

    unassigned = unassigned_summary(stations)
    reasons = dict(zip(unassigned["station_id"], unassigned["reason"]))
    assert reasons == {"blank": "city not recorded", "conflict": "conflicting city names"}
    assert unassigned["station_id"].tolist().count("blank") == 1
    assert search_cards(cards, "pat") == [cards[1]]
    assert search_cards(cards, "bihar") == [cards[1]]
    assert search_cards(cards, "not-a-city") == []


def test_city_filters_use_station_ids_dates_and_do_not_mix_cities() -> None:
    stations, _metrics = _places()
    daily = _daily()
    delhi_ids = city_station_ids(stations, "Delhi", "Delhi")
    assert delhi_ids == ["delhi-a", "delhi-b"]

    kept = filter_station_ids(stations, delhi_ids)
    assert set(kept["station_id"]) == {"delhi-a", "delhi-b"}
    assert "patna" not in set(kept["station_id"])
    assert "blank" not in set(kept["station_id"])

    january_first = city_period(daily, "Delhi", "Delhi", "2024-01-01", "2024-01-01", "PM2.5")
    assert set(january_first["city_name"]) == {"Delhi"}
    assert set(pd.to_datetime(january_first["observation_date"]).dt.date.astype(str)) == {"2024-01-01"}
    summary = pollutant_summaries(january_first)["PM2.5"]
    assert summary["observations"] == 3
    assert summary["spike_count"] == 1
    assert summary["eligible_observations"] == 3
    assert summary["mean_valid_value"] == pytest.approx(20)
    assert summary["spike_rate_of_eligible"] == pytest.approx(1 / 3)

    both_days = city_rows(daily, "Delhi", "Delhi")
    assert set(both_days["city_name"]) == {"Delhi"}
    assert "Patna" not in set(both_days["city_name"])
    patna = city_rows(daily, "Patna", "Bihar")
    assert int(patna["observations"].sum()) == 9
    assert int(both_days["observations"].sum()) == 5

    delhi_stations = filter_station_ids(stations, delhi_ids)
    assert set(unlocated_stations(delhi_stations)["station_id"]) == {"delhi-b"}
    assert set(located_stations(delhi_stations)["station_id"]) == {"delhi-a"}
    assert unlocated_stations(delhi_stations)["latitude"].isna().all()


@pytest.mark.skipif(
    not (PROJECT_ROOT / "data" / "processed" / "city_portal" / "city_daily" / "data.parquet").is_file(),
    reason="City series have not been built",
)
def test_saved_city_series_match_city_pollutant_and_stay_separated() -> None:
    portal = load_city_portal(PROJECT_ROOT)
    cities = pd.read_parquet(PROJECT_ROOT / "data" / "processed" / "analytics" / "city_pollutant" / "data.parquet")
    agra = cities.loc[cities["city_name"].eq("Agra") & cities["state_name"].eq("Uttar Pradesh")]
    agra_daily = city_rows(portal.daily, "Agra", "Uttar Pradesh")
    for pollutant in ("PM2.5", "PM10"):
        saved = agra.loc[agra["parameter_name"].eq(pollutant)].iloc[0]
        rolled = pollutant_summaries(agra_daily)[pollutant]
        assert rolled["observations"] == int(saved["observations"])
        assert rolled["spike_count"] == int(saved["spike_count"])
        assert rolled["eligible_observations"] == int(saved["eligible_observations"])
        assert rolled["mean_valid_value"] == pytest.approx(float(saved["mean_valid_value"]), rel=1e-6, abs=1e-4)
    assert set(agra_daily["city_name"]) == {"Agra"}
    assert "Patna" not in set(portal.daily.loc[portal.daily["city_name"].eq("Agra"), "city_name"])
    assert portal.report["method"]["national_daily_used"] is False
    assert portal.report["method"]["spikes_rescored"] is False
    assert portal.report["city_pollutant_match"] is True


def _places() -> tuple[pd.DataFrame, pd.DataFrame]:
    stations = pd.DataFrame(
        {
            "station_id": ["delhi-a", "delhi-a", "delhi-b", "patna", "blank", "conflict"],
            "parameter_name": ["PM2.5", "PM10", "PM2.5", "PM2.5", "PM2.5", "PM2.5"],
            "city_name": ["Delhi", "Delhi", "Delhi", "Patna", None, None],
            "state_name": ["Delhi", "Delhi", "Delhi", "Bihar", None, None],
            "city_conflict": [False, False, False, False, False, True],
            "latitude": [28.6, 28.6, None, 25.6, None, None],
            "longitude": [77.2, 77.2, None, 85.1, None, None],
            "observations": [3, 1, 1, 9, 1, 2],
            "valid_observations": [3, 1, 1, 9, 1, 2],
            "mean_valid_value": [20.0, 50.0, 20.0, 100.0, 5.0, 8.0],
            "spike_count": [1, 1, 0, 1, 0, 0],
            "eligible_observations": [3, 1, 1, 9, 1, 2],
            "spike_rate_of_eligible": [1 / 3, 1.0, 0.0, 1 / 9, 0.0, 0.0],
        }
    )
    metrics = pd.DataFrame(
        {
            "city_name": ["Delhi", "Delhi", "Patna"],
            "state_name": ["Delhi", "Delhi", "Bihar"],
            "parameter_name": ["PM2.5", "PM10", "PM2.5"],
            "observations": [4, 1, 9],
            "spike_count": [1, 1, 1],
            "eligible_observations": [4, 1, 9],
            "mean_valid_value": [25.0, 50.0, 100.0],
        }
    )
    return stations, metrics


def _daily() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "city_name": ["Delhi", "Delhi", "Delhi", "Patna"],
            "state_name": ["Delhi", "Delhi", "Delhi", "Bihar"],
            "observation_date": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-01", "2024-01-01"]),
            "parameter_name": ["PM2.5", "PM2.5", "PM10", "PM2.5"],
            "observations": [3, 1, 1, 9],
            "valid_observations": [3, 1, 1, 9],
            "mean_valid_value": [20.0, 40.0, 50.0, 100.0],
            "spike_count": [1, 0, 1, 1],
            "eligible_observations": [3, 1, 1, 9],
            "spike_rate_of_observations": [1 / 3, 0.0, 1.0, 1 / 9],
            "spike_rate_of_eligible": [1 / 3, 0.0, 1.0, 1 / 9],
        }
    )


def test_portal_user_file_is_json() -> None:
    document = json.loads((PROJECT_ROOT / "config" / "portal_users.json").read_text(encoding="utf-8"))
    assert document["algorithm"] == "pbkdf2_hmac_sha256"
    assert document["users"][0]["email"] == "analyst@aeroscope.local"
