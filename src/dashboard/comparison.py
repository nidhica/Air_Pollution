"""Compare two or three saved cities without reading national aggregates.

Means, spike counts, and eligible rates stay inside one pollutant. A missing
pollutant stays missing. A zero count is kept only when that pollutant series
exists. These functions do not rescore measurements.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from dashboard.cities import city_period, city_rows, city_station_ids, pollutant_summaries
from dashboard.filters import filter_pollutant, pool_hours

POLLUTANTS = ("PM2.5", "PM10")
OPTIONAL_CITY = "None"
_PREFIX = {"PM2.5": "pm25", "PM10": "pm10"}


def place_label(city_name: str, state_name: str | None) -> str:
    """Show the city and state together so identical city names stay distinct."""

    state = state_name if state_name else "State not recorded"
    return f"{city_name}, {state}"


def places_from_cards(cards: list[dict[str, Any]]) -> list[tuple[str, str | None]]:
    return [(str(card["city_name"]), card["state_name"]) for card in cards]


def dropdown_options(
    labels: list[str],
    current: list[str | None],
    index: int,
    *,
    optional: bool = False,
) -> list[str]:
    """Labels still available for one dropdown. The same city-state cannot be chosen twice."""

    taken = {
        value
        for position, value in enumerate(current)
        if position != index and value not in (None, "", OPTIONAL_CITY)
    }
    available = [label for label in labels if label not in taken]
    if optional:
        return [OPTIONAL_CITY, *available]
    return available


def selected_places(
    labels: list[str],
    places: list[tuple[str, str | None]],
    chosen: list[str | None],
) -> list[tuple[str, str | None]]:
    """Resolve dropdown labels to city-state pairs. The optional blank choice is omitted."""

    lookup = dict(zip(labels, places))
    resolved = []
    for label in chosen:
        if label in (None, "", OPTIONAL_CITY):
            continue
        if label not in lookup:
            continue
        resolved.append(lookup[label])
    return resolved


def restore_saved_value(state: dict[str, Any], key: str, value: object) -> None:
    """Restore a control after Streamlit drops it. An existing value stays in place."""

    if key not in state and value is not None:
        state[key] = value


def selection_error(places: list[tuple[str, str | None]]) -> str | None:
    """Return a message when the selection is outside two or three distinct cities."""

    if len(places) < 2:
        return "Select at least two cities."
    if len(places) > 3:
        return "Select at most three cities."
    if len(places) != len(set(places)):
        return "Select each city and state combination once."
    return None


def comparison_rows(
    daily: pd.DataFrame,
    places: list[tuple[str, str | None]],
    stations: pd.DataFrame,
    city_metrics: pd.DataFrame,
    start: object,
    end: object,
) -> list[dict[str, Any]]:
    """One row per selected city. Pollutant averages are never mixed."""

    rows = []
    for city_name, state_name in places:
        known = _known_pollutants(city_metrics, daily, city_name, state_name)
        summaries = pollutant_summaries(city_period(daily, city_name, state_name, start, end))
        row: dict[str, Any] = {
            "label": place_label(city_name, state_name),
            "city_name": city_name,
            "state_name": state_name,
            "stations": len(city_station_ids(stations, city_name, state_name)),
        }
        observed = 0
        saw_series = False
        for pollutant in POLLUTANTS:
            prefix = _PREFIX[pollutant]
            if pollutant not in known:
                row[f"{prefix}_mean"] = None
                row[f"{prefix}_observations"] = None
                row[f"{prefix}_spike_count"] = None
                row[f"{prefix}_eligible"] = None
                row[f"{prefix}_rate"] = None
                row[f"{prefix}_status"] = "missing"
                continue
            saw_series = True
            summary = summaries.get(pollutant)
            if summary is None:
                row[f"{prefix}_mean"] = None
                row[f"{prefix}_observations"] = 0
                row[f"{prefix}_spike_count"] = 0
                row[f"{prefix}_eligible"] = 0
                row[f"{prefix}_rate"] = None
                row[f"{prefix}_status"] = "no rows in range"
                continue
            row[f"{prefix}_mean"] = summary["mean_valid_value"]
            row[f"{prefix}_observations"] = int(summary["observations"])
            row[f"{prefix}_spike_count"] = int(summary["spike_count"])
            row[f"{prefix}_eligible"] = int(summary["eligible_observations"])
            row[f"{prefix}_rate"] = summary["spike_rate_of_eligible"]
            row[f"{prefix}_status"] = "available"
            observed += int(summary["observations"])
        row["total_observations"] = observed if saw_series else None
        rows.append(row)
    return rows


def metric_frame(rows: list[dict[str, Any]], field: str) -> pd.DataFrame:
    """Long chart frame. Missing pollutant values are left out rather than drawn as zero."""

    records = []
    for row in rows:
        for pollutant in POLLUTANTS:
            value = row.get(f"{_PREFIX[pollutant]}_{field}")
            if value is None or pd.isna(value):
                continue
            records.append({"label": row["label"], "parameter_name": pollutant, "value": value})
    return pd.DataFrame(records)


def daily_lines(
    daily: pd.DataFrame,
    places: list[tuple[str, str | None]],
    pollutant: str,
    start: object,
    end: object,
) -> pd.DataFrame:
    """Daily city series for one pollutant. Rows from other cities are excluded."""

    frames = []
    for city_name, state_name in places:
        part = city_period(daily, city_name, state_name, start, end, pollutant)
        if part.empty:
            continue
        copied = part.copy()
        copied["city_label"] = place_label(city_name, state_name)
        frames.append(copied)
    if not frames:
        return pd.DataFrame(columns=["observation_date", "city_label", "parameter_name", "mean_valid_value"])
    return pd.concat(frames, ignore_index=True)


def hourly_lines(
    hourly: pd.DataFrame,
    hour_of_day: pd.DataFrame,
    places: list[tuple[str, str | None]],
    pollutant: str,
    start: object,
    end: object,
    full_start: object,
    full_end: object,
) -> pd.DataFrame:
    """Hour-of-day means for the same date range in every selected city."""

    use_saved_pattern = _same_day(start, full_start) and _same_day(end, full_end)
    frames = []
    for city_name, state_name in places:
        if use_saved_pattern:
            part = filter_pollutant(city_rows(hour_of_day, city_name, state_name), pollutant)
        else:
            window = city_period(hourly, city_name, state_name, start, end, pollutant)
            part = pool_hours(window)
        if part.empty:
            continue
        copied = part.copy()
        copied["city_label"] = place_label(city_name, state_name)
        frames.append(copied)
    if not frames:
        return pd.DataFrame(columns=["observation_hour", "city_label", "parameter_name", "mean_valid_value"])
    return pd.concat(frames, ignore_index=True)


def comparison_insights(rows: list[dict[str, Any]]) -> list[str]:
    """Rank saved averages, eligible rates, and station coverage separately."""

    return [
        _highest(rows, "pm25_mean", "PM2.5 average concentration", suffix=" µg/m³"),
        _highest(rows, "pm10_mean", "PM10 average concentration", suffix=" µg/m³"),
        _highest(rows, "pm25_rate", "PM2.5 eligible spike rate", percent=True),
        _highest(rows, "pm10_rate", "PM10 eligible spike rate", percent=True),
        _highest(rows, "stations", "monitoring station coverage", suffix=" stations", integer=True),
        (
            "Average concentration, eligible spike rate, and station coverage are reported as separate measures. "
            "Candidate spike counts stay on their own chart and are not turned into a city ranking."
        ),
    ]


def display_table(rows: list[dict[str, Any]]) -> pd.DataFrame:
    """Readable comparison table. Missing measures stay blank of a fabricated zero."""

    records = []
    for row in rows:
        records.append(
            {
                "City": row["label"],
                "Monitoring stations": row["stations"],
                "PM2.5 average (µg/m³)": _show_number(row["pm25_mean"]),
                "PM10 average (µg/m³)": _show_number(row["pm10_mean"]),
                "PM2.5 observations": _show_count(row["pm25_observations"]),
                "PM10 observations": _show_count(row["pm10_observations"]),
                "PM2.5 candidate spikes": _show_count(row["pm25_spike_count"]),
                "PM10 candidate spikes": _show_count(row["pm10_spike_count"]),
                "PM2.5 eligible observations": _show_count(row["pm25_eligible"]),
                "PM10 eligible observations": _show_count(row["pm10_eligible"]),
                "PM2.5 eligible spike rate": _show_percent(row["pm25_rate"]),
                "PM10 eligible spike rate": _show_percent(row["pm10_rate"]),
                "Total observations": _show_count(row["total_observations"]),
            }
        )
    return pd.DataFrame(records)


def _known_pollutants(
    city_metrics: pd.DataFrame,
    daily: pd.DataFrame,
    city_name: str,
    state_name: str | None,
) -> set[str]:
    metrics = city_rows(city_metrics, city_name, state_name)
    if not metrics.empty and "parameter_name" in metrics.columns:
        return {str(value) for value in metrics["parameter_name"].dropna().unique()}
    series = city_rows(daily, city_name, state_name)
    if series.empty or "parameter_name" not in series.columns:
        return set()
    return {str(value) for value in series["parameter_name"].dropna().unique()}


def _highest(
    rows: list[dict[str, Any]],
    field: str,
    label: str,
    *,
    suffix: str = "",
    percent: bool = False,
    integer: bool = False,
) -> str:
    usable = []
    missing = []
    for row in rows:
        value = row.get(field)
        if value is None or pd.isna(value):
            missing.append(row["label"])
            continue
        usable.append((row["label"], value))
    if not usable:
        return f"No selected city has a saved {label} in this date range."
    best = max(value for _, value in usable)
    winners = [name for name, value in usable if value == best]
    rendered = _show_percent(best) if percent else (str(int(best)) if integer else f"{float(best):.2f}")
    names = _name_list(winners)
    verb = "have" if len(winners) > 1 else "has"
    text = f"{names} {verb} the highest {label}: {rendered}{'' if percent else suffix}."
    if missing:
        missing_verb = "have" if len(missing) > 1 else "has"
        text += f" {_name_list(missing)} {missing_verb} no saved value for this measure."
    return text


def _name_list(names: list[str]) -> str:
    if len(names) == 1:
        return names[0]
    if len(names) == 2:
        return f"{names[0]} and {names[1]}"
    return ", ".join(names[:-1]) + f", and {names[-1]}"


def _same_day(left: object, right: object) -> bool:
    return pd.Timestamp(left).normalize() == pd.Timestamp(right).normalize()


def _show_number(value: object) -> str:
    if value is None or pd.isna(value):
        return "not available"
    return f"{float(value):,.2f}"


def _show_count(value: object) -> str:
    if value is None or pd.isna(value):
        return "not available"
    return f"{int(value):,}"


def _show_percent(value: object) -> str:
    if value is None or pd.isna(value):
        return "not available"
    return f"{float(value):.2%}"
