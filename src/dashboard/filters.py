"""Filter and roll up the small analytics tables.

These functions do not read files and do not fill missing city, state, or
coordinate values.
"""

from __future__ import annotations

import pandas as pd


def filter_pollutant(frame: pd.DataFrame, pollutant: str | None, column: str = "parameter_name") -> pd.DataFrame:
    """Keep one pollutant. An empty selection returns every row."""

    if pollutant in (None, "", "All"):
        return frame.copy()
    return frame.loc[frame[column].eq(pollutant)].copy()


def filter_date_range(frame: pd.DataFrame, column: str, start: object, end: object) -> pd.DataFrame:
    """Keep rows whose timestamp or date is inside the inclusive range."""

    if frame.empty:
        return frame.copy()
    values = pd.to_datetime(frame[column]).dt.normalize()
    start_ts = pd.Timestamp(start).normalize()
    end_ts = pd.Timestamp(end).normalize()
    if start_ts > end_ts:
        start_ts, end_ts = end_ts, start_ts
    return frame.loc[(values >= start_ts) & (values <= end_ts)].copy()


def filter_place(
    frame: pd.DataFrame,
    *,
    state: str | None = None,
    city: str | None = None,
    station_id: str | None = None,
) -> pd.DataFrame:
    """Apply place filters that were actually selected.

    A selected city matches that city name only. Rows with a missing city
    stay out of a named city filter.
    """

    result = frame
    if state not in (None, "", "All") and "state_name" in result.columns:
        result = result.loc[result["state_name"].eq(state)]
    if city not in (None, "", "All") and "city_name" in result.columns:
        result = result.loc[result["city_name"].eq(city)]
    if station_id not in (None, "", "All") and "station_id" in result.columns:
        result = result.loc[result["station_id"].eq(station_id)]
    return result.copy()


def located_stations(frame: pd.DataFrame) -> pd.DataFrame:
    """Return stations that have both a latitude and a longitude."""

    return frame.loc[frame["latitude"].notna() & frame["longitude"].notna()].copy()


def unlocated_stations(frame: pd.DataFrame) -> pd.DataFrame:
    """Return stations that cannot be mapped because a coordinate is missing."""

    return frame.loc[frame["latitude"].isna() | frame["longitude"].isna()].copy()


def period_summary(frame: pd.DataFrame) -> dict[str, float | int | None]:
    """Sum a date or hour partition and weight the mean by valid observations."""

    if frame.empty:
        return {
            "observations": 0,
            "valid_observations": 0,
            "mean_valid_value": None,
            "spike_count": 0,
            "eligible_observations": 0,
            "spike_rate_of_observations": None,
            "spike_rate_of_eligible": None,
        }
    observations = int(frame["observations"].sum())
    valid = int(frame["valid_observations"].sum())
    spikes = int(frame["spike_count"].sum())
    eligible = int(frame["eligible_observations"].sum())
    return {
        "observations": observations,
        "valid_observations": valid,
        "mean_valid_value": _weighted_mean(frame),
        "spike_count": spikes,
        "eligible_observations": eligible,
        "spike_rate_of_observations": _rate(spikes, observations),
        "spike_rate_of_eligible": _rate(spikes, eligible),
    }


def pool_hours(hourly: pd.DataFrame) -> pd.DataFrame:
    """Collapse date-hour aggregates to an hour-of-day pattern."""

    rows: list[dict[str, object]] = []
    if hourly.empty:
        return pd.DataFrame(rows)
    grouped = hourly.groupby(["observation_hour", "parameter_name"], dropna=False, sort=True)
    for (hour, pollutant), part in grouped:
        rows.append(
            {
                "observation_hour": int(hour),
                "parameter_name": pollutant,
                **period_summary(part),
            }
        )
    return pd.DataFrame(rows)


def top_by(frame: pd.DataFrame, column: str, limit: int = 15) -> pd.DataFrame:
    """Return the largest values. Missing rates sort after measured rates."""

    if frame.empty:
        return frame.copy()
    ordered = frame.sort_values(
        [column, "eligible_observations", "station_id"],
        ascending=[False, False, True],
        na_position="last",
    )
    return ordered.head(limit).copy()


def comparison_columns(pollutant: str) -> list[str]:
    """Column names for one pollutant on the wide station-comparison table."""

    prefix = pollutant.replace(".", "_").replace(" ", "_")
    return [
        f"{prefix}_mean_valid_value",
        f"{prefix}_valid_observations",
        f"{prefix}_spike_count",
        f"{prefix}_eligible_observations",
        f"{prefix}_spike_rate_of_eligible",
    ]


def named_choices(frame: pd.DataFrame, column: str) -> list[str]:
    """Sorted non-null values for a select box. Missing labels are omitted."""

    if column not in frame.columns:
        return []
    values = [str(value) for value in frame[column].dropna().unique()]
    return sorted(set(values))


def _weighted_mean(frame: pd.DataFrame) -> float | None:
    valid = frame["valid_observations"].fillna(0)
    total = float(valid.sum())
    if total == 0:
        return None
    means = frame["mean_valid_value"]
    usable = valid.gt(0) & means.notna()
    if float(valid[usable].sum()) != total:
        return None
    return float((means[usable] * valid[usable]).sum() / total)


def _rate(part: int, whole: int) -> float | None:
    if whole == 0:
        return None
    return part / whole
