"""Read saved analytics outputs for the dashboard.

The loaders return the pre-aggregated tables and the report summary. They
do not start Spark, do not rescore spikes, and do not write under
``data/raw`` or ``data/processed``.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

_TABLES = (
    "hourly_by_pollutant",
    "daily_by_pollutant",
    "hour_of_day",
    "station_pollutant",
    "station_comparison",
    "city_pollutant",
    "state_pollutant",
    "elevated_stations",
)
_REPORT_KEYS = (
    "generated_at",
    "timezone",
    "denominators",
    "parameters",
    "counts",
    "coverage",
    "pollutant_totals",
)
_COUNT_KEYS = (
    "total_records",
    "valid_measurements",
    "eligible_records",
    "spike_records",
    "null_timestamps",
    "spike_rate_of_observations",
    "spike_rate_of_eligible",
)
_PARAMETER_KEYS = (
    "minimum_baseline",
    "elevated_minimum_valid_observations",
    "elevated_percentile",
    "unusual_sigmas",
    "eligible_rule",
)
_COVERAGE_KEYS = (
    "stations",
    "stations_without_city",
    "stations_without_state",
    "stations_without_coordinates",
    "stations_with_coordinates",
    "stations_with_city_conflict",
    "stations_with_coordinate_conflict",
    "rows_without_city",
    "rows_without_state",
    "rows_without_latitude",
    "rows_without_longitude",
    "eligible_rows_without_city",
    "eligible_rows_without_coordinates",
    "rows_in_city_aggregates",
    "distinct_city_names",
    "distinct_state_names",
    "note",
)
_FLAG_COLUMNS = (
    "unusually_high_spike_count",
    "unusually_high_eligible_rate",
    "consistently_elevated",
    "city_conflict",
    "state_conflict",
    "coordinate_conflict",
)
_SPIKE_COLUMNS = (
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
)


class DashboardDataError(Exception):
    """A saved analytics file is missing or does not have the expected fields."""


@dataclass(frozen=True)
class CityPortal:
    """City-specific series. These tables are not the national daily or hourly aggregates."""

    report: dict[str, Any]
    daily: pd.DataFrame
    hourly: pd.DataFrame
    hour_of_day: pd.DataFrame


@dataclass(frozen=True)
class AnalyticsBundle:
    """Small tables the dashboard can filter without rereading measurements."""

    root: Path
    report: dict[str, Any]
    hourly: pd.DataFrame
    daily: pd.DataFrame
    hour_of_day: pd.DataFrame
    stations: pd.DataFrame
    comparison: pd.DataFrame
    cities: pd.DataFrame
    states: pd.DataFrame
    elevated: pd.DataFrame
    spike_events: pd.DataFrame
    annotated_rows: int
    spike_rows: int


def repository_root() -> Path:
    return Path(__file__).resolve().parents[2]


def load_bundle(root: Path) -> AnalyticsBundle:
    """Load the report, aggregate tables, spike rows, and file row counts."""

    root = root.resolve()
    report = load_report(root / "data" / "interim" / "analytics_report.json")
    analytics = root / "data" / "processed" / "analytics"
    tables = {name: _read_aggregate(analytics / name / "data.parquet") for name in _TABLES}
    annotated = root / "data" / "processed" / "spikes" / "annotated" / "data.parquet"
    spikes = root / "data" / "processed" / "spikes" / "spikes_only" / "data.parquet"
    return AnalyticsBundle(
        root=root,
        report=report,
        hourly=_normalize(tables["hourly_by_pollutant"], date_columns=("observation_date",)),
        daily=_normalize(tables["daily_by_pollutant"], date_columns=("observation_date",)),
        hour_of_day=_normalize(tables["hour_of_day"]),
        stations=_normalize(tables["station_pollutant"]),
        comparison=_normalize(tables["station_comparison"]),
        cities=_normalize(tables["city_pollutant"]),
        states=_normalize(tables["state_pollutant"]),
        elevated=_normalize(tables["elevated_stations"]),
        spike_events=_read_spike_events(spikes),
        annotated_rows=parquet_row_count(annotated),
        spike_rows=parquet_row_count(spikes),
    )


def load_report(path: Path) -> dict[str, Any]:
    """Read the summary fields from the analytics report."""

    if not path.is_file():
        raise DashboardDataError(
            f"Analytics report not found: {path}. Run analyze before opening the dashboard."
        )
    document = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(document, dict):
        raise DashboardDataError(f"Analytics report is not a JSON object: {path}")
    missing = [key for key in _REPORT_KEYS if key not in document]
    if missing:
        raise DashboardDataError("Analytics report is missing: " + ", ".join(missing))
    _require_keys(document["counts"], _COUNT_KEYS, "counts")
    _require_keys(document["parameters"], _PARAMETER_KEYS, "parameters")
    _require_keys(document["coverage"], _COVERAGE_KEYS, "coverage")
    if not isinstance(document["pollutant_totals"], list):
        raise DashboardDataError("Analytics report field pollutant_totals must be a list.")
    return {key: document[key] for key in _REPORT_KEYS}


def load_city_portal(root: Path) -> CityPortal:
    """Load the city daily, hourly, and hour-of-day Parquet files."""

    root = root.resolve()
    report_path = root / "data" / "interim" / "city_portal_report.json"
    portal = root / "data" / "processed" / "city_portal"
    if not report_path.is_file():
        raise DashboardDataError(
            "City series report not found. Run "
            "`.\\.venv\\Scripts\\python.exe main.py build-city-series` before opening the portal."
        )
    document = json.loads(report_path.read_text(encoding="utf-8"))
    if not isinstance(document, dict) or "counts" not in document or "method" not in document:
        raise DashboardDataError(f"City series report is missing counts or method: {report_path}")
    if document["method"].get("national_daily_used") is not False:
        raise DashboardDataError("City series report does not say that national daily aggregates were left unused.")
    return CityPortal(
        report={key: document[key] for key in ("generated_at", "timezone", "method", "counts", "city_pollutant_match")},
        daily=_normalize(_read_city_table(portal / "city_daily" / "data.parquet"), date_columns=("observation_date",)),
        hourly=_normalize(
            _read_city_table(portal / "city_hourly" / "data.parquet"),
            date_columns=("observation_date",),
        ),
        hour_of_day=_normalize(_read_city_table(portal / "city_hour_of_day" / "data.parquet")),
    )


def _read_city_table(path: Path) -> pd.DataFrame:
    if not path.is_file():
        raise DashboardDataError(
            f"City series Parquet not found: {path}. Run build-city-series before opening the portal."
        )
    return pd.read_parquet(path)


def parquet_row_count(path: Path) -> int:
    """Return the Parquet row count from file metadata, without reading the columns."""

    if not path.is_file():
        raise DashboardDataError(f"Parquet file not found: {path}")
    return int(pq.ParquetFile(path).metadata.num_rows)


def _read_aggregate(path: Path) -> pd.DataFrame:
    if not path.is_file():
        raise DashboardDataError(
            f"Aggregate Parquet not found: {path}. Run analyze before opening the dashboard."
        )
    return pd.read_parquet(path)


def _read_spike_events(path: Path) -> pd.DataFrame:
    if not path.is_file():
        raise DashboardDataError(f"Spike Parquet not found: {path}. Run detect-spikes before opening the dashboard.")
    schema_names = set(pq.ParquetFile(path).schema_arrow.names)
    missing = [name for name in _SPIKE_COLUMNS if name not in schema_names]
    if missing:
        raise DashboardDataError(
            "Spike Parquet is missing columns: "
            + ", ".join(missing)
            + ". Actual columns: "
            + ", ".join(sorted(schema_names))
        )
    frame = pd.read_parquet(path, columns=list(_SPIKE_COLUMNS))
    return _normalize(frame, date_columns=("collected_at",))


def _normalize(frame: pd.DataFrame, date_columns: tuple[str, ...] = ()) -> pd.DataFrame:
    normalized = frame.copy()
    for column in date_columns:
        if column in normalized.columns:
            normalized[column] = pd.to_datetime(normalized[column])
    if "observation_hour" in normalized.columns:
        normalized["observation_hour"] = normalized["observation_hour"].astype("int64")
    for column in _FLAG_COLUMNS:
        if column in normalized.columns:
            normalized[column] = normalized[column].fillna(False).astype(bool)
    return normalized


def _require_keys(value: object, keys: tuple[str, ...], label: str) -> None:
    if not isinstance(value, dict):
        raise DashboardDataError(f"Analytics report field {label} must be an object.")
    missing = [key for key in keys if key not in value]
    if missing:
        raise DashboardDataError(f"Analytics report {label} is missing: " + ", ".join(missing))
