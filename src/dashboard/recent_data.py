"""Load prepared data.gov.in observations for the dashboard.

This module reads the Spark output only. It does not call the API and does
not read the XKDR measurement tables.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from dashboard.loading import DashboardDataError


@dataclass(frozen=True)
class RecentRecords:
    observations: pd.DataFrame
    coverage: pd.DataFrame
    report: dict[str, object]


def recent_paths(root: Path) -> tuple[Path, Path, Path]:
    base = root / "data" / "processed" / "data_gov_in"
    report = root / "data" / "interim" / "data_gov_in" / "prepare_report.json"
    return base / "observations" / "data.parquet", base / "series_coverage" / "data.parquet", report


def load_recent(root: Path) -> RecentRecords | None:
    """Return the prepared snapshot, or None when that output has not been built."""

    observations_path, coverage_path, report_path = recent_paths(root)
    if not observations_path.is_file() or not coverage_path.is_file() or not report_path.is_file():
        return None
    try:
        report = json.loads(report_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise DashboardDataError(f"data.gov.in preparation report is not valid JSON: {report_path}") from exc
    if report.get("spike_algorithm_applied") is not False:
        raise DashboardDataError("The data.gov.in preparation report does not confirm that spikes were left unscored.")
    if report.get("aqi_field_created") is not False:
        raise DashboardDataError("The data.gov.in preparation report created an AQI field.")
    observations = pd.read_parquet(observations_path)
    coverage = pd.read_parquet(coverage_path)
    if "avg_value" not in observations.columns or "observed_at" not in observations.columns:
        raise DashboardDataError("Prepared data.gov.in observations are missing the reported value or timestamp.")
    return RecentRecords(observations=observations, coverage=coverage, report=report)


def latest_moment(frame: pd.DataFrame) -> pd.Timestamp | None:
    if frame.empty or frame["observed_at"].isna().all():
        return None
    return pd.Timestamp(frame["observed_at"].max())


def latest_retrieval(frame: pd.DataFrame) -> str | None:
    if frame.empty or "retrieved_at" not in frame.columns:
        return None
    values = sorted(str(value) for value in frame["retrieved_at"].dropna().unique())
    return values[-1] if values else None


PROJECT_POLLUTANTS = ("PM2.5", "PM10")


def project_pollutants(frame: pd.DataFrame) -> pd.DataFrame:
    """Copy the rows whose pollutant_id is PM2.5 or PM10.

    Other stored pollutant ids are omitted. The caller's frame is not changed.
    """

    if frame.empty or "pollutant_id" not in frame.columns:
        return frame.iloc[0:0].copy()
    selected = frame.loc[frame["pollutant_id"].isin(PROJECT_POLLUTANTS)]
    return selected.copy()


def pollutant_choices(stored: object, *, all_label: str = "All") -> list[str]:
    """Offer All plus the project pollutants that are actually stored."""

    names = sorted({str(value) for value in pd.Series(stored).dropna().tolist() if str(value) in PROJECT_POLLUTANTS})
    return [all_label, *names]


def filtered_observations(
    frame: pd.DataFrame,
    *,
    state: str,
    city: str,
    station: str | None,
    pollutant: str,
) -> pd.DataFrame:
    rows = frame.loc[frame["state"].eq(state) & frame["city"].eq(city) & frame["pollutant_id"].eq(pollutant)]
    if station:
        rows = rows.loc[rows["station"].eq(station)]
    return rows.copy()


def at_timestamp(frame: pd.DataFrame, moment: pd.Timestamp | None) -> pd.DataFrame:
    if moment is None or frame.empty:
        return frame.iloc[0:0].copy()
    stamps = pd.to_datetime(frame["observed_at"])
    return frame.loc[stamps.eq(pd.Timestamp(moment))].copy()


def trend_frame(frame: pd.DataFrame) -> pd.DataFrame | None:
    """Return one station-pollutant series when at least two API timestamps are stored.

    The time column is the parsed API ``last_update``. A repeated retrieval of
    the same timestamp is one point, even when the local retrieval time differs.
    """

    if frame.empty or "observed_at" not in frame.columns or "station" not in frame.columns:
        return None
    if frame["station"].nunique(dropna=True) != 1:
        return None
    if "pollutant_id" in frame.columns and frame["pollutant_id"].nunique(dropna=True) != 1:
        return None
    usable = frame.loc[frame["observed_at"].notna()].sort_values("observed_at")
    usable = usable.drop_duplicates(subset=["observed_at"], keep="first")
    if len(usable) < 2:
        return None
    return usable.reset_index(drop=True)


@dataclass(frozen=True)
class RecentPeriod:
    """Stored data.gov.in observations inside one selected period.

    ``kind`` is ``empty`` when no non-null ``avg_value`` is stored, ``snapshot``
    when those values share one ``observed_at``, and ``trend`` when two or more
    timestamps are stored. Points are the mean of the non-null station
    averages. Null averages are omitted, not replaced with zero. Missing
    timestamps are not inserted.
    """

    kind: str
    observations: int
    stations: int
    distinct_timestamps: int
    distinct_dates: int
    first_observed: pd.Timestamp | None
    last_observed: pd.Timestamp | None
    points: pd.DataFrame


def recent_period(frame: pd.DataFrame) -> RecentPeriod:
    """Summarise one filtered frame without filling absent timestamps."""

    empty_points = pd.DataFrame(columns=["observed_at", "pollutant_id", "avg_value", "station_count", "observation_count"])
    frame = project_pollutants(frame)
    if frame.empty or "observed_at" not in frame.columns:
        return RecentPeriod("empty", 0, 0, 0, 0, None, None, empty_points)
    stamps = pd.to_datetime(frame["observed_at"], errors="coerce")
    dated = frame.loc[stamps.notna()].copy()
    if dated.empty or "avg_value" not in dated.columns:
        return RecentPeriod("empty", 0, 0, 0, 0, None, None, empty_points)
    dated["observed_at"] = pd.to_datetime(dated["observed_at"])
    dated["avg_value"] = pd.to_numeric(dated["avg_value"], errors="coerce")
    usable = dated.loc[dated["avg_value"].notna()].copy()
    if usable.empty:
        return RecentPeriod("empty", 0, 0, 0, 0, None, None, empty_points)
    timestamps = usable["observed_at"].drop_duplicates().sort_values()
    dates = timestamps.dt.normalize().drop_duplicates()
    grouped = (
        usable.groupby(["observed_at", "pollutant_id"], dropna=False)
        .agg(
            avg_value=("avg_value", "mean"),
            station_count=("station", "nunique"),
            observation_count=("avg_value", "size"),
        )
        .reset_index()
        .sort_values(["pollutant_id", "observed_at"])
    )
    kind = "trend" if len(timestamps) >= 2 else "snapshot"
    return RecentPeriod(
        kind=kind,
        observations=int(len(usable)),
        stations=int(usable["station"].nunique(dropna=True)) if "station" in usable.columns else 0,
        distinct_timestamps=int(len(timestamps)),
        distinct_dates=int(len(dates)),
        first_observed=pd.Timestamp(timestamps.iloc[0]),
        last_observed=pd.Timestamp(timestamps.iloc[-1]),
        points=grouped.reset_index(drop=True),
    )


def recent_period_text(period: RecentPeriod, *, selected_days: int, unit: str) -> str:
    """Describe the stored dates and the source of the values."""

    if period.kind == "empty" or period.first_observed is None or period.last_observed is None:
        return (
            "No valid data.gov.in averages are stored inside the selected dates. "
            "Null averages were not replaced with zero. Missing dates were not filled."
        )
    span = period.first_observed.strftime("%Y-%m-%d %H:%M")
    if period.last_observed != period.first_observed:
        span = f"{span} to {period.last_observed.strftime('%Y-%m-%d %H:%M')}"
    return (
        f"Source: data.gov.in real-time observations. "
        f"{period.observations:,} valid observations from {period.stations:,} contributing stations. "
        f"{period.distinct_timestamps:,} distinct observation timestamps on "
        f"{period.distinct_dates:,} dates ({span}). "
        f"The selected period has {selected_days:,} days. "
        "Dates without a stored last_update are omitted and were not interpolated. "
        "These values are not CPCB CAAQM measurements. "
        f"Unit: {unit}."
    )


def trend_unavailable_message(frame: pd.DataFrame, pollutant: str) -> str:
    """Explain why one station and pollutant cannot be drawn as a trend."""

    count = 0
    if not frame.empty and "observed_at" in frame.columns:
        count = int(frame["observed_at"].nunique(dropna=True))
    subject = f"this station and {pollutant}" if pollutant else "this station and pollutant"
    if count <= 0:
        return (
            f"No parsed API last_update is stored for {subject}. "
            "A trend needs at least two distinct source timestamps."
        )
    if count == 1:
        return (
            f"One API timestamp is stored for {subject}. "
            "Another source update, with a different last_update, is needed before a trend can be shown. "
            "Fetching the same hour again does not add a point."
        )
    return (
        f"{count} API timestamps are stored, but they do not belong to one station and pollutant. "
        "Choose one station and pollutant."
    )


def pipeline_status_path(root: Path) -> Path:
    return root / "logs" / "data_gov_automation" / "status.json"


def load_pipeline_status(root: Path) -> dict[str, object] | None:
    """Read the last automation record. Missing or unreadable files stay empty."""

    path = pipeline_status_path(root)
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def pipeline_status_text(status: dict[str, object] | None) -> str:
    """Describe the last automatic run without calling the API or Spark."""

    if not status:
        return (
            "No automatic hourly run has been recorded. "
            "Refreshing this page reads the prepared file and does not call data.gov.in."
        )
    fetch = _step_label(status.get("fetch_ran"), status.get("fetch_exit_code"))
    prepare = _step_label(status.get("prepare_ran"), status.get("prepare_exit_code"))
    success = status.get("last_success_at") or "none recorded"
    finished = status.get("finished_at") or "unknown"
    return (
        f"Last automatic run finished {finished}. Fetch {fetch}. Preparation {prepare}. "
        f"Last successful run: {success}. "
        "API last_update is the source observation time. "
        "Local retrieval time is when AEROSCOPE stored the response. "
        "An hourly run can repeat the same source hour and add no new observations."
    )


def _step_label(ran: object, code: object) -> str:
    if ran is not True:
        return "not started"
    if code == 0:
        return "succeeded"
    return f"failed (exit {code})"
