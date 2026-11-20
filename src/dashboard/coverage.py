"""Describe which months are stored, processed, or still absent.

This reads file names and prepared daily dates. It does not download data,
does not start Spark, and does not treat a missing file as proof that the
publisher has no archive for that month.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

AVAILABLE_REMOTELY = "available_remotely"
DOWNLOADED_NOT_PROCESSED = "downloaded_not_processed"
FAILED = "failed"
INCOMPLETE = "incomplete"
NOT_DOWNLOADED = "not_downloaded"
PROCESSED = "processed"
PARTIAL = "partial"
UNAVAILABLE_REMOTELY = "unavailable_remotely"

_STATUS_LABELS = {
    PROCESSED: "Processed",
    PARTIAL: "Processed, partial month",
    DOWNLOADED_NOT_PROCESSED: "Downloaded, not in the prepared series",
    AVAILABLE_REMOTELY: "Available remotely, not downloaded",
    UNAVAILABLE_REMOTELY: "Unavailable from the source",
    FAILED: "Download failed",
    INCOMPLETE: "Incomplete download",
    NOT_DOWNLOADED: "Not downloaded",
}


@dataclass(frozen=True)
class MonthCoverage:
    year: int
    month: int
    status: str
    days_with_observations: int
    days_in_month: int

    @property
    def label(self) -> str:
        return _STATUS_LABELS[self.status]


def remote_month_status(root: Path) -> dict[tuple[int, int], str]:
    """Read confirmed remote statuses from the acquisition manifest.

    A manifest written before the file list was retrieved contributes nothing.
    Unknown remote availability stays out of this map so the month remains
    "not downloaded" rather than "unavailable."
    """

    path = root / "data" / "interim" / "xkdr_acquisition_manifest.json"
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    if not isinstance(payload, dict) or payload.get("remote_listing") != "retrieved":
        return {}
    months = payload.get("months")
    if not isinstance(months, list):
        return {}
    found: dict[tuple[int, int], str] = {}
    for item in months:
        if not isinstance(item, dict):
            continue
        year = item.get("year")
        month = item.get("month")
        if not isinstance(year, int) or not isinstance(month, int):
            continue
        key = (year, month)
        if item.get("download_status") == "failed" and item.get("local_availability") != "present":
            found[key] = "failed"
        elif item.get("remote_availability") == "unavailable":
            found[key] = "unavailable"
        elif item.get("remote_availability") == "available" and item.get("local_availability") != "present":
            found[key] = "available"
    return found


def partial_month_dirs(root: Path) -> list[tuple[int, int]]:
    """Months that have a ``.partial`` file and no final Parquet file."""

    base = root / "data" / "raw" / "v1" / "measurements"
    found: list[tuple[int, int]] = []
    if not base.is_dir():
        return found
    for path in base.glob("year=*/month=*/data.parquet.partial"):
        final = path.with_name("data.parquet")
        if final.is_file():
            continue
        year = int(path.parents[1].name.split("=", 1)[1])
        month = int(path.parents[0].name.split("=", 1)[1])
        found.append((year, month))
    return sorted(set(found))


def measurement_month_dirs(root: Path) -> list[tuple[int, int]]:
    """Year and month folders present under the raw XKDR measurement tree."""

    base = root / "data" / "raw" / "v1" / "measurements"
    found: list[tuple[int, int]] = []
    if not base.is_dir():
        return found
    for path in base.glob("year=*/month=*/data.parquet"):
        year = int(path.parents[1].name.split("=", 1)[1])
        month = int(path.parents[0].name.split("=", 1)[1])
        found.append((year, month))
    return sorted(set(found))


def prepared_observation_days(daily: pd.DataFrame) -> tuple[pd.Timestamp, ...]:
    """Distinct normalized dates in a prepared daily table."""

    if daily.empty or "observation_date" not in daily.columns:
        return ()
    days = pd.to_datetime(daily["observation_date"], errors="coerce").dt.normalize().dropna()
    return tuple(pd.Timestamp(value) for value in sorted(days.unique()))


def classify_months(
    downloaded: list[tuple[int, int]],
    observation_days: tuple[pd.Timestamp, ...],
    *,
    remote: dict[tuple[int, int], str] | None = None,
    partial: list[tuple[int, int]] | None = None,
) -> tuple[MonthCoverage, ...]:
    """Classify every month from the first known month through the last.

    A month with no local file and no confirmed remote status is
    ``not_downloaded``. That is not the same statement as "the source has no
    data." ``unavailable_remotely`` is used only when a retrieved file list
    omits the month. A downloaded month with no prepared days is
    ``downloaded_not_processed``. A prepared month that does not cover every
    calendar day is ``partial``.
    """

    if not downloaded and not observation_days:
        return ()
    downloaded_set = set(downloaded)
    remote = remote or {}
    partial_set = set(partial or [])
    by_month: dict[tuple[int, int], list[pd.Timestamp]] = {}
    for day in observation_days:
        by_month.setdefault((day.year, day.month), []).append(day)
    keys = set(downloaded_set) | set(by_month) | set(remote) | partial_set
    start = min(keys)
    end = max(keys)
    cursor = pd.Timestamp(year=start[0], month=start[1], day=1)
    last = pd.Timestamp(year=end[0], month=end[1], day=1)
    rows: list[MonthCoverage] = []
    while cursor <= last:
        key = (cursor.year, cursor.month)
        days = by_month.get(key, [])
        length = int(cursor.days_in_month)
        if days and len(days) >= length:
            status = PROCESSED
        elif days:
            status = PARTIAL
        elif key in downloaded_set:
            status = DOWNLOADED_NOT_PROCESSED
        elif key in partial_set or remote.get(key) == "incomplete":
            status = INCOMPLETE
        elif remote.get(key) == "failed":
            status = FAILED
        elif remote.get(key) == "unavailable":
            status = UNAVAILABLE_REMOTELY
        elif remote.get(key) == "available":
            status = AVAILABLE_REMOTELY
        else:
            status = NOT_DOWNLOADED
        rows.append(
            MonthCoverage(
                year=cursor.year,
                month=cursor.month,
                status=status,
                days_with_observations=len(days),
                days_in_month=length,
            )
        )
        cursor = cursor + pd.DateOffset(months=1)
    return tuple(rows)


def coverage_frame(months: tuple[MonthCoverage, ...]) -> pd.DataFrame:
    """One row per month for a heatmap. The value is a category, not a count."""

    if not months:
        return pd.DataFrame(columns=["year", "month", "label", "days_with_observations", "days_in_month"])
    return pd.DataFrame(
        {
            "year": [item.year for item in months],
            "month": [item.month for item in months],
            "label": [item.label for item in months],
            "days_with_observations": [item.days_with_observations for item in months],
            "days_in_month": [item.days_in_month for item in months],
        }
    )
