"""Choose prepared observations for a date range.

XKDR rows are concentrations in the unit stored with those measurements.
data.gov.in rows keep the API min, max, and average fields. This module
does not average those two kinds of values together, does not start Spark,
and does not read raw snapshot files.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from dashboard.filters import filter_date_range, filter_pollutant, period_summary, pool_hours

XKDR_SOURCE = "xkdr"
GOV_SOURCE = "data_gov_in"
XKDR_LABEL = "XKDR historical CPCB CAAQM"
GOV_LABEL = "data.gov.in real-time resource"
XKDR_UNIT = "µg/m³"
GOV_UNIT = "not documented by the API"
XKDR_KIND = "concentration"
GOV_KIND = "api-reported pollutant value"
ALL = "All"
PRESETS = ("Last 7 days", "Last 30 days", "Last 90 days", "Last 1 year", "Custom")

_XKDR_MEASUREMENT_COLUMNS = (
    "station_id",
    "station_name",
    "state_name",
    "city_name",
    "parameter_name",
    "collected_at",
    "value",
    "unit",
    "latitude",
    "longitude",
)
_SPIKE_COLUMNS = (
    "station_id",
    "station_name",
    "state_name",
    "city_name",
    "parameter_name",
    "collected_at",
    "value",
    "unit",
    "latitude",
    "longitude",
)


class IncompatibleMeasurements(ValueError):
    """Two sources do not share a unit and a measurement definition."""


@dataclass(frozen=True)
class SourceSpan:
    source: str
    label: str
    unit: str
    value_kind: str
    start: pd.Timestamp | None
    end: pd.Timestamp | None
    days: tuple[pd.Timestamp, ...]


@dataclass(frozen=True)
class TimelineCatalog:
    xkdr_daily: pd.DataFrame
    xkdr_hourly: pd.DataFrame
    xkdr_measurements: pd.DataFrame
    xkdr_spikes: pd.DataFrame
    gov_observations: pd.DataFrame
    xkdr: SourceSpan
    gov: SourceSpan


@dataclass(frozen=True)
class DateWindow:
    start: pd.Timestamp
    end: pd.Timestamp


@dataclass(frozen=True)
class SourceMatch:
    source: str
    label: str
    unit: str
    value_kind: str
    available_start: pd.Timestamp | None
    available_end: pd.Timestamp | None
    selected_start: pd.Timestamp
    selected_end: pd.Timestamp
    overlap_start: pd.Timestamp | None
    overlap_end: pd.Timestamp | None
    days_selected: int
    days_with_observations: int
    observations: int

    @property
    def status(self) -> str:
        if self.overlap_start is None:
            return "none"
        if self.days_with_observations < self.days_selected:
            return "partial"
        return "full"


def preset_bounds(preset: str, today: date) -> tuple[date, date] | None:
    """Inclusive local dates for a named preset. Custom has no implied dates."""

    offsets = {
        "Last 7 days": 6,
        "Last 30 days": 29,
        "Last 90 days": 89,
        "Last 1 year": 365,
    }
    if preset == "Custom":
        return None
    if preset not in offsets:
        raise ValueError(f"Unknown date preset: {preset}")
    return today - timedelta(days=offsets[preset]), today


def ordered_window(start: object, end: object) -> DateWindow:
    """Normalize an inclusive date window and put the earlier date first."""

    first = pd.Timestamp(start).normalize()
    second = pd.Timestamp(end).normalize()
    if first > second:
        first, second = second, first
    return DateWindow(start=first, end=second)


def prepared_fingerprint(root: Path) -> tuple[tuple[str, int, int], ...]:
    """File sizes and modification times for the prepared tables this page reads."""

    parts: list[tuple[str, int, int]] = []
    for path in _prepared_paths(root).values():
        if path.is_file():
            stat = path.stat()
            parts.append((path.name, stat.st_mtime_ns, stat.st_size))
        else:
            parts.append((path.name, 0, 0))
    return tuple(parts)


def load_timeline(root: Path) -> TimelineCatalog:
    """Read prepared Parquet tables. Missing outputs become empty frames."""

    paths = _prepared_paths(root)
    daily = _read_table(paths["xkdr_daily"])
    hourly = _read_table(paths["xkdr_hourly"])
    measurements = _read_columns(paths["xkdr_measurements"], _XKDR_MEASUREMENT_COLUMNS)
    spikes = _read_columns(paths["xkdr_spikes"], _SPIKE_COLUMNS)
    gov = _read_table(paths["gov_observations"])
    return make_catalog(
        xkdr_daily=daily,
        xkdr_hourly=hourly,
        xkdr_measurements=measurements,
        xkdr_spikes=spikes,
        gov_observations=gov,
    )


def make_catalog(
    *,
    xkdr_daily: pd.DataFrame | None = None,
    xkdr_hourly: pd.DataFrame | None = None,
    xkdr_measurements: pd.DataFrame | None = None,
    xkdr_spikes: pd.DataFrame | None = None,
    gov_observations: pd.DataFrame | None = None,
) -> TimelineCatalog:
    """Build a catalog and its date coverage from frames already in memory."""

    daily = _empty_if_missing(xkdr_daily)
    hourly = _empty_if_missing(xkdr_hourly)
    measurements = _empty_if_missing(xkdr_measurements)
    spikes = _empty_if_missing(xkdr_spikes)
    gov = _empty_if_missing(gov_observations)
    return TimelineCatalog(
        xkdr_daily=daily,
        xkdr_hourly=hourly,
        xkdr_measurements=measurements,
        xkdr_spikes=spikes,
        gov_observations=gov,
        xkdr=_xkdr_span(measurements, daily),
        gov=_gov_span(gov),
    )


def assess_range(catalog: TimelineCatalog, start: object, end: object) -> tuple[SourceMatch, SourceMatch]:
    """Describe each source inside the selected dates. An empty overlap stays empty."""

    window = ordered_window(start, end)
    return (
        _match(catalog.xkdr, window, _xkdr_observation_count(catalog, window)),
        _match(catalog.gov, window, _gov_observation_count(catalog, window)),
    )


def coverage_text(match: SourceMatch) -> str:
    """Explain what the selected dates actually contain for one source."""

    available = _span_text(match.available_start, match.available_end)
    selected = _span_text(match.selected_start, match.selected_end)
    if match.available_start is None:
        return f"{match.label} has no prepared observations. Selected dates: {selected}."
    if match.status == "none":
        return (
            f"{match.label} has observations from {available} ({match.unit}). "
            f"None of those dates fall inside {selected}."
        )
    overlap = _span_text(match.overlap_start, match.overlap_end)
    return (
        f"{match.label} has observations from {available} ({match.unit}). "
        f"Inside {selected}, stored observations run from {overlap} "
        f"on {match.days_with_observations} of {match.days_selected} selected days "
        f"({match.observations} observations)."
    )


def assert_same_measurement_scale(
    left_unit: str,
    right_unit: str,
    left_kind: str,
    right_kind: str,
) -> None:
    """Refuse a combined average when the unit or the meaning of the value differs."""

    if left_unit.strip() != right_unit.strip() or left_kind.strip() != right_kind.strip():
        raise IncompatibleMeasurements(
            "These sources use different measurement definitions. "
            f"{left_kind} ({left_unit}) cannot be averaged with {right_kind} ({right_unit})."
        )


def sources_can_share_scale(xkdr: SourceMatch, gov: SourceMatch) -> bool:
    """True only when both overlapping sources describe the same kind of measurement."""

    if xkdr.status == "none" or gov.status == "none":
        return False
    try:
        assert_same_measurement_scale(xkdr.unit, gov.unit, xkdr.value_kind, gov.value_kind)
    except IncompatibleMeasurements:
        return False
    return True


def filter_choices(
    catalog: TimelineCatalog,
    start: object,
    end: object,
    *,
    state: str = ALL,
    city: str = ALL,
) -> dict[str, list[str]]:
    """State, city, station, and pollutant values present in the selected dates."""

    places = _places_in_range(catalog, ordered_window(start, end))
    if state != ALL:
        places = places.loc[places["state"].eq(state)]
    if city != ALL:
        places = places.loc[places["city"].eq(city)]
    return {
        "state": _choices(places["state"] if "state" in places.columns else []),
        "city": _choices(places["city"] if "city" in places.columns else []),
        "station": _choices(places["station"] if "station" in places.columns else []),
        "pollutant": _choices(places["pollutant"] if "pollutant" in places.columns else []),
    }


def xkdr_daily_series(
    catalog: TimelineCatalog,
    start: object,
    end: object,
    *,
    state: str = ALL,
    city: str = ALL,
    station: str = ALL,
    pollutant: str = ALL,
) -> pd.DataFrame:
    """One row per date and pollutant. Means stay weighted by valid observations."""

    window = ordered_window(start, end)
    if station != ALL and "collected_at" in catalog.xkdr_measurements.columns:
        return _series_from_measurements(catalog, window, state, city, station, pollutant, by_hour=False)
    rows = _filter_xkdr_aggregate(catalog.xkdr_daily, window, state=state, city=city, pollutant=pollutant)
    if rows.empty:
        return rows
    grouped = rows.groupby(["observation_date", "parameter_name"], dropna=False, sort=True)
    records = []
    for (day, name), part in grouped:
        summary = period_summary(part)
        records.append({"observation_date": day, "parameter_name": name, **summary})
    return pd.DataFrame(records)


def xkdr_hour_pattern(
    catalog: TimelineCatalog,
    start: object,
    end: object,
    *,
    state: str = ALL,
    city: str = ALL,
    station: str = ALL,
    pollutant: str = ALL,
) -> pd.DataFrame:
    """Hour-of-day pattern from hourly rows inside the selected dates."""

    window = ordered_window(start, end)
    if station != ALL and "collected_at" in catalog.xkdr_measurements.columns:
        return _series_from_measurements(catalog, window, state, city, station, pollutant, by_hour=True)
    rows = _filter_xkdr_aggregate(catalog.xkdr_hourly, window, state=state, city=city, pollutant=pollutant)
    return pool_hours(rows)


def xkdr_summary(
    catalog: TimelineCatalog,
    start: object,
    end: object,
    *,
    state: str = ALL,
    city: str = ALL,
    station: str = ALL,
    pollutant: str = ALL,
) -> dict[str, object]:
    """Counts and the latest timestamp from the filtered XKDR rows."""

    window = ordered_window(start, end)
    measured = _filter_measurements(catalog.xkdr_measurements, window, state, city, station, pollutant)
    spikes = _filter_measurements(catalog.xkdr_spikes, window, state, city, station, pollutant)
    if not measured.empty:
        latest = pd.to_datetime(measured["collected_at"]).max()
        missing = int(measured["value"].isna().sum()) if "value" in measured.columns else 0
        return {
            "source": XKDR_LABEL,
            "unit": catalog.xkdr.unit,
            "observations": int(len(measured)),
            "stations": int(measured["station_id"].nunique(dropna=True)),
            "latest": None if pd.isna(latest) else pd.Timestamp(latest),
            "missing_values": missing,
            "spike_count": int(len(spikes)),
            "selected_start": window.start,
            "selected_end": window.end,
        }
    daily = xkdr_daily_series(catalog, window.start, window.end, state=state, city=city, pollutant=pollutant)
    summary = period_summary(daily) if not daily.empty else period_summary(pd.DataFrame())
    return {
        "source": XKDR_LABEL,
        "unit": catalog.xkdr.unit,
        "observations": int(summary["observations"] or 0),
        "stations": 0,
        "latest": None if daily.empty else pd.Timestamp(daily["observation_date"].max()),
        "missing_values": int((summary["observations"] or 0) - (summary["valid_observations"] or 0)),
        "spike_count": int(summary["spike_count"] or 0),
        "selected_start": window.start,
        "selected_end": window.end,
    }


def xkdr_station_ranking(
    catalog: TimelineCatalog,
    start: object,
    end: object,
    *,
    state: str = ALL,
    city: str = ALL,
    pollutant: str = ALL,
    limit: int = 12,
) -> pd.DataFrame:
    """Mean stored concentration and stored spike count for each station in the range."""

    window = ordered_window(start, end)
    measured = _filter_measurements(catalog.xkdr_measurements, window, state, city, ALL, pollutant)
    spikes = _filter_measurements(catalog.xkdr_spikes, window, state, city, ALL, pollutant)
    if measured.empty:
        return measured
    rows: list[dict[str, object]] = []
    grouped = measured.groupby(["station_id", "parameter_name"], dropna=False, sort=True)
    spike_counts = (
        spikes.groupby(["station_id", "parameter_name"], dropna=False).size()
        if not spikes.empty
        else pd.Series(dtype="int64")
    )
    for (station_id, pollutant_name), part in grouped:
        values = part["value"]
        key = (station_id, pollutant_name)
        rows.append(
            {
                "station_id": station_id,
                "parameter_name": pollutant_name,
                "city_name": _first_text(part, "city_name"),
                "state_name": _first_text(part, "state_name"),
                "latitude": _first_number(part, "latitude"),
                "longitude": _first_number(part, "longitude"),
                "observations": int(len(part)),
                "mean_valid_value": None if values.dropna().empty else float(values.mean()),
                "spike_count": int(spike_counts.get(key, 0)),
            }
        )
    frame = pd.DataFrame(rows)
    frame = frame.sort_values(["mean_valid_value", "station_id"], ascending=[False, True], na_position="last")
    return frame.head(limit).copy()


def xkdr_station_map_frame(
    catalog: TimelineCatalog,
    start: object,
    end: object,
    *,
    state: str = ALL,
    city: str = ALL,
    pollutant: str,
) -> pd.DataFrame:
    """Stations with coordinates, shaped for the API-style point map."""

    ranking = xkdr_station_ranking(
        catalog,
        start,
        end,
        state=state,
        city=city,
        pollutant=pollutant,
        limit=10_000,
    )
    if ranking.empty:
        return ranking
    return pd.DataFrame(
        {
            "station": ranking["station_id"],
            "city": ranking["city_name"],
            "state": ranking["state_name"],
            "latitude": ranking["latitude"],
            "longitude": ranking["longitude"],
            "avg_value": ranking["mean_valid_value"],
        }
    )


def xkdr_city_bars(
    catalog: TimelineCatalog,
    start: object,
    end: object,
    *,
    state: str = ALL,
    pollutant: str = ALL,
) -> pd.DataFrame:
    """City means for the selected dates, weighted by valid observations."""

    series = xkdr_daily_series(catalog, start, end, state=state, city=ALL, pollutant=pollutant)
    if series.empty:
        return series
    daily = _filter_xkdr_aggregate(
        catalog.xkdr_daily,
        ordered_window(start, end),
        state=state,
        city=ALL,
        pollutant=pollutant,
    )
    grouped = daily.groupby(["city_name", "state_name", "parameter_name"], dropna=False, sort=True)
    records = []
    for (city_name, state_name, name), part in grouped:
        summary = period_summary(part)
        if summary["mean_valid_value"] is None:
            continue
        label = str(city_name) if pd.notna(city_name) else "City not recorded"
        if pd.notna(state_name) and str(state_name) not in label:
            label = f"{label}, {state_name}"
        records.append(
            {
                "label": label,
                "parameter_name": name,
                "value": summary["mean_valid_value"],
                "observations": summary["observations"],
            }
        )
    frame = pd.DataFrame(records)
    if frame.empty:
        return frame
    return frame.sort_values(["value", "label"], ascending=[False, True]).groupby("parameter_name", sort=False).head(12)


def _series_from_measurements(
    catalog: TimelineCatalog,
    window: DateWindow,
    state: str,
    city: str,
    station: str,
    pollutant: str,
    *,
    by_hour: bool,
) -> pd.DataFrame:
    measured = _filter_measurements(catalog.xkdr_measurements, window, state, city, station, pollutant)
    spikes = _filter_measurements(catalog.xkdr_spikes, window, state, city, station, pollutant)
    if measured.empty:
        return measured
    stamps = pd.to_datetime(measured["collected_at"])
    working = measured.copy()
    working["observation_date"] = stamps.dt.normalize()
    working["observation_hour"] = stamps.dt.hour
    keys = ["observation_hour", "parameter_name"] if by_hour else ["observation_date", "parameter_name"]
    spike_keys = keys
    spike_frame = spikes.copy()
    if not spike_frame.empty:
        spike_stamps = pd.to_datetime(spike_frame["collected_at"])
        spike_frame["observation_date"] = spike_stamps.dt.normalize()
        spike_frame["observation_hour"] = spike_stamps.dt.hour
        spike_counts = spike_frame.groupby(spike_keys, dropna=False).size()
    else:
        spike_counts = pd.Series(dtype="int64")
    records = []
    for key, part in working.groupby(keys, dropna=False, sort=True):
        values = part["value"]
        valid = int(values.notna().sum())
        records.append(
            {
                "observation_date": part["observation_date"].iloc[0],
                "observation_hour": int(part["observation_hour"].iloc[0]) if by_hour else None,
                "parameter_name": part["parameter_name"].iloc[0],
                "observations": int(len(part)),
                "valid_observations": valid,
                "mean_valid_value": None if values.dropna().empty else float(values.mean()),
                "spike_count": int(spike_counts.get(key, 0)),
                "eligible_observations": valid,
            }
        )
    frame = pd.DataFrame(records)
    if by_hour:
        return pool_hours(frame)
    return frame


def xkdr_spike_events(
    catalog: TimelineCatalog,
    start: object,
    end: object,
    *,
    state: str = ALL,
    city: str = ALL,
    station: str = ALL,
    pollutant: str = ALL,
) -> pd.DataFrame:
    """Stored candidate spikes whose timestamps fall in the selection."""

    return _filter_measurements(
        catalog.xkdr_spikes,
        ordered_window(start, end),
        state,
        city,
        station,
        pollutant,
    )


def gov_observations(
    catalog: TimelineCatalog,
    start: object,
    end: object,
    *,
    state: str = ALL,
    city: str = ALL,
    station: str = ALL,
    pollutant: str = ALL,
) -> pd.DataFrame:
    """data.gov.in rows inside the selected dates. Values stay API-reported."""

    frame = catalog.gov_observations
    if frame.empty or "observed_at" not in frame.columns:
        return frame.iloc[0:0].copy()
    window = ordered_window(start, end)
    rows = filter_date_range(frame, "observed_at", window.start, window.end)
    rows = _match_column(rows, "state", state)
    rows = _match_column(rows, "city", city)
    rows = _match_column(rows, "station", station)
    rows = _match_column(rows, "pollutant_id", pollutant)
    return rows.copy()


def gov_summary(frame: pd.DataFrame, *, unit: str, start: object, end: object) -> dict[str, object]:
    """Counts from one already filtered data.gov.in frame."""

    window = ordered_window(start, end)
    if frame.empty:
        return {
            "source": GOV_LABEL,
            "unit": unit,
            "observations": 0,
            "stations": 0,
            "latest": None,
            "missing_values": 0,
            "selected_start": window.start,
            "selected_end": window.end,
        }
    latest = pd.to_datetime(frame["observed_at"]).max()
    missing = int(frame["avg_value"].isna().sum()) if "avg_value" in frame.columns else 0
    return {
        "source": GOV_LABEL,
        "unit": unit,
        "observations": int(len(frame)),
        "stations": int(frame["station"].nunique(dropna=True)) if "station" in frame.columns else 0,
        "latest": None if pd.isna(latest) else pd.Timestamp(latest),
        "missing_values": missing,
        "selected_start": window.start,
        "selected_end": window.end,
    }


def _prepared_paths(root: Path) -> dict[str, Path]:
    return {
        "xkdr_daily": root / "data" / "processed" / "city_portal" / "city_daily" / "data.parquet",
        "xkdr_hourly": root / "data" / "processed" / "city_portal" / "city_hourly" / "data.parquet",
        "xkdr_measurements": root / "data" / "processed" / "measurements" / "data.parquet",
        "xkdr_spikes": root / "data" / "processed" / "spikes" / "spikes_only" / "data.parquet",
        "gov_observations": root / "data" / "processed" / "data_gov_in" / "observations" / "data.parquet",
    }


def _read_table(path: Path) -> pd.DataFrame:
    if not path.is_file():
        return pd.DataFrame()
    return pd.read_parquet(path)


def _read_columns(path: Path, columns: tuple[str, ...]) -> pd.DataFrame:
    if not path.is_file():
        return pd.DataFrame()
    available = _parquet_columns(path)
    selected = [column for column in columns if column in available]
    if not selected:
        return pd.DataFrame()
    return pd.read_parquet(path, columns=selected)


def _parquet_columns(path: Path) -> set[str]:
    import pyarrow.parquet as pq

    return set(pq.ParquetFile(path).schema_arrow.names)


def _empty_if_missing(frame: pd.DataFrame | None) -> pd.DataFrame:
    if frame is None:
        return pd.DataFrame()
    return frame


def _xkdr_span(measurements: pd.DataFrame, daily: pd.DataFrame) -> SourceSpan:
    if not measurements.empty and "collected_at" in measurements.columns:
        days = _unique_days(measurements["collected_at"])
        unit = _single_text(measurements, "unit", XKDR_UNIT)
    elif not daily.empty and "observation_date" in daily.columns:
        days = _unique_days(daily["observation_date"])
        unit = XKDR_UNIT
    else:
        days = ()
        unit = XKDR_UNIT
    return _span(XKDR_SOURCE, XKDR_LABEL, unit, XKDR_KIND, days)


def _gov_span(observations: pd.DataFrame) -> SourceSpan:
    if observations.empty or "observed_at" not in observations.columns:
        return _span(GOV_SOURCE, GOV_LABEL, GOV_UNIT, GOV_KIND, ())
    return _span(
        GOV_SOURCE,
        GOV_LABEL,
        _single_text(observations, "reported_unit", GOV_UNIT),
        _single_text(observations, "value_kind", GOV_KIND),
        _unique_days(observations["observed_at"]),
    )


def _span(source: str, label: str, unit: str, kind: str, days: tuple[pd.Timestamp, ...]) -> SourceSpan:
    if not days:
        return SourceSpan(source, label, unit, kind, None, None, ())
    return SourceSpan(source, label, unit, kind, days[0], days[-1], days)


def _unique_days(values: pd.Series) -> tuple[pd.Timestamp, ...]:
    stamps = pd.to_datetime(values, errors="coerce")
    if getattr(stamps.dt, "tz", None) is not None:
        stamps = stamps.dt.tz_convert("Asia/Kolkata").dt.tz_localize(None)
    days = stamps.dt.normalize().dropna().drop_duplicates().sort_values()
    return tuple(pd.Timestamp(value) for value in days.tolist())


def _single_text(frame: pd.DataFrame, column: str, fallback: str) -> str:
    if column not in frame.columns:
        return fallback
    values = sorted({str(value) for value in frame[column].dropna().unique()})
    if len(values) == 1:
        return values[0]
    return fallback


def _match(span: SourceSpan, window: DateWindow, observations: int) -> SourceMatch:
    selected_days = _days_between(window.start, window.end)
    overlapped = tuple(day for day in span.days if window.start <= day <= window.end)
    if not overlapped:
        return SourceMatch(
            source=span.source,
            label=span.label,
            unit=span.unit,
            value_kind=span.value_kind,
            available_start=span.start,
            available_end=span.end,
            selected_start=window.start,
            selected_end=window.end,
            overlap_start=None,
            overlap_end=None,
            days_selected=len(selected_days),
            days_with_observations=0,
            observations=0,
        )
    return SourceMatch(
        source=span.source,
        label=span.label,
        unit=span.unit,
        value_kind=span.value_kind,
        available_start=span.start,
        available_end=span.end,
        selected_start=window.start,
        selected_end=window.end,
        overlap_start=overlapped[0],
        overlap_end=overlapped[-1],
        days_selected=len(selected_days),
        days_with_observations=len(overlapped),
        observations=observations,
    )


def _days_between(start: pd.Timestamp, end: pd.Timestamp) -> tuple[pd.Timestamp, ...]:
    return tuple(pd.Timestamp(value) for value in pd.date_range(start, end, freq="D"))


def _xkdr_observation_count(catalog: TimelineCatalog, window: DateWindow) -> int:
    measured = _filter_measurements(catalog.xkdr_measurements, window, ALL, ALL, ALL, ALL)
    if not measured.empty or "collected_at" in catalog.xkdr_measurements.columns:
        return int(len(measured))
    daily = _filter_xkdr_aggregate(catalog.xkdr_daily, window, state=ALL, city=ALL, pollutant=ALL)
    if daily.empty or "observations" not in daily.columns:
        return 0
    return int(daily["observations"].sum())


def _gov_observation_count(catalog: TimelineCatalog, window: DateWindow) -> int:
    return int(len(gov_observations(catalog, window.start, window.end)))


def _filter_xkdr_aggregate(
    frame: pd.DataFrame,
    window: DateWindow,
    *,
    state: str,
    city: str,
    pollutant: str,
) -> pd.DataFrame:
    if frame.empty or "observation_date" not in frame.columns:
        return frame.iloc[0:0].copy()
    rows = filter_date_range(frame, "observation_date", window.start, window.end)
    rows = _match_column(rows, "state_name", state)
    rows = _match_column(rows, "city_name", city)
    return filter_pollutant(rows, None if pollutant == ALL else pollutant)


def _filter_measurements(
    frame: pd.DataFrame,
    window: DateWindow,
    state: str,
    city: str,
    station: str,
    pollutant: str,
) -> pd.DataFrame:
    if frame.empty or "collected_at" not in frame.columns:
        return frame.iloc[0:0].copy()
    rows = filter_date_range(frame, "collected_at", window.start, window.end)
    rows = _match_column(rows, "state_name", state)
    rows = _match_column(rows, "city_name", city)
    rows = _match_column(rows, "station_id", station)
    return filter_pollutant(rows, None if pollutant == ALL else pollutant)


def _match_column(frame: pd.DataFrame, column: str, choice: str) -> pd.DataFrame:
    if choice in (ALL, "", None) or column not in frame.columns:
        return frame
    return frame.loc[frame[column].astype(str).eq(str(choice))].copy()


def _places_in_range(catalog: TimelineCatalog, window: DateWindow) -> pd.DataFrame:
    frames = [
        _place_rows(
            _filter_measurements(catalog.xkdr_measurements, window, ALL, ALL, ALL, ALL),
            state="state_name",
            city="city_name",
            station="station_id",
            pollutant="parameter_name",
        ),
        _place_rows(
            gov_observations(catalog, window.start, window.end),
            state="state",
            city="city",
            station="station",
            pollutant="pollutant_id",
        ),
    ]
    present = [frame for frame in frames if not frame.empty]
    if not present:
        return pd.DataFrame(columns=["state", "city", "station", "pollutant"])
    return pd.concat(present, ignore_index=True)


def _place_rows(frame: pd.DataFrame, *, state: str, city: str, station: str, pollutant: str) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=["state", "city", "station", "pollutant"])
    return pd.DataFrame(
        {
            "state": frame[state] if state in frame.columns else pd.Series(dtype="object"),
            "city": frame[city] if city in frame.columns else pd.Series(dtype="object"),
            "station": frame[station] if station in frame.columns else pd.Series(dtype="object"),
            "pollutant": frame[pollutant] if pollutant in frame.columns else pd.Series(dtype="object"),
        }
    )


def _choices(values: object) -> list[str]:
    series = pd.Series(values).dropna().astype(str)
    return [ALL, *sorted(set(series))]


def _first_text(frame: pd.DataFrame, column: str) -> object:
    if column not in frame.columns or frame[column].dropna().empty:
        return None
    return frame[column].dropna().iloc[0]


def _first_number(frame: pd.DataFrame, column: str) -> float | None:
    if column not in frame.columns or frame[column].dropna().empty:
        return None
    return float(frame[column].dropna().iloc[0])


def _span_text(start: pd.Timestamp | None, end: pd.Timestamp | None) -> str:
    if start is None or end is None:
        return "no stored dates"
    if start == end:
        return start.strftime("%Y-%m-%d")
    return f"{start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')}"
