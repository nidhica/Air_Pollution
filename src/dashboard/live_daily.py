"""Choose prepared Parquet or a published XKDR daily query from the selected dates.

``today`` is the current date in Asia/Kolkata. The live window is the
inclusive range from ``today - 6 days`` through ``today``. A daily aggregate
query is sent only for the part of that window that is also inside the
published archive month from ``/v1/meta``. Months after ``last_month`` are
not requested. Dates inside the archive but outside the prepared dataset are
not downloaded from this module.

The API key stays inside the metadata and measurements clients. Completed
measurement responses use the existing seven-day query cache. A failed or
truncated response is not cached and is not replaced with prepared rows.
data.gov.in rows are not read.
"""

from __future__ import annotations

import io
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from dashboard.query_cache import CacheError, QueryCache, default_cache_directory
from dashboard.timeline import ALL
from dashboard.xkdr_archive import ArchiveInfo, load_archive
from ingestion.xkdr_query import MeasurementQuery, QueryError, QueryTransport, fetch_measurements

LIVE_POLLUTANTS = ("PM2.5", "PM10")
API_DAILY_COLUMNS = (
    "station_id",
    "parameter_name",
    "unit",
    "period_start",
    "mean",
    "min",
    "max",
    "n",
)
HISTORICAL_LABEL = "Historical observations — prepared CPCB dataset"
LIVE_LABEL = "Recent observations — live XKDR API"
ON_DEMAND_HEADING = "XKDR on-demand daily measurements"
LIVE_SECTION_CAPTION = (
    "Source: XKDR measurements API, CPCB CAAQM. Aggregation: daily. "
    "Each row is one station-level daily aggregate (period_start, mean, min, max, n). "
    "Sum of n counts the underlying hourly observations. "
    "These are not raw hourly rows and they are not merged with data.gov.in values. "
    "period_start is the observation date, not the time the API was called."
)
_KOLKATA = ZoneInfo("Asia/Kolkata")
_SECRET_TEXT = re.compile(r"Bearer\s+\S+|aqi_[A-Za-z0-9_-]+", re.IGNORECASE)
_CHART_COLUMNS = (
    "observation_date",
    "parameter_name",
    "mean_valid_value",
    "observations",
    "eligible_observations",
)


@dataclass(frozen=True)
class LiveDailyResult:
    """Station-level API rows, a chart frame, and where those rows came from."""

    rows: pd.DataFrame
    chart: pd.DataFrame
    origin: str
    detail: str


@dataclass(frozen=True)
class SourcePlan:
    """Inclusive bounds for one selected range. The slices do not share a date."""

    today: date
    live_start: date
    live_end: date
    selected_start: date
    selected_end: date
    historical_start: date | None
    historical_end: date | None
    live_query_start: date | None
    live_query_end: date | None
    future_start: date | None
    future_end: date | None
    archive_end: date | None = None
    unprepared: tuple[tuple[date, date], ...] = ()
    unpublished_start: date | None = None
    unpublished_end: date | None = None
    unconfirmed_start: date | None = None
    unconfirmed_end: date | None = None
    historical_tail: tuple[tuple[date, date], ...] = ()

    @property
    def mode(self) -> str:
        parts: list[str] = []
        if self.historical_start is not None:
            parts.append("historical")
        if self.unprepared:
            parts.append("unprepared")
        if self.live_query_start is not None:
            parts.append("recent")
        if self.unpublished_start is not None:
            parts.append("unpublished")
        if self.unconfirmed_start is not None:
            parts.append("unconfirmed")
        if not parts:
            return "future" if self.future_start is not None else "empty"
        if parts == ["historical"]:
            return "historical"
        if parts == ["recent"]:
            return "recent"
        if parts == ["unprepared"]:
            return "unprepared"
        if parts == ["unpublished"]:
            return "unpublished"
        if parts == ["unconfirmed"]:
            return "unconfirmed"
        return "mixed"


@dataclass(frozen=True)
class LiveSectionView:
    """What the Timeline should show for one recent query."""

    status: str
    message: str
    show_chart: bool
    show_metrics: bool


def present_live_section(result: LiveDailyResult | None, start: object, end: object) -> LiveSectionView:
    """Describe the on-demand section without hiding an empty or failed query."""

    requested = f"{_as_date(start).isoformat()} to {_as_date(end).isoformat()}"
    if result is None or result.origin not in {"api", "cache"}:
        detail = "Recent observations are unavailable." if result is None else result.detail
        return LiveSectionView(
            status="failed",
            message=f"Requested {requested}. {detail} No daily chart is shown for this request.",
            show_chart=False,
            show_metrics=False,
        )
    if result.rows.empty:
        return LiveSectionView(
            status="empty",
            message=(
                f"Requested {requested}. "
                "No XKDR daily observations were returned for these dates. "
                "Missing dates were not filled."
            ),
            show_chart=False,
            show_metrics=False,
        )
    return LiveSectionView(
        status="ready",
        message=f"Requested {requested}. {result.detail}",
        show_chart=not result.chart.empty,
        show_metrics=True,
    )


@dataclass(frozen=True)
class TimelineSources:
    """The date split, the archive month, and the live result when one was requested."""

    plan: SourcePlan
    live: LiveDailyResult | None
    archive: ArchiveInfo


def kolkata_today(moment: datetime | None = None) -> date:
    """Return the calendar date in Asia/Kolkata. Naive clock readings are refused."""

    current = datetime.now(_KOLKATA) if moment is None else moment
    if current.tzinfo is None:
        raise ValueError("A clock reading needs a timezone.")
    return current.astimezone(_KOLKATA).date()


def plan_sources(
    start: object,
    end: object,
    *,
    today: date | None = None,
    archive_end: date | None = None,
    prepared_start: date | None = None,
    prepared_end: date | None = None,
) -> SourcePlan:
    """Split a selection into prepared, published-unprepared, and unpublished dates.

    A daily query exists only where the recent seven-day window is also on or
    before ``archive_end``. ``archive_end is None`` means the archive month is
    unknown, so no daily query is created.
    """

    current = _as_date(today if today is not None else kolkata_today())
    published_end = None if archive_end is None else _as_date(archive_end)
    prepared = _prepared_pair(prepared_start, prepared_end)
    live_window_start = current - timedelta(days=6)
    first, last = _ordered_dates(start, end)
    future_start = None
    future_end = None
    observed_end = last
    if last > current:
        future_start = max(first, current + timedelta(days=1))
        future_end = last
        observed_end = current
    live_query_start = None
    live_query_end = None
    historical_ranges: list[tuple[date, date]] = []
    unprepared: list[tuple[date, date]] = []
    unpublished: tuple[date, date] | None = None
    unconfirmed: tuple[date, date] | None = None
    if first <= observed_end:
        live_query = _live_query(first, observed_end, live_window_start, current, published_end)
        if live_query is not None:
            live_query_start, live_query_end = live_query
        for segment in _outside(first, observed_end, live_query):
            classified = _classify_segment(segment, published_end, prepared)
            if classified[0] is not None:
                historical_ranges.append(classified[0])
            unprepared.extend(classified[1])
            unpublished = _merge_span(unpublished, classified[2])
            unconfirmed = _merge_span(unconfirmed, classified[3])
    first_historical = historical_ranges[0] if historical_ranges else None
    return SourcePlan(
        today=current,
        live_start=live_window_start,
        live_end=current,
        selected_start=first,
        selected_end=last,
        historical_start=None if first_historical is None else first_historical[0],
        historical_end=None if first_historical is None else first_historical[1],
        live_query_start=live_query_start,
        live_query_end=live_query_end,
        future_start=future_start,
        future_end=future_end,
        archive_end=published_end,
        unprepared=tuple(unprepared),
        unpublished_start=None if unpublished is None else unpublished[0],
        unpublished_end=None if unpublished is None else unpublished[1],
        unconfirmed_start=None if unconfirmed is None else unconfirmed[0],
        unconfirmed_end=None if unconfirmed is None else unconfirmed[1],
        historical_tail=tuple(historical_ranges[1:]),
    )


def resolve_timeline_sources(
    *,
    project_root: Path,
    catalog: object,
    start: object,
    end: object,
    today: date | None = None,
    pollutant: str = ALL,
    state: str = ALL,
    city: str = ALL,
    station: str = ALL,
    transport: QueryTransport | None = None,
    cache: QueryCache | None = None,
    api_key: str | None = None,
    archive: ArchiveInfo | None = None,
) -> TimelineSources:
    """Query the daily API only for a recent slice inside the published archive."""

    info = archive if archive is not None else load_archive(
        project_root=project_root,
        transport=transport,
        api_key=api_key,
    )
    prepared_start, prepared_end = prepared_bounds(catalog)
    plan = plan_sources(
        start,
        end,
        today=today,
        archive_end=info.archive_end,
        prepared_start=prepared_start,
        prepared_end=prepared_end,
    )
    if plan.live_query_start is None or plan.live_query_end is None:
        return TimelineSources(plan=plan, live=None, archive=info)
    live = load_live_daily(
        project_root=project_root,
        catalog=catalog,
        start=plan.live_query_start,
        end=plan.live_query_end,
        pollutant=pollutant,
        state=state,
        city=city,
        station=station,
        transport=transport,
        cache=cache,
        api_key=api_key,
        archive_end=info.archive_end,
    )
    return TimelineSources(plan=plan, live=live, archive=info)


def prepared_bounds(catalog: object) -> tuple[date | None, date | None]:
    """Return the prepared XKDR span. An empty catalog has no prepared dates."""

    span = getattr(catalog, "xkdr", None)
    if span is None:
        return None, None
    start = getattr(span, "start", None)
    end = getattr(span, "end", None)
    if start is None or end is None:
        return None, None
    return _as_date(start), _as_date(end)


def availability_messages(plan: SourcePlan, archive: ArchiveInfo) -> tuple[str, ...]:
    """Explain unpublished, unprepared, and unconfirmed dates without inventing rows."""

    messages: list[str] = []
    if plan.unpublished_start is not None and plan.unpublished_end is not None:
        month = archive.last_month or "the published archive month"
        messages.append(
            f"XKDR has not published months after {month}. "
            f"{plan.unpublished_start.isoformat()} to {plan.unpublished_end.isoformat()} "
            "were not requested as daily aggregates. "
            "No XKDR observations are available for those dates. "
            "data.gov.in values were not substituted."
        )
    for start, end in plan.unprepared:
        messages.append(
            f"XKDR has published {start.isoformat()} to {end.isoformat()}, "
            "but those dates are not in the prepared CPCB dataset. "
            "This page does not download bulk months. "
            "Fetch one explicit month with acquire-xkdr --download --month YYYY-MM, then prepare it. "
            "Missing dates were not filled."
        )
    if plan.unconfirmed_start is not None and plan.unconfirmed_end is not None:
        messages.append(
            f"{archive.detail} "
            f"{plan.unconfirmed_start.isoformat()} to {plan.unconfirmed_end.isoformat()} "
            "were not requested. Publication of those months was not confirmed."
        )
    return tuple(messages)


def historical_slices(plan: SourcePlan) -> tuple[tuple[date, date], ...]:
    """Prepared ranges in the selection. The live query is not included."""

    if plan.historical_start is None or plan.historical_end is None:
        return plan.historical_tail
    return ((plan.historical_start, plan.historical_end),) + plan.historical_tail


def _prepared_pair(start: date | None, end: date | None) -> tuple[date, date] | None:
    if start is None or end is None:
        return None
    first, last = _ordered_dates(start, end)
    return first, last


def _live_query(
    observed_start: date,
    observed_end: date,
    window_start: date,
    window_end: date,
    archive_end: date | None,
) -> tuple[date, date] | None:
    if archive_end is None:
        return None
    start = max(observed_start, window_start)
    end = min(observed_end, window_end, archive_end)
    if start > end:
        return None
    return start, end


def _outside(
    start: date,
    end: date,
    removed: tuple[date, date] | None,
) -> tuple[tuple[date, date], ...]:
    if removed is None:
        return ((start, end),)
    pieces: list[tuple[date, date]] = []
    if start < removed[0]:
        pieces.append((start, removed[0] - timedelta(days=1)))
    if end > removed[1]:
        pieces.append((removed[1] + timedelta(days=1), end))
    return tuple(pieces)


def _classify_segment(
    segment: tuple[date, date],
    archive_end: date | None,
    prepared: tuple[date, date] | None,
) -> tuple[
    tuple[date, date] | None,
    tuple[tuple[date, date], ...],
    tuple[date, date] | None,
    tuple[date, date] | None,
]:
    start, end = segment
    historical = _overlap_span((start, end), prepared)
    outside = _outside(start, end, historical) if historical is not None else ((start, end),)
    unprepared: list[tuple[date, date]] = []
    unpublished: tuple[date, date] | None = None
    unconfirmed: tuple[date, date] | None = None
    for piece_start, piece_end in outside:
        if archive_end is None:
            unconfirmed = _merge_span(unconfirmed, (piece_start, piece_end))
            continue
        if piece_end > archive_end:
            unpublished = _merge_span(unpublished, (max(piece_start, archive_end + timedelta(days=1)), piece_end))
        published_end = min(piece_end, archive_end)
        if piece_start <= published_end:
            unprepared.append((piece_start, published_end))
    return historical, tuple(unprepared), unpublished, unconfirmed


def _overlap_span(
    segment: tuple[date, date] | None,
    bounds: tuple[date, date] | None,
) -> tuple[date, date] | None:
    if segment is None or bounds is None:
        return None
    start = max(segment[0], bounds[0])
    end = min(segment[1], bounds[1])
    if start > end:
        return None
    return start, end


def _merge_span(
    current: tuple[date, date] | None,
    extra: tuple[date, date] | None,
) -> tuple[date, date] | None:
    if extra is None:
        return current
    if current is None:
        return extra
    return min(current[0], extra[0]), max(current[1], extra[1])


def load_live_daily(
    *,
    project_root: Path,
    catalog: object,
    start: object,
    end: object,
    pollutant: str = ALL,
    state: str = ALL,
    city: str = ALL,
    station: str = ALL,
    transport: QueryTransport | None = None,
    cache: QueryCache | None = None,
    api_key: str | None = None,
    archive_end: date | None = None,
) -> LiveDailyResult:
    """Return one complete daily result, or an explanation when the query is refused."""

    if archive_end is not None and _as_date(end) > _as_date(archive_end):
        return LiveDailyResult(
            rows=_empty_rows(),
            chart=_empty_chart(),
            origin="unpublished",
            detail=(
                "Daily aggregate queries are not sent for months after the published XKDR archive. "
                f"The published archive ends on {_as_date(archive_end).isoformat()}. "
                "No XKDR observations were substituted from another source."
            ),
        )
    parameters = _parameters_for(pollutant)
    if parameters is None:
        return LiveDailyResult(
            rows=_empty_rows(),
            chart=_empty_chart(),
            origin="unsupported_pollutant",
            detail=(
                "The on-demand daily query retrieves PM2.5 and PM10. "
                "No XKDR rows are shown for the selected pollutant."
            ),
        )
    query = _query(start, end, parameters, state, city, station)
    store = cache if cache is not None else QueryCache(default_cache_directory(project_root))
    cached = _cached_frame(store, query)
    if cached is not None:
        return _accepted(cached, query, pollutant, station, origin="cache", cache_note="")
    try:
        result = fetch_measurements(
            query,
            project_root=project_root,
            transport=transport,
            api_key=api_key,
        )
    except QueryError as exc:
        return _failure(str(exc))
    except Exception:
        return _failure("The XKDR daily query failed before a complete table was returned.")
    cache_note = ""
    try:
        store.put(query, result.payload)
    except CacheError:
        cache_note = " The completed response could not be written to the query cache."
    return _accepted(
        result.table.to_pandas(),
        query,
        pollutant,
        station,
        origin="api",
        cache_note=cache_note,
    )


def station_daily_chart(frame: pd.DataFrame) -> pd.DataFrame:
    """Weight station daily means by ``n``. Days without a usable row are absent."""

    chart, _skipped = _chart_rows(frame)
    return chart


def _parameters_for(pollutant: str) -> tuple[str, ...] | None:
    if pollutant in (ALL, "", None):
        return LIVE_POLLUTANTS
    if pollutant in LIVE_POLLUTANTS:
        return (pollutant,)
    return None


def _query(
    start: object,
    end: object,
    parameters: tuple[str, ...],
    state: str,
    city: str,
    station: str,
) -> MeasurementQuery:
    return MeasurementQuery(
        start=_as_date(start),
        end=_as_date(end),
        source="cpcb_caaqm",
        parameters=parameters,
        aggregation="daily",
        response_format="parquet",
        state=None if state in (ALL, "", None) else state,
        city=None if city in (ALL, "", None) else city,
        stations=() if station in (ALL, "", None) else (str(station),),
    )


def _cached_frame(store: QueryCache, query: MeasurementQuery) -> pd.DataFrame | None:
    entry = store.get(query)
    if entry is None:
        return None
    frame = _frame_from_payload(entry.read_bytes())
    if frame is None or not _has_daily_columns(frame):
        return None
    return frame


def _frame_from_payload(payload: bytes) -> pd.DataFrame | None:
    try:
        table = pq.read_table(io.BytesIO(payload))
    except (OSError, ValueError, pa.ArrowException):
        return None
    return table.to_pandas()


def _accepted(
    frame: pd.DataFrame,
    query: MeasurementQuery,
    pollutant: str,
    station: str,
    *,
    origin: str,
    cache_note: str,
) -> LiveDailyResult:
    filtered = _filter_rows(frame, query, pollutant, station)
    chart, skipped = _chart_rows(filtered)
    detail = _success_detail(origin, query, pollutant, station, len(frame), len(filtered), skipped)
    return LiveDailyResult(rows=filtered, chart=chart, origin=origin, detail=_public(detail + cache_note))


def _failure(reason: str) -> LiveDailyResult:
    return LiveDailyResult(
        rows=_empty_rows(),
        chart=_empty_chart(),
        origin="unavailable",
        detail=(
            f"Live retrieval failed: {_public(reason)} "
            "Recent observations are unavailable. "
            "Prepared historical measurements were not substituted for these dates. "
            "Missing dates were not filled. data.gov.in values were not substituted."
        ),
    )


def _filter_rows(
    frame: pd.DataFrame,
    query: MeasurementQuery,
    pollutant: str,
    station: str,
) -> pd.DataFrame:
    rows = frame.copy()
    rows["_day"] = period_start_days(rows["period_start"])
    start = pd.Timestamp(query.start)
    end = pd.Timestamp(query.end)
    rows = rows.loc[rows["_day"].notna() & rows["_day"].ge(start) & rows["_day"].le(end)].copy()
    if pollutant not in (ALL, "", None):
        rows = rows.loc[rows["parameter_name"].astype(str).eq(pollutant)].copy()
    if station not in (ALL, "", None):
        rows = rows.loc[rows["station_id"].astype(str).eq(str(station))].copy()
    return rows.drop(columns=["_day"]).reset_index(drop=True)


def _chart_rows(frame: pd.DataFrame) -> tuple[pd.DataFrame, tuple[str, ...]]:
    if frame.empty or not _has_daily_columns(frame):
        return _empty_chart(), ()
    working = frame.copy()
    working["_day"] = period_start_days(working["period_start"])
    working["_mean"] = pd.to_numeric(working["mean"], errors="coerce")
    working["_n"] = pd.to_numeric(working["n"], errors="coerce")
    usable = working.loc[
        working["_day"].notna() & working["_mean"].notna() & working["_n"].notna() & working["_n"].gt(0)
    ]
    skipped: list[str] = []
    records: list[dict[str, object]] = []
    if not usable.empty:
        for (day, name), part in usable.groupby(["_day", "parameter_name"], dropna=False, sort=True):
            units = {str(value) for value in part["unit"].dropna().unique()}
            if len(units) != 1:
                skipped.append(str(name))
                continue
            weight = part["_n"]
            records.append(
                {
                    "observation_date": pd.Timestamp(day),
                    "parameter_name": name,
                    "mean_valid_value": float((part["_mean"] * weight).sum() / weight.sum()),
                    "observations": int(weight.sum()),
                    "eligible_observations": int(weight.sum()),
                }
            )
    if not records:
        return _empty_chart(), tuple(skipped)
    return pd.DataFrame(records), tuple(skipped)


def _success_detail(
    origin: str,
    query: MeasurementQuery,
    pollutant: str,
    station: str,
    received: int,
    kept: int,
    skipped: tuple[str, ...],
) -> str:
    source = "Reused the cached XKDR daily response" if origin == "cache" else "Retrieved from the XKDR measurements API"
    selection = _selection_text(query, pollutant, station)
    if received and kept == 0:
        text = (
            f"{source}. Aggregation: daily. Source: cpcb_caaqm. {selection}. "
            "The daily response did not match the selected filters. Unmatched rows were not displayed."
        )
    elif kept == 0:
        text = (
            f"{source}. Aggregation: daily. Source: cpcb_caaqm. {selection}. "
            "The daily query returned no station rows for this selection."
        )
    else:
        text = (
            f"{source}. Aggregation: daily. Source: cpcb_caaqm. {selection}. "
            f"{kept} station-level daily rows. "
            "The chart is the mean of those rows weighted by n, not a raw hourly series."
        )
    if skipped:
        text += " Mixed units were not averaged for: " + ", ".join(skipped) + "."
    return text


def _selection_text(query: MeasurementQuery, pollutant: str, station: str) -> str:
    parts = ["pollutants PM2.5 and PM10" if pollutant in (ALL, "", None) else f"pollutant {pollutant}"]
    if query.state is not None:
        parts.append(f"state {query.state}")
    if query.city is not None:
        parts.append(f"city {query.city}")
    if station not in (ALL, "", None):
        parts.append(f"station {station}")
    if query.state is not None or query.city is not None:
        parts.append("place filters were sent to the API and rows were not relabelled with a city")
    return "; ".join(parts)


def _has_daily_columns(frame: pd.DataFrame) -> bool:
    return all(column in frame.columns for column in API_DAILY_COLUMNS)


def _empty_rows() -> pd.DataFrame:
    return pd.DataFrame(columns=list(API_DAILY_COLUMNS))


def _empty_chart() -> pd.DataFrame:
    return pd.DataFrame(columns=list(_CHART_COLUMNS))


def period_start_days(values: pd.Series) -> pd.Series:
    """Calendar dates of ``period_start`` without moving a naive timestamp.

    Timezone-aware values are converted to Asia/Kolkata first. Naive values
    stay on the clock time stored in the column.
    """

    stamps = pd.to_datetime(values, errors="coerce")
    if getattr(stamps.dt, "tz", None) is not None:
        stamps = stamps.dt.tz_convert(_KOLKATA).dt.tz_localize(None)
    return stamps.dt.normalize()


def _ordered_dates(start: object, end: object) -> tuple[date, date]:
    first = _as_date(start)
    last = _as_date(end)
    if last < first:
        return last, first
    return first, last


def _as_date(value: object) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return pd.Timestamp(value).date()


def _public(text: str) -> str:
    return _SECRET_TEXT.sub("[redacted]", text)
