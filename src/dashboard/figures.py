"""Plotly figures for the saved analytics tables."""

from __future__ import annotations

from datetime import date, datetime, timedelta

import pandas as pd
import plotly.graph_objects as go

POLLUTANT_COLORS = {
    "PM2.5": "#2EC4B6",
    "PM10": "#E0A45A",
}
_FALLBACK_COLORS = ("#3D4C63", "#6E4B7C", "#4E7C4A")
NO_PREPARED_OBSERVATIONS = "No prepared observations are available for this period."
_MISSING_SHADE = "rgba(142, 182, 255, 0.18)"
_MISSING_LABEL = "No prepared observations"


def time_series(
    frame: pd.DataFrame,
    *,
    y: str,
    title: str,
    y_title: str,
    highlight: str | None = None,
    highlight_name: str = "Flagged period",
    source: str | None = None,
    place: str | None = None,
    coverage_window: tuple[object, object] | None = None,
) -> go.Figure:
    """Draw one line per pollutant, with optional markers for stored flags.

    ``coverage_window`` shades calendar days in that inclusive range which have
    no row in ``frame``. It does not add or change measurement values.
    """

    figure = _base(title, "Date", y_title)
    if frame.empty:
        return figure
    for index, (pollutant, part) in enumerate(_pollutant_groups(frame)):
        color = _color(pollutant, index)
        plotted = _break_date_gaps(part)
        custom = list(
            zip(
                _hover_values(plotted, "observations"),
                _hover_values(plotted, "eligible_observations"),
            )
        )
        details = [f"Pollutant: {pollutant}"]
        if place:
            details.append(place)
        if source:
            details.append(f"Source: {source}")
        figure.add_trace(
            go.Scatter(
                x=plotted["observation_date"],
                y=plotted[y],
                name=str(pollutant),
                mode="lines+markers",
                connectgaps=False,
                line={"color": color, "width": 2},
                marker={"size": 6},
                customdata=custom,
                hovertemplate=(
                    "%{x|%d %b %Y}<br>"
                    + "<br>".join(details)
                    + "<br>"
                    + y_title
                    + ": %{y:.4g}<br>Observations in this point: %{customdata[0]}"
                    + "<br>Eligible observations: %{customdata[1]}<extra></extra>"
                ),
            )
        )
        if highlight and highlight in part.columns:
            flagged = part.loc[part[highlight]]
            figure.add_trace(
                go.Scatter(
                    x=flagged["observation_date"],
                    y=flagged[y],
                    name=f"{pollutant} {highlight_name}",
                    mode="markers",
                    marker={"size": 13, "symbol": "diamond", "color": color, "line": {"width": 1, "color": "#E8EEF4"}},
                    hovertemplate="%{x|%d %b %Y}<br>" + highlight_name + "<extra>" + str(pollutant) + "</extra>",
                )
            )
    if coverage_window is not None:
        _shade_missing_coverage(figure, frame, coverage_window)
    return figure


def missing_observation_periods(
    observed: object,
    start: object,
    end: object,
) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    """Inclusive calendar stretches inside the window that have no stored day.

    A day is kept clear when any supplied observation falls on it. The result
    does not invent a value for the missing days.
    """

    first = _as_date(start)
    last = _as_date(end)
    if first > last:
        first, last = last, first
    observed_days = _normalized_days(observed)
    missing: list[date] = []
    cursor = first
    while cursor <= last:
        if cursor not in observed_days:
            missing.append(cursor)
        cursor += timedelta(days=1)
    if not missing:
        return []
    periods: list[tuple[pd.Timestamp, pd.Timestamp]] = []
    gap_start = missing[0]
    previous = missing[0]
    for day in missing[1:]:
        if day != previous + timedelta(days=1):
            periods.append((pd.Timestamp(gap_start), pd.Timestamp(previous)))
            gap_start = day
        previous = day
    periods.append((pd.Timestamp(gap_start), pd.Timestamp(previous)))
    return periods


def hour_bars(frame: pd.DataFrame, *, y: str, title: str, y_title: str) -> go.Figure:
    figure = _base(title, "Hour of day", y_title)
    if frame.empty:
        return figure
    for index, (pollutant, part) in enumerate(_pollutant_groups(frame)):
        ordered = part.sort_values("observation_hour")
        figure.add_trace(
            go.Bar(
                x=ordered["observation_hour"],
                y=ordered[y],
                name=str(pollutant),
                marker_color=_color(pollutant, index),
                hovertemplate="Hour %{x}<br>" + y_title + ": %{y:.4g}<extra>" + str(pollutant) + "</extra>",
            )
        )
    figure.update_layout(barmode="group", xaxis={"dtick": 1})
    return figure


def coverage_heatmap(frame: pd.DataFrame) -> go.Figure:
    """Colour months by whether they were processed, partial, downloaded, or absent."""

    figure = go.Figure()
    figure.update_layout(
        title={"text": "Monthly XKDR coverage"},
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        font={"color": "#E8EEF4"},
        height=420,
    )
    if frame.empty:
        return figure
    codes = {
        "Not downloaded": 0,
        "Available remotely, not downloaded": 1,
        "Incomplete download": 2,
        "Download failed": 3,
        "Downloaded, not in the prepared series": 4,
        "Unavailable from the source": 5,
        "Processed, partial month": 6,
        "Processed": 7,
    }
    plot = frame.copy()
    plot["code"] = plot["label"].map(codes)
    grid = plot.pivot(index="year", columns="month", values="code")
    text = plot.pivot(index="year", columns="month", values="days_with_observations")
    figure.add_trace(
        go.Heatmap(
            z=grid.values,
            x=[f"{int(column):02d}" for column in grid.columns],
            y=[str(int(index)) for index in grid.index],
            text=text.values,
            coloraxis="coloraxis",
            hovertemplate="Year %{y}<br>Month %{x}<br>Days with observations: %{text}<extra></extra>",
        )
    )
    figure.update_layout(
        coloraxis={
            "colorscale": [
                [0.0, "#3D4C63"],
                [0.16, "#8EB6FF"],
                [0.32, "#C46B6B"],
                [0.48, "#A33B3B"],
                [0.64, "#6E8CA0"],
                [0.80, "#6E4B7C"],
                [0.92, "#E0A45A"],
                [1.0, "#2EC4B6"],
            ],
            "cmin": 0,
            "cmax": 7,
            "colorbar": {
                "tickmode": "array",
                "tickvals": [0, 1, 2, 3, 4, 5, 6, 7],
                "ticktext": [
                    "Not downloaded",
                    "Available remotely",
                    "Incomplete",
                    "Failed",
                    "Downloaded only",
                    "Unavailable",
                    "Partial",
                    "Processed",
                ],
            },
        },
        xaxis_title="Month",
        yaxis_title="Year",
    )
    return figure


def ranked_bars(frame: pd.DataFrame, *, label: str, value: str, title: str, x_title: str) -> go.Figure:
    """Horizontal bars for one already-filtered metric. Missing values are omitted."""

    figure = _base(title, x_title, "")
    if frame.empty or label not in frame.columns or value not in frame.columns:
        return figure
    ordered = frame.loc[frame[value].notna()].iloc[::-1]
    if ordered.empty:
        return figure
    figure.add_trace(
        go.Bar(
            x=ordered[value],
            y=ordered[label].astype(str),
            orientation="h",
            marker_color="#2EC4B6",
            hovertemplate="%{y}<br>" + x_title + ": %{x:.4g}<extra></extra>",
        )
    )
    figure.update_layout(showlegend=False, height=max(360, 28 * len(ordered) + 80), yaxis_title="")
    return figure


def station_bars(frame: pd.DataFrame, *, y: str, title: str, y_title: str) -> go.Figure:
    figure = _base(title, "Station", y_title)
    if frame.empty:
        return figure
    ordered = frame.iloc[::-1]
    custom = list(
        zip(
            ordered["eligible_observations"],
            ordered["spike_count"],
            ordered["spike_rate_of_eligible"].map(lambda value: "null" if pd.isna(value) else f"{value:.2%}"),
            ordered["city_name"].map(lambda value: "city not recorded" if pd.isna(value) else value),
        )
    )
    figure.add_trace(
        go.Bar(
            x=ordered[y],
            y=ordered["station_id"],
            orientation="h",
            marker_color="#2EC4B6",
            customdata=custom,
            hovertemplate=(
                "%{y}<br>"
                + y_title
                + ": %{x:.4g}<br>Spikes: %{customdata[1]}<br>Eligible: %{customdata[0]}"
                + "<br>Eligible rate: %{customdata[2]}<br>%{customdata[3]}<extra></extra>"
            ),
        )
    )
    figure.update_layout(showlegend=False, height=max(360, 28 * len(ordered) + 80))
    return figure


def station_map(
    frame: pd.DataFrame,
    *,
    color: str,
    color_title: str,
    title: str,
) -> go.Figure:
    """Map station points. Rows without coordinates are not added."""

    figure = go.Figure()
    located = frame.loc[frame["latitude"].notna() & frame["longitude"].notna() & frame[color].notna()]
    if located.empty:
        figure.update_layout(title={"text": title})
        return figure
    figure.add_trace(
        go.Scattermap(
            lat=located["latitude"],
            lon=located["longitude"],
            mode="markers",
            customdata=_map_custom(located, color),
            marker={
                "size": 11,
                "color": located[color],
                "colorscale": "YlOrRd",
                "showscale": True,
                "colorbar": {"title": {"text": color_title, "side": "right"}},
            },
            hovertemplate=(
                "%{customdata[0]}<br>%{customdata[1]}, %{customdata[2]}<br>"
                + color_title
                + ": %{customdata[6]}<br>Spikes: %{customdata[3]}<br>Eligible: %{customdata[4]}"
                + "<br>Mean: %{customdata[5]}<extra></extra>"
            ),
        )
    )
    figure.update_layout(
        title={"text": title},
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        font={"color": "#E8EEF4"},
        map_style="open-street-map",
        map={"center": {"lat": float(located["latitude"].mean()), "lon": float(located["longitude"].mean())}, "zoom": 9},
        margin={"l": 0, "r": 0, "t": 48, "b": 0},
        height=640,
    )
    return figure


def _map_custom(frame: pd.DataFrame, color_name: str) -> list[list[object]]:
    rows = []
    for record in frame.itertuples(index=False):
        city = getattr(record, "city_name")
        state = getattr(record, "state_name")
        rows.append(
            [
                record.station_id,
                "city not recorded" if pd.isna(city) else city,
                "state not recorded" if pd.isna(state) else state,
                int(record.spike_count),
                int(record.eligible_observations),
                "not available" if pd.isna(record.mean_valid_value) else f"{float(record.mean_valid_value):.2f}",
                "not available" if pd.isna(getattr(record, color_name)) else f"{float(getattr(record, color_name)):.4g}",
            ]
        )
    return rows


CITY_LINE_COLORS = ("#2EC4B6", "#E0A45A", "#8EB6FF")


def grouped_metric_bars(
    frame: pd.DataFrame,
    *,
    title: str,
    y_title: str,
    order: list[str],
    as_percent: bool = False,
) -> go.Figure:
    """Grouped bars for selected cities. Missing values are already absent from the frame."""

    figure = _base(title, "City", y_title)
    if frame.empty:
        return figure
    for index, (pollutant, part) in enumerate(_pollutant_groups(frame)):
        y_values = part["value"] * 100 if as_percent else part["value"]
        figure.add_trace(
            go.Bar(
                x=part["label"],
                y=y_values,
                name=str(pollutant),
                marker_color=_color(pollutant, index),
                hovertemplate="%{x}<br>" + y_title + ": %{y:.4g}<extra>" + str(pollutant) + "</extra>",
            )
        )
    figure.update_layout(barmode="group")
    figure.update_xaxes(categoryorder="array", categoryarray=order)
    if as_percent:
        figure.update_yaxes(ticksuffix="%")
    return figure


def multi_city_lines(
    frame: pd.DataFrame,
    *,
    x: str,
    y: str,
    title: str,
    x_title: str,
    y_title: str,
    order: list[str],
) -> go.Figure:
    """One line per selected city. The frame contains one pollutant."""

    figure = _base(title, x_title, y_title)
    if frame.empty or "city_label" not in frame.columns:
        return figure
    for index, label in enumerate(order):
        part = frame.loc[frame["city_label"].eq(label)].sort_values(x)
        if part.empty:
            continue
        figure.add_trace(
            go.Scatter(
                x=part[x],
                y=part[y],
                name=label,
                mode="lines+markers",
                line={"color": CITY_LINE_COLORS[index % len(CITY_LINE_COLORS)], "width": 2},
                marker={"size": 6},
                hovertemplate="%{x}<br>" + y_title + ": %{y:.4g}<extra>" + label + "</extra>",
            )
        )
    return figure


def reported_point_map(frame: pd.DataFrame, *, value: str, title: str, value_title: str) -> go.Figure:
    """Map stations that have valid coordinates. The colour is an API-reported value."""

    figure = go.Figure()
    located = frame.loc[frame["latitude"].notna() & frame["longitude"].notna() & frame[value].notna()]
    if located.empty:
        figure.update_layout(title={"text": title}, template="plotly_dark")
        return figure
    figure.add_trace(
        go.Scattermap(
            lat=located["latitude"],
            lon=located["longitude"],
            mode="markers",
            text=located["station"],
            customdata=list(
                zip(
                    located["city"].fillna("city not recorded"),
                    located["state"].fillna("state not recorded"),
                    located[value],
                )
            ),
            marker={
                "size": 11,
                "color": located[value],
                "colorscale": "YlOrRd",
                "showscale": True,
                "colorbar": {"title": {"text": value_title, "side": "right"}},
            },
            hovertemplate=(
                "%{text}<br>%{customdata[0]}, %{customdata[1]}<br>"
                + value_title
                + ": %{customdata[2]:.4g}<extra></extra>"
            ),
        )
    )
    figure.update_layout(
        title={"text": title},
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        font={"color": "#E8EEF4"},
        map_style="open-street-map",
        map={
            "center": {"lat": float(located["latitude"].mean()), "lon": float(located["longitude"].mean())},
            "zoom": 5,
        },
        margin={"l": 0, "r": 0, "t": 48, "b": 0},
        height=560,
    )
    return figure


def snapshot_trend(frame: pd.DataFrame, *, title: str) -> go.Figure:
    """Draw reported minimum, maximum, and average against API last_update.

    Each distinct observation timestamp is one x position. Missing numbers stay
    missing, and the local retrieval time is not used as the time axis.
    """

    figure = _base(title, "API last_update", "API-reported value")
    if frame.empty or "observed_at" not in frame.columns:
        return figure
    ordered = frame.sort_values("observed_at").drop_duplicates(subset=["observed_at"], keep="first")
    series = (
        ("min_value", "Reported minimum", "#8EB6FF"),
        ("max_value", "Reported maximum", "#E0A45A"),
        ("avg_value", "Reported average", "#2EC4B6"),
    )
    for column, name, color in series:
        if column not in ordered.columns:
            continue
        values = [None if pd.isna(value) else float(value) for value in ordered[column]]
        if all(value is None for value in values):
            continue
        custom = list(
            zip(
                _hover_text(ordered, "station"),
                _hover_text(ordered, "city"),
                _hover_text(ordered, "pollutant_id"),
                _hover_text(ordered, "unit", fallback="not documented by the API"),
            )
        )
        figure.add_trace(
            go.Scatter(
                x=ordered["observed_at"],
                y=values,
                name=name,
                mode="lines+markers",
                connectgaps=False,
                line={"color": color, "width": 2},
                marker={"size": 7},
                customdata=custom,
                hovertemplate=(
                    "%{x|%d %b %Y %H:%M}<br>Station: %{customdata[0]}<br>City: %{customdata[1]}"
                    "<br>Pollutant: %{customdata[2]}<br>"
                    + f"{name}: %{{y:.4g}}<br>Unit: %{{customdata[3]}}<br>Source: data.gov.in<extra></extra>"
                ),
            )
        )
    return figure


def recent_avg_series(
    frame: pd.DataFrame,
    *,
    title: str,
    y_title: str,
    markers_only: bool = False,
) -> go.Figure:
    """Draw the mean of stored avg_value at each API last_update.

    One trace is one pollutant. Timestamps that were not stored are not added,
    and a missing average stays missing. ``markers_only`` draws the single
    stored timestamp without a line.
    """

    figure = _base(title, "Observation timestamp", y_title)
    if frame.empty or "observed_at" not in frame.columns or "pollutant_id" not in frame.columns:
        return figure
    pollutants = sorted(str(value) for value in frame["pollutant_id"].dropna().unique())
    for index, pollutant in enumerate(pollutants):
        part = frame.loc[frame["pollutant_id"].astype(str).eq(pollutant)].sort_values("observed_at")
        values = [None if pd.isna(value) else float(value) for value in part["avg_value"]]
        if all(value is None for value in values):
            continue
        counts = part["station_count"] if "station_count" in part.columns else pd.Series([None] * len(part))
        color = POLLUTANT_COLORS.get(pollutant, _FALLBACK_COLORS[index % len(_FALLBACK_COLORS)])
        figure.add_trace(
            go.Scatter(
                x=part["observed_at"],
                y=values,
                name=pollutant,
                mode="markers" if markers_only else "lines+markers",
                connectgaps=False,
                line={"color": color, "width": 2},
                marker={"size": 8, "color": color},
                customdata=list(counts),
                hovertemplate=(
                    "%{x|%d %b %Y %H:%M}<br>"
                    + pollutant
                    + ": %{y:.4g}<br>Contributing stations: %{customdata}<br>"
                    "Source: data.gov.in real-time observations<extra></extra>"
                ),
            )
        )
    return figure


def recent_snapshot_bars(frame: pd.DataFrame, *, title: str, y_title: str) -> go.Figure:
    """Draw one bar per pollutant for a single stored API timestamp."""

    figure = _base(title, "Pollutant", y_title)
    if frame.empty or "pollutant_id" not in frame.columns or "avg_value" not in frame.columns:
        return figure
    part = frame.loc[frame["avg_value"].notna()].sort_values("pollutant_id")
    if part.empty:
        return figure
    figure.add_trace(
        go.Bar(
            x=part["pollutant_id"].astype(str),
            y=[float(value) for value in part["avg_value"]],
            name="API avg_value",
            marker={"color": "#2EC4B6"},
            hovertemplate=(
                "%{x}<br>API avg_value: %{y:.4g}<br>"
                "Source: data.gov.in real-time observations<extra></extra>"
            ),
        )
    )
    return figure


def _shade_missing_coverage(
    figure: go.Figure,
    frame: pd.DataFrame,
    coverage_window: tuple[object, object],
) -> None:
    """Shade empty days on the selected axis. Observed dates stay unshaded."""

    start, end = coverage_window
    first = _as_date(start)
    last = _as_date(end)
    if first > last:
        first, last = last, first
    periods = missing_observation_periods(
        frame["observation_date"] if "observation_date" in frame.columns else [],
        first,
        last,
    )
    figure.update_xaxes(
        range=[
            datetime.combine(first, datetime.min.time()),
            datetime.combine(last, datetime.min.time()) + timedelta(days=1),
        ]
    )
    if not periods:
        return
    for gap_start, gap_end in periods:
        start_at = datetime.combine(gap_start.date(), datetime.min.time())
        end_at = datetime.combine(gap_end.date(), datetime.min.time()) + timedelta(days=1, microseconds=-1)
        figure.add_vrect(
            x0=start_at,
            x1=end_at,
            fillcolor=_MISSING_SHADE,
            opacity=1,
            layer="below",
            line_width=0,
        )
        if (gap_end.date() - gap_start.date()).days + 1 >= 7:
            figure.add_annotation(
                x=start_at + (end_at - start_at) / 2,
                y=0.98,
                yref="paper",
                yanchor="top",
                text=_MISSING_LABEL,
                showarrow=False,
                font={"color": "#E8EEF4", "size": 12},
                bgcolor="rgba(11, 31, 51, 0.72)",
            )
    figure.add_trace(
        go.Scatter(
            x=[None],
            y=[None],
            name=_MISSING_LABEL,
            mode="markers",
            marker={"size": 12, "symbol": "square", "color": "rgba(142, 182, 255, 0.7)"},
            hoverinfo="skip",
            showlegend=True,
        )
    )


def _as_date(value: object) -> date:
    return pd.Timestamp(value).normalize().date()


def _normalized_days(observed: object) -> set[date]:
    if observed is None:
        return set()
    values = pd.to_datetime(pd.Series(list(observed)), errors="coerce").dropna()
    return {_as_date(value) for value in values}


def _hover_values(frame: pd.DataFrame, column: str) -> list[object]:
    if column not in frame.columns:
        return [None] * len(frame)
    return [None if pd.isna(value) else value for value in frame[column]]


def _hover_text(frame: pd.DataFrame, column: str, fallback: str = "not recorded") -> list[str]:
    if column not in frame.columns:
        return [fallback] * len(frame)
    return [fallback if pd.isna(value) or str(value).strip() == "" else str(value) for value in frame[column]]


def _break_date_gaps(frame: pd.DataFrame, max_gap_days: int = 1) -> pd.DataFrame:
    """Insert an empty point where successive dates are more than one day apart.

    The empty point is not an observation. With ``connectgaps`` off, the line
    stops instead of drawing across a month that was never measured.
    """

    if frame.empty or "observation_date" not in frame.columns or len(frame) < 2:
        return frame
    ordered = frame.sort_values("observation_date")
    records: list[dict[str, object]] = []
    previous: pd.Timestamp | None = None
    for record in ordered.to_dict(orient="records"):
        current = pd.Timestamp(record["observation_date"])
        if previous is not None and (current.normalize() - previous.normalize()).days > max_gap_days:
            gap = {key: None for key in record}
            gap["observation_date"] = previous.normalize() + pd.DateOffset(days=max_gap_days)
            if "parameter_name" in record:
                gap["parameter_name"] = record["parameter_name"]
            records.append(gap)
        records.append(record)
        previous = current
    return pd.DataFrame(records)


def _pollutant_groups(frame: pd.DataFrame):
    if "parameter_name" not in frame.columns:
        return [("Series", frame)]
    return list(frame.groupby("parameter_name", sort=True))


def _color(pollutant: object, index: int) -> str:
    return POLLUTANT_COLORS.get(str(pollutant), _FALLBACK_COLORS[index % len(_FALLBACK_COLORS)])


def _base(title: str, x_title: str, y_title: str) -> go.Figure:
    figure = go.Figure()
    figure.update_layout(
        title={"text": title},
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font={"color": "#E8EEF4"},
        xaxis_title=x_title,
        yaxis_title=y_title,
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "x": 0},
        margin={"l": 48, "r": 24, "t": 72, "b": 48},
        height=440,
    )
    return figure
