"""Plotly figures for the saved analytics tables."""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go

POLLUTANT_COLORS = {
    "PM2.5": "#2EC4B6",
    "PM10": "#E0A45A",
}
_FALLBACK_COLORS = ("#3D4C63", "#6E4B7C", "#4E7C4A")


def time_series(
    frame: pd.DataFrame,
    *,
    y: str,
    title: str,
    y_title: str,
    highlight: str | None = None,
    highlight_name: str = "Flagged period",
) -> go.Figure:
    """Draw one line per pollutant, with optional markers for stored flags."""

    figure = _base(title, "Date", y_title)
    if frame.empty:
        return figure
    for index, (pollutant, part) in enumerate(_pollutant_groups(frame)):
        color = _color(pollutant, index)
        custom = part["eligible_observations"] if "eligible_observations" in part.columns else None
        figure.add_trace(
            go.Scatter(
                x=part["observation_date"],
                y=part[y],
                name=str(pollutant),
                mode="lines+markers",
                line={"color": color, "width": 2},
                marker={"size": 6},
                customdata=custom,
                hovertemplate=(
                    "%{x|%d %b %Y}<br>"
                    + y_title
                    + ": %{y:.4g}<br>Eligible observations: %{customdata}<extra>"
                    + str(pollutant)
                    + "</extra>"
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
    return figure


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
