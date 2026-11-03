"""Aggregate annotated CAAQM measurements by time, station, and place.

The caller supplies ``AppConfig`` and the shared ``SparkSession``. This
module does not build a session, does not score spikes, and does not write
under ``data/raw`` or over the measurement and spike Parquet files.

Averages use valid measurements only. Spike rates use two denominators:
every observation in the group, and the eligible observations from spike
detection. City and state names are attached only when a station has one
non-null value. Coordinates remain station points.
"""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from preprocessing.clean import PreprocessingError, _fingerprint, _write_parquet
from utils.config_loader import AppConfig, inventory_inputs

_REPORT_NAME = "analytics_report.json"
_INPUT_PARTS = ("spikes", "annotated", "data.parquet")

DENOMINATORS = {
    "mean_valid_value": (
        "Average of values whose is_valid_measurement flag is true. "
        "Missing, non-numeric, non-finite, and negative values stay in the observation count "
        "and are excluded from the average."
    ),
    "spike_count": "Rows with is_spike true. This is a raw count, not a rate.",
    "eligible_observations": (
        "Rows that spike detection marked eligible. When spike_threshold is present, "
        "eligibility is a non-null spike_threshold. That column was set only for a valid, "
        "uniquely ordered measurement with at least spikes.minimum_baseline previous valid "
        "observations and a sample standard deviation. Otherwise eligibility is recomputed "
        "with the same rule from is_valid_measurement, ordering_defined, baseline_count, "
        "and rolling_stddev."
    ),
    "spike_rate_of_observations": (
        "spike_count divided by observations. Null when the group has no observations."
    ),
    "spike_rate_of_eligible": (
        "spike_count divided by eligible_observations. Null when the group has no eligible observations. "
        "Station spike percentages use this denominator."
    ),
    "consistently_elevated": (
        "A high typical concentration, not a high spike count. "
        "The mean of valid measurements is compared with other station-pollutant series "
        "of the same pollutant that have at least elevated_minimum_valid_observations valid values. "
        "A series is consistently elevated when at least two series are in that comparison "
        "and its mean is at or above the configured exact percentile of those means."
    ),
    "unusually_high": (
        "Computed separately for each pollutant across observed dates and across observed "
        "date-hour groups. A period is unusually high on counts when its raw spike_count is "
        "strictly greater than the mean plus unusual_sigmas sample standard deviations of "
        "spike_count. It is unusually high on rates when its spike_rate_of_eligible meets the "
        "same test. Count flags and rate flags are independent. Hours with no rows are left "
        "absent rather than filled with zeros."
    ),
    "geography": (
        "A city or state name is used only when that station has exactly one distinct non-null value. "
        "Conflicting names are not collapsed, and the station is left out of that aggregate. "
        "Latitude and longitude are station points copied from the annotated rows. "
        "They are not administrative boundaries and are not averaged into a city or state centroid. "
        "A station with more than one latitude or longitude keeps neither coordinate. "
        "Missing city, state, and coordinate values stay missing."
    ),
}


class AnalyticsError(Exception):
    """The annotated measurements cannot be aggregated as configured."""


@dataclass(frozen=True)
class AnalyticsReport:
    path: Path
    output_dir: Path
    document: dict[str, Any]


def annotated_spikes_file(config: AppConfig) -> Path:
    path = config.paths.processed_dir
    for part in _INPUT_PARTS:
        path = path / part
    return path


def analyze_dataset(
    config: AppConfig,
    spark: SparkSession,
    *,
    report_path: Path | None = None,
    output_dir: Path | None = None,
) -> AnalyticsReport:
    """Aggregate the annotated spike Parquet file and write the analytics outputs."""

    source = annotated_spikes_file(config)
    if not source.is_file():
        raise AnalyticsError(
            f"Annotated measurements not found: {source}. Run detect-spikes before analyze."
        )
    if spark is None:
        raise AnalyticsError("analyze_dataset requires the shared SparkSession.")

    destination = output_dir or (config.paths.processed_dir / "analytics")
    _refuse_output(config, source, destination)
    fingerprints = _fingerprints(_protected_files(config, source))

    frame = spark.read.parquet(str(source.resolve()))
    prepared = _prepare(frame, config).cache()
    try:
        published = _publish_all(prepared, spark, config, destination)
        document = _report_document(config, source, destination, prepared, published)
    finally:
        prepared.unpersist()

    _require_unchanged(fingerprints)
    document["raw_files_modified"] = False
    document["input_file_modified"] = False
    document["protected_file_fingerprints"] = fingerprints
    report_destination = report_path if report_path is not None else config.paths.interim_dir / _REPORT_NAME
    _write_json(report_destination, document)
    return AnalyticsReport(path=report_destination, output_dir=destination, document=document)


def format_analytics_report(report: AnalyticsReport) -> str:
    document = report.document
    counts = document["counts"]
    coverage = document["coverage"]
    lines = [
        f"Analytics report: {report.path}",
        f"Analytics directory: {report.output_dir}",
        f"Total records: {counts['total_records']}",
        f"Valid measurements: {counts['valid_measurements']}",
        f"Eligible records: {counts['eligible_records']}",
        f"Candidate spikes: {counts['spike_records']}",
        (
            "Eligible denominator: "
            f"{document['parameters']['eligible_rule']}"
        ),
        (
            "Stations: "
            f"{coverage['stations']}; "
            f"without city: {coverage['stations_without_city']}; "
            f"without coordinates: {coverage['stations_without_coordinates']}; "
            f"city conflicts: {coverage['stations_with_city_conflict']}; "
            f"coordinate conflicts: {coverage['stations_with_coordinate_conflict']}"
        ),
        f"Consistently elevated series: {len(document['elevated_stations'])}",
        f"Unusually high dates: {len(document['unusual_dates'])}",
        f"Unusually high hours: {len(document['unusual_hours'])}",
        "Highest raw spike counts:",
    ]
    rankings = document["highest_spike_counts"] or []
    if not rankings:
        lines.append("  none")
    for row in rankings:
        rate = row["spike_rate_of_eligible"]
        rate_text = "null" if rate is None else f"{rate:.6f}"
        lines.append(
            "  "
            f"{row['parameter_name']} rank {row['spike_count_rank']}: "
            f"{row['station_id']} spikes {row['spike_count']} "
            f"eligible {row['eligible_observations']} "
            f"eligible rate {rate_text}"
        )
    return "\n".join(lines)


def _prepare(frame: DataFrame, config: AppConfig) -> DataFrame:
    columns = config.dataset.columns
    required = [
        columns.station_id,
        columns.parameter,
        columns.timestamp,
        columns.value,
        "is_valid_measurement",
        "ordering_defined",
        "baseline_count",
        "rolling_stddev",
        "is_spike",
    ]
    missing = [name for name in required if name not in frame.columns]
    if missing:
        raise AnalyticsError(
            "Annotated measurements are missing columns: "
            + ", ".join(missing)
            + ". Actual columns: "
            + ", ".join(frame.columns)
        )

    prepared = _null_if_blank(frame, columns.city)
    if "state_name" in prepared.columns:
        prepared = _null_if_blank(prepared, "state_name")

    valid = F.coalesce(F.col("is_valid_measurement").cast("boolean"), F.lit(False))
    ordered = F.coalesce(F.col("ordering_defined").cast("boolean"), F.lit(False))
    recomputed = (
        valid
        & ordered
        & F.col("baseline_count").isNotNull()
        & (F.col("baseline_count") >= F.lit(config.spikes.minimum_baseline))
        & F.col("rolling_stddev").isNotNull()
    )
    if "spike_threshold" in prepared.columns:
        eligible = F.col("spike_threshold").isNotNull()
    else:
        eligible = recomputed

    return (
        prepared.withColumn("_valid", valid)
        .withColumn("_recomputed_eligible", recomputed)
        .withColumn("_eligible", eligible)
        .withColumn("_spike", F.coalesce(F.col("is_spike").cast("boolean"), F.lit(False)))
        .withColumn("_valid_value", F.when(valid, F.col(columns.value).cast("double")))
        .withColumn("_observation_date", F.to_date(F.col(columns.timestamp)))
        .withColumn("_observation_hour", F.hour(F.col(columns.timestamp)))
    )


def _publish_all(
    prepared: DataFrame,
    spark: SparkSession,
    config: AppConfig,
    output_dir: Path,
) -> dict[str, dict[str, Any]]:
    columns = config.dataset.columns
    parameter = columns.parameter
    station = columns.station_id
    places = _station_places(prepared, config).cache()
    try:
        temporal = prepared.where(F.col(columns.timestamp).isNotNull())
        hourly = _mark_unusual(
            _with_rates(
                temporal.groupBy("_observation_date", "_observation_hour", parameter).agg(*_aggregates())
            ),
            parameter,
            config.analytics.unusual_sigmas,
        )
        daily = _mark_unusual(
            _with_rates(temporal.groupBy("_observation_date", parameter).agg(*_aggregates())),
            parameter,
            config.analytics.unusual_sigmas,
        )
        by_hour = _with_rates(temporal.groupBy("_observation_hour", parameter).agg(*_aggregates()))
        station_metrics = _with_ranks(
            _with_rates(prepared.groupBy(station, parameter).agg(*_aggregates())),
            parameter,
        )
        station_summary = _with_elevated(station_metrics.join(places, station, "left"), config)
        comparison = _station_comparison(places, station_summary, config)
        located = prepared.join(
            places.select(
                station,
                F.col("city_name").alias("_city"),
                F.col("state_name").alias("_state"),
                F.col("latitude").alias("_latitude"),
                F.col("longitude").alias("_longitude"),
                "city_conflict",
                "state_conflict",
            ),
            station,
            "left",
        )
        city = _with_rates(
            located.where(F.col("_city").isNotNull() & ~F.col("city_conflict"))
            .groupBy(F.col("_city").alias("city_name"), F.col("_state").alias("state_name"), parameter)
            .agg(*_aggregates(station))
        )
        state = _with_rates(
            located.where(F.col("_state").isNotNull() & ~F.col("state_conflict"))
            .groupBy(F.col("_state").alias("state_name"), parameter)
            .agg(*_aggregates(station))
        )
        elevated = station_summary.where(F.col("consistently_elevated"))

        datasets = {
            "hourly_by_pollutant": (
                hourly,
                ["observation_date", "observation_hour", parameter],
                _TEMPORAL_COLUMNS,
            ),
            "daily_by_pollutant": (daily, ["observation_date", parameter], _DAILY_COLUMNS),
            "hour_of_day": (by_hour, ["observation_hour", parameter], _PATTERN_COLUMNS),
            "station_pollutant": (
                station_summary,
                [parameter, "spike_count_rank", station],
                _STATION_COLUMNS,
            ),
            "station_comparison": (comparison, [station], list(comparison.columns)),
            "city_pollutant": (city, ["city_name", "state_name", parameter], _PLACE_COLUMNS),
            "state_pollutant": (state, ["state_name", parameter], _STATE_COLUMNS),
            "elevated_stations": (
                elevated,
                [parameter, "spike_count_rank", station],
                _STATION_COLUMNS,
            ),
        }
        published: dict[str, dict[str, Any]] = {}
        csv_dir = output_dir / "csv"
        for name, (frame, order, selected) in datasets.items():
            projected = _project_temporal(frame, selected) if name in {"hourly_by_pollutant", "daily_by_pollutant", "hour_of_day"} else frame.select(*selected)
            published[name] = _publish(
                projected,
                spark,
                config,
                output_dir / name,
                csv_dir / f"{name}.csv",
                order,
            )
        published["pollutant_totals"] = {
            "records": _records(
                _with_rates(prepared.groupBy(parameter).agg(*_aggregates())).orderBy(parameter)
            )
        }
        return published
    finally:
        places.unpersist()


def _project_temporal(frame: DataFrame, columns: list[str]) -> DataFrame:
    renamed = frame.withColumnRenamed("_observation_date", "observation_date").withColumnRenamed(
        "_observation_hour", "observation_hour"
    )
    return renamed.select(*columns)


def _publish(
    frame: DataFrame,
    spark: SparkSession,
    config: AppConfig,
    directory: Path,
    csv_path: Path,
    order: list[str],
) -> dict[str, Any]:
    ordered = frame.orderBy(*[F.col(name) for name in order])
    try:
        parquet_path = _write_parquet(ordered, directory, config)
    except PreprocessingError as exc:
        raise AnalyticsError(str(exc)) from exc
    records = _records(ordered)
    _write_csv(csv_path, ordered.columns, records)
    read_count = int(spark.read.parquet(str(parquet_path.resolve())).count())
    if read_count != len(records):
        raise AnalyticsError(
            f"{directory.name} has {read_count} rows on disk and {len(records)} aggregated rows."
        )
    return {
        "parquet": str(parquet_path.resolve()),
        "csv": str(csv_path.resolve()),
        "rows": len(records),
        "records": records,
    }


def _aggregates(station_column: str | None = None) -> list[F.Column]:
    items = [
        F.count(F.lit(1)).alias("observations"),
        F.sum(F.when(F.col("_valid"), F.lit(1)).otherwise(F.lit(0))).alias("valid_observations"),
        F.avg("_valid_value").alias("mean_valid_value"),
        F.sum(F.when(F.col("_spike"), F.lit(1)).otherwise(F.lit(0))).alias("spike_count"),
        F.sum(F.when(F.col("_eligible"), F.lit(1)).otherwise(F.lit(0))).alias("eligible_observations"),
    ]
    if station_column is not None:
        items.append(F.countDistinct(F.col(station_column)).alias("stations"))
        items.append(
            F.countDistinct(
                F.when(
                    F.col("_latitude").isNotNull() & F.col("_longitude").isNotNull(),
                    F.col(station_column),
                )
            ).alias("stations_with_coordinates")
        )
    return items


def _with_rates(frame: DataFrame) -> DataFrame:
    spikes = F.col("spike_count").cast("double")
    return frame.withColumn(
        "spike_rate_of_observations",
        F.when(F.col("observations") > 0, spikes / F.col("observations").cast("double")),
    ).withColumn(
        "spike_rate_of_eligible",
        F.when(F.col("eligible_observations") > 0, spikes / F.col("eligible_observations").cast("double")),
    )


def _mark_unusual(frame: DataFrame, parameter: str, sigmas: float) -> DataFrame:
    window = Window.partitionBy(parameter)
    count_mean = F.avg("spike_count").over(window)
    count_std = F.stddev_samp("spike_count").over(window)
    rate_mean = F.avg("spike_rate_of_eligible").over(window)
    rate_std = F.stddev_samp("spike_rate_of_eligible").over(window)
    count_threshold = count_mean + F.lit(sigmas) * count_std
    rate_threshold = rate_mean + F.lit(sigmas) * rate_std
    count_flag = count_threshold.isNotNull() & (F.col("spike_count").cast("double") > count_threshold)
    rate_flag = (
        rate_threshold.isNotNull()
        & F.col("spike_rate_of_eligible").isNotNull()
        & (F.col("spike_rate_of_eligible") > rate_threshold)
    )
    return (
        frame.withColumn("spike_count_mean", count_mean)
        .withColumn("spike_count_stddev", count_std)
        .withColumn("spike_count_threshold", count_threshold)
        .withColumn("unusually_high_spike_count", F.coalesce(count_flag, F.lit(False)))
        .withColumn("eligible_spike_rate_mean", rate_mean)
        .withColumn("eligible_spike_rate_stddev", rate_std)
        .withColumn("eligible_spike_rate_threshold", rate_threshold)
        .withColumn("unusually_high_eligible_rate", F.coalesce(rate_flag, F.lit(False)))
    )


def _with_ranks(frame: DataFrame, parameter: str) -> DataFrame:
    count_order = Window.partitionBy(parameter).orderBy(F.desc("spike_count"))
    rate_order = Window.partitionBy(parameter).orderBy(F.desc_nulls_last("spike_rate_of_eligible"))
    return frame.withColumn("spike_count_rank", F.rank().over(count_order)).withColumn(
        "spike_rate_rank", F.rank().over(rate_order)
    )


def _with_elevated(frame: DataFrame, config: AppConfig) -> DataFrame:
    parameter = config.dataset.columns.parameter
    minimum = config.analytics.elevated_minimum_valid_observations
    covered = frame.where(
        (F.col("valid_observations") >= F.lit(minimum)) & F.col("mean_valid_value").isNotNull()
    )
    thresholds = covered.groupBy(parameter).agg(
        F.count(F.lit(1)).alias("comparison_series"),
        F.percentile("mean_valid_value", float(config.analytics.elevated_percentile)).alias("elevated_threshold"),
    )
    joined = frame.join(thresholds, parameter, "left").withColumn(
        "comparison_series",
        F.coalesce(F.col("comparison_series"), F.lit(0)),
    )
    elevated = (
        (F.col("comparison_series") >= F.lit(2))
        & (F.col("valid_observations") >= F.lit(minimum))
        & F.col("mean_valid_value").isNotNull()
        & F.col("elevated_threshold").isNotNull()
        & (F.col("mean_valid_value") >= F.col("elevated_threshold"))
    )
    return joined.withColumn("consistently_elevated", F.coalesce(elevated, F.lit(False)))


def _station_places(frame: DataFrame, config: AppConfig) -> DataFrame:
    station = config.dataset.columns.station_id
    city = config.dataset.columns.city if config.dataset.columns.city in frame.columns else None
    state = "state_name" if "state_name" in frame.columns else None
    latitude = "latitude" if "latitude" in frame.columns else None
    longitude = "longitude" if "longitude" in frame.columns else None
    aggregates = [F.count(F.lit(1)).alias("_rows")]
    if city is not None:
        aggregates.extend(
            [F.countDistinct(F.col(city)).alias("_city_n"), F.min(F.col(city)).alias("_city_min")]
        )
    if state is not None:
        aggregates.extend(
            [F.countDistinct(F.col(state)).alias("_state_n"), F.min(F.col(state)).alias("_state_min")]
        )
    if latitude is not None and longitude is not None:
        aggregates.extend(
            [
                F.countDistinct(F.col(latitude)).alias("_lat_n"),
                F.min(F.col(latitude)).alias("_lat_min"),
                F.countDistinct(F.col(longitude)).alias("_lon_n"),
                F.min(F.col(longitude)).alias("_lon_min"),
            ]
        )
    grouped = frame.groupBy(station).agg(*aggregates)
    city_name = (
        F.when(F.col("_city_n") == F.lit(1), F.col("_city_min")).cast("string")
        if city is not None
        else F.lit(None).cast("string")
    )
    city_conflict = (F.col("_city_n") > F.lit(1)) if city is not None else F.lit(False)
    state_name = (
        F.when(F.col("_state_n") == F.lit(1), F.col("_state_min")).cast("string")
        if state is not None
        else F.lit(None).cast("string")
    )
    state_conflict = (F.col("_state_n") > F.lit(1)) if state is not None else F.lit(False)
    if latitude is not None and longitude is not None:
        coordinate_conflict = (F.col("_lat_n") > F.lit(1)) | (F.col("_lon_n") > F.lit(1))
        resolved_lat = F.when((F.col("_lat_n") == F.lit(1)) & ~coordinate_conflict, F.col("_lat_min"))
        resolved_lon = F.when((F.col("_lon_n") == F.lit(1)) & ~coordinate_conflict, F.col("_lon_min"))
    else:
        coordinate_conflict = F.lit(False)
        resolved_lat = F.lit(None).cast("double")
        resolved_lon = F.lit(None).cast("double")
    return grouped.select(
        F.col(station),
        city_name.alias("city_name"),
        state_name.alias("state_name"),
        resolved_lat.alias("latitude"),
        resolved_lon.alias("longitude"),
        city_conflict.alias("city_conflict"),
        state_conflict.alias("state_conflict"),
        coordinate_conflict.alias("coordinate_conflict"),
    )


def _station_comparison(places: DataFrame, station_summary: DataFrame, config: AppConfig) -> DataFrame:
    station = config.dataset.columns.station_id
    parameter = config.dataset.columns.parameter
    comparison = places
    value_columns = [
        "mean_valid_value",
        "valid_observations",
        "spike_count",
        "eligible_observations",
        "spike_rate_of_eligible",
    ]
    for pollutant in config.dataset.pollutants:
        prefix = pollutant.replace(".", "_").replace(" ", "_")
        subset = station_summary.where(F.col(parameter) == F.lit(pollutant)).select(
            station,
            *[F.col(name).alias(f"{prefix}_{name}") for name in value_columns],
        )
        comparison = comparison.join(subset, station, "left")
    return comparison


def _report_document(
    config: AppConfig,
    source: Path,
    output_dir: Path,
    prepared: DataFrame,
    published: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    columns = config.dataset.columns
    totals = prepared.agg(
        F.count(F.lit(1)).alias("rows"),
        F.sum(F.when(F.col("_valid"), F.lit(1)).otherwise(F.lit(0))).alias("valid"),
        F.sum(F.when(F.col("_eligible"), F.lit(1)).otherwise(F.lit(0))).alias("eligible"),
        F.sum(F.when(F.col("_recomputed_eligible"), F.lit(1)).otherwise(F.lit(0))).alias("recomputed_eligible"),
        F.sum(F.when(F.col("_spike"), F.lit(1)).otherwise(F.lit(0))).alias("spikes"),
        F.sum(F.when(F.col(columns.timestamp).isNull(), F.lit(1)).otherwise(F.lit(0))).alias("null_timestamps"),
        F.sum(F.when(~F.col("_eligible").eqNullSafe(F.col("_recomputed_eligible")), F.lit(1)).otherwise(F.lit(0))).alias(
            "eligible_disagreement"
        ),
    ).collect()[0]
    total_rows = int(totals["rows"] or 0)
    valid_rows = int(totals["valid"] or 0)
    eligible_rows = int(totals["eligible"] or 0)
    spike_rows = int(totals["spikes"] or 0)
    null_timestamps = int(totals["null_timestamps"] or 0)
    _require_partition_totals(published, total_rows, null_timestamps)

    coverage = _coverage(prepared, config, published)
    station_rows = published["station_pollutant"]["records"]
    daily_rows = published["daily_by_pollutant"]["records"]
    hourly_rows = published["hourly_by_pollutant"]["records"]
    rule = (
        "non-null spike_threshold"
        if "spike_threshold" in prepared.columns
        else "recomputed from baseline columns and spikes.minimum_baseline"
    )
    generated_at = datetime.now(ZoneInfo(config.dataset.timezone)).isoformat(timespec="seconds")
    outputs = {
        name: {"parquet": item["parquet"], "csv": item["csv"], "rows": item["rows"]}
        for name, item in published.items()
        if "parquet" in item
    }
    return {
        "schema_version": 1,
        "generated_at": generated_at,
        "timezone": config.dataset.timezone,
        "input_file": str(source.resolve()),
        "output_dir": str(output_dir.resolve()),
        "output_writer": "pyarrow",
        "denominators": DENOMINATORS,
        "parameters": {
            "minimum_baseline": config.spikes.minimum_baseline,
            "elevated_minimum_valid_observations": config.analytics.elevated_minimum_valid_observations,
            "elevated_percentile": config.analytics.elevated_percentile,
            "unusual_sigmas": config.analytics.unusual_sigmas,
            "standard_deviation": "sample",
            "eligible_rule": rule,
            "current_config_recomputed_eligible_records": int(totals["recomputed_eligible"] or 0),
            "eligible_definition_disagreement": int(totals["eligible_disagreement"] or 0),
        },
        "counts": {
            "total_records": total_rows,
            "valid_measurements": valid_rows,
            "eligible_records": eligible_rows,
            "spike_records": spike_rows,
            "null_timestamps": null_timestamps,
            "spike_rate_of_observations": _rate(spike_rows, total_rows),
            "spike_rate_of_eligible": _rate(spike_rows, eligible_rows),
        },
        "coverage": coverage,
        "pollutant_totals": published["pollutant_totals"]["records"],
        "daily_by_pollutant": daily_rows,
        "hour_of_day": published["hour_of_day"]["records"],
        "station_rankings": station_rows,
        "station_comparison": published["station_comparison"]["records"],
        "city_pollutant": published["city_pollutant"]["records"],
        "state_pollutant": published["state_pollutant"]["records"],
        "elevated_stations": published["elevated_stations"]["records"],
        "unusual_dates": [
            row
            for row in daily_rows
            if row["unusually_high_spike_count"] or row["unusually_high_eligible_rate"]
        ],
        "unusual_hours": [
            row
            for row in hourly_rows
            if row["unusually_high_spike_count"] or row["unusually_high_eligible_rate"]
        ],
        "highest_spike_counts": [
            row for row in station_rows if row["spike_count_rank"] is not None and row["spike_count_rank"] <= 10
        ],
        "outputs": outputs,
    }


def _coverage(
    prepared: DataFrame,
    config: AppConfig,
    published: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    columns = config.dataset.columns
    station = columns.station_id
    city_present = columns.city in prepared.columns
    state_present = "state_name" in prepared.columns
    coordinates_present = "latitude" in prepared.columns and "longitude" in prepared.columns
    places = {
        (row[station], row["parameter_name"])
        for row in published["station_pollutant"]["records"]
    }
    station_ids = {row[station] for row in published["station_comparison"]["records"]}
    city_names = {row["city_name"] for row in published["city_pollutant"]["records"]}
    state_names = {row["state_name"] for row in published["state_pollutant"]["records"]}
    city_rows = sum(row["observations"] for row in published["city_pollutant"]["records"])
    state_rows = sum(row["observations"] for row in published["state_pollutant"]["records"])
    city_stations = {
        row[station]
        for row in published["station_comparison"]["records"]
        if row["city_name"] is not None and not row["city_conflict"]
    }
    state_stations = {
        row[station]
        for row in published["station_comparison"]["records"]
        if row["state_name"] is not None and not row["state_conflict"]
    }
    without_city = sum(1 for row in published["station_comparison"]["records"] if row["city_name"] is None and not row["city_conflict"])
    city_conflicts = sum(1 for row in published["station_comparison"]["records"] if row["city_conflict"])
    without_state = sum(
        1 for row in published["station_comparison"]["records"] if row["state_name"] is None and not row["state_conflict"]
    )
    state_conflicts = sum(1 for row in published["station_comparison"]["records"] if row["state_conflict"])
    without_coordinates = sum(
        1
        for row in published["station_comparison"]["records"]
        if row["latitude"] is None and row["longitude"] is None and not row["coordinate_conflict"]
    )
    coordinate_conflicts = sum(1 for row in published["station_comparison"]["records"] if row["coordinate_conflict"])
    with_coordinates = sum(
        1
        for row in published["station_comparison"]["records"]
        if row["latitude"] is not None and row["longitude"] is not None
    )

    raw_counts = _raw_place_counts(prepared, config)
    return {
        "stations": len(station_ids),
        "station_pollutant_series": len(places),
        "city_column_present": city_present,
        "state_column_present": state_present,
        "coordinate_columns_present": coordinates_present,
        "distinct_city_names": None if not city_present else len(city_names),
        "distinct_state_names": None if not state_present else len(state_names),
        "stations_without_city": None if not city_present else without_city,
        "stations_without_state": None if not state_present else without_state,
        "stations_without_coordinates": None if not coordinates_present else without_coordinates,
        "stations_with_coordinates": None if not coordinates_present else with_coordinates,
        "stations_with_city_conflict": None if not city_present else city_conflicts,
        "stations_with_state_conflict": None if not state_present else state_conflicts,
        "stations_with_coordinate_conflict": None if not coordinates_present else coordinate_conflicts,
        "stations_in_city_aggregates": None if not city_present else len(city_stations),
        "stations_in_state_aggregates": None if not state_present else len(state_stations),
        "rows_without_city": None if not city_present else raw_counts["rows_without_city"],
        "rows_without_state": None if not state_present else raw_counts["rows_without_state"],
        "rows_without_latitude": None if not coordinates_present else raw_counts["rows_without_latitude"],
        "rows_without_longitude": None if not coordinates_present else raw_counts["rows_without_longitude"],
        "eligible_rows_without_city": None if not city_present else raw_counts["eligible_rows_without_city"],
        "eligible_rows_without_coordinates": (
            None if not coordinates_present else raw_counts["eligible_rows_without_coordinates"]
        ),
        "rows_in_city_aggregates": None if not city_present else city_rows,
        "rows_in_state_aggregates": None if not state_present else state_rows,
        "note": DENOMINATORS["geography"],
    }


def _raw_place_counts(prepared: DataFrame, config: AppConfig) -> dict[str, int]:
    columns = config.dataset.columns
    city = F.col(columns.city).isNull() if columns.city in prepared.columns else F.lit(False)
    state = F.col("state_name").isNull() if "state_name" in prepared.columns else F.lit(False)
    latitude = F.col("latitude").isNull() if "latitude" in prepared.columns else F.lit(False)
    longitude = F.col("longitude").isNull() if "longitude" in prepared.columns else F.lit(False)
    missing_point = latitude | longitude
    row = prepared.agg(
        F.sum(F.when(city, F.lit(1)).otherwise(F.lit(0))).alias("rows_without_city"),
        F.sum(F.when(state, F.lit(1)).otherwise(F.lit(0))).alias("rows_without_state"),
        F.sum(F.when(latitude, F.lit(1)).otherwise(F.lit(0))).alias("rows_without_latitude"),
        F.sum(F.when(longitude, F.lit(1)).otherwise(F.lit(0))).alias("rows_without_longitude"),
        F.sum(F.when(city & F.col("_eligible"), F.lit(1)).otherwise(F.lit(0))).alias("eligible_rows_without_city"),
        F.sum(F.when(missing_point & F.col("_eligible"), F.lit(1)).otherwise(F.lit(0))).alias(
            "eligible_rows_without_coordinates"
        ),
    ).collect()[0]
    return {name: int(row[name] or 0) for name in row.asDict()}


def _require_partition_totals(published: dict[str, dict[str, Any]], total_rows: int, null_timestamps: int) -> None:
    station_rows = sum(row["observations"] for row in published["station_pollutant"]["records"])
    if station_rows != total_rows:
        raise AnalyticsError(
            f"Station aggregates cover {station_rows} observations; the annotated file has {total_rows}."
        )
    timed_rows = total_rows - null_timestamps
    for name in ("daily_by_pollutant", "hourly_by_pollutant", "hour_of_day"):
        covered = sum(row["observations"] for row in published[name]["records"])
        if covered != timed_rows:
            raise AnalyticsError(
                f"{name} covers {covered} observations; rows with a timestamp number {timed_rows}."
            )


def _null_if_blank(frame: DataFrame, column: str) -> DataFrame:
    if column not in frame.columns:
        return frame
    text = F.trim(F.col(column).cast("string"))
    blank = F.col(column).isNull() | (text == F.lit(""))
    return frame.withColumn(column, F.when(blank, F.lit(None).cast("string")).otherwise(F.col(column).cast("string")))


def _records(frame: DataFrame) -> list[dict[str, Any]]:
    names = frame.columns
    return [{name: _json_value(row[name]) for name in names} for row in frame.collect()]


def _json_value(value: object) -> object:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, float):
        return value
    if isinstance(value, int):
        return value
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def _write_csv(path: Path, fieldnames: list[str], records: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for record in records:
            writer.writerow({name: _csv_cell(record.get(name)) for name in fieldnames})


def _csv_cell(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _rate(part: int, whole: int) -> float | None:
    if whole == 0:
        return None
    return part / whole


def _refuse_output(config: AppConfig, source: Path, output_dir: Path) -> None:
    resolved = output_dir.resolve()
    blocked = [
        config.paths.raw_root.resolve(),
        source.parent.resolve(),
        (config.paths.processed_dir / "measurements").resolve(),
        (config.paths.processed_dir / "spikes" / "spikes_only").resolve(),
    ]
    for item in blocked:
        if resolved == item or item in resolved.parents:
            raise AnalyticsError(f"Refusing to write analytics under {item}.")


def _protected_files(config: AppConfig, source: Path) -> list[Path]:
    inventory = inventory_inputs(config)
    files = list(inventory.measurement_files)
    if inventory.stations_present:
        files.append(config.paths.stations_file)
    if inventory.parameters_present:
        files.append(config.paths.parameters_file)
    measurements = config.paths.processed_dir / "measurements" / "data.parquet"
    spikes_only = config.paths.processed_dir / "spikes" / "spikes_only" / "data.parquet"
    for path in (measurements, source, spikes_only):
        if path.is_file() and path not in files:
            files.append(path)
    return files


def _fingerprints(paths: list[Path]) -> list[dict[str, Any]]:
    return [_fingerprint(path) for path in paths]


def _require_unchanged(fingerprints: list[dict[str, Any]]) -> None:
    for item in fingerprints:
        current = _fingerprint(Path(item["path"]))
        if current["sha256"] != item["sha256"] or current["bytes"] != item["bytes"]:
            raise AnalyticsError(f"Protected file changed while aggregating: {item['path']}")


def _write_json(path: Path, document: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(document, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    temporary.replace(path)


_RATE_COLUMNS = [
    "observations",
    "valid_observations",
    "mean_valid_value",
    "spike_count",
    "eligible_observations",
    "spike_rate_of_observations",
    "spike_rate_of_eligible",
]
_UNUSUAL_COLUMNS = [
    "spike_count_mean",
    "spike_count_stddev",
    "spike_count_threshold",
    "unusually_high_spike_count",
    "eligible_spike_rate_mean",
    "eligible_spike_rate_stddev",
    "eligible_spike_rate_threshold",
    "unusually_high_eligible_rate",
]
_PATTERN_COLUMNS = ["observation_hour", "parameter_name", *_RATE_COLUMNS]
_DAILY_COLUMNS = ["observation_date", "parameter_name", *_RATE_COLUMNS, *_UNUSUAL_COLUMNS]
_TEMPORAL_COLUMNS = ["observation_date", "observation_hour", "parameter_name", *_RATE_COLUMNS, *_UNUSUAL_COLUMNS]
_STATION_COLUMNS = [
    "station_id",
    "parameter_name",
    "city_name",
    "state_name",
    "latitude",
    "longitude",
    "city_conflict",
    "state_conflict",
    "coordinate_conflict",
    *_RATE_COLUMNS,
    "spike_count_rank",
    "spike_rate_rank",
    "comparison_series",
    "elevated_threshold",
    "consistently_elevated",
]
_PLACE_COLUMNS = [
    "city_name",
    "state_name",
    "parameter_name",
    "stations",
    "stations_with_coordinates",
    *_RATE_COLUMNS,
]
_STATE_COLUMNS = [
    "state_name",
    "parameter_name",
    "stations",
    "stations_with_coordinates",
    *_RATE_COLUMNS,
]
