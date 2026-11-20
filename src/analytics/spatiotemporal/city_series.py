"""City-specific daily and hourly aggregates.

``daily_by_pollutant`` and ``hourly_by_pollutant`` describe every selected
station together. This module reads the annotated measurements, keeps the
stored spike flags, and writes a separate city series under
``data/processed/city_portal``. A station contributes to a city only when
that station has one non-null city name. Existing raw, cleaned, spike, and
analytics files are hashed and left unchanged.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from analytics.spatiotemporal.summarize import (
    _aggregates,
    _prepare,
    _station_places,
    _with_rates,
    annotated_spikes_file,
)
from preprocessing.clean import _fingerprint, _write_parquet
from utils.config_loader import AppConfig, inventory_inputs

_PORTAL_DIR = "city_portal"
_REPORT_NAME = "city_portal_report.json"
_MEAN_TOLERANCE = 1e-4


class CitySeriesError(Exception):
    """City series cannot be written from the annotated measurements."""


@dataclass(frozen=True)
class CitySeriesReport:
    path: Path
    output_dir: Path
    document: dict[str, Any]


def city_portal_dir(config: AppConfig) -> Path:
    return config.paths.processed_dir / _PORTAL_DIR


def build_city_series(
    config: AppConfig,
    spark: SparkSession | None,
    *,
    output_dir: Path | None = None,
    report_path: Path | None = None,
) -> CitySeriesReport:
    """Aggregate annotated rows by the station's resolved city and write Parquet."""

    source = annotated_spikes_file(config)
    if not source.is_file():
        raise CitySeriesError(
            f"Annotated measurements not found: {source}. Run detect-spikes before build-city-series."
        )
    destination = output_dir or city_portal_dir(config)
    _refuse_output(config, source, destination)
    if spark is None:
        raise CitySeriesError("build_city_series requires the shared SparkSession.")

    before = _fingerprints(_protected_files(config, source, destination))
    parameter = config.dataset.columns.parameter
    station = config.dataset.columns.station_id
    frame = spark.read.parquet(str(source.resolve()))
    prepared = _prepare(frame, config).cache()
    try:
        places = _station_places(prepared, config).cache()
        try:
            located = prepared.join(
                places.select(
                    station,
                    F.col("city_name").alias("_city"),
                    F.col("state_name").alias("_state"),
                    F.col("latitude").alias("_latitude"),
                    F.col("longitude").alias("_longitude"),
                    "city_conflict",
                ),
                station,
                "left",
            )
            named = located.where(F.col("_city").isNotNull() & ~F.col("city_conflict"))
            dated = named.where(F.col("_observation_date").isNotNull())
            daily = _with_rates(
                dated.groupBy("_city", "_state", "_observation_date", parameter).agg(*_aggregates(station))
            )
            hourly = _with_rates(
                dated.groupBy("_city", "_state", "_observation_date", "_observation_hour", parameter).agg(
                    *_aggregates(station)
                )
            )
            by_hour = _with_rates(
                dated.groupBy("_city", "_state", "_observation_hour", parameter).agg(*_aggregates(station))
            )
            totals = _with_rates(named.groupBy("_city", "_state", parameter).agg(*_aggregates(station)))
            coverage = _coverage(places)
            match = _match_saved_cities(spark, config, totals, parameter)
            outputs = {
                "city_daily": _write_table(
                    _project(daily, parameter, include_date=True, include_hour=False),
                    spark,
                    config,
                    destination / "city_daily",
                    ["city_name", "state_name", "observation_date", parameter],
                ),
                "city_hourly": _write_table(
                    _project(hourly, parameter, include_date=True, include_hour=True),
                    spark,
                    config,
                    destination / "city_hourly",
                    ["city_name", "state_name", "observation_date", "observation_hour", parameter],
                ),
                "city_hour_of_day": _write_table(
                    _project(by_hour, parameter, include_date=False, include_hour=True),
                    spark,
                    config,
                    destination / "city_hour_of_day",
                    ["city_name", "state_name", "observation_hour", parameter],
                ),
            }
        finally:
            places.unpersist()
    finally:
        prepared.unpersist()

    _require_unchanged(before)
    document = _document(config, source, destination, coverage, outputs, match)
    report_destination = report_path or (config.paths.interim_dir / _REPORT_NAME)
    _write_json(report_destination, document)
    return CitySeriesReport(path=report_destination, output_dir=destination, document=document)


def format_city_series_report(report: CitySeriesReport) -> str:
    document = report.document
    counts = document["counts"]
    match = document["city_pollutant_match"]
    match_text = "not compared" if match is None else str(match)
    return "\n".join(
        [
            f"City series report: {report.path}",
            f"City portal directory: {report.output_dir}",
            f"Cities: {counts['cities']}",
            f"Daily rows: {counts['city_daily_rows']}",
            f"Hourly rows: {counts['city_hourly_rows']}",
            f"Hour-of-day rows: {counts['city_hour_of_day_rows']}",
            f"Stations without a city: {counts['stations_without_city']}",
            f"Stations with conflicting city names: {counts['stations_with_city_conflict']}",
            f"Saved city_pollutant totals match: {match_text}",
            f"Protected outputs unchanged: {document['protected_outputs_unchanged']}",
        ]
    )


def _project(frame: DataFrame, parameter: str, *, include_date: bool, include_hour: bool) -> DataFrame:
    projected = frame.withColumnRenamed("_city", "city_name").withColumnRenamed("_state", "state_name")
    if include_date:
        projected = projected.withColumnRenamed("_observation_date", "observation_date")
    if include_hour:
        projected = projected.withColumnRenamed("_observation_hour", "observation_hour")
    columns = ["city_name", "state_name"]
    if include_date:
        columns.append("observation_date")
    if include_hour:
        columns.append("observation_hour")
    columns.extend(
        [
            parameter,
            "stations",
            "stations_with_coordinates",
            "observations",
            "valid_observations",
            "mean_valid_value",
            "spike_count",
            "eligible_observations",
            "spike_rate_of_observations",
            "spike_rate_of_eligible",
        ]
    )
    return projected.select(*columns)


def _write_table(
    frame: DataFrame,
    spark: SparkSession,
    config: AppConfig,
    directory: Path,
    order: list[str],
) -> dict[str, Any]:
    ordered = frame.orderBy(*[F.col(name).asc_nulls_last() for name in order])
    path = _write_parquet(ordered, directory, config)
    read_back = int(spark.read.parquet(str(path.resolve())).count())
    expected = int(ordered.count())
    if read_back != expected:
        raise CitySeriesError(f"{directory.name} has {read_back} rows on disk and {expected} aggregated rows.")
    return {"parquet": str(path.resolve()), "rows": read_back}


def _coverage(places: DataFrame) -> dict[str, int]:
    state_key = F.coalesce(F.col("state_name"), F.lit(""))
    city_key = F.when(
        F.col("city_name").isNotNull() & ~F.col("city_conflict"),
        F.concat_ws("\u001f", F.col("city_name"), state_key),
    )
    row = places.agg(
        F.countDistinct(city_key).alias("cities"),
        F.sum(F.when(F.col("city_name").isNull() & ~F.col("city_conflict"), F.lit(1)).otherwise(F.lit(0))).alias(
            "stations_without_city"
        ),
        F.sum(F.when(F.col("city_conflict"), F.lit(1)).otherwise(F.lit(0))).alias("stations_with_city_conflict"),
    ).collect()[0]
    return {
        "cities": int(row["cities"] or 0),
        "stations_without_city": int(row["stations_without_city"] or 0),
        "stations_with_city_conflict": int(row["stations_with_city_conflict"] or 0),
    }


def _match_saved_cities(
    spark: SparkSession,
    config: AppConfig,
    totals: DataFrame,
    parameter: str,
) -> bool | None:
    """Compare city totals with the saved city_pollutant table when that file exists."""

    saved_path = config.paths.processed_dir / "analytics" / "city_pollutant" / "data.parquet"
    if not saved_path.is_file():
        return None
    saved = spark.read.parquet(str(saved_path.resolve()))
    new_rows = _comparison_view(totals, parameter, "new")
    saved_rows = _comparison_view(saved, parameter, "saved")
    keys = ["city_key", "state_key", "parameter_name"]
    joined = new_rows.join(saved_rows, keys, "full")
    mean_gap = F.abs(F.col("new_mean") - F.col("saved_mean"))
    mean_disagrees = (
        (F.col("new_mean").isNull() & F.col("saved_mean").isNotNull())
        | (F.col("new_mean").isNotNull() & F.col("saved_mean").isNull())
        | (F.col("new_mean").isNotNull() & F.col("saved_mean").isNotNull() & (mean_gap > F.lit(_MEAN_TOLERANCE)))
    )
    disagreed = joined.where(
        F.col("new_observations").isNull()
        | F.col("saved_observations").isNull()
        | (F.col("new_observations") != F.col("saved_observations"))
        | (F.col("new_valid_observations") != F.col("saved_valid_observations"))
        | (F.col("new_spike_count") != F.col("saved_spike_count"))
        | (F.col("new_eligible_observations") != F.col("saved_eligible_observations"))
        | mean_disagrees
    )
    sample = disagreed.limit(5).collect()
    if sample:
        preview = [
            {
                "city_key": row["city_key"],
                "state_key": row["state_key"],
                "parameter_name": row["parameter_name"],
                "new_observations": row["new_observations"],
                "saved_observations": row["saved_observations"],
            }
            for row in sample
        ]
        raise CitySeriesError(
            "City totals do not match the saved city_pollutant aggregate. "
            f"Examples: {preview}"
        )
    return True


def _comparison_view(frame: DataFrame, parameter: str, prefix: str) -> DataFrame:
    city = F.col("city_name") if "city_name" in frame.columns else F.col("_city")
    state = F.col("state_name") if "state_name" in frame.columns else F.col("_state")
    return frame.select(
        city.alias("city_key"),
        F.coalesce(state, F.lit("")).alias("state_key"),
        F.col(parameter).alias("parameter_name"),
        F.col("observations").cast("long").alias(f"{prefix}_observations"),
        F.col("valid_observations").cast("long").alias(f"{prefix}_valid_observations"),
        F.col("spike_count").cast("long").alias(f"{prefix}_spike_count"),
        F.col("eligible_observations").cast("long").alias(f"{prefix}_eligible_observations"),
        F.col("mean_valid_value").cast("double").alias(f"{prefix}_mean"),
    )


def _document(
    config: AppConfig,
    source: Path,
    destination: Path,
    coverage: dict[str, int],
    outputs: dict[str, dict[str, Any]],
    match: bool | None,
) -> dict[str, Any]:
    generated_at = datetime.now(ZoneInfo(config.dataset.timezone)).isoformat(timespec="seconds")
    return {
        "schema_version": 1,
        "generated_at": generated_at,
        "timezone": config.dataset.timezone,
        "input_file": str(source.resolve()),
        "output_dir": str(destination.resolve()),
        "output_writer": "spark_parquet",
        "method": {
            "spikes_rescored": False,
            "eligibility": "non-null spike_threshold when that column is present; otherwise the stored baseline rule",
            "city_rule": (
                "A station is included only when it has one distinct non-null city name. "
                "Conflicting city names and missing city names stay out of every city series."
            ),
            "national_daily_used": False,
            "national_hourly_used": False,
        },
        "counts": {
            "cities": coverage["cities"],
            "stations_without_city": coverage["stations_without_city"],
            "stations_with_city_conflict": coverage["stations_with_city_conflict"],
            "city_daily_rows": outputs["city_daily"]["rows"],
            "city_hourly_rows": outputs["city_hourly"]["rows"],
            "city_hour_of_day_rows": outputs["city_hour_of_day"]["rows"],
        },
        "outputs": outputs,
        "city_pollutant_match": match,
        "protected_outputs_unchanged": True,
    }


def _refuse_output(config: AppConfig, source: Path, output_dir: Path) -> None:
    resolved = output_dir.resolve()
    blocked = [
        config.paths.raw_root.resolve(),
        source.parent.resolve(),
        (config.paths.processed_dir / "measurements").resolve(),
        (config.paths.processed_dir / "spikes").resolve(),
        (config.paths.processed_dir / "analytics").resolve(),
    ]
    for item in blocked:
        if resolved == item or item in resolved.parents:
            raise CitySeriesError(f"Refusing to write city series under {item}.")


def _protected_files(config: AppConfig, source: Path, destination: Path) -> list[Path]:
    inventory = inventory_inputs(config)
    files = list(inventory.measurement_files)
    if inventory.stations_present:
        files.append(config.paths.stations_file)
    if inventory.parameters_present:
        files.append(config.paths.parameters_file)
    measurements = config.paths.processed_dir / "measurements" / "data.parquet"
    spikes_only = config.paths.processed_dir / "spikes" / "spikes_only" / "data.parquet"
    for path in (measurements, source, spikes_only):
        if path.is_file():
            files.append(path)
    analytics = config.paths.processed_dir / "analytics"
    if analytics.is_dir():
        files.extend(path for path in analytics.rglob("*") if path.is_file())
    destination_text = str(destination.resolve())
    unique: list[Path] = []
    seen: set[str] = set()
    for path in files:
        resolved = str(path.resolve())
        if resolved.startswith(destination_text) or resolved in seen or not path.is_file():
            continue
        seen.add(resolved)
        unique.append(path)
    return unique


def _fingerprints(paths: list[Path]) -> list[dict[str, Any]]:
    return [_fingerprint(path) for path in paths]


def _require_unchanged(fingerprints: list[dict[str, Any]]) -> None:
    for item in fingerprints:
        current = _fingerprint(Path(item["path"]))
        if current["sha256"] != item["sha256"] or current["bytes"] != item["bytes"]:
            raise CitySeriesError(f"A protected file changed while building city series: {item['path']}")


def _write_json(path: Path, document: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)
