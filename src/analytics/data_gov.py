"""Clean stored data.gov.in snapshots with the shared Spark session.

The values stay API-reported numbers. This module does not label them as
micrograms per cubic metre, does not invent an AQI field, and does not apply
the XKDR rolling spike rule.
"""

from __future__ import annotations

import json
from hashlib import sha256
from pathlib import Path
from typing import Any

from utils.config_loader import AppConfig

_TIMESTAMP_FORMAT = "dd-MM-yyyy HH:mm:ss"
_REPORTED_UNIT = "not documented by the API"
_VALUE_KIND = "api-reported pollutant value"
_PROTECTED_RELATIVE = (
    ("raw", "v1", "measurements", "year=2024", "month=01", "data.parquet"),
    ("raw", "v1", "measurements", "year=2025", "month=08", "data.parquet"),
    ("processed", "measurements", "data.parquet"),
    ("processed", "spikes", "annotated", "data.parquet"),
    ("processed", "analytics", "daily_by_pollutant", "data.parquet"),
    ("processed", "city_portal", "city_daily", "data.parquet"),
)


class DataGovPrepareError(Exception):
    """The stored data.gov.in records cannot be cleaned as configured."""


def prepared_root(config: AppConfig) -> Path:
    return config.paths.processed_dir / "data_gov_in"


def prepare_report_file(config: AppConfig) -> Path:
    return config.paths.interim_dir / "data_gov_in" / "prepare_report.json"


def prepare_data_gov(config: AppConfig, spark: Any) -> dict[str, Any]:
    """Parse the normalized snapshot table and write Spark aggregates."""

    from ingestion.data_gov import normalized_records_file

    source = normalized_records_file(config)
    if not source.is_file():
        raise DataGovPrepareError(
            "Normalized data.gov.in records were not found. Run fetch-data-gov before prepare-data-gov."
        )
    protected = _fingerprint_existing(config)
    output = prepared_root(config)
    _refuse_protected_output(config, output)
    frame = spark.read.parquet(str(source.resolve()))
    cleaned = _clean(frame)
    observations = output / "observations"
    coverage = output / "series_coverage"
    _write_parquet(cleaned, observations, config)
    _write_parquet(_coverage(cleaned), coverage, config)
    _require_unchanged(protected)
    report = _report(cleaned, source, observations, coverage, protected)
    destination = prepare_report_file(config)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return report


def format_prepare_report(report: dict[str, Any]) -> str:
    counts = report["counts"]
    return (
        "data.gov.in Spark preparation\n"
        f"Records: {counts['records']}\n"
        f"Parsed timestamps: {counts['parsed_timestamps']}\n"
        f"Unparsed timestamps: {counts['unparsed_timestamps']}\n"
        f"Distinct API timestamps: {counts['distinct_timestamps']}\n"
        f"Valid coordinates: {counts['valid_coordinates']}\n"
        f"Reported unit: {report['reported_unit']}\n"
        f"Spike algorithm applied: {report['spike_algorithm_applied']}\n"
        f"AQI field created: {report['aqi_field_created']}\n"
        f"XKDR files modified: {report['xkdr_files_modified']}\n"
        f"Observations: {report['observations']}"
    )


def _clean(frame: Any) -> Any:
    from pyspark.sql import functions as F

    required = {
        "country",
        "state",
        "city",
        "station",
        "last_update",
        "latitude",
        "longitude",
        "pollutant_id",
        "min_value",
        "max_value",
        "avg_value",
        "retrieved_at",
    }
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise DataGovPrepareError(f"Normalized records are missing columns: {', '.join(missing)}.")
    parsed_time = F.try_to_timestamp(F.col("last_update"), F.lit(_TIMESTAMP_FORMAT)).cast("timestamp_ntz")
    latitude = F.col("latitude").try_cast("double")
    longitude = F.col("longitude").try_cast("double")
    blank_latitude = F.col("latitude").isNull() | (F.length(F.trim(F.col("latitude"))) == 0)
    blank_longitude = F.col("longitude").isNull() | (F.length(F.trim(F.col("longitude"))) == 0)
    coordinate_status = (
        F.when(blank_latitude | blank_longitude, F.lit("missing"))
        .when(latitude.isNull() | longitude.isNull(), F.lit("malformed"))
        .when(
            latitude.between(F.lit(-90.0), F.lit(90.0)) & longitude.between(F.lit(-180.0), F.lit(180.0)),
            F.lit("valid"),
        )
        .otherwise(F.lit("out of range"))
    )
    timestamp_status = (
        F.when(F.col("last_update").isNull() | (F.length(F.trim(F.col("last_update"))) == 0), F.lit("missing"))
        .when(parsed_time.isNotNull(), F.lit("parsed"))
        .otherwise(F.lit("unparsed"))
    )
    return frame.select(
        F.col("country"),
        F.col("state"),
        F.col("city"),
        F.col("station"),
        F.col("pollutant_id"),
        F.col("last_update").alias("last_update_text"),
        parsed_time.alias("observed_at"),
        timestamp_status.alias("timestamp_status"),
        F.col("latitude").alias("latitude_text"),
        F.col("longitude").alias("longitude_text"),
        F.when(coordinate_status == F.lit("valid"), latitude).alias("latitude"),
        F.when(coordinate_status == F.lit("valid"), longitude).alias("longitude"),
        coordinate_status.alias("coordinate_status"),
        F.col("min_value").alias("min_value_text"),
        F.col("max_value").alias("max_value_text"),
        F.col("avg_value").alias("avg_value_text"),
        F.col("min_value").try_cast("double").alias("min_value"),
        F.col("max_value").try_cast("double").alias("max_value"),
        F.col("avg_value").try_cast("double").alias("avg_value"),
        F.lit(_REPORTED_UNIT).alias("reported_unit"),
        F.lit(_VALUE_KIND).alias("value_kind"),
        F.col("retrieved_at"),
    )


def _coverage(frame: Any) -> Any:
    from pyspark.sql import functions as F

    return frame.groupBy("state", "city", "station", "pollutant_id").agg(
        F.countDistinct("observed_at").alias("timestamp_count"),
        F.min("observed_at").alias("first_observed_at"),
        F.max("observed_at").alias("last_observed_at"),
    )


def _report(
    frame: Any,
    source: Path,
    observations: Path,
    coverage: Path,
    protected: list[dict[str, Any]],
) -> dict[str, Any]:
    from pyspark.sql import functions as F

    counts = frame.agg(
        F.count(F.lit(1)).alias("records"),
        F.sum(F.when(F.col("timestamp_status") == F.lit("parsed"), 1).otherwise(0)).alias("parsed_timestamps"),
        F.sum(F.when(F.col("timestamp_status") != F.lit("parsed"), 1).otherwise(0)).alias("unparsed_timestamps"),
        F.countDistinct("observed_at").alias("distinct_timestamps"),
        F.sum(F.when(F.col("coordinate_status") == F.lit("valid"), 1).otherwise(0)).alias("valid_coordinates"),
        F.sum(F.when(F.col("min_value").isNull(), 1).otherwise(0)).alias("null_min_values"),
        F.sum(F.when(F.col("avg_value").isNull(), 1).otherwise(0)).alias("null_avg_values"),
    ).collect()[0]
    return {
        "schema_version": 1,
        "source": "data.gov.in",
        "input_file": str(source.resolve()),
        "observations": str((observations / "data.parquet").resolve()),
        "series_coverage": str((coverage / "data.parquet").resolve()),
        "timestamp_format": "DD-MM-YYYY HH:MM:SS",
        "reported_unit": _REPORTED_UNIT,
        "value_kind": _VALUE_KIND,
        "spike_algorithm_applied": False,
        "aqi_field_created": False,
        "missing_values_filled_with_zero": False,
        "xkdr_files_modified": False,
        "counts": {name: int(counts[name] or 0) for name in counts.asDict()},
        "protected_xkdr_files": protected,
    }


def _write_parquet(frame: Any, output_dir: Path, config: AppConfig) -> Path:
    import pyarrow.parquet as pq

    resolved = output_dir.resolve()
    _refuse_protected_output(config, resolved)
    output_file = resolved / "data.parquet"
    if output_file.exists():
        output_file.unlink()
    resolved.mkdir(parents=True, exist_ok=True)
    table = frame.toArrow()
    pq.write_table(table, output_file, compression=config.spark.parquet_compression)
    return output_file


def _refuse_protected_output(config: AppConfig, path: Path) -> None:
    resolved = path.resolve()
    protected = [
        config.paths.raw_root.resolve(),
        (config.paths.processed_dir / "measurements").resolve(),
        (config.paths.processed_dir / "spikes").resolve(),
        (config.paths.processed_dir / "analytics").resolve(),
        (config.paths.processed_dir / "city_portal").resolve(),
    ]
    for root in protected:
        if resolved == root or root in resolved.parents:
            raise DataGovPrepareError(f"Refusing to write data.gov.in output under {root}.")


def _fingerprint_existing(config: AppConfig) -> list[dict[str, Any]]:
    found = []
    for parts in _PROTECTED_RELATIVE:
        base = config.paths.raw_root if parts[0] == "raw" else config.paths.processed_dir
        path = base.joinpath(*parts[1:])
        if path.is_file():
            found.append(_fingerprint(path))
    return found


def _fingerprint(path: Path) -> dict[str, Any]:
    digest = sha256()
    size = 0
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
            size += len(chunk)
    return {"path": str(path.resolve()), "bytes": size, "sha256": digest.hexdigest()}


def _require_unchanged(protected: list[dict[str, Any]]) -> None:
    for item in protected:
        current = _fingerprint(Path(item["path"]))
        if current["sha256"] != item["sha256"] or current["bytes"] != item["bytes"]:
            raise DataGovPrepareError(f"An XKDR file changed during data.gov.in preparation: {item['path']}")
