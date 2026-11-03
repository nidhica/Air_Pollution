"""Clean selected CAAQM measurements and record what changed.

The caller supplies ``AppConfig`` and the shared ``SparkSession``. This
module does not build a session and does not write under ``data/raw``.

Column roles configured in ``dataset.columns`` are required. ``state_name``,
``unit``, ``latitude``, ``longitude``, and ``station_name`` are used only
when those names are present in the Parquet files that were read.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from ingestion.inspect import InspectionError, read_measurement_files, select_measurements
from utils.config_loader import AppConfig, inventory_inputs

_REPORT_NAME = "preprocessing_report.json"
_OUTPUT_DIR_NAME = "measurements"
_STATE_COLUMN = "state_name"
_UNIT_COLUMN = "unit"
_LATITUDE_COLUMN = "latitude"
_LONGITUDE_COLUMN = "longitude"
_STATION_NAME_COLUMN = "station_name"
_MEASUREMENT_SOURCE = "measurement"
_STATION_SOURCE = "station_metadata"
_MISSING_SOURCE = "missing"
_STATUS_VALID = "valid"
_STATUS_MISSING = "missing"
_STATUS_NEGATIVE = "negative"
_STATUS_NON_FINITE = "non_finite"
_STATUS_NON_NUMERIC = "non_numeric"

CLEANING_RULES: dict[str, str] = {
    "selection": (
        "Rows are kept only when source equals dataset.source_filter, "
        "parameter is one of dataset.pollutants, and the parsed timestamp "
        "falls inside the configured inclusive date window."
    ),
    "removed_rows": (
        "A selected row is removed only when station_id is null, the "
        "parameter is null, the timestamp is null or cannot be parsed, or "
        "it repeats every column of another kept row. Each reason is counted."
    ),
    "invalid_measurements": (
        "Null, blank, non-numeric, non-finite, and negative measurement "
        "values are kept. measurement_status records which rule applied. "
        "No row is deleted because of its measurement value, and values at "
        "the observed maximum are kept."
    ),
    "location": (
        "A null or blank city or state is copied only from a unique "
        "station_id row in stations.parquet for the same source when that "
        "station field is non-blank. A location already present on the "
        "measurement is never overwritten. A disagreement is flagged and the "
        "measurement value is kept. If the station field is also null, the "
        "measurement stays null."
    ),
    "coordinates": (
        "latitude and longitude are copied from a unique matching station "
        "row when the measurements file does not already contain those "
        "columns. Missing coordinates stay null."
    ),
    "raw_files": "Files under data/raw are read and hashed. They are not opened for writing.",
}


class PreprocessingError(Exception):
    """The local archive cannot be cleaned as configured."""


@dataclass(frozen=True)
class PreprocessingReport:
    path: Path
    output_dir: Path
    output_file: Path
    document: dict[str, Any]


def classify_numeric_value(value: float | None) -> str:
    """Classify one numeric measurement. This is the rule the Spark column follows."""

    if value is None:
        return _STATUS_MISSING
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return _STATUS_NON_FINITE
    if value < 0:
        return _STATUS_NEGATIVE
    return _STATUS_VALID


def recover_location(
    measurement: str | None,
    station: str | None,
    *,
    ambiguous: bool,
) -> tuple[str | None, str]:
    """Return the location to keep and where it came from.

    A non-blank measurement wins. A station value is copied only when the
    measurement is blank, the station value is non-blank, and the station id
    matches exactly one station row.
    """

    if measurement is not None and measurement.strip() != "":
        return measurement, _MEASUREMENT_SOURCE
    if ambiguous or station is None or str(station).strip() == "":
        return None, _MISSING_SOURCE
    return station, _STATION_SOURCE


def locations_conflict(
    measurement: str | None,
    station: str | None,
    *,
    ambiguous: bool,
) -> bool:
    """True when both sides have text and they differ. Ambiguous stations do not count."""

    if ambiguous or measurement is None or station is None:
        return False
    if measurement.strip() == "" or str(station).strip() == "":
        return False
    return measurement.strip() != str(station).strip()


def preprocess_dataset(
    config: AppConfig,
    spark: SparkSession,
    *,
    report_path: Path | None = None,
    output_dir: Path | None = None,
) -> PreprocessingReport:
    """Filter, flag, and enrich measurements, then write Parquet and a JSON report."""

    inventory = inventory_inputs(config)
    if not inventory.measurement_files:
        raise PreprocessingError(
            "No measurement Parquet files found. Run acquire before preprocess, "
            f"or place files under {config.paths.measurements_glob}."
        )
    if not inventory.stations_present:
        raise PreprocessingError(f"Stations file not found: {config.paths.stations_file}")
    if not inventory.parameters_present:
        raise PreprocessingError(f"Parameters file not found: {config.paths.parameters_file}")
    if spark is None:
        raise PreprocessingError("preprocess_dataset requires the shared SparkSession.")

    raw_fingerprints = _fingerprints(
        list(inventory.measurement_files) + [config.paths.stations_file, config.paths.parameters_file]
    )
    raw = read_measurement_files(config, spark)
    stations = spark.read.parquet(_spark_path(config.paths.stations_file))
    parameters = spark.read.parquet(_spark_path(config.paths.parameters_file))
    try:
        scoped = select_measurements(config, raw, include_date_window=False)
    except InspectionError as exc:
        raise PreprocessingError(str(exc)) from exc

    columns = config.dataset.columns
    if columns.city not in scoped.columns:
        raise PreprocessingError(
            f"Measurement files have no configured city column {columns.city}. "
            "Actual columns: " + ", ".join(scoped.columns)
        )
    if columns.station_id not in stations.columns:
        raise PreprocessingError(
            "stations.parquet has no column "
            f"{columns.station_id}. Actual columns: " + ", ".join(stations.columns)
        )

    parsed, timestamp_stats = _parse_timestamp(scoped, columns.timestamp)
    parsed = parsed.cache()
    cached: list[DataFrame] = [parsed]
    try:
        document = _clean(
            config,
            spark,
            parsed,
            stations,
            parameters,
            raw,
            timestamp_stats,
            cached,
            output_dir if output_dir is not None else config.paths.processed_dir / _OUTPUT_DIR_NAME,
        )
    finally:
        for frame in cached:
            frame.unpersist()

    _require_unchanged(raw_fingerprints)
    document["raw_file_fingerprints"] = raw_fingerprints
    document["raw_files_modified"] = False
    destination = report_path if report_path is not None else config.paths.interim_dir / _REPORT_NAME
    _write_json(destination, document)
    return PreprocessingReport(
        path=destination,
        output_dir=Path(document["output_dir"]),
        output_file=Path(document["output_file"]),
        document=document,
    )


def format_preprocessing_report(report: PreprocessingReport) -> str:
    document = report.document
    counts = document["counts"]
    invalid = document["invalid_measurements"]
    location = document["location_enrichment"]
    duplicates = document["duplicates"]
    return (
        f"Preprocessing report: {report.path}\n"
        f"Cleaned Parquet: {report.output_file}\n"
        f"Source filter: {document['source_filter']}\n"
        f"Pollutants: {', '.join(document['pollutants'])}\n"
        f"Rows in downloaded files: {counts['rows_in_downloaded_files']}\n"
        f"Rows matching source and pollutants: {counts['rows_matching_source_and_pollutants']}\n"
        f"Rows outside the date window: {counts['rows_outside_date_window']}\n"
        f"Rows removed: {counts['rows_removed']}\n"
        f"Rows written: {counts['rows_written']}\n"
        f"Invalid measurements kept: missing {invalid['missing']}, "
        f"non_numeric {invalid['non_numeric']}, non_finite {invalid['non_finite']}, "
        f"negative {invalid['negative']}\n"
        f"Exact duplicate extras removed: {duplicates['exact_duplicate_extras_removed']}\n"
        f"Key-conflict rows kept: {duplicates['key_conflict_rows_kept']}\n"
        f"City filled from stations: {location['city_filled_from_station']}\n"
        f"State filled from stations: {location['state_filled_from_station']}\n"
        f"City still missing: {location['city_still_missing']}\n"
        f"State still missing: {location['state_still_missing']}\n"
        f"Raw files modified: {document['raw_files_modified']}"
    )


def _clean(
    config: AppConfig,
    spark: SparkSession,
    parsed: DataFrame,
    stations: DataFrame,
    parameters: DataFrame,
    raw: DataFrame,
    timestamp_stats: dict[str, Any],
    cached: list[DataFrame],
    output_dir: Path,
) -> dict[str, Any]:
    columns = config.dataset.columns
    matching_rows = _row_count(parsed)
    keyed = parsed.where(_complete_key(columns))
    keyed_rows = _row_count(keyed)
    incomplete_rows = matching_rows - keyed_rows
    windowed = select_measurements(config, keyed, include_date_window=True)
    windowed = windowed.cache()
    cached.append(windowed)
    windowed_rows = _row_count(windowed)
    outside_window = keyed_rows - windowed_rows
    missing_before = _missing_counts(windowed)
    deduped = windowed.dropDuplicates()
    deduped_rows = _row_count(deduped)
    exact_extras = windowed_rows - deduped_rows

    prepared = _with_measurement_status(deduped, columns.value)
    prepared = _with_key_conflict(prepared, columns)
    prepared, location_stats = _enrich_locations(prepared, stations, config)
    prepared, unit_stats = _with_unit_status(prepared, parameters, columns)
    prepared = prepared.cache()
    cached.append(prepared)

    status_counts = _status_counts(prepared)
    value_summary = _value_summary(prepared, columns.value)
    written_rows = _row_count(prepared)
    partition_columns = [name for name in config.dataset.partitions if name in prepared.columns]
    output_file = _write_parquet(prepared, output_dir, config)
    read_back = spark.read.parquet(_spark_path(output_file))
    read_back_rows = _row_count(read_back)
    if read_back_rows != written_rows:
        raise PreprocessingError(
            f"Wrote {written_rows} rows to {output_dir} but read back {read_back_rows}."
        )

    removal_reasons = _removal_reasons(parsed, columns)
    generated_at = datetime.now(ZoneInfo(config.dataset.timezone)).isoformat(timespec="seconds")
    return {
        "schema_version": 1,
        "generated_at": generated_at,
        "timezone": config.dataset.timezone,
        "source_filter": config.dataset.source_filter,
        "pollutants": list(config.dataset.pollutants),
        "date_start": None if config.dataset.date_start is None else config.dataset.date_start.isoformat(),
        "date_end": None if config.dataset.date_end is None else config.dataset.date_end.isoformat(),
        "output_dir": str(output_dir.resolve()),
        "output_file": str(output_file.resolve()),
        "partition_columns_retained": partition_columns,
        "output_writer": "pyarrow",
        "rules": CLEANING_RULES,
        "schemas": {
            "measurements": _schema(raw),
            "stations": _schema(stations),
            "parameters": _schema(parameters),
        },
        "counts": {
            "rows_in_downloaded_files": _row_count(raw),
            "rows_matching_source_and_pollutants": matching_rows,
            "rows_outside_date_window": outside_window,
            "rows_removed_incomplete_key": incomplete_rows,
            "rows_removed": incomplete_rows + exact_extras,
            "rows_removed_by_reason": removal_reasons,
            "rows_written": written_rows,
            "rows_read_back": read_back_rows,
        },
        "timestamp": timestamp_stats,
        "missing_values_before": missing_before,
        "missing_values_after": _missing_counts(read_back),
        "invalid_measurements": {
            "missing": status_counts.get(_STATUS_MISSING, 0),
            "non_numeric": status_counts.get(_STATUS_NON_NUMERIC, 0),
            "non_finite": status_counts.get(_STATUS_NON_FINITE, 0),
            "negative": status_counts.get(_STATUS_NEGATIVE, 0),
            "valid": status_counts.get(_STATUS_VALID, 0),
            "retained_in_output": True,
        },
        "duplicates": {
            "exact_duplicate_extras_removed": exact_extras,
            "key_conflict_rows_kept": _row_count(prepared.where(F.col("key_conflict"))),
            "key_columns": [columns.station_id, columns.parameter, columns.timestamp],
        },
        "location_enrichment": location_stats,
        "units": unit_stats,
        "value_summary": value_summary,
        "station_count": _row_count(prepared.select(columns.station_id).distinct()),
        "pollutant_counts": _pollutant_counts(prepared, columns.parameter),
        "date_min": _timestamp_bound(prepared, columns.timestamp, F.min),
        "date_max": _timestamp_bound(prepared, columns.timestamp, F.max),
    }


def _parse_timestamp(frame: DataFrame, column_name: str) -> tuple[DataFrame, dict[str, Any]]:
    type_name = _field_type(frame, column_name)
    if _is_temporal(type_name):
        null_rows = _sum_when(frame, F.col(column_name).isNull())
        return frame, {
            "input_type": type_name,
            "null_rows": null_rows,
            "unparseable_rows": 0,
        }
    if type_name != "string":
        raise PreprocessingError(
            f"Timestamp column {column_name} has unsupported type {type_name}. "
            "Expected a timestamp, date, or string."
        )
    text_name = "_timestamp_text"
    working = frame.withColumn(text_name, F.trim(F.col(column_name)))
    text = F.col(text_name)
    # try_to_timestamp returns null for malformed text. Plain to_timestamp
    # aborts the job when ANSI mode is on.
    parsed = F.coalesce(
        F.try_to_timestamp(text),
        F.try_to_timestamp(text, F.lit("yyyy-MM-dd HH:mm:ss")),
        F.try_to_timestamp(text, F.lit("yyyy-MM-dd'T'HH:mm:ss")),
    ).cast("timestamp_ntz")
    blank = text.isNull() | (text == F.lit(""))
    unparseable = (~blank) & parsed.isNull()
    stats = {
        "input_type": type_name,
        "null_rows": _sum_when(working, blank),
        "unparseable_rows": _sum_when(working, unparseable),
    }
    return working.withColumn(column_name, parsed).drop(text_name), stats


def _with_measurement_status(frame: DataFrame, value_name: str) -> DataFrame:
    type_name = _field_type(frame, value_name)
    if _is_numeric(type_name):
        numeric = F.col(value_name).cast("double")
        status = _numeric_status(numeric, floating=_is_floating(type_name))
        return frame.withColumn("measurement_status", status).withColumn(value_name, numeric)
    if type_name != "string":
        raise PreprocessingError(
            f"Value column {value_name} has unsupported type {type_name}. "
            "Expected a number or a string."
        )
    text = F.trim(F.col(value_name))
    parsed = text.try_cast("double")
    blank = text.isNull() | (text == F.lit(""))
    status = (
        F.when(blank, F.lit(_STATUS_MISSING))
        .when(parsed.isNull(), F.lit(_STATUS_NON_NUMERIC))
        .when(_non_finite(parsed), F.lit(_STATUS_NON_FINITE))
        .when(parsed < F.lit(0), F.lit(_STATUS_NEGATIVE))
        .otherwise(F.lit(_STATUS_VALID))
    )
    return (
        frame.withColumn("measurement_status", status)
        .withColumn("value_text", F.when(F.col("measurement_status") == F.lit(_STATUS_NON_NUMERIC), text))
        .withColumn(value_name, parsed)
    )


def _numeric_status(value: F.Column, *, floating: bool) -> F.Column:
    status = F.when(value.isNull(), F.lit(_STATUS_MISSING))
    if floating:
        status = status.when(_non_finite(value), F.lit(_STATUS_NON_FINITE))
    return status.when(value < F.lit(0), F.lit(_STATUS_NEGATIVE)).otherwise(F.lit(_STATUS_VALID))


def _non_finite(value: F.Column) -> F.Column:
    return F.isnan(value) | (value > F.lit(1e308)) | (value < F.lit(-1e308))


def _with_key_conflict(frame: DataFrame, columns: Any) -> DataFrame:
    keys = [columns.station_id, columns.parameter, columns.timestamp]
    counts = F.count(F.lit(1)).over(Window.partitionBy(*keys))
    return frame.withColumn("key_conflict", counts > F.lit(1))


def _enrich_locations(
    frame: DataFrame,
    stations: DataFrame,
    config: AppConfig,
) -> tuple[DataFrame, dict[str, Any]]:
    columns = config.dataset.columns
    station_id = columns.station_id
    city = columns.city
    state = _STATE_COLUMN if _STATE_COLUMN in frame.columns and _STATE_COLUMN in stations.columns else None
    lookup = stations
    if columns.source in stations.columns:
        lookup = lookup.where(F.col(columns.source) == F.lit(config.dataset.source_filter))
    counts = lookup.groupBy(station_id).agg(F.count(F.lit(1)).alias("_n"))
    unique_ids = counts.where(F.col("_n") == F.lit(1)).select(station_id)
    ambiguous_ids = counts.where(F.col("_n") > F.lit(1)).select(station_id)
    unique = lookup.join(unique_ids, station_id, "inner")
    renamed = _station_lookup(unique, columns, state)
    enriched = frame.join(renamed, F.col(station_id) == F.col("_station_key"), "left")
    ambiguous = ambiguous_ids.select(F.col(station_id).alias("_ambiguous_key")).withColumn(
        "_ambiguous", F.lit(True)
    )
    enriched = enriched.join(ambiguous, F.col(station_id) == F.col("_ambiguous_key"), "left")
    enriched = enriched.withColumn("_ambiguous", F.coalesce(F.col("_ambiguous"), F.lit(False)))
    enriched, city_stats = _fill_place(enriched, city, "_station_city")
    state_stats: dict[str, Any]
    if state is None:
        enriched = enriched.withColumn("location_conflict", F.coalesce(city_stats["conflict"], F.lit(False)))
        state_stats = {
            "filled_from_station": 0,
            "still_missing": None,
            "measurement_value_kept": None,
            "unrecoverable_because_station_value_is_null": None,
        }
    else:
        enriched, filled_state = _fill_place(enriched, state, "_station_state")
        enriched = enriched.withColumn(
            "location_conflict",
            F.coalesce(city_stats["conflict"] | filled_state["conflict"], F.lit(False)),
        )
        state_stats = _place_summary(enriched, filled_state["source_column"], "_station_state")
    city_summary = _place_summary(enriched, city_stats["source_column"], "_station_city")
    matched = F.col("_station_key").isNotNull()
    no_station = _sum_when(enriched, ~matched & ~F.col("_ambiguous"))
    ambiguous_rows = _sum_when(enriched, F.col("_ambiguous"))
    name_mismatches = _station_name_mismatches(enriched)
    enriched = _attach_coordinates(enriched)
    location_conflicts = _sum_when(enriched, F.col("location_conflict"))
    helper_columns = [
        name
        for name in enriched.columns
        if name.startswith("_station_") or name in {"_ambiguous", "_ambiguous_key", "_n"}
    ]
    result = enriched.drop(*helper_columns)
    return result, {
        "city_column": city,
        "state_column": state,
        "city_filled_from_station": city_summary["filled_from_station"],
        "state_filled_from_station": state_stats["filled_from_station"],
        "city_still_missing": city_summary["still_missing"],
        "state_still_missing": state_stats["still_missing"],
        "city_measurement_value_kept": city_summary["measurement_value_kept"],
        "state_measurement_value_kept": state_stats["measurement_value_kept"],
        "location_conflicts": location_conflicts,
        "rows_without_station_record": no_station,
        "ambiguous_station_rows": ambiguous_rows,
        "city_unrecoverable_because_station_value_is_null": city_summary[
            "unrecoverable_because_station_value_is_null"
        ],
        "state_unrecoverable_because_station_value_is_null": state_stats[
            "unrecoverable_because_station_value_is_null"
        ],
        "latitude_non_null": _sum_when(result, F.col(_LATITUDE_COLUMN).isNotNull())
        if _LATITUDE_COLUMN in result.columns
        else None,
        "longitude_non_null": _sum_when(result, F.col(_LONGITUDE_COLUMN).isNotNull())
        if _LONGITUDE_COLUMN in result.columns
        else None,
        "station_name_mismatches": name_mismatches,
    }


def _station_lookup(unique: DataFrame, columns: Any, state: str | None) -> DataFrame:
    fields = [F.col(columns.station_id).alias("_station_key")]
    if columns.city in unique.columns:
        fields.append(F.col(columns.city).alias("_station_city"))
    else:
        fields.append(F.lit(None).cast("string").alias("_station_city"))
    if state is not None:
        fields.append(F.col(state).alias("_station_state"))
    if _LATITUDE_COLUMN in unique.columns:
        fields.append(F.col(_LATITUDE_COLUMN).alias("_station_latitude"))
    if _LONGITUDE_COLUMN in unique.columns:
        fields.append(F.col(_LONGITUDE_COLUMN).alias("_station_longitude"))
    if _STATION_NAME_COLUMN in unique.columns:
        fields.append(F.col(_STATION_NAME_COLUMN).alias("_station_name_meta"))
    return unique.select(*fields)


def _fill_place(frame: DataFrame, column_name: str, station_column: str) -> tuple[DataFrame, dict[str, Any]]:
    measurement_blank = _blank(column_name)
    station_blank = _blank(station_column)
    ambiguous = F.col("_ambiguous")
    conflict = (~measurement_blank) & (~station_blank) & (
        F.trim(F.col(column_name).cast("string")) != F.trim(F.col(station_column).cast("string"))
    )
    source_column = f"{column_name}_source"
    filled = (
        F.when(~measurement_blank, F.col(column_name))
        .when(~ambiguous & ~station_blank, F.col(station_column))
        .otherwise(F.lit(None).cast("string"))
    )
    source = (
        F.when(~measurement_blank, F.lit(_MEASUREMENT_SOURCE))
        .when(~ambiguous & ~station_blank, F.lit(_STATION_SOURCE))
        .otherwise(F.lit(_MISSING_SOURCE))
    )
    updated = frame.withColumn(source_column, source).withColumn(column_name, filled)
    return updated, {"source_column": source_column, "conflict": conflict}


def _place_summary(frame: DataFrame, source_column: str, station_column: str) -> dict[str, Any]:
    source = F.col(source_column)
    unrecoverable = None
    if station_column in frame.columns:
        unrecoverable = _sum_when(
            frame,
            (source == F.lit(_MISSING_SOURCE))
            & F.col("_station_key").isNotNull()
            & _blank(station_column)
            & ~F.col("_ambiguous"),
        )
    return {
        "filled_from_station": _sum_when(frame, source == F.lit(_STATION_SOURCE)),
        "still_missing": _sum_when(frame, source == F.lit(_MISSING_SOURCE)),
        "measurement_value_kept": _sum_when(frame, source == F.lit(_MEASUREMENT_SOURCE)),
        "unrecoverable_because_station_value_is_null": unrecoverable,
    }


def _attach_coordinates(frame: DataFrame) -> DataFrame:
    if _LATITUDE_COLUMN in frame.columns or "_station_latitude" not in frame.columns:
        attached = frame
    else:
        attached = frame.withColumn(_LATITUDE_COLUMN, F.col("_station_latitude"))
    if _LONGITUDE_COLUMN in frame.columns or "_station_longitude" not in frame.columns:
        return attached
    return attached.withColumn(_LONGITUDE_COLUMN, F.col("_station_longitude"))


def _station_name_mismatches(frame: DataFrame) -> int | None:
    if _STATION_NAME_COLUMN not in frame.columns or "_station_name_meta" not in frame.columns:
        return None
    return _sum_when(
        frame,
        F.col("_station_name_meta").isNotNull()
        & F.col(_STATION_NAME_COLUMN).isNotNull()
        & (F.col(_STATION_NAME_COLUMN) != F.col("_station_name_meta"))
        & ~F.col("_ambiguous"),
    )


def _with_unit_status(
    frame: DataFrame,
    parameters: DataFrame,
    columns: Any,
) -> tuple[DataFrame, dict[str, Any]]:
    if _UNIT_COLUMN not in frame.columns or _UNIT_COLUMN not in parameters.columns:
        return frame, {
            "checked": False,
            "reason": "The measurement file or parameters.parquet has no unit column.",
        }
    if columns.parameter not in parameters.columns:
        return frame, {
            "checked": False,
            "reason": f"parameters.parquet has no {columns.parameter} column.",
        }
    reference = parameters.select(columns.parameter, _UNIT_COLUMN).dropDuplicates()
    repeated = (
        reference.groupBy(columns.parameter)
        .agg(F.count(F.lit(1)).alias("_n"))
        .where(F.col("_n") > F.lit(1))
        .select(columns.parameter)
    )
    unique = reference.join(repeated, columns.parameter, "left_anti").withColumnRenamed(
        _UNIT_COLUMN, "_expected_unit"
    )
    compared = frame.join(unique, columns.parameter, "left")
    status = (
        F.when(F.col("_expected_unit").isNull(), F.lit("no_reference"))
        .when(_blank(_UNIT_COLUMN), F.lit("missing_unit"))
        .when(F.col(_UNIT_COLUMN) != F.col("_expected_unit"), F.lit("unexpected"))
        .otherwise(F.lit("match"))
    )
    compared = compared.withColumn("unit_status", status).drop("_expected_unit")
    counts = {
        row["unit_status"]: int(row["n"])
        for row in compared.groupBy("unit_status").agg(F.count(F.lit(1)).alias("n")).collect()
    }
    return compared, {"checked": True, "counts": counts}


def _complete_key(columns: Any) -> F.Column:
    return (
        F.col(columns.station_id).isNotNull()
        & (F.trim(F.col(columns.station_id).cast("string")) != F.lit(""))
        & F.col(columns.parameter).isNotNull()
        & (F.trim(F.col(columns.parameter).cast("string")) != F.lit(""))
        & F.col(columns.timestamp).isNotNull()
    )


def _removal_reasons(frame: DataFrame, columns: Any) -> dict[str, int]:
    """Count incomplete keys. A row with several missing keys is included in each count."""

    station_blank = F.col(columns.station_id).isNull() | (
        F.trim(F.col(columns.station_id).cast("string")) == F.lit("")
    )
    parameter_blank = F.col(columns.parameter).isNull() | (
        F.trim(F.col(columns.parameter).cast("string")) == F.lit("")
    )
    timestamp_blank = F.col(columns.timestamp).isNull()
    return {
        "null_or_blank_station_id": _sum_when(frame, station_blank),
        "null_or_blank_parameter": _sum_when(frame, parameter_blank),
        "null_or_unparseable_timestamp": _sum_when(frame, timestamp_blank),
    }


def _status_counts(frame: DataFrame) -> dict[str, int]:
    rows = frame.groupBy("measurement_status").agg(F.count(F.lit(1)).alias("n")).collect()
    return {row["measurement_status"]: int(row["n"]) for row in rows}


def _value_summary(frame: DataFrame, value_name: str) -> dict[str, Any]:
    finite = frame.where(F.col("measurement_status").isin(_STATUS_VALID, _STATUS_NEGATIVE))
    row = finite.agg(
        F.min(F.col(value_name)).alias("min"),
        F.max(F.col(value_name)).alias("max"),
        F.count(F.lit(1)).alias("finite_rows"),
    ).collect()[0]
    minimum = row["min"]
    maximum = row["max"]
    at_maximum = 0
    if maximum is not None:
        at_maximum = _sum_when(finite, F.col(value_name) == F.lit(float(maximum)))
    return {
        "finite_rows": int(row["finite_rows"]),
        "min": None if minimum is None else float(minimum),
        "max": None if maximum is None else float(maximum),
        "rows_at_maximum": at_maximum,
    }


def _pollutant_counts(frame: DataFrame, parameter_name: str) -> dict[str, int]:
    rows = frame.groupBy(parameter_name).agg(F.count(F.lit(1)).alias("n")).collect()
    return {str(row[parameter_name]): int(row["n"]) for row in rows}


def _timestamp_bound(frame: DataFrame, column_name: str, function: Any) -> str | None:
    value = frame.agg(function(F.col(column_name)).alias("bound")).collect()[0]["bound"]
    if value is None:
        return None
    return str(value)


def _missing_counts(frame: DataFrame) -> dict[str, int]:
    expressions = [
        F.sum(F.when(F.col(name).isNull(), F.lit(1)).otherwise(F.lit(0))).alias(name) for name in frame.columns
    ]
    if not expressions:
        return {}
    row = frame.agg(*expressions).collect()[0]
    return {name: 0 if row[name] is None else int(row[name]) for name in frame.columns}


def _schema(frame: DataFrame) -> list[dict[str, Any]]:
    return [
        {"name": field.name, "type": field.dataType.simpleString(), "nullable": field.nullable}
        for field in frame.schema.fields
    ]


def _field_type(frame: DataFrame, column_name: str) -> str:
    return next(field.dataType.simpleString() for field in frame.schema.fields if field.name == column_name)


def _is_temporal(type_name: str) -> bool:
    return type_name == "date" or type_name.startswith("timestamp")


def _is_numeric(type_name: str) -> bool:
    base = type_name.split("(", 1)[0]
    return base in {"double", "float", "int", "bigint", "smallint", "tinyint", "decimal"}


def _is_floating(type_name: str) -> bool:
    base = type_name.split("(", 1)[0]
    return base in {"double", "float"}


def _blank(column_name: str) -> F.Column:
    value = F.col(column_name)
    return value.isNull() | (F.trim(value.cast("string")) == F.lit(""))


def _sum_when(frame: DataFrame, condition: F.Column) -> int:
    value = frame.agg(F.sum(F.when(condition, F.lit(1)).otherwise(F.lit(0))).alias("n")).collect()[0]["n"]
    return 0 if value is None else int(value)


def _row_count(frame: DataFrame) -> int:
    return int(frame.count())


def _fingerprints(paths: list[Path]) -> list[dict[str, Any]]:
    return [_fingerprint(path) for path in paths]


def _fingerprint(path: Path) -> dict[str, Any]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
            size += len(chunk)
    return {"path": str(path.resolve()), "bytes": size, "sha256": digest.hexdigest()}


def _require_unchanged(fingerprints: list[dict[str, Any]]) -> None:
    for item in fingerprints:
        current = _fingerprint(Path(item["path"]))
        if current["sha256"] != item["sha256"] or current["bytes"] != item["bytes"]:
            raise PreprocessingError(f"Raw file changed while preprocessing: {item['path']}")


def _write_parquet(frame: DataFrame, output_dir: Path, config: AppConfig) -> Path:
    """Write one Parquet file with pyarrow and return its path.

    Spark's local file committer calls winutils.exe, which is not installed.
    A directory of files also cannot be listed by this Hadoop build. Python
    writes one file, and Spark reads that file path directly. ``year`` and
    ``month`` stay as columns. The rows are the Spark result.
    """

    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    resolved = output_dir.resolve()
    raw_root = config.paths.raw_root.resolve()
    if resolved == raw_root or raw_root in resolved.parents:
        raise PreprocessingError(f"Refusing to write cleaned data under {raw_root}.")
    output_file = resolved / "data.parquet"
    if output_file.exists():
        output_file.unlink()
    resolved.mkdir(parents=True, exist_ok=True)
    table = _naive_timestamps(frame.toArrow(), pa, pc)
    pq.write_table(table, output_file, compression=config.spark.parquet_compression)
    return output_file


def _naive_timestamps(table: object, pa: object, pc: object) -> object:
    columns = []
    fields = []
    for field in table.schema:  # type: ignore[attr-defined]
        column = table.column(field.name)  # type: ignore[attr-defined]
        if pa.types.is_timestamp(field.type):  # type: ignore[attr-defined]
            if field.type.tz is not None:
                column = pc.local_timestamp(column)  # type: ignore[attr-defined]
            column = column.cast(pa.timestamp("us"))  # type: ignore[attr-defined]
            field = pa.field(field.name, pa.timestamp("us"), nullable=field.nullable)  # type: ignore[attr-defined]
        columns.append(column)
        fields.append(field)
    return pa.table(columns, schema=pa.schema(fields))  # type: ignore[attr-defined]


def _spark_path(path: Path) -> str:
    # A normal Windows path keeps spaces and hive names such as year=2024.
    return str(path.resolve())


def _write_json(path: Path, document: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(document, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    temporary.replace(path)
