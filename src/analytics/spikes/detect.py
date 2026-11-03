"""Flag pollution spikes from a cleaned measurement table.

The caller supplies ``AppConfig`` and the shared ``SparkSession``. This
module does not build a session and does not write under ``data/raw``.

For each station and pollutant, Spark orders rows by ``collected_at`` and
looks at the previous valid observations only. The current row is outside
that frame. A row is a candidate spike when its value is strictly greater
than the baseline mean plus the configured number of sample standard
deviations, and at least ``minimum_baseline`` earlier valid observations
exist.

Rows that share ``station_id``, ``parameter_name``, and ``collected_at``
are not ordered against each other. No extra column is used as a
tie-breaker. Those rows stay in the output with ``is_spike`` false and are
left out of every baseline.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from preprocessing.clean import _fingerprint, _require_unchanged, _write_parquet
from utils.config_loader import AppConfig, inventory_inputs

_REPORT_NAME = "spike_report.json"
_ANNOTATED_DIR = ("spikes", "annotated")
_SPIKE_DIR = ("spikes", "spikes_only")

TIE_POLICY = (
    "Rows are ordered by collected_at within each station_id and parameter_name. "
    "When that timestamp is unique, the order is complete and the previous-window "
    "baseline is deterministic. When several rows share station_id, parameter_name, "
    "and collected_at, no other column is used to break the tie. Those rows are "
    "preserved, timestamp_tie is true, is_spike is false, and they are omitted from "
    "every baseline because their relative order is undefined."
)


class SpikeDetectionError(Exception):
    """The cleaned measurements cannot be scored as configured."""


@dataclass(frozen=True)
class SpikeReport:
    path: Path
    annotated_file: Path
    spike_file: Path
    document: dict[str, Any]


def cleaned_measurements_file(config: AppConfig) -> Path:
    return config.paths.processed_dir / "measurements" / "data.parquet"


def annotate_measurements(frame: DataFrame, config: AppConfig) -> DataFrame:
    """Add baseline and spike columns without dropping input rows."""

    columns = config.dataset.columns
    missing = [
        name
        for name in (columns.station_id, columns.parameter, columns.timestamp, columns.value)
        if name not in frame.columns
    ]
    if missing:
        raise SpikeDetectionError(
            "Cleaned measurements are missing columns: "
            + ", ".join(missing)
            + ". Actual columns: "
            + ", ".join(frame.columns)
        )
    value_type = _field_type(frame, columns.value)
    if not _is_numeric(value_type):
        raise SpikeDetectionError(
            f"Value column {columns.value} has type {value_type}. Expected a number."
        )

    station = columns.station_id
    parameter = columns.parameter
    timestamp = columns.timestamp
    marked = frame.withColumn(
        "_key_n",
        F.count(F.lit(1)).over(Window.partitionBy(station, parameter, timestamp)),
    )
    marked = marked.withColumn("timestamp_tie", F.col("_key_n") > F.lit(1))
    marked = marked.withColumn(
        "ordering_defined",
        F.col(timestamp).isNotNull() & (F.col("_key_n") == F.lit(1)),
    )
    marked = marked.withColumn("is_valid_measurement", _valid_value(marked, columns.value))
    marked = marked.drop("_key_n")

    defined = marked.where(F.col("ordering_defined"))
    ties = marked.where(~F.col("ordering_defined"))
    scored = _score_ordered_rows(defined, config)
    unscored = _unscored_ties(ties)
    return scored.unionByName(unscored)


def detect_spikes(
    config: AppConfig,
    spark: SparkSession,
    *,
    report_path: Path | None = None,
    annotated_dir: Path | None = None,
    spike_dir: Path | None = None,
) -> SpikeReport:
    """Score the cleaned Parquet file and write the annotated and spike outputs."""

    source = cleaned_measurements_file(config)
    if not source.is_file():
        raise SpikeDetectionError(
            f"Cleaned measurements not found: {source}. Run preprocess before detect-spikes."
        )
    if spark is None:
        raise SpikeDetectionError("detect_spikes requires the shared SparkSession.")

    annotated_directory = annotated_dir or _processed_dir(config, _ANNOTATED_DIR)
    spike_directory = spike_dir or _processed_dir(config, _SPIKE_DIR)
    _refuse_protected_output(config, source, annotated_directory)
    _refuse_protected_output(config, source, spike_directory)

    fingerprints = _fingerprints(_protected_files(config, source))
    frame = spark.read.parquet(str(source.resolve()))
    annotated = annotate_measurements(frame, config).cache()
    try:
        annotated_file = _write_parquet(annotated, annotated_directory, config)
        spikes = annotated.where(F.col("is_spike"))
        spike_file = _write_parquet(spikes, spike_directory, config)
        read_back = spark.read.parquet(str(annotated_file.resolve()))
        spike_back = spark.read.parquet(str(spike_file.resolve()))
        input_rows = int(frame.count())
        annotated_rows = int(annotated.count())
        read_rows = int(read_back.count())
        spike_rows = int(spikes.count())
        spike_read_rows = int(spike_back.count())
        if annotated_rows != input_rows or read_rows != input_rows:
            raise SpikeDetectionError(
                f"Annotated row count changed: input {input_rows}, "
                f"scored {annotated_rows}, read back {read_rows}."
            )
        if spike_read_rows != spike_rows:
            raise SpikeDetectionError(
                f"Spike file has {spike_read_rows} rows; the scored filter has {spike_rows}."
            )
        document = _report_document(
            config,
            source,
            annotated_file,
            spike_file,
            annotated,
            input_rows,
            spike_rows,
        )
    finally:
        annotated.unpersist()

    _require_unchanged(fingerprints)
    document["raw_files_modified"] = False
    document["input_file_modified"] = False
    document["protected_file_fingerprints"] = fingerprints
    destination = report_path if report_path is not None else config.paths.interim_dir / _REPORT_NAME
    _write_json(destination, document)
    return SpikeReport(
        path=destination,
        annotated_file=annotated_file,
        spike_file=spike_file,
        document=document,
    )


def format_spike_report(report: SpikeReport) -> str:
    document = report.document
    counts = document["counts"]
    ordering = document["ordering"]
    pollutant_lines = "\n".join(
        f"  {row['parameter_name']}: {row['spikes']}"
        for row in document["spikes_by_pollutant"]
    ) or "  none"
    return (
        f"Spike report: {report.path}\n"
        f"Annotated Parquet: {report.annotated_file}\n"
        f"Spike Parquet: {report.spike_file}\n"
        f"Total records: {counts['total_records']}\n"
        f"Valid measurements: {counts['valid_measurements']}\n"
        f"Eligible records: {counts['eligible_records']}\n"
        f"Spike records: {counts['spike_records']}\n"
        f"Percentage of observations flagged: {counts['spike_percentage_of_observations']}\n"
        f"Percentage of eligible observations flagged: {counts['spike_percentage_of_eligible']}\n"
        f"Duplicate timestamp keys: {ordering['duplicate_key_rows']}\n"
        f"Spikes by pollutant:\n{pollutant_lines}\n"
        f"Raw files modified: {document['raw_files_modified']}"
    )


def _score_ordered_rows(frame: DataFrame, config: AppConfig) -> DataFrame:
    columns = config.dataset.columns
    settings = config.spikes
    station = columns.station_id
    parameter = columns.parameter
    timestamp = columns.timestamp
    value = columns.value
    valid = frame.where(F.col("is_valid_measurement"))
    inclusive = (
        Window.partitionBy(station, parameter)
        .orderBy(F.col(timestamp))
        .rowsBetween(-(settings.baseline_window - 1), 0)
    )
    trailing = valid.select(
        F.col(station).alias("_trail_station"),
        F.col(parameter).alias("_trail_parameter"),
        F.col(timestamp).alias("_trail_timestamp"),
        F.count(F.col(value)).over(inclusive).alias("_incl_count"),
        F.avg(F.col(value)).over(inclusive).alias("_incl_mean"),
        F.stddev_samp(F.col(value)).over(inclusive).alias("_incl_std"),
    )
    joined = frame.join(
        trailing,
        (F.col(station) == F.col("_trail_station"))
        & (F.col(parameter) == F.col("_trail_parameter"))
        & (F.col(timestamp) == F.col("_trail_timestamp")),
        "left",
    ).drop("_trail_station", "_trail_parameter", "_trail_timestamp")
    before = (
        Window.partitionBy(station, parameter)
        .orderBy(F.col(timestamp))
        .rowsBetween(Window.unboundedPreceding, -1)
    )
    # The inclusive stats sit on each valid row and include that row.
    # Carrying the previous non-null stats forward excludes the current row.
    baseline_count = F.coalesce(F.last("_incl_count", ignorenulls=True).over(before), F.lit(0))
    rolling_mean = F.last("_incl_mean", ignorenulls=True).over(before)
    rolling_stddev = F.last("_incl_std", ignorenulls=True).over(before)
    scored = (
        joined.withColumn("baseline_count", baseline_count)
        .withColumn("rolling_mean", rolling_mean)
        .withColumn("rolling_stddev", rolling_stddev)
        .drop("_incl_count", "_incl_mean", "_incl_std")
    )
    established = (
        F.col("is_valid_measurement")
        & (F.col("baseline_count") >= F.lit(settings.minimum_baseline))
        & F.col("rolling_stddev").isNotNull()
    )
    threshold = F.when(
        established,
        F.col("rolling_mean") + F.lit(settings.threshold_sigmas) * F.col("rolling_stddev"),
    )
    scored = scored.withColumn("spike_threshold", threshold)
    scored = scored.withColumn(
        "is_spike",
        F.when(
            F.col("spike_threshold").isNotNull() & (F.col(value) > F.col("spike_threshold")),
            F.lit(True),
        ).otherwise(F.lit(False)),
    )
    return scored


def _unscored_ties(frame: DataFrame) -> DataFrame:
    return (
        frame.withColumn("baseline_count", F.lit(None).cast("long"))
        .withColumn("rolling_mean", F.lit(None).cast("double"))
        .withColumn("rolling_stddev", F.lit(None).cast("double"))
        .withColumn("spike_threshold", F.lit(None).cast("double"))
        .withColumn("is_spike", F.lit(False))
    )


def _valid_value(frame: DataFrame, value_name: str) -> F.Column:
    value = F.col(value_name)
    finite_and_non_negative = (
        value.isNotNull()
        & ~F.isnan(value)
        & (value < F.lit(1e308))
        & (value > F.lit(-1e308))
        & (value >= F.lit(0))
    )
    if "measurement_status" in frame.columns:
        return finite_and_non_negative & (F.col("measurement_status") == F.lit("valid"))
    return finite_and_non_negative


def _report_document(
    config: AppConfig,
    source: Path,
    annotated_file: Path,
    spike_file: Path,
    annotated: DataFrame,
    input_rows: int,
    spike_rows: int,
) -> dict[str, Any]:
    columns = config.dataset.columns
    settings = config.spikes
    eligible = (
        F.col("is_valid_measurement")
        & F.col("ordering_defined")
        & (F.col("baseline_count") >= F.lit(settings.minimum_baseline))
        & F.col("rolling_stddev").isNotNull()
    )
    summary = annotated.agg(
        F.sum(F.when(F.col("is_valid_measurement"), 1).otherwise(0)).alias("valid"),
        F.sum(F.when(eligible, 1).otherwise(0)).alias("eligible"),
        F.sum(F.when(F.col("timestamp_tie"), 1).otherwise(0)).alias("ties"),
        F.sum(F.when(F.col(columns.timestamp).isNull(), 1).otherwise(0)).alias("null_timestamps"),
        F.countDistinct(
            F.when(F.col("timestamp_tie"), F.struct(columns.station_id, columns.parameter, columns.timestamp))
        ).alias("tie_groups"),
    ).collect()[0]
    valid_rows = int(summary["valid"] or 0)
    eligible_rows = int(summary["eligible"] or 0)
    tie_rows = int(summary["ties"] or 0)
    tie_groups = int(summary["tie_groups"] or 0)
    generated_at = datetime.now(ZoneInfo(config.dataset.timezone)).isoformat(timespec="seconds")
    return {
        "schema_version": 1,
        "generated_at": generated_at,
        "timezone": config.dataset.timezone,
        "input_file": str(source.resolve()),
        "annotated_file": str(annotated_file.resolve()),
        "spike_file": str(spike_file.resolve()),
        "parameters": {
            "baseline_window": settings.baseline_window,
            "minimum_baseline": settings.minimum_baseline,
            "threshold_sigmas": settings.threshold_sigmas,
            "standard_deviation": "sample",
            "comparison": "value > rolling_mean + threshold_sigmas * rolling_stddev",
            "current_observation_in_baseline": False,
        },
        "ordering": {
            "key_columns": [columns.station_id, columns.parameter, columns.timestamp],
            "order_by": [columns.timestamp],
            "duplicate_key_rows": tie_rows,
            "duplicate_key_groups": tie_groups,
            "null_timestamps": int(summary["null_timestamps"] or 0),
            "unique_within_station_and_parameter": tie_rows == 0,
            "tie_policy": TIE_POLICY,
        },
        "counts": {
            "total_records": input_rows,
            "valid_measurements": valid_rows,
            "eligible_records": eligible_rows,
            "spike_records": spike_rows,
            "spike_percentage_of_observations": _percentage(spike_rows, input_rows),
            "spike_percentage_of_eligible": _percentage(spike_rows, eligible_rows),
        },
        "spikes_by_pollutant": _grouped_counts(
            annotated,
            [(columns.parameter, F.col(columns.parameter))],
        ),
        "spikes_by_station": _grouped_counts(
            annotated,
            [
                (columns.station_id, F.col(columns.station_id)),
                (columns.parameter, F.col(columns.parameter)),
            ],
        ),
        "spikes_by_date": _grouped_counts(
            annotated,
            [
                ("observation_date", F.to_date(F.col(columns.timestamp))),
                (columns.parameter, F.col(columns.parameter)),
            ],
        ),
    }


def _grouped_counts(frame: DataFrame, keys: list[tuple[str, F.Column]]) -> list[dict[str, Any]]:
    names = [name for name, _expression in keys]
    rows = (
        frame.groupBy(*[expression.alias(name) for name, expression in keys])
        .agg(
            F.count(F.lit(1)).alias("observations"),
            F.sum(F.when(F.col("is_spike"), 1).otherwise(0)).alias("spikes"),
        )
        .orderBy(*names)
        .collect()
    )
    documents = []
    for row in rows:
        item = {name: _json_value(row[name]) for name in names}
        item["observations"] = int(row["observations"])
        item["spikes"] = int(row["spikes"] or 0)
        documents.append(item)
    return documents


def _json_value(value: object) -> object:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def _percentage(part: int, whole: int) -> float | None:
    if whole == 0:
        return None
    return part / whole * 100


def _processed_dir(config: AppConfig, parts: tuple[str, ...]) -> Path:
    directory = config.paths.processed_dir
    for part in parts:
        directory = directory / part
    return directory


def _refuse_protected_output(config: AppConfig, source: Path, output_dir: Path) -> None:
    resolved = output_dir.resolve()
    raw_root = config.paths.raw_root.resolve()
    if resolved == raw_root or raw_root in resolved.parents:
        raise SpikeDetectionError(f"Refusing to write spike output under {raw_root}.")
    if resolved == source.parent.resolve():
        raise SpikeDetectionError(f"Refusing to overwrite the cleaned measurements in {source.parent}.")


def _protected_files(config: AppConfig, source: Path) -> list[Path]:
    inventory = inventory_inputs(config)
    files = list(inventory.measurement_files)
    if inventory.stations_present:
        files.append(config.paths.stations_file)
    if inventory.parameters_present:
        files.append(config.paths.parameters_file)
    files.append(source)
    return files


def _fingerprints(paths: list[Path]) -> list[dict[str, Any]]:
    return [_fingerprint(path) for path in paths]


def _field_type(frame: DataFrame, column_name: str) -> str:
    return next(field.dataType.simpleString() for field in frame.schema.fields if field.name == column_name)


def _is_numeric(type_name: str) -> bool:
    base = type_name.split("(", 1)[0]
    return base in {"double", "float", "int", "bigint", "smallint", "tinyint", "decimal"}


def _write_json(path: Path, document: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(document, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    temporary.replace(path)
