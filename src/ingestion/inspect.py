"""Inspect local XKDR Parquet files with a caller-supplied Spark session.

The session comes from ``utils.spark_session``. This module does not build
another one. Statistics are Spark results for the files on disk. An empty
selection is reported as empty; it is not replaced with example numbers.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from utils.config_loader import AppConfig, inventory_inputs

_REPORT_NAME = "inspection_report.json"


class InspectionError(Exception):
    """The local archive cannot be inspected as configured."""


@dataclass(frozen=True)
class InspectionReport:
    path: Path
    document: dict[str, Any]

    @property
    def selection_ok(self) -> bool:
        return bool(self.document["selection_ok"])


def inspect_dataset(
    config: AppConfig,
    spark: SparkSession,
    *,
    report_path: Path | None = None,
) -> InspectionReport:
    """Read the downloaded Parquet files and write ``inspection_report.json``."""

    inventory = inventory_inputs(config)
    if not inventory.measurement_files:
        raise InspectionError(
            "No measurement Parquet files found. Run acquire before inspect, "
            f"or place files under {config.paths.measurements_glob}."
        )
    if spark is None:
        raise InspectionError("inspect_dataset requires the shared SparkSession.")
    raw = read_measurement_files(config, spark)
    selected = select_measurements(config, raw)
    document = _measurement_document(config, raw, selected)
    document["stations_file"] = _metadata_document(spark, config.paths.stations_file, config)
    document["parameters_file"] = _metadata_document(spark, config.paths.parameters_file, config)
    destination = report_path if report_path is not None else config.paths.interim_dir / _REPORT_NAME
    _write_report(destination, document)
    return InspectionReport(path=destination, document=document)


def read_measurement_files(config: AppConfig, spark: SparkSession) -> DataFrame:
    """Read the monthly bulk files, including their ``year`` and ``month`` directories."""

    inventory = inventory_inputs(config)
    if not inventory.measurement_files:
        raise InspectionError(
            "No measurement Parquet files found. Run acquire before inspect, "
            f"or place files under {config.paths.measurements_glob}."
        )
    base = _measurement_base(config.paths.measurements_glob)
    paths = [_spark_path(path) for path in inventory.measurement_files]
    return spark.read.option("basePath", _spark_path(base)).parquet(*paths)


def select_measurements(
    config: AppConfig,
    frame: DataFrame,
    *,
    include_date_window: bool = True,
) -> DataFrame:
    """Keep rows for the configured source and pollutants.

    The date window is applied by default. Preprocessing turns it off so a
    timestamp can be parsed and counted before rows outside the window are
    excluded.
    """

    columns = config.dataset.columns
    missing = [
        name
        for name in (columns.source, columns.parameter, columns.timestamp, columns.station_id, columns.value)
        if name not in frame.columns
    ]
    if missing:
        raise InspectionError(
            "Measurement files are missing configured columns: "
            + ", ".join(missing)
            + ". Actual columns: "
            + ", ".join(frame.columns)
        )
    selected = frame.where(F.col(columns.source) == F.lit(config.dataset.source_filter)).where(
        F.col(columns.parameter).isin(list(config.dataset.pollutants))
    )
    if not include_date_window:
        return selected
    return _apply_date_window(selected, config)


def format_inspection_report(report: InspectionReport) -> str:
    document = report.document
    pollutant_lines = "\n".join(
        f"  {name}: {count}" for name, count in document["pollutant_counts"].items()
    ) or "  none"
    missing_lines = "\n".join(
        f"  {name}: {count}" for name, count in document["missing_values"].items() if count
    ) or "  none"
    failures = document["selection_failures"]
    failure_text = "\n".join(f"  {item}" for item in failures) if failures else "  none"
    return (
        f"Inspection report: {report.path}\n"
        f"Time zone: {document['timezone']}\n"
        f"Measurement files: {len(document['measurement_files'])}\n"
        f"Rows in downloaded files: {document['measurement_row_count']}\n"
        f"Rows after {document['source_filter']} and pollutant selection: {document['selected_row_count']}\n"
        f"Sources in selection: {_joined(document['distinct_sources'])}\n"
        f"Parameters in selection: {_joined(document['distinct_parameters'])}\n"
        f"Date range: {document['date_min']} to {document['date_max']}\n"
        f"Stations: {document['station_count']}\n"
        f"Pollutants:\n{pollutant_lines}\n"
        f"Missing values:\n{missing_lines}\n"
        f"Extra exact duplicate rows: {document['extra_exact_duplicate_rows']}\n"
        f"Extra key duplicate rows: {document['extra_key_duplicate_rows']}\n"
        f"Selection check: {'passed' if document['selection_ok'] else 'failed'}\n"
        f"Selection failures:\n{failure_text}"
    )


def _measurement_document(config: AppConfig, raw: DataFrame, selected: DataFrame) -> dict[str, Any]:
    columns = config.dataset.columns
    inventory = inventory_inputs(config)
    measurement_row_count = raw.count()
    selected_row_count = selected.count()
    distinct_rows = selected.distinct().count()
    key_columns = [columns.station_id, columns.parameter, columns.timestamp]
    distinct_keys = selected.select(*key_columns).distinct().count()
    bounds = selected.agg(F.min(columns.timestamp).alias("date_min"), F.max(columns.timestamp).alias("date_max")).collect()[0]
    station_count = selected.select(columns.station_id).where(F.col(columns.station_id).isNotNull()).distinct().count()
    pollutant_rows = selected.groupBy(columns.parameter).count().collect()
    pollutant_counts = {
        _text(row[columns.parameter]): int(row["count"])
        for row in sorted(pollutant_rows, key=lambda item: _text(item[columns.parameter]))
    }
    source_values = _distinct_values(selected, columns.source)
    parameter_values = _distinct_values(selected, columns.parameter)
    failures = _selection_failures(
        selected_row_count,
        source_values,
        parameter_values,
        config.dataset.source_filter,
        set(config.dataset.pollutants),
    )
    return {
        "schema_version": 1,
        "generated_at": datetime.now(ZoneInfo(config.dataset.timezone)).isoformat(),
        "timezone": config.dataset.timezone,
        "source_filter": config.dataset.source_filter,
        "pollutants": list(config.dataset.pollutants),
        "date_start": _optional_date(config.dataset.date_start),
        "date_end": _optional_date(config.dataset.date_end),
        "measurement_files": [str(path) for path in inventory.measurement_files],
        "measurement_row_count": measurement_row_count,
        "selected_row_count": selected_row_count,
        "columns": [
            {"name": field.name, "type": field.dataType.simpleString(), "nullable": field.nullable}
            for field in raw.schema.fields
        ],
        "date_min": _optional_timestamp(bounds["date_min"]),
        "date_max": _optional_timestamp(bounds["date_max"]),
        "station_count": station_count,
        "pollutant_counts": pollutant_counts,
        "missing_values": _missing_counts(selected),
        "extra_exact_duplicate_rows": selected_row_count - distinct_rows,
        "extra_key_duplicate_rows": selected_row_count - distinct_keys,
        "key_columns": key_columns,
        "distinct_sources": source_values,
        "distinct_parameters": parameter_values,
        "pollutants_absent": sorted(set(config.dataset.pollutants) - set(parameter_values)),
        "selection_ok": not failures,
        "selection_failures": failures,
    }


def _metadata_document(spark: SparkSession, path: Path, config: AppConfig) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    frame = spark.read.parquet(_spark_path(path))
    document: dict[str, Any] = {
        "path": str(path),
        "row_count": frame.count(),
        "columns": [
            {"name": field.name, "type": field.dataType.simpleString(), "nullable": field.nullable}
            for field in frame.schema.fields
        ],
        "missing_values": _missing_counts(frame),
    }
    source_column = config.dataset.columns.source
    if source_column in frame.columns:
        document["distinct_sources"] = _distinct_values(frame, source_column)
    return document


def _selection_failures(
    selected_row_count: int,
    sources: list[str | None],
    parameters: list[str | None],
    source_filter: str,
    allowed_parameters: set[str],
) -> list[str]:
    failures: list[str] = []
    if selected_row_count == 0:
        failures.append("No rows remained after the source, pollutant, and date filters.")
    if any(value is None for value in sources):
        failures.append("Selected rows include a null source.")
    extra_sources = sorted(value for value in set(sources) if value not in {None, source_filter})
    if extra_sources:
        failures.append("Selected rows include other sources: " + ", ".join(extra_sources))
    if any(value is None for value in parameters):
        failures.append("Selected rows include a null pollutant.")
    extra_parameters = sorted(value for value in set(parameters) if value is not None and value not in allowed_parameters)
    if extra_parameters:
        failures.append("Selected rows include other pollutants: " + ", ".join(extra_parameters))
    return failures


def _apply_date_window(frame: DataFrame, config: AppConfig) -> DataFrame:
    start = config.dataset.date_start
    end = config.dataset.date_end
    if start is None and end is None:
        return frame
    column_name = config.dataset.columns.timestamp
    as_date = F.to_date(F.col(column_name))
    if start is not None:
        frame = frame.where(as_date >= F.lit(start.isoformat()))
    if end is not None:
        frame = frame.where(as_date < F.lit((end + timedelta(days=1)).isoformat()))
    return frame


def _missing_counts(frame: DataFrame) -> dict[str, int]:
    if not frame.columns:
        return {}
    expressions = [
        F.sum(F.when(F.col(name).isNull(), F.lit(1)).otherwise(F.lit(0))).alias(name) for name in frame.columns
    ]
    row = frame.agg(*expressions).collect()[0]
    return {name: 0 if row[name] is None else int(row[name]) for name in frame.columns}


def _distinct_values(frame: DataFrame, column_name: str) -> list[str | None]:
    values = [None if row[column_name] is None else str(row[column_name]) for row in frame.select(column_name).distinct().collect()]
    return sorted(values, key=lambda value: (value is None, value or ""))


def _measurement_base(measurements_glob: str) -> Path:
    token = "year=*"
    position = measurements_glob.find(token)
    if position <= 0:
        raise InspectionError("paths.measurements_glob has no year=* partition.")
    return Path(measurements_glob[:position])


def _spark_path(path: Path) -> str:
    # A normal Windows path keeps spaces and hive names such as year=2024.
    # file:// URIs encode those characters and Spark then misses the file.
    return str(path.resolve())


def _write_report(path: Path, document: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(document, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    temporary.replace(path)


def _optional_date(value: date | None) -> str | None:
    return None if value is None else value.isoformat()


def _optional_timestamp(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _joined(values: list[object]) -> str:
    if not values:
        return "none"
    return ", ".join(_text(value) for value in values)


def _text(value: object) -> str:
    return "null" if value is None else str(value)
