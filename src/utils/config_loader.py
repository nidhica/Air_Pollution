"""Load and validate config/settings.yaml.

The loader resolves paths from the repository root, which is the parent
of the ``config`` directory. It does not start Spark and does not read
the air-quality files.
"""

from __future__ import annotations

import glob
import os
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import yaml

_DATE_FORMAT = "%Y-%m-%d"
_MEMORY_UNITS = frozenset({"g", "G", "m", "M"})
_LOG_LEVELS = frozenset({"ALL", "DEBUG", "ERROR", "FATAL", "INFO", "OFF", "TRACE", "WARN"})
_SOURCE_FILTER = "cpcb_caaqm"
_CATALOG = "in-memory"


class ConfigError(Exception):
    """The project configuration is missing a required setting or has an invalid value."""


@dataclass(frozen=True)
class ColumnMap:
    """Role names used by project code, mapped to physical XKDR column names."""

    station_id: str
    parameter: str
    timestamp: str
    value: str
    city: str
    source: str


@dataclass(frozen=True)
class DatasetSettings:
    name: str
    provider: str
    documentation_url: str
    licence: str
    source_filter: str
    pollutants: tuple[str, ...]
    timezone: str
    date_start: date | None
    date_end: date | None
    columns: ColumnMap
    partitions: tuple[str, ...]


@dataclass(frozen=True)
class PathSettings:
    project_root: Path
    raw_root: Path
    measurements_glob: str
    stations_file: Path
    parameters_file: Path
    interim_dir: Path
    processed_dir: Path
    spark_warehouse_dir: Path
    logs_dir: Path


@dataclass(frozen=True)
class SparkSettings:
    app_name: str
    master: str
    shuffle_partitions: int
    driver_memory: str
    executor_memory: str
    timezone: str
    log_level: str
    ui_enabled: bool
    adaptive_execution: bool
    arrow_enabled: bool
    parquet_compression: str
    catalog: str


@dataclass(frozen=True)
class SpikeSettings:
    """Rolling baseline used to flag a pollution spike."""

    baseline_window: int
    minimum_baseline: int
    threshold_sigmas: float


@dataclass(frozen=True)
class AnalyticsSettings:
    """Thresholds for elevated stations and unusually high periods.

    These settings describe comparisons among aggregated series. They do not
    change the spike flags already stored on the annotated measurements.
    """

    elevated_minimum_valid_observations: int
    elevated_percentile: float
    unusual_sigmas: float


@dataclass(frozen=True)
class AppConfig:
    source_file: Path
    project_name: str
    dataset: DatasetSettings
    paths: PathSettings
    spark: SparkSettings
    spikes: SpikeSettings
    analytics: AnalyticsSettings


@dataclass(frozen=True)
class InputInventory:
    """Which configured XKDR files are present on disk. Empty is a valid state."""

    measurement_files: tuple[Path, ...]
    stations_present: bool
    parameters_present: bool


def repository_root() -> Path:
    """Return the repository root inferred from this module's location."""

    return Path(__file__).resolve().parents[2]


def default_config_path() -> Path:
    return repository_root() / "config" / "settings.yaml"


def load_config(path: str | Path | None = None) -> AppConfig:
    """Load the project configuration.

    ``path`` overrides the ``AIR_POLLUTION_CONFIG`` environment variable,
    which overrides ``config/settings.yaml``.
    """

    chosen = _select_config_path(path)
    config_path = chosen if chosen.is_absolute() else Path.cwd() / chosen
    config_path = config_path.resolve()
    if not config_path.is_file():
        raise ConfigError(f"Configuration file not found: {config_path}")

    loaded = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    project_root = config_path.parent.parent
    return parse_config(loaded, project_root=project_root, source_file=config_path)


def parse_config(loaded: object, *, project_root: Path, source_file: Path) -> AppConfig:
    """Validate a configuration mapping and resolve its paths."""

    root_mapping = _require_mapping(loaded, "configuration")
    project = _require_mapping(_require_key(root_mapping, "project", "configuration"), "project")
    dataset = _require_mapping(_require_key(root_mapping, "dataset", "configuration"), "dataset")
    paths = _require_mapping(_require_key(root_mapping, "paths", "configuration"), "paths")
    spark = _require_mapping(_require_key(root_mapping, "spark", "configuration"), "spark")

    project_root = project_root.resolve()
    date_start = _optional_date(_require_key(dataset, "date_start", "dataset"), "dataset.date_start")
    date_end = _optional_date(_require_key(dataset, "date_end", "dataset"), "dataset.date_end")
    if date_start is not None and date_end is not None and date_start > date_end:
        raise ConfigError("dataset.date_start must be on or before dataset.date_end.")

    source_filter = _require_text(dataset, "source_filter", "dataset")
    if source_filter != _SOURCE_FILTER:
        raise ConfigError(
            "dataset.source_filter must be 'cpcb_caaqm'. "
            "This project uses CPCB CAAQM records only."
        )

    return AppConfig(
        source_file=source_file,
        project_name=_require_text(project, "name", "project"),
        dataset=DatasetSettings(
            name=_require_text(dataset, "name", "dataset"),
            provider=_require_text(dataset, "provider", "dataset"),
            documentation_url=_require_text(dataset, "documentation_url", "dataset"),
            licence=_require_text(dataset, "licence", "dataset"),
            source_filter=source_filter,
            pollutants=_pollutants(dataset.get("pollutants")),
            timezone=_require_text(dataset, "timezone", "dataset"),
            date_start=date_start,
            date_end=date_end,
            columns=_columns(_require_key(dataset, "columns", "dataset")),
            partitions=_text_tuple(_require_key(dataset, "partitions", "dataset"), "dataset.partitions"),
        ),
        paths=PathSettings(
            project_root=project_root,
            raw_root=_resolve_path(project_root, paths, "raw_root"),
            measurements_glob=_resolve_glob(project_root, paths.get("measurements_glob")),
            stations_file=_resolve_path(project_root, paths, "stations_file"),
            parameters_file=_resolve_path(project_root, paths, "parameters_file"),
            interim_dir=_resolve_path(project_root, paths, "interim_dir"),
            processed_dir=_resolve_path(project_root, paths, "processed_dir"),
            spark_warehouse_dir=_resolve_path(project_root, paths, "spark_warehouse_dir"),
            logs_dir=_resolve_path(project_root, paths, "logs_dir"),
        ),
        spark=SparkSettings(
            app_name=_require_text(spark, "app_name", "spark"),
            master=_require_text(spark, "master", "spark"),
            shuffle_partitions=_positive_int(spark.get("shuffle_partitions"), "spark.shuffle_partitions"),
            driver_memory=_memory(spark.get("driver_memory"), "spark.driver_memory"),
            executor_memory=_memory(spark.get("executor_memory"), "spark.executor_memory"),
            timezone=_require_text(spark, "timezone", "spark"),
            log_level=_log_level(spark.get("log_level")),
            ui_enabled=_require_bool(spark, "ui_enabled", "spark"),
            adaptive_execution=_require_bool(spark, "adaptive_execution", "spark"),
            arrow_enabled=_require_bool(spark, "arrow_enabled", "spark"),
            parquet_compression=_require_text(spark, "parquet_compression", "spark"),
            catalog=_catalog(spark.get("catalog")),
        ),
        spikes=_spike_settings(root_mapping.get("spikes")),
        analytics=_analytics_settings(root_mapping.get("analytics")),
    )


def _spike_settings(value: object) -> SpikeSettings:
    if value is None:
        return SpikeSettings(baseline_window=24, minimum_baseline=12, threshold_sigmas=3.0)
    mapping = _require_mapping(value, "spikes")
    window = _positive_int(mapping.get("baseline_window", 24), "spikes.baseline_window")
    minimum = _positive_int(mapping.get("minimum_baseline", 12), "spikes.minimum_baseline")
    sigmas = mapping.get("threshold_sigmas", 3.0)
    if isinstance(sigmas, bool) or not isinstance(sigmas, (int, float)) or sigmas <= 0:
        raise ConfigError("spikes.threshold_sigmas must be a number greater than 0.")
    if minimum < 2:
        raise ConfigError(
            "spikes.minimum_baseline must be at least 2 so the sample standard deviation is defined."
        )
    if minimum > window:
        raise ConfigError("spikes.minimum_baseline cannot exceed spikes.baseline_window.")
    return SpikeSettings(
        baseline_window=window,
        minimum_baseline=minimum,
        threshold_sigmas=float(sigmas),
    )


def _analytics_settings(value: object) -> AnalyticsSettings:
    if value is None:
        return AnalyticsSettings(
            elevated_minimum_valid_observations=168,
            elevated_percentile=0.9,
            unusual_sigmas=2.0,
        )
    mapping = _require_mapping(value, "analytics")
    minimum = _positive_int(
        mapping.get("elevated_minimum_valid_observations", 168),
        "analytics.elevated_minimum_valid_observations",
    )
    percentile = mapping.get("elevated_percentile", 0.9)
    if isinstance(percentile, bool) or not isinstance(percentile, (int, float)) or not 0 < float(percentile) < 1:
        raise ConfigError(
            "analytics.elevated_percentile must be a number greater than 0 and less than 1."
        )
    sigmas = mapping.get("unusual_sigmas", 2.0)
    if isinstance(sigmas, bool) or not isinstance(sigmas, (int, float)) or sigmas <= 0:
        raise ConfigError("analytics.unusual_sigmas must be a number greater than 0.")
    return AnalyticsSettings(
        elevated_minimum_valid_observations=minimum,
        elevated_percentile=float(percentile),
        unusual_sigmas=float(sigmas),
    )


def inventory_inputs(config: AppConfig) -> InputInventory:
    """Report which configured dataset files exist. Missing files are not an error."""

    matches = tuple(Path(match) for match in sorted(glob.glob(config.paths.measurements_glob)))
    return InputInventory(
        measurement_files=matches,
        stations_present=config.paths.stations_file.is_file(),
        parameters_present=config.paths.parameters_file.is_file(),
    )


def _select_config_path(path: str | Path | None) -> Path:
    if path is not None:
        return Path(path)
    from_env = os.environ.get("AIR_POLLUTION_CONFIG")
    if from_env:
        return Path(from_env)
    return default_config_path()


def _require_mapping(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ConfigError(f"{label} must be a mapping.")
    return value


def _require_key(mapping: dict[str, Any], key: str, label: str) -> object:
    if key not in mapping:
        raise ConfigError(f"{label} is missing '{key}'.")
    return mapping[key]


def _require_text(mapping: dict[str, Any], key: str, label: str) -> str:
    value = _require_key(mapping, key, label)
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{label}.{key} must be a non-empty string.")
    return value.strip()


def _require_bool(mapping: dict[str, Any], key: str, label: str) -> bool:
    value = _require_key(mapping, key, label)
    if not isinstance(value, bool):
        raise ConfigError(f"{label}.{key} must be a boolean.")
    return value


def _pollutants(value: object) -> tuple[str, ...]:
    pollutants = _text_tuple(value, "dataset.pollutants")
    if len(set(pollutants)) != len(pollutants):
        raise ConfigError("dataset.pollutants contains duplicates.")
    return pollutants


def _text_tuple(value: object, label: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise ConfigError(f"{label} must be a non-empty list of strings.")
    texts: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item.strip():
            raise ConfigError(f"{label} must contain only non-empty strings.")
        texts.append(item.strip())
    return tuple(texts)


def _columns(value: object) -> ColumnMap:
    mapping = _require_mapping(value, "dataset.columns")
    return ColumnMap(
        station_id=_require_text(mapping, "station_id", "dataset.columns"),
        parameter=_require_text(mapping, "parameter", "dataset.columns"),
        timestamp=_require_text(mapping, "timestamp", "dataset.columns"),
        value=_require_text(mapping, "value", "dataset.columns"),
        city=_require_text(mapping, "city", "dataset.columns"),
        source=_require_text(mapping, "source", "dataset.columns"),
    )


def _optional_date(value: object, label: str) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        try:
            return datetime.strptime(value, _DATE_FORMAT).date()
        except ValueError as exc:
            raise ConfigError(f"{label} must be YYYY-MM-DD.") from exc
    raise ConfigError(f"{label} must be YYYY-MM-DD or null.")


def _positive_int(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ConfigError(f"{label} must be an integer of 1 or greater.")
    return value


def _memory(value: object, label: str) -> str:
    if not isinstance(value, str) or len(value) < 2 or not value[:-1].isdigit() or value[-1] not in _MEMORY_UNITS:
        raise ConfigError(f"{label} must be a size such as 2g or 512m.")
    if value[:-1].startswith("0"):
        raise ConfigError(f"{label} must be a size such as 2g or 512m.")
    return value


def _log_level(value: object) -> str:
    if not isinstance(value, str) or value not in _LOG_LEVELS:
        allowed = ", ".join(sorted(_LOG_LEVELS))
        raise ConfigError(f"spark.log_level must be one of: {allowed}.")
    return value


def _catalog(value: object) -> str:
    if value != _CATALOG:
        raise ConfigError(
            "spark.catalog must be 'in-memory'. "
            "Processed data is written to explicit Parquet paths."
        )
    return value


def _resolve_path(project_root: Path, mapping: dict[str, Any], key: str) -> Path:
    raw = _require_text(mapping, key, "paths")
    candidate = Path(raw)
    if candidate.is_absolute():
        return candidate
    return project_root / candidate


def _resolve_glob(project_root: Path, value: object) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ConfigError("paths.measurements_glob must be a non-empty string.")
    candidate = Path(value.strip())
    absolute = candidate if candidate.is_absolute() else project_root / candidate
    return str(absolute)
