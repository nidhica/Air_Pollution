"""Configuration tests. These do not start a JVM or read pollution data."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from utils.config_loader import ConfigError, inventory_inputs, load_config, parse_config

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _payload() -> dict:
    return {
        "project": {"name": "Air pollution project"},
        "dataset": {
            "name": "India Air Quality Database",
            "provider": "XKDR Forum",
            "documentation_url": "https://airquality.xkdr.org/",
            "licence": "CC-BY-4.0",
            "source_filter": "cpcb_caaqm",
            "pollutants": ["PM2.5", "PM10"],
            "timezone": "Asia/Kolkata",
            "date_start": None,
            "date_end": None,
            "columns": {
                "station_id": "station_id",
                "parameter": "parameter_name",
                "timestamp": "collected_at",
                "value": "value",
                "city": "city_name",
                "source": "source",
            },
            "partitions": ["year", "month"],
        },
        "paths": {
            "raw_root": "data/raw",
            "measurements_glob": "data/raw/v1/measurements/year=*/month=*/data.parquet",
            "stations_file": "data/raw/v1/stations.parquet",
            "parameters_file": "data/raw/v1/parameters.parquet",
            "interim_dir": "data/interim",
            "processed_dir": "data/processed",
            "spark_warehouse_dir": "data/spark-warehouse",
            "logs_dir": "logs",
        },
        "spark": {
            "app_name": "air-pollution-bda",
            "master": "local[*]",
            "shuffle_partitions": 8,
            "driver_memory": "2g",
            "executor_memory": "2g",
            "timezone": "Asia/Kolkata",
            "log_level": "WARN",
            "ui_enabled": True,
            "adaptive_execution": True,
            "arrow_enabled": True,
            "parquet_compression": "snappy",
            "catalog": "in-memory",
        },
    }


def test_repository_config_selects_cpcb_caaqm_pm() -> None:
    config = load_config()

    assert config.project_name.startswith("Air Pollution Spike Detection")
    assert config.dataset.source_filter == "cpcb_caaqm"
    assert config.dataset.pollutants == ("PM2.5", "PM10")
    assert config.dataset.timezone == "Asia/Kolkata"
    assert config.dataset.date_start == date(2025, 8, 1)
    assert config.dataset.date_end == date(2025, 8, 31)
    assert config.dataset.columns.parameter == "parameter_name"
    assert config.dataset.columns.timestamp == "collected_at"
    assert config.dataset.partitions == ("year", "month")
    assert config.spikes.baseline_window == 24
    assert config.spikes.minimum_baseline == 12
    assert config.spikes.threshold_sigmas == 3
    assert config.analytics.elevated_minimum_valid_observations == 168
    assert config.analytics.elevated_percentile == 0.9
    assert config.analytics.unusual_sigmas == 2
    assert config.spark.catalog == "in-memory"
    assert config.spark.master == "local[*]"
    assert config.paths.project_root == PROJECT_ROOT
    assert config.paths.stations_file == PROJECT_ROOT / "data" / "raw" / "v1" / "stations.parquet"
    assert "year=*" in config.paths.measurements_glob
    assert "month=*" in config.paths.measurements_glob


def test_load_config_does_not_depend_on_working_directory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    config = load_config()
    assert config.paths.project_root == PROJECT_ROOT


def test_inventory_is_empty_when_archive_is_absent(tmp_path: Path) -> None:
    config = parse_config(
        _payload(),
        project_root=tmp_path,
        source_file=tmp_path / "config" / "settings.yaml",
    )
    inventory = inventory_inputs(config)

    assert inventory.measurement_files == ()
    assert inventory.stations_present is False
    assert inventory.parameters_present is False


def test_inventory_sees_a_monthly_file(tmp_path: Path) -> None:
    # Empty placeholder so the glob can be checked. Not a measurement file.
    month_dir = tmp_path / "data" / "raw" / "v1" / "measurements" / "year=2024" / "month=11"
    month_dir.mkdir(parents=True)
    parquet_path = month_dir / "data.parquet"
    parquet_path.write_bytes(b"")
    stations = tmp_path / "data" / "raw" / "v1" / "stations.parquet"
    stations.parent.mkdir(parents=True, exist_ok=True)
    stations.write_bytes(b"")

    config = parse_config(
        _payload(),
        project_root=tmp_path,
        source_file=tmp_path / "config" / "settings.yaml",
    )
    inventory = inventory_inputs(config)

    assert [path.resolve() for path in inventory.measurement_files] == [parquet_path.resolve()]
    assert inventory.stations_present is True
    assert inventory.parameters_present is False


def test_source_filter_rejects_embassy_records(tmp_path: Path) -> None:
    payload = _payload()
    payload["dataset"]["source_filter"] = "us_embassy"

    with pytest.raises(ConfigError, match="cpcb_caaqm"):
        parse_config(payload, project_root=tmp_path, source_file=tmp_path / "settings.yaml")


def test_date_window_must_be_ordered(tmp_path: Path) -> None:
    payload = _payload()
    payload["dataset"]["date_start"] = "2024-12-01"
    payload["dataset"]["date_end"] = "2024-01-01"

    with pytest.raises(ConfigError, match="date_start"):
        parse_config(payload, project_root=tmp_path, source_file=tmp_path / "settings.yaml")


def test_driver_memory_must_use_a_size_suffix(tmp_path: Path) -> None:
    payload = _payload()
    payload["spark"]["driver_memory"] = "2"

    with pytest.raises(ConfigError, match="driver_memory"):
        parse_config(payload, project_root=tmp_path, source_file=tmp_path / "settings.yaml")
