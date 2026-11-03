"""Build an AppConfig rooted at a temporary directory."""

from __future__ import annotations

from pathlib import Path

from utils.config_loader import AppConfig, parse_config


def make_config(root: Path, *, date_start: str | None = None, date_end: str | None = None) -> AppConfig:
    payload = {
        "project": {"name": "Air pollution project"},
        "dataset": {
            "name": "India Air Quality Database",
            "provider": "XKDR Forum",
            "documentation_url": "https://airquality.xkdr.org/",
            "licence": "CC-BY-4.0",
            "source_filter": "cpcb_caaqm",
            "pollutants": ["PM2.5", "PM10"],
            "timezone": "Asia/Kolkata",
            "date_start": date_start,
            "date_end": date_end,
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
            "log_level": "ERROR",
            "ui_enabled": False,
            "adaptive_execution": True,
            "arrow_enabled": True,
            "parquet_compression": "snappy",
            "catalog": "in-memory",
        },
    }
    return parse_config(payload, project_root=root, source_file=root / "config" / "settings.yaml")
