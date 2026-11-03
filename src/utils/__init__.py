"""Shared utilities.

Importing this package loads configuration helpers only. The Spark
session factory stays in ``utils.spark_session`` so configuration tests
do not import PySpark.
"""

from utils.config_loader import ConfigError, load_config

__all__ = ["ConfigError", "load_config"]
