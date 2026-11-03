"""Read and write project Parquet outputs.

Planned responsibility: persist Spark results under ``data/processed``
and ``data/interim`` using the paths in the configuration. The Spark
catalog is in-memory, so results are files, not Hive tables.

Functions added here should accept an ``AppConfig`` and a ``SparkSession``
created by the caller. This package currently has no I/O logic.
"""

__all__: list[str] = []
