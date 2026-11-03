"""Download the XKDR bulk archive and inspect the local Parquet files.

``acquire_dataset`` calls the documented file API and does not start Spark.
``inspect_dataset`` is in ``ingestion.inspect``. It reads Parquet with the
``SparkSession`` the caller supplies.
"""

from ingestion.acquire import AcquisitionError, acquire_dataset

__all__ = ["AcquisitionError", "acquire_dataset"]
