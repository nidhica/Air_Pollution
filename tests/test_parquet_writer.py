"""Parquet writer tests.

Spark fixtures are temporary and are not XKDR measurements. The writer must
keep an existing output file when a later write fails.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest
from pyspark.sql import functions as F

from preprocessing.clean import PreprocessingError, _naive_timestamps, _write_parquet
from preprocessing.parquet_store import _publish_parts
from tests.config_factory import make_config
from utils.config_loader import load_config
from utils.spark_session import build_spark_session, stop_spark


@pytest.fixture(scope="module")
def spark():
    base = load_config()
    tuned = replace(base, spark=replace(base.spark, ui_enabled=False, log_level="ERROR"))
    session = build_spark_session(tuned, force_new=True)
    yield session
    stop_spark(session)


def test_naive_timestamps_keep_local_wall_time() -> None:
    # assume_timezone treats 00:30 as Asia/Kolkata wall time. A tz-aware array
    # built directly from a naive datetime stores that clock time as UTC.
    zoned = pc.assume_timezone(
        pa.array([datetime(2024, 2, 1, 0, 30)], type=pa.timestamp("us")),
        "Asia/Kolkata",
    )
    naive = pa.array([datetime(2024, 2, 1, 1, 15)], type=pa.timestamp("us"))
    converted = _naive_timestamps(pa.table({"zoned_at": zoned, "collected_at": naive}), pa, pc)

    assert converted.schema.field("zoned_at").type == pa.timestamp("us")
    assert converted.schema.field("zoned_at").type.tz is None
    assert converted.schema.field("collected_at").type == pa.timestamp("us")
    assert converted.column("zoned_at")[0].as_py() == pc.local_timestamp(zoned)[0].as_py()
    assert converted.column("collected_at")[0].as_py() == datetime(2024, 2, 1, 1, 15)


@pytest.mark.spark
def test_schema_and_wall_clock_timestamps_round_trip(tmp_path: Path, spark) -> None:
    frame = _sample_frame(spark)
    directory = tmp_path / "measurements"
    directory.mkdir()
    (directory / "data.parquet").write_bytes(b"previous")

    written = _write_parquet(frame, directory, make_config(tmp_path))

    assert written == directory / "data.parquet"
    assert written.is_file()
    assert not list(directory.parent.glob("*.writing-*"))
    stored = pq.ParquetFile(written)
    try:
        assert stored.metadata.num_rows == 2
        assert _logical(stored, "collected_at") == "wall-clock-micros"
        assert _logical(stored, "zoned_at") == "wall-clock-micros"
        assert stored.schema_arrow.field("observation_date").type == pa.date32()
        assert stored.schema_arrow.field("value").type == pa.float64()
        assert stored.schema_arrow.field("year").type == pa.int32()
        assert stored.schema_arrow.field("flag").type == pa.bool_()
        rows = stored.read().to_pylist()
    finally:
        stored.close()

    by_station = {row["station_id"]: row for row in rows}
    assert by_station["s1"]["collected_at"] == datetime(2024, 2, 1, 0, 30)
    assert by_station["s1"]["zoned_at"] == datetime(2024, 2, 1, 0, 30)
    assert by_station["s1"]["missing_at"] is None
    assert by_station["s1"]["value"] == 12.5
    assert by_station["s2"]["collected_at"] == datetime(2024, 2, 1, 1, 15)

    read_back = spark.read.parquet(str(written)).where(F.col("station_id") == "s1").collect()
    assert len(read_back) == 1
    assert read_back[0]["collected_at"] == datetime(2024, 2, 1, 0, 30)
    assert read_back[0]["zoned_at"] == datetime(2024, 2, 1, 0, 30)
    assert str(read_back[0]["zoned_at"])[:19] == "2024-02-01 00:30:00"


@pytest.mark.spark
def test_failed_write_keeps_the_previous_file(tmp_path: Path, spark, monkeypatch: pytest.MonkeyPatch) -> None:
    directory = tmp_path / "annotated"
    directory.mkdir()
    previous = directory / "data.parquet"
    previous.write_bytes(b"keep-this-file")
    frame = _sample_frame(spark)

    def fail_before_publish(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("commit failed")

    monkeypatch.setattr("preprocessing.parquet_store._publish_parts", fail_before_publish)
    with pytest.raises(PreprocessingError, match="commit failed"):
        _write_parquet(frame, directory, make_config(tmp_path))

    assert previous.read_bytes() == b"keep-this-file"
    assert not list(directory.parent.glob("*.writing-*"))
    assert list(directory.iterdir()) == [previous]


@pytest.mark.spark
def test_spark_failure_keeps_the_previous_file(tmp_path: Path, spark, monkeypatch: pytest.MonkeyPatch) -> None:
    directory = tmp_path / "spikes_only"
    directory.mkdir()
    previous = directory / "data.parquet"
    previous.write_bytes(b"still-here")

    def fail_write(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("executor lost")

    monkeypatch.setattr("preprocessing.parquet_store._write_parts", fail_write)
    with pytest.raises(PreprocessingError, match="Could not write Parquet"):
        _write_parquet(_sample_frame(spark), directory, make_config(tmp_path))

    assert previous.read_bytes() == b"still-here"
    assert not list(directory.parent.glob("*.writing-*"))


@pytest.mark.spark
def test_repartitioned_frame_uses_the_spark_writer(tmp_path: Path, spark) -> None:
    rows = 100_000
    frame = (
        spark.range(rows)
        .select(
            F.col("id"),
            F.lit("2024-02-01 00:30:00").cast("timestamp_ntz").alias("collected_at"),
            (F.col("id") % 10).cast("double").alias("value"),
        )
        .repartition(4)
    )
    directory = tmp_path / "scaled"
    written = _write_parquet(frame, directory, make_config(tmp_path))

    stored = pq.ParquetFile(written)
    try:
        assert stored.metadata.num_rows == rows
        assert _logical(stored, "collected_at") == "wall-clock-micros"
        sample = stored.read_row_group(0).slice(0, 1).to_pylist()[0]
    finally:
        stored.close()
    assert sample["collected_at"] == datetime(2024, 2, 1, 0, 30)
    assert spark.read.parquet(str(written)).count() == rows
    assert written.is_file()
    assert not list(directory.parent.glob("*.writing-*"))


def test_stitch_keeps_timestamp_values_and_schema(tmp_path: Path) -> None:
    first = tmp_path / "part-00000.parquet"
    second = tmp_path / "part-00001.parquet"
    column_type = pa.timestamp("us")
    pq.write_table(
        pa.table({"collected_at": pa.array([datetime(2024, 2, 1, 0, 30)], type=column_type), "value": [1.0]}),
        first,
    )
    pq.write_table(
        pa.table({"collected_at": pa.array([datetime(2024, 2, 1, 1, 15)], type=column_type), "value": [2.0]}),
        second,
    )
    destination = tmp_path / "data.parquet"
    _publish_parts([first, second], destination, "snappy")

    stored = pq.ParquetFile(destination)
    try:
        rows = stored.read().to_pylist()
        assert _logical(stored, "collected_at") == "wall-clock-micros"
    finally:
        stored.close()
    assert rows == [
        {"collected_at": datetime(2024, 2, 1, 0, 30), "value": 1.0},
        {"collected_at": datetime(2024, 2, 1, 1, 15), "value": 2.0},
    ]


def _sample_frame(spark):
    # SQL literals stay in the JVM. A local Python collection would start a worker.
    return spark.sql(
        """
        SELECT *
        FROM VALUES
          ('s1', 2024, 2, CAST(12.5 AS DOUBLE), true, DATE '2024-02-01',
           TIMESTAMP_NTZ '2024-02-01 00:30:00', TIMESTAMP '2024-02-01 00:30:00',
           CAST(NULL AS TIMESTAMP_NTZ)),
          ('s2', 2024, 2, CAST(3.0 AS DOUBLE), false, DATE '2024-02-01',
           TIMESTAMP_NTZ '2024-02-01 01:15:00', TIMESTAMP '2024-02-01 01:15:00',
           CAST(NULL AS TIMESTAMP_NTZ))
        AS sample(
          station_id, year, month, value, flag, observation_date,
          collected_at, zoned_at, missing_at
        )
        """
    )


def _logical(stored: pq.ParquetFile, name: str) -> str:
    schema = stored.schema
    logical = str(schema.column(schema.names.index(name)).logical_type)
    if "isAdjustedToUTC=false" in logical and "microseconds" in logical:
        return "wall-clock-micros"
    return logical
