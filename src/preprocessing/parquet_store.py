"""Write one Parquet file without collecting a Spark DataFrame on the driver.

Spark calculates the rows and writes part files with ``DataFrame.write.parquet``.
Those parts stay in a temporary sibling directory until the job commits and the
published file has been checked. The previous ``data.parquet`` is replaced only
after that check. A failed write leaves the previous file in place.
"""

from __future__ import annotations

import hashlib
import os
import shutil
import subprocess
import tempfile
import uuid
from pathlib import Path

import pyarrow.parquet as pq
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampNTZType, TimestampType

from utils.config_loader import AppConfig

_FILE_NAME = "data.parquet"
_FILESYSTEM_CLASS = "aeroscope.fs.LocalFileSystemNoChmod"
_TIMESTAMP_OUTPUT = "TIMESTAMP_MICROS"
_BATCH_ROWS = 65536


def store_parquet(frame: DataFrame, output_dir: Path, config: AppConfig) -> Path:
    """Write ``output_dir/data.parquet`` and return that file."""

    from preprocessing.clean import PreprocessingError

    resolved = output_dir.resolve()
    raw_root = config.paths.raw_root.resolve()
    if resolved == raw_root or raw_root in resolved.parents:
        raise PreprocessingError(f"Refusing to write cleaned data under {raw_root}.")

    spark = frame.sparkSession
    projected = _wall_clock_timestamps(frame)
    published_name = _FILE_NAME
    final_file = resolved / _FILE_NAME
    resolved.mkdir(parents=True, exist_ok=True)
    staging = _staging_directory(resolved)
    previous_timestamp_output = _replace_timestamp_output(spark, _TIMESTAMP_OUTPUT)
    try:
        _enable_local_filesystem(spark)
        published = staging / published_name
        _write_parts(projected, staging, config.spark.parquet_compression)
        parts = _part_files(staging)
        if not (staging / "_SUCCESS").is_file():
            raise PreprocessingError(
                f"Spark did not commit the Parquet write under {staging}."
            )
        expected = int(projected.count())
        if expected == 0 and not parts:
            _write_empty(projected, published, config.spark.parquet_compression)
        else:
            if not parts:
                raise PreprocessingError(f"Spark committed no Parquet parts under {staging}.")
            written = _part_rows(parts)
            if written != expected:
                raise PreprocessingError(
                    f"Parquet parts contain {written} rows; the DataFrame has {expected}."
                )
            _publish_parts(parts, published, config.spark.parquet_compression)
        _require_readable(published, projected, expected)
        os.replace(published, final_file)
    except PreprocessingError:
        raise
    except Exception as exc:
        raise PreprocessingError(f"Could not write Parquet to {resolved}: {exc}") from exc
    finally:
        _restore_timestamp_output(spark, previous_timestamp_output)
        shutil.rmtree(staging, ignore_errors=True)
    return final_file


def _wall_clock_timestamps(frame: DataFrame) -> DataFrame:
    """Store timestamp instants as session-local wall time.

    ``timestamp_ntz`` values are already wall-clock time and are left unchanged.
    A zoned ``timestamp`` is cast to ``timestamp_ntz`` in the session timezone.
    That is the same conversion ``_naive_timestamps`` applies when Arrow exposes
    a zoned timestamp and ``local_timestamp`` removes the zone. The Parquet
    logical type is then microseconds with ``isAdjustedToUTC=false``.
    """

    projected = frame
    for field in frame.schema.fields:
        if isinstance(field.dataType, TimestampType):
            projected = projected.withColumn(field.name, F.col(field.name).cast(TimestampNTZType()))
    return projected


def _write_parts(frame: DataFrame, staging: Path, compression: str) -> None:
    """Write Spark part files. This does not collect rows into Python."""

    (
        frame.write.mode("overwrite")
        .option("compression", compression)
        .parquet(_spark_output_path(staging))
    )


def _publish_parts(parts: list[Path], destination: Path, compression: str) -> None:
    if len(parts) == 1:
        shutil.copyfile(parts[0], destination)
        return
    writer = None
    try:
        for part in parts:
            parquet_file = pq.ParquetFile(part)
            try:
                for batch in parquet_file.iter_batches(batch_size=_BATCH_ROWS):
                    if batch.num_rows == 0:
                        continue
                    if writer is None:
                        writer = pq.ParquetWriter(
                            destination,
                            batch.schema,
                            compression=compression,
                        )
                    writer.write_batch(batch)
            finally:
                parquet_file.close()
        if writer is None:
            shutil.copyfile(parts[0], destination)
    finally:
        if writer is not None:
            writer.close()


def _write_empty(frame: DataFrame, destination: Path, compression: str) -> None:
    empty = frame.sparkSession.createDataFrame([], frame.schema)
    table = _wall_clock_arrow(empty)
    pq.write_table(table, destination, compression=compression)


def _wall_clock_arrow(frame: DataFrame) -> object:
    """Arrow conversion for an empty frame, used only to keep its schema."""

    import pyarrow as pa
    import pyarrow.compute as pc

    from preprocessing.clean import _naive_timestamps

    return _naive_timestamps(frame.toArrow(), pa, pc)


def _require_readable(path: Path, frame: DataFrame, expected: int) -> None:
    from preprocessing.clean import PreprocessingError

    parquet_file = pq.ParquetFile(path)
    try:
        rows = parquet_file.metadata.num_rows
        if rows != expected:
            raise PreprocessingError(
                f"Published Parquet has {rows} rows; the DataFrame has {expected}."
            )
        schema = parquet_file.schema
        for field in frame.schema.fields:
            if not isinstance(field.dataType, (TimestampType, TimestampNTZType)):
                continue
            if field.name not in schema.names:
                raise PreprocessingError(f"Published Parquet is missing timestamp column {field.name}.")
            logical = str(schema.column(schema.names.index(field.name)).logical_type)
            if "isAdjustedToUTC=false" not in logical or "microseconds" not in logical:
                raise PreprocessingError(
                    f"Column {field.name} was stored as {logical}. "
                    "Timestamp values must stay microsecond wall-clock time."
                )
    finally:
        parquet_file.close()


def _part_files(staging: Path) -> list[Path]:
    files = [
        path
        for path in staging.rglob("*.parquet")
        if path.is_file() and path.name.startswith("part-")
    ]
    return sorted(files, key=lambda path: path.name)


def _part_rows(parts: list[Path]) -> int:
    total = 0
    for part in parts:
        parquet_file = pq.ParquetFile(part)
        try:
            total += parquet_file.metadata.num_rows
        finally:
            parquet_file.close()
    return total


def _replace_timestamp_output(spark: SparkSession, value: str) -> str | None:
    key = "spark.sql.parquet.outputTimestampType"
    try:
        previous = spark.conf.get(key)
    except Exception:
        previous = None
    spark.conf.set(key, value)
    return previous


def _restore_timestamp_output(spark: SparkSession, previous: str | None) -> None:
    key = "spark.sql.parquet.outputTimestampType"
    if previous is None:
        spark.conf.unset(key)
    else:
        spark.conf.set(key, previous)


def _staging_directory(output_dir: Path) -> Path:
    """Return a sibling directory whose path Spark can create.

    Hadoop does not decode ``%20`` in a ``file:`` URI, so a profile path such
    as ``NIDHI C A`` must be the Windows short path. The parent already exists.
    The staging directory itself is created by the Spark committer.
    """

    from preprocessing.clean import PreprocessingError
    from utils.spark_session import _windows_short_path

    parent = output_dir.parent
    parent.mkdir(parents=True, exist_ok=True)
    short_parent = Path(_windows_short_path(str(parent.resolve())))
    if os.name == "nt" and " " in str(short_parent):
        raise PreprocessingError(
            f"Spark cannot write under a path that still contains spaces: {short_parent}"
        )
    return short_parent / f"{output_dir.name}.writing-{uuid.uuid4().hex}"


def _spark_output_path(path: Path) -> str:
    text = str(path)
    if os.name == "nt" and " " in text:
        from preprocessing.clean import PreprocessingError

        raise PreprocessingError(f"Spark cannot write to a path that still contains spaces: {text}")
    return text


def _enable_local_filesystem(spark: SparkSession) -> None:
    """Load a local filesystem whose permission call does not require winutils."""

    from preprocessing.clean import PreprocessingError

    classes = _compiled_classes()
    jvm = spark._jvm
    if jvm is None:
        raise PreprocessingError("Spark's JVM is not available for the Parquet writer.")
    loader = jvm.java.lang.Thread.currentThread().getContextClassLoader()
    url = jvm.java.io.File(str(classes)).toURI().toURL()
    try:
        loader.addURL(url)
    except Exception:
        clazz = jvm.java.lang.Class.forName("java.net.URLClassLoader")
        method = clazz.getDeclaredMethod("addURL", jvm.java.net.URL)
        method.setAccessible(True)
        method.invoke(loader, url)
    configuration = spark._jsc.hadoopConfiguration()
    configuration.setClassLoader(loader)
    configuration.set("fs.file.impl", _FILESYSTEM_CLASS)
    configuration.set("fs.file.impl.disable.cache", "true")
    jvm.java.lang.Class.forName(_FILESYSTEM_CLASS, True, loader)


def _compiled_classes() -> Path:
    from preprocessing.clean import PreprocessingError

    source = Path(__file__).resolve().parent / "hadoop" / "LocalFileSystemNoChmod.java"
    if not source.is_file():
        raise PreprocessingError(f"Local filesystem source is missing: {source}")
    digest = hashlib.sha256(source.read_bytes()).hexdigest()[:16]
    destination = Path(tempfile.gettempdir()) / "aeroscope-fs" / digest
    marker = destination / "aeroscope" / "fs" / "LocalFileSystemNoChmod.class"
    if marker.is_file():
        return destination
    destination.mkdir(parents=True, exist_ok=True)
    jar = _hadoop_api_jar()
    javac = _javac()
    completed = subprocess.run(
        [str(javac), "--release", "17", "-cp", str(jar), "-d", str(destination), str(source)],
        capture_output=True,
        text=True,
        check=False,
    )
    if completed.returncode != 0 or not marker.is_file():
        detail = (completed.stderr or completed.stdout or "javac produced no class file").strip()
        raise PreprocessingError(f"Could not compile the local filesystem helper: {detail}")
    return destination


def _hadoop_api_jar() -> Path:
    from preprocessing.clean import PreprocessingError

    import pyspark

    jars = Path(pyspark.__file__).resolve().parent / "jars"
    matches = sorted(jars.glob("hadoop-client-api-*.jar"))
    if not matches:
        raise PreprocessingError(f"hadoop-client-api was not found under {jars}.")
    return matches[-1]


def _javac() -> Path:
    from preprocessing.clean import PreprocessingError

    from utils.spark_session import detect_java

    runtime = detect_java()
    if runtime.home is None:
        raise PreprocessingError("JAVA_HOME is required to compile the local filesystem helper.")
    javac = runtime.home / "bin" / ("javac.exe" if os.name == "nt" else "javac")
    if not javac.is_file():
        raise PreprocessingError(f"javac was not found at {javac}.")
    return javac
