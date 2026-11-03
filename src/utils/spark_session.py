"""Construct the single Spark session used by this project.

Callers pass an ``AppConfig`` from ``utils.config_loader``. Pipeline
modules must not call ``SparkSession.builder`` themselves. That keeps
session settings in one place and lets tests supply a session without
reading the air-quality archive.

``build_spark_session`` checks ``JAVA_HOME`` before launching a JVM.
Spark 4 runs on JDK 17 or 21. A ``java.exe`` earlier on PATH is ignored
when ``JAVA_HOME`` is set.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from utils.config_loader import AppConfig, load_config

_SUPPORTED_JAVA_MAJORS = frozenset({17, 21})
_VERSION_PATTERN = re.compile(r'version "(?P<major>\d+)(?:\.(?P<minor>\d+))?')


class SparkEnvironmentError(Exception):
    """The local JDK is missing or is not a version Spark 4 can run."""


@dataclass(frozen=True)
class JavaRuntime:
    major: int
    executable: Path
    home: Path | None


def detect_java() -> JavaRuntime:
    """Return the JDK Spark will launch.

    ``JAVA_HOME`` wins over ``PATH``. This matters on machines where an
    older ``java.exe`` is installed beside JDK 17.
    """

    home_value = os.environ.get("JAVA_HOME")
    if home_value:
        home = Path(home_value)
        executable = home / "bin" / ("java.exe" if os.name == "nt" else "java")
        if not executable.is_file():
            raise SparkEnvironmentError(
                f"JAVA_HOME does not contain a Java executable: {executable}"
            )
        return JavaRuntime(
            major=_java_major(executable),
            executable=executable,
            home=home,
        )

    raise SparkEnvironmentError(
        "JAVA_HOME is unset. Spark 4 requires JDK 17 or 21. "
        "Set JAVA_HOME to that JDK and open a new shell. "
        "The java executable on PATH is not used, because an older Java "
        "is often found there first."
    )


def assert_supported_java() -> JavaRuntime:
    runtime = detect_java()
    if runtime.major not in _SUPPORTED_JAVA_MAJORS:
        supported = ", ".join(str(major) for major in sorted(_SUPPORTED_JAVA_MAJORS))
        raise SparkEnvironmentError(
            f"PySpark 4 requires JDK {supported}. "
            f"Found Java {runtime.major} at {runtime.executable}."
        )
    return runtime


def build_spark_session(config: AppConfig | None = None, *, force_new: bool = False) -> Any:
    """Create a ``SparkSession`` from project settings.

    The first session in a process owns the JVM. ``force_new`` stops an
    existing session before building another. Driver memory is fixed when
    that JVM starts and does not change for later sessions in the process.

    PySpark is imported here so that configuration tests can import this
    module without the dependency installed. The JDK is checked first so
    a wrong Java version is reported before the JVM launch.
    """

    assert_supported_java()
    python_executable = _use_current_interpreter()
    _pin_spark_home()
    from pyspark.sql import SparkSession

    active = SparkSession.getActiveSession()
    if active is not None and force_new:
        active.stop()

    settings = config if config is not None else load_config()
    spark_settings = settings.spark
    warehouse = settings.paths.spark_warehouse_dir
    warehouse.mkdir(parents=True, exist_ok=True)

    builder = (
        SparkSession.builder.appName(spark_settings.app_name)
        .master(spark_settings.master)
        .config("spark.sql.shuffle.partitions", str(spark_settings.shuffle_partitions))
        .config("spark.driver.memory", spark_settings.driver_memory)
        .config("spark.executor.memory", spark_settings.executor_memory)
        .config("spark.sql.session.timeZone", spark_settings.timezone)
        .config("spark.sql.adaptive.enabled", _flag(spark_settings.adaptive_execution))
        .config("spark.sql.parquet.compression.codec", spark_settings.parquet_compression)
        .config("spark.sql.execution.arrow.pyspark.enabled", _flag(spark_settings.arrow_enabled))
        .config("spark.sql.catalogImplementation", spark_settings.catalog)
        .config("spark.ui.enabled", _flag(spark_settings.ui_enabled))
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.warehouse.dir", warehouse.resolve().as_uri())
        .config("spark.pyspark.python", python_executable)
        .config("spark.pyspark.driver.python", python_executable)
    )
    if spark_settings.master.startswith("local"):
        builder = builder.config("spark.driver.bindAddress", "127.0.0.1").config(
            "spark.driver.host", "127.0.0.1"
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(spark_settings.log_level)
    return spark


def verify_spark(spark: Any) -> dict[str, object]:
    """Run a SQL probe and a Python-worker probe.

    Neither probe reads project data. The worker probe matters on Windows
    with Python 3.12, where older PySpark releases crash before returning
    a result.
    """

    row = spark.sql("SELECT 1 AS spark_ok").collect()[0]
    worker_values = spark.sparkContext.parallelize([1]).map(lambda number: number + 1).collect()
    return {
        "spark_version": spark.version,
        "master": spark.sparkContext.master,
        "timezone": spark.conf.get("spark.sql.session.timeZone"),
        "sql_probe": int(row["spark_ok"]),
        "python_worker_probe": worker_values,
    }


def stop_spark(spark: Any | None = None) -> None:
    session = spark
    if session is None:
        from pyspark.sql import SparkSession

        session = SparkSession.getActiveSession()
    if session is not None:
        session.stop()


def _java_major(executable: Path) -> int:
    completed = subprocess.run(
        [str(executable), "-version"],
        capture_output=True,
        text=True,
        check=False,
    )
    version_text = f"{completed.stderr}\n{completed.stdout}"
    match = _VERSION_PATTERN.search(version_text)
    if match is None:
        raise SparkEnvironmentError(
            f"Could not read a Java version from {executable}. Output was: {version_text.strip()}"
        )
    major = int(match.group("major"))
    minor = match.group("minor")
    # Java 8 reports itself as 1.8.
    if major == 1 and minor is not None:
        return int(minor)
    return major


def _use_current_interpreter() -> str:
    """Point Spark workers at the interpreter that launched the driver.

    PySpark reads ``PYSPARK_PYTHON`` when the context starts and otherwise
    launches ``python3``. On Windows that name is often the Microsoft Store
    alias, which exits before a worker can connect. The driver can still
    look healthy because SQL runs in the JVM.

    Paths with spaces are shortened on Windows. ``spark-submit`` detects
    its home through an unquoted batch command, which splits a path such
    as ``C:\\Users\\Some Name\\project`` and treats the first token as a
    program name.
    """

    executable = _windows_short_path(sys.executable)
    os.environ["PYSPARK_PYTHON"] = executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = executable
    return executable


def _pin_spark_home() -> None:
    """Set ``SPARK_HOME`` to the pip-installed PySpark directory when unset."""

    if os.environ.get("SPARK_HOME"):
        return
    from importlib.util import find_spec

    spec = find_spec("pyspark")
    if spec is None or not spec.origin:
        return
    spark_home = Path(spec.origin).resolve().parent
    if (spark_home / "bin").is_dir() and (spark_home / "jars").is_dir():
        os.environ["SPARK_HOME"] = _windows_short_path(str(spark_home))


def _windows_short_path(path: str) -> str:
    if os.name != "nt" or " " not in path:
        return path
    import ctypes

    buffer = ctypes.create_unicode_buffer(32768)
    length = ctypes.windll.kernel32.GetShortPathNameW(path, buffer, len(buffer))
    if length == 0:
        return path
    return buffer.value


def _flag(enabled: bool) -> str:
    return "true" if enabled else "false"
