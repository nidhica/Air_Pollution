"""Time the existing pipeline stages without replacing their saved outputs.

Each stage is called with the shared ``SparkSession`` when it needs one.
Writes go to a sandbox directory. The analytical functions are not modified.
"""

from __future__ import annotations

import hashlib
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

from pyspark.sql import SparkSession

from performance.report import make_stage, machine_profile, redact, timestamp
from utils.config_loader import AppConfig, inventory_inputs

_CANONICAL_REPORTS = (
    "inspection_report.json",
    "preprocessing_report.json",
    "spike_report.json",
    "analytics_report.json",
)


class PerformanceError(Exception):
    """The performance run would replace a saved project output."""


def run_evaluation(
    config: AppConfig,
    spark: SparkSession | None,
    *,
    sandbox: Path,
    session_startup_seconds: float | None = None,
    secrets: tuple[str, ...] = (),
) -> dict[str, Any]:
    """Time acquisition, inspection, preprocessing, spike detection, and analytics."""

    sandbox_path = _require_sandbox(config, sandbox)
    secrets = _with_configured_secret(config, secrets)
    zone = ZoneInfo(config.dataset.timezone)
    before = _fingerprint_canonical(config)
    stages = [
        _acquisition(config, zone, secrets),
        _inspect(config, spark, sandbox_path, zone, secrets),
        _preprocess(config, spark, sandbox_path, zone, secrets),
        _spikes(config, spark, sandbox_path, zone, secrets),
        _analytics(config, spark, sandbox_path, zone, secrets),
    ]
    after = _fingerprint_canonical(config)
    java_description = _java_description()
    spark_version = None if spark is None else spark.version
    spark_master = None if spark is None else spark.sparkContext.master
    generated_at = timestamp(datetime.now(zone))
    from performance.report import assemble_report

    return assemble_report(
        generated_at=generated_at,
        timezone_name=config.dataset.timezone,
        machine=machine_profile(
            spark_version=spark_version,
            spark_master=spark_master,
            java_description=java_description,
        ),
        dataset=_dataset(config, stages),
        stages=stages,
        session_startup_seconds=session_startup_seconds,
        canonical_outputs_unchanged=before == after,
    )


def format_performance_report(document: dict[str, Any]) -> str:
    lines = [
        f"Performance report generated: {document['generated_at']}",
        f"Execution: {document['execution_mode']}",
        f"Canonical outputs unchanged: {document['canonical_outputs_unchanged']}",
    ]
    for stage in document["stages"]:
        seconds = stage["wall_clock_seconds"]
        seconds_text = "unavailable" if seconds is None else f"{seconds:.3f}s"
        lines.append(f"{stage['name']}: success={stage['success']} wall_clock={seconds_text}")
    return "\n".join(lines)


def _acquisition(config: AppConfig, zone: ZoneInfo, secrets: tuple[str, ...]) -> dict[str, Any]:
    from ingestion.acquire import acquire_dataset

    def call() -> Any:
        return acquire_dataset(config)

    result, success, error, started, ended, elapsed = _time_call(call, zone, secrets)
    unavailable: list[dict[str, str]] = []
    input_bytes = _directory_bytes(config.paths.raw_root) if config.paths.raw_root.exists() else None
    output_bytes = None
    detail: dict[str, Any] = {
        "input_kind": "XKDR file list",
        "output_kind": "local raw files",
        "network_download_timed": False,
    }
    if error is not None:
        detail["error"] = error
        unavailable.append({"field": "network_download_seconds", "reason": "acquisition did not complete"})
    if result is not None:
        files = [
            {"key": item.key, "status": item.status, "bytes": item.bytes_written}
            for item in result.files
        ]
        detail["files"] = files
        detail["ignored_keys"] = len(result.ignored_keys)
        downloaded = [item for item in result.files if item.status in {"downloaded", "resumed"}]
        detail["network_download_timed"] = bool(downloaded)
        if downloaded:
            output_bytes = sum(item.bytes_written for item in downloaded)
        else:
            unavailable.append(
                {
                    "field": "output_bytes",
                    "reason": "no bytes were written because the selected files were already present",
                }
            )
            unavailable.append(
                {
                    "field": "network_download_seconds",
                    "reason": (
                        "The selected files were already present, so this timing is the skip check. "
                        "The original download duration was not recorded."
                    ),
                }
            )
        success = bool(result.ok)
    return make_stage(
        name="acquisition",
        started_at=started,
        ended_at=ended,
        wall_clock_seconds=elapsed,
        success=success,
        input_rows=None,
        output_rows=None,
        input_bytes=input_bytes,
        output_bytes=output_bytes,
        unavailable=unavailable,
        detail=detail,
    )


def _inspect(
    config: AppConfig,
    spark: SparkSession | None,
    sandbox: Path,
    zone: ZoneInfo,
    secrets: tuple[str, ...],
) -> dict[str, Any]:
    from ingestion.inspect import inspect_dataset

    report_path = sandbox / "inspection_report.json"
    return _spark_stage(
        name="inspection",
        spark=spark,
        zone=zone,
        secrets=secrets,
        call=lambda: inspect_dataset(config, spark, report_path=report_path),
        input_bytes=_input_archive_bytes(config),
        extract=_inspect_counts,
        output_paths=[report_path],
        input_kind="raw measurement, station, and parameter files",
        output_kind="inspection report",
    )


def _preprocess(
    config: AppConfig,
    spark: SparkSession | None,
    sandbox: Path,
    zone: ZoneInfo,
    secrets: tuple[str, ...],
) -> dict[str, Any]:
    from preprocessing.clean import preprocess_dataset

    output_dir = sandbox / "measurements"
    report_path = sandbox / "preprocessing_report.json"
    return _spark_stage(
        name="preprocessing",
        spark=spark,
        zone=zone,
        secrets=secrets,
        call=lambda: preprocess_dataset(config, spark, report_path=report_path, output_dir=output_dir),
        input_bytes=_input_archive_bytes(config),
        extract=_preprocess_counts,
        output_paths=[output_dir, report_path],
        input_kind="raw measurement, station, and parameter files",
        output_kind="cleaned measurements Parquet",
    )


def _spikes(
    config: AppConfig,
    spark: SparkSession | None,
    sandbox: Path,
    zone: ZoneInfo,
    secrets: tuple[str, ...],
) -> dict[str, Any]:
    from analytics.spikes.detect import cleaned_measurements_file, detect_spikes

    annotated = sandbox / "spikes" / "annotated"
    spikes = sandbox / "spikes" / "spikes_only"
    report_path = sandbox / "spike_report.json"
    source = cleaned_measurements_file(config)
    return _spark_stage(
        name="spike_detection",
        spark=spark,
        zone=zone,
        secrets=secrets,
        call=lambda: detect_spikes(
            config,
            spark,
            report_path=report_path,
            annotated_dir=annotated,
            spike_dir=spikes,
        ),
        input_bytes=_file_bytes(source),
        extract=_spike_counts,
        output_paths=[annotated, spikes, report_path],
        input_kind="cleaned measurements",
        output_kind="annotated measurements and spike rows",
    )


def _analytics(
    config: AppConfig,
    spark: SparkSession | None,
    sandbox: Path,
    zone: ZoneInfo,
    secrets: tuple[str, ...],
) -> dict[str, Any]:
    from analytics.spatiotemporal.summarize import analyze_dataset, annotated_spikes_file

    output_dir = sandbox / "analytics"
    report_path = sandbox / "analytics_report.json"
    source = annotated_spikes_file(config)
    return _spark_stage(
        name="spatiotemporal_analytics",
        spark=spark,
        zone=zone,
        secrets=secrets,
        call=lambda: analyze_dataset(config, spark, report_path=report_path, output_dir=output_dir),
        input_bytes=_file_bytes(source),
        extract=_analytics_counts,
        output_paths=[output_dir, report_path],
        input_kind="annotated measurements",
        output_kind="aggregate tables",
    )


def _spark_stage(
    *,
    name: str,
    spark: SparkSession | None,
    zone: ZoneInfo,
    secrets: tuple[str, ...],
    call: Callable[[], Any],
    input_bytes: int | None,
    extract: Callable[[Any], tuple[int | None, int | None, dict[str, Any]]],
    output_paths: list[Path],
    input_kind: str,
    output_kind: str,
) -> dict[str, Any]:
    unavailable: list[dict[str, str]] = []
    if spark is None:
        return make_stage(
            name=name,
            started_at=None,
            ended_at=None,
            wall_clock_seconds=None,
            success=False,
            input_rows=None,
            output_rows=None,
            input_bytes=input_bytes,
            output_bytes=None,
            unavailable=[{"field": "wall_clock_seconds", "reason": "no shared Spark session was supplied"}],
            detail={"input_kind": input_kind, "output_kind": output_kind, "error": "Spark session is missing"},
        )
    result, success, error, started, ended, elapsed = _time_call(call, zone, secrets)
    input_rows = None
    output_rows = None
    detail: dict[str, Any] = {"input_kind": input_kind, "output_kind": output_kind}
    if result is not None:
        input_rows, output_rows, extra = extract(result)
        detail.update(extra)
    if error is not None:
        detail["error"] = error
    output_bytes = _sum_bytes(output_paths)
    return make_stage(
        name=name,
        started_at=started,
        ended_at=ended,
        wall_clock_seconds=elapsed,
        success=success,
        input_rows=input_rows,
        output_rows=output_rows,
        input_bytes=input_bytes,
        output_bytes=output_bytes,
        unavailable=unavailable,
        detail=detail,
    )


def _inspect_counts(report: Any) -> tuple[int | None, int | None, dict[str, Any]]:
    document = report.document
    return (
        _optional_int(document.get("measurement_row_count")),
        _optional_int(document.get("selected_row_count")),
        {"date_min": document.get("date_min"), "date_max": document.get("date_max"), "station_count": document.get("station_count")},
    )


def _preprocess_counts(report: Any) -> tuple[int | None, int | None, dict[str, Any]]:
    counts = report.document["counts"]
    return (
        _optional_int(counts.get("rows_in_downloaded_files")),
        _optional_int(counts.get("rows_written")),
        {"output_writer": report.document.get("output_writer")},
    )


def _spike_counts(report: Any) -> tuple[int | None, int | None, dict[str, Any]]:
    counts = report.document["counts"]
    return (
        _optional_int(counts.get("total_records")),
        _optional_int(counts.get("total_records")),
        {
            "eligible_observations": counts.get("eligible_records"),
            "candidate_spikes": counts.get("spike_records"),
            "output_writer": "pyarrow",
        },
    )


def _analytics_counts(report: Any) -> tuple[int | None, int | None, dict[str, Any]]:
    counts = report.document["counts"]
    outputs = report.document.get("outputs") or {}
    output_rows = sum(int(item["rows"]) for item in outputs.values())
    return (
        _optional_int(counts.get("total_records")),
        output_rows,
        {
            "eligible_observations": counts.get("eligible_records"),
            "candidate_spikes": counts.get("spike_records"),
            "output_writer": report.document.get("output_writer"),
            "output_tables": len(outputs),
        },
    )


def _dataset(config: AppConfig, stages: list[dict[str, Any]]) -> dict[str, Any]:
    by_name = {stage["name"]: stage for stage in stages}
    inspection = by_name["inspection"]
    preprocessing = by_name["preprocessing"]
    spikes = by_name["spike_detection"]
    unavailable: list[dict[str, str]] = []
    input_observations = inspection["input_rows"]
    if input_observations is None:
        input_observations = preprocessing["input_rows"]
    output_observations = preprocessing["output_rows"]
    candidate_spikes = spikes["detail"].get("candidate_spikes")
    eligible = spikes["detail"].get("eligible_observations")
    if input_observations is None:
        unavailable.append({"field": "input_observations", "reason": "inspection and preprocessing did not record an input row count"})
    if output_observations is None:
        unavailable.append({"field": "output_observations", "reason": "preprocessing did not record the written row count"})
    if candidate_spikes is None:
        unavailable.append({"field": "candidate_spikes", "reason": "spike detection did not record a spike count"})
    if eligible is None:
        unavailable.append({"field": "eligible_observations", "reason": "spike detection did not record an eligible count"})
    saved = _saved_counts(config)
    match = _counts_match(
        input_observations,
        output_observations,
        candidate_spikes,
        eligible,
        saved,
    )
    return {
        "date_start": None if config.dataset.date_start is None else config.dataset.date_start.isoformat(),
        "date_end": None if config.dataset.date_end is None else config.dataset.date_end.isoformat(),
        "months": _month_count(config.dataset.date_start, config.dataset.date_end),
        "measurement_files": len(inventory_inputs(config).measurement_files),
        "input_observations": input_observations,
        "output_observations": output_observations,
        "candidate_spikes": candidate_spikes,
        "eligible_observations": eligible,
        "saved_reports": saved,
        "counts_match_saved_reports": match,
        "unavailable": unavailable,
    }


def _saved_counts(config: AppConfig) -> dict[str, Any]:
    inspection = _read_json(config.paths.interim_dir / "inspection_report.json")
    preprocessing = _read_json(config.paths.interim_dir / "preprocessing_report.json")
    spikes = _read_json(config.paths.interim_dir / "spike_report.json")
    analytics = _read_json(config.paths.interim_dir / "analytics_report.json")
    return {
        "input_observations": _nested_int(inspection, "measurement_row_count"),
        "selected_observations": _nested_int(inspection, "selected_row_count"),
        "output_observations": _nested_int(preprocessing, "counts", "rows_written"),
        "candidate_spikes": _nested_int(spikes, "counts", "spike_records"),
        "eligible_observations": _nested_int(spikes, "counts", "eligible_records"),
        "analytics_observations": _nested_int(analytics, "counts", "total_records"),
        "analytics_candidate_spikes": _nested_int(analytics, "counts", "spike_records"),
        "analytics_eligible_observations": _nested_int(analytics, "counts", "eligible_records"),
    }


def _counts_match(
    input_observations: int | None,
    output_observations: int | None,
    candidate_spikes: int | None,
    eligible: int | None,
    saved: dict[str, Any],
) -> bool | None:
    pairs = [
        (input_observations, saved.get("input_observations")),
        (output_observations, saved.get("output_observations")),
        (candidate_spikes, saved.get("candidate_spikes")),
        (eligible, saved.get("eligible_observations")),
        (candidate_spikes, saved.get("analytics_candidate_spikes")),
        (eligible, saved.get("analytics_eligible_observations")),
    ]
    comparable = [(left, right) for left, right in pairs if left is not None and right is not None]
    if not comparable:
        return None
    return all(left == right for left, right in comparable)


def _time_call(
    call: Callable[[], Any],
    zone: ZoneInfo,
    secrets: tuple[str, ...],
) -> tuple[Any, bool, str | None, str, str, float]:
    started = datetime.now(zone)
    clock = time.perf_counter()
    try:
        result = call()
        error = None
        success = True
    except Exception as exc:
        result = None
        error = redact(str(exc), secrets)
        success = False
    elapsed = time.perf_counter() - clock
    ended = datetime.now(zone)
    return result, success, error, timestamp(started), timestamp(ended), elapsed


def _require_sandbox(config: AppConfig, sandbox: Path) -> Path:
    resolved = sandbox.resolve()
    protected = [
        config.paths.raw_root.resolve(),
        config.paths.processed_dir.resolve(),
        config.paths.interim_dir.resolve(),
    ]
    for item in protected:
        if resolved == item or item in resolved.parents or resolved in item.parents:
            raise PerformanceError(
                f"Refusing to write performance outputs in {item}. "
                "Choose a sandbox outside data/raw, data/interim, and data/processed."
            )
    for name in _CANONICAL_REPORTS:
        if resolved.name == name:
            raise PerformanceError(f"Refusing to replace the saved report {name}.")
    resolved.mkdir(parents=True, exist_ok=True)
    return resolved


def _fingerprint_canonical(config: AppConfig) -> list[dict[str, Any]]:
    inventory = inventory_inputs(config)
    paths = list(inventory.measurement_files)
    if inventory.stations_present:
        paths.append(config.paths.stations_file)
    if inventory.parameters_present:
        paths.append(config.paths.parameters_file)
    paths.extend(
        [
            config.paths.processed_dir / "measurements" / "data.parquet",
            config.paths.processed_dir / "spikes" / "annotated" / "data.parquet",
            config.paths.processed_dir / "spikes" / "spikes_only" / "data.parquet",
            config.paths.interim_dir / "inspection_report.json",
            config.paths.interim_dir / "preprocessing_report.json",
            config.paths.interim_dir / "spike_report.json",
            config.paths.interim_dir / "analytics_report.json",
        ]
    )
    analytics = config.paths.processed_dir / "analytics"
    if analytics.is_dir():
        paths.extend(path for path in analytics.rglob("*") if path.is_file())
    return [_fingerprint(path) for path in paths if path.is_file()]


def _fingerprint(path: Path) -> dict[str, Any]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
            size += len(chunk)
    return {"path": str(path.resolve()), "bytes": size, "sha256": digest.hexdigest()}


def _input_archive_bytes(config: AppConfig) -> int | None:
    inventory = inventory_inputs(config)
    paths = list(inventory.measurement_files)
    if inventory.stations_present:
        paths.append(config.paths.stations_file)
    if inventory.parameters_present:
        paths.append(config.paths.parameters_file)
    if not paths:
        return None
    return sum(_file_bytes(path) or 0 for path in paths)


def _sum_bytes(paths: list[Path]) -> int | None:
    present = [path for path in paths if path.exists()]
    if not present:
        return None
    total = 0
    for path in present:
        size = _file_bytes(path) if path.is_file() else _directory_bytes(path)
        if size is None:
            return None
        total += size
    return total


def _file_bytes(path: Path) -> int | None:
    if not path.is_file():
        return None
    return path.stat().st_size


def _directory_bytes(path: Path) -> int | None:
    if not path.exists():
        return None
    if path.is_file():
        return path.stat().st_size
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    import json

    loaded = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        return None
    return loaded


def _nested_int(document: dict[str, Any] | None, *keys: str) -> int | None:
    value: Any = document
    for key in keys:
        if not isinstance(value, dict) or key not in value:
            return None
        value = value[key]
    return _optional_int(value)


def _optional_int(value: object) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        return value
    return None


def _month_count(start: object, end: object) -> int | None:
    if start is None or end is None:
        return None
    return (end.year - start.year) * 12 + (end.month - start.month) + 1


def _with_configured_secret(config: AppConfig, secrets: tuple[str, ...]) -> tuple[str, ...]:
    """Add the local API key to the redaction list. The key is not stored in the report."""

    values = [item for item in secrets if item]
    try:
        from ingestion.acquire import read_api_key

        key = read_api_key(config.paths.project_root).strip()
    except Exception:
        key = ""
    if key and key not in values:
        values.append(key)
    return tuple(values)


def _java_description() -> str | None:
    try:
        from utils.spark_session import detect_java
    except Exception:
        return None
    try:
        runtime = detect_java()
    except Exception:
        return None
    return f"{runtime.major} ({runtime.executable})"
