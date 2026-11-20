"""Assemble a performance report from measured stage results.

This module does not run the pipeline and does not read environment variables.
A missing measurement stays null and is listed under ``unavailable``.
"""

from __future__ import annotations

import json
import os
import platform
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

LIMITATIONS = (
    "The timed stages used one local Spark session on this machine. This is not a distributed cluster benchmark.",
    "No second hardware configuration or cluster size was timed, so no speedup is reported.",
    "Cleaned, spike, and analytics outputs are one Parquet file per table. Spark writes that file through its Parquet writer; the previous file is replaced only after the write commits.",
    "The stages ran one after another in the same process. A later stage may benefit from data that the operating system still has cached.",
    "These measurements describe pipeline execution. They are not prediction-accuracy scores and are not compared with machine-learning metrics from a research paper.",
)


def rows_per_second(rows: int | None, seconds: float | None) -> tuple[float | None, str | None]:
    """Return throughput and, when it cannot be computed, the reason."""

    if rows is None:
        return None, "the row count was not recorded"
    if seconds is None:
        return None, "the wall-clock time was not recorded"
    if seconds <= 0:
        return None, "the wall-clock time was not positive"
    return rows / seconds, None


def make_stage(
    *,
    name: str,
    started_at: str | None,
    ended_at: str | None,
    wall_clock_seconds: float | None,
    success: bool | None,
    input_rows: int | None,
    output_rows: int | None,
    input_bytes: int | None,
    output_bytes: int | None,
    unavailable: list[dict[str, str]] | None = None,
    detail: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build one stage record. Throughput is omitted when an input is missing."""

    reasons = list(unavailable or [])
    input_rate, input_reason = rows_per_second(input_rows, wall_clock_seconds)
    output_rate, output_reason = rows_per_second(output_rows, wall_clock_seconds)
    if input_reason is not None:
        reasons.append({"field": "input_rows_per_second", "reason": input_reason})
    if output_reason is not None:
        reasons.append({"field": "output_rows_per_second", "reason": output_reason})
    return {
        "name": name,
        "started_at": started_at,
        "ended_at": ended_at,
        "wall_clock_seconds": wall_clock_seconds,
        "success": success,
        "input_rows": input_rows,
        "output_rows": output_rows,
        "input_bytes": input_bytes,
        "output_bytes": output_bytes,
        "input_rows_per_second": input_rate,
        "output_rows_per_second": output_rate,
        "unavailable": reasons,
        "detail": detail or {},
    }


def redact(text: str, secrets: tuple[str, ...] = ()) -> str:
    """Remove configured secret values from a message before it is stored."""

    redacted = text
    for secret in secrets:
        if secret:
            redacted = redacted.replace(secret, "[redacted]")
    return redacted


def machine_profile(*, spark_version: str | None, spark_master: str | None, java_description: str | None) -> dict[str, Any]:
    """Record runtime facts supplied by the caller or the local platform."""

    unavailable: list[dict[str, str]] = []
    cpu_name = platform.processor() or None
    if cpu_name is None:
        unavailable.append({"field": "cpu_name", "reason": "platform.processor() returned an empty value"})
    ram_bytes = _ram_bytes()
    if ram_bytes is None:
        unavailable.append({"field": "ram_bytes", "reason": "the operating system did not return a physical memory total"})
    try:
        import pyspark

        pyspark_version = pyspark.__version__
    except Exception as exc:  # pragma: no cover - pyspark is installed in this project
        pyspark_version = None
        unavailable.append({"field": "pyspark", "reason": str(exc)})
    if java_description is None:
        unavailable.append({"field": "java", "reason": "Java was not detected from JAVA_HOME"})
    if spark_version is None:
        unavailable.append({"field": "spark", "reason": "no Spark session was supplied"})
    if spark_master is None:
        unavailable.append({"field": "spark_master", "reason": "no Spark session was supplied"})
    return {
        "execution": "single local machine",
        "distributed_cluster": False,
        "speedup_measured": False,
        "cpu_count": os.cpu_count(),
        "cpu_name": cpu_name,
        "ram_bytes": ram_bytes,
        "os": platform.platform(),
        "python": sys.version.split()[0],
        "java": java_description,
        "pyspark": pyspark_version,
        "spark": spark_version,
        "spark_master": spark_master,
        "unavailable": unavailable,
    }


def assemble_report(
    *,
    generated_at: str,
    timezone_name: str,
    machine: dict[str, Any],
    dataset: dict[str, Any],
    stages: list[dict[str, Any]],
    session_startup_seconds: float | None,
    canonical_outputs_unchanged: bool | None,
    limitations: tuple[str, ...] = LIMITATIONS,
) -> dict[str, Any]:
    """Combine measured stages into the JSON document."""

    startup_unavailable = []
    if session_startup_seconds is None:
        startup_unavailable.append(
            {"field": "spark_session_startup_seconds", "reason": "session startup was not timed"}
        )
    return {
        "schema_version": 1,
        "generated_at": generated_at,
        "timezone": timezone_name,
        "execution_mode": "local single machine",
        "distributed_cluster": False,
        "speedup_measured": False,
        "spark_session_startup_seconds": session_startup_seconds,
        "canonical_outputs_unchanged": canonical_outputs_unchanged,
        "machine": machine,
        "dataset": dataset,
        "stages": stages,
        "limitations": list(limitations),
        "unavailable": startup_unavailable,
    }


def render_markdown(document: dict[str, Any]) -> str:
    """Render the JSON document as a readable report. It does not add measurements."""

    dataset = document["dataset"]
    lines = [
        "# Performance evaluation",
        "",
        f"Generated: {document['generated_at']} ({document['timezone']})",
        "",
        "This report records one local run. It is not a distributed cluster benchmark, and it does not report a speedup.",
        "",
        "## Machine",
        "",
        _machine_table(document["machine"]),
        "",
        "## Dataset",
        "",
        f"- Date range: {_text(dataset.get('date_start'))} to {_text(dataset.get('date_end'))}",
        f"- Months: {_text(dataset.get('months'))}",
        f"- Measurement files: {_text(dataset.get('measurement_files'))}",
        f"- Input observations: {_text(dataset.get('input_observations'))}",
        f"- Output observations: {_text(dataset.get('output_observations'))}",
        f"- Candidate spikes: {_text(dataset.get('candidate_spikes'))}",
        f"- Eligible observations: {_text(dataset.get('eligible_observations'))}",
        "",
        _saved_comparison(dataset),
        "## Stages",
        "",
        "| Stage | Success | Wall-clock seconds | Input rows | Output rows | Input bytes | Output bytes | Input rows/s | Output rows/s |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for stage in document["stages"]:
        lines.append(
            "| {name} | {success} | {seconds} | {input_rows} | {output_rows} | {input_bytes} | {output_bytes} | {input_rate} | {output_rate} |".format(
                name=stage["name"],
                success=_text(stage["success"]),
                seconds=_seconds(stage["wall_clock_seconds"]),
                input_rows=_text(stage["input_rows"]),
                output_rows=_text(stage["output_rows"]),
                input_bytes=_text(stage["input_bytes"]),
                output_bytes=_text(stage["output_bytes"]),
                input_rate=_rate(stage["input_rows_per_second"]),
                output_rate=_rate(stage["output_rows_per_second"]),
            )
        )
    lines.extend(["", "Spark session startup seconds: " + _seconds(document["spark_session_startup_seconds"]), ""])
    lines.append("Canonical raw, interim, processed, and analytics files unchanged: " + _text(document["canonical_outputs_unchanged"]))
    lines.extend(["", "## Unavailable measurements", ""])
    unavailable_rows = _all_unavailable(document)
    if not unavailable_rows:
        lines.append("Every requested stage field was recorded.")
    else:
        for item in unavailable_rows:
            lines.append(f"- {item}")
    lines.extend(["", "## Stage notes", ""])
    for stage in document["stages"]:
        lines.append(f"### {stage['name']}")
        lines.append("")
        lines.append(f"- Started: {_text(stage['started_at'])}")
        lines.append(f"- Ended: {_text(stage['ended_at'])}")
        detail = stage.get("detail") or {}
        for key, value in detail.items():
            if key == "files":
                continue
            lines.append(f"- {key}: {_text(value)}")
        files = detail.get("files")
        if isinstance(files, list) and files:
            lines.append("- Files:")
            for item in files:
                lines.append(
                    f"  - {item.get('key')}: {item.get('status')}, {_text(item.get('bytes'))} bytes"
                )
        lines.append("")
    lines.extend(["## Limitations", ""])
    for item in document["limitations"]:
        lines.append(f"- {item}")
    lines.append("")
    return "\n".join(lines)


def write_report(document: dict[str, Any], json_path: Path, markdown_path: Path) -> None:
    json_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    _replace(json_path, json.dumps(document, indent=2, ensure_ascii=False) + "\n")
    _replace(markdown_path, render_markdown(document))


def _replace(path: Path, text: str) -> None:
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(text, encoding="utf-8")
    temporary.replace(path)


def _machine_table(machine: dict[str, Any]) -> str:
    rows = [
        ("Execution", machine.get("execution")),
        ("CPU", machine.get("cpu_name")),
        ("CPU count", machine.get("cpu_count")),
        ("RAM bytes", machine.get("ram_bytes")),
        ("OS", machine.get("os")),
        ("Python", machine.get("python")),
        ("Java", machine.get("java")),
        ("PySpark", machine.get("pyspark")),
        ("Spark", machine.get("spark")),
        ("Spark master", machine.get("spark_master")),
        ("Distributed cluster", machine.get("distributed_cluster")),
        ("Speedup measured", machine.get("speedup_measured")),
    ]
    lines = ["| Item | Value |", "| --- | --- |"]
    for label, value in rows:
        lines.append(f"| {label} | {_text(value)} |")
    return "\n".join(lines)


def _saved_comparison(dataset: dict[str, Any]) -> str:
    saved = dataset.get("saved_reports")
    if not isinstance(saved, dict):
        return "Saved-report comparison: unavailable\n"
    lines = [
        "Saved reports already stored these counts before this timing run:",
        "",
        f"- Saved input observations: {_text(saved.get('input_observations'))}",
        f"- Saved output observations: {_text(saved.get('output_observations'))}",
        f"- Saved candidate spikes: {_text(saved.get('candidate_spikes'))}",
        f"- Saved eligible observations: {_text(saved.get('eligible_observations'))}",
        f"- Timed counts match saved reports: {_text(dataset.get('counts_match_saved_reports'))}",
        "",
    ]
    return "\n".join(lines)


def _all_unavailable(document: dict[str, Any]) -> list[str]:
    rows: list[str] = []
    for item in document.get("unavailable") or []:
        rows.append(f"{item['field']}: {item['reason']}")
    for group in (document.get("machine") or {}).get("unavailable") or []:
        rows.append(f"machine.{group['field']}: {group['reason']}")
    for stage in document["stages"]:
        for item in stage.get("unavailable") or []:
            rows.append(f"{stage['name']}.{item['field']}: {item['reason']}")
    for item in (document.get("dataset") or {}).get("unavailable") or []:
        rows.append(f"dataset.{item['field']}: {item['reason']}")
    return rows


def _text(value: object) -> str:
    if value is None:
        return "unavailable"
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, float):
        return f"{value:.3f}"
    return str(value)


def _seconds(value: object) -> str:
    if value is None:
        return "unavailable"
    return f"{float(value):.3f}"


def _rate(value: object) -> str:
    if value is None:
        return "unavailable"
    return f"{float(value):,.1f}"


def _ram_bytes() -> int | None:
    import ctypes

    if os.name == "nt":
        class MemoryStatus(ctypes.Structure):
            _fields_ = [
                ("dwLength", ctypes.c_ulong),
                ("dwMemoryLoad", ctypes.c_ulong),
                ("ullTotalPhys", ctypes.c_ulonglong),
                ("ullAvailPhys", ctypes.c_ulonglong),
                ("ullTotalPageFile", ctypes.c_ulonglong),
                ("ullAvailPageFile", ctypes.c_ulonglong),
                ("ullTotalVirtual", ctypes.c_ulonglong),
                ("ullAvailVirtual", ctypes.c_ulonglong),
                ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
            ]

        status = MemoryStatus()
        status.dwLength = ctypes.sizeof(MemoryStatus)
        if not ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status)):
            return None
        total = int(status.ullTotalPhys)
        return total or None
    try:
        page_size = os.sysconf("SC_PAGE_SIZE")
        pages = os.sysconf("SC_PHYS_PAGES")
    except (AttributeError, ValueError, OSError):
        return None
    if not isinstance(page_size, int) or not isinstance(pages, int):
        return None
    return page_size * pages


def timestamp(moment: datetime) -> str:
    return moment.isoformat(timespec="seconds")
