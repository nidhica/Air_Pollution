"""Plan XKDR monthly downloads without fetching month files unless asked.

``plan_acquisition`` lists the configured window, compares it with local
files, and writes ``data/interim/xkdr_acquisition_manifest.json``. It calls
``GET /v1/files`` only when an API key is configured. It does not download a
month file.

A download runs only when ``download`` is true and the caller passed an
explicit month or month range. An existing non-empty raw file is skipped.
"""

from __future__ import annotations

import json
from calendar import monthrange
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from utils.config_loader import AppConfig

from ingestion.acquire import (
    AcquisitionError,
    BulkFile,
    Transport,
    _FILES_PATH,
    _PARAMETERS_KEY,
    _STATIONS_KEY,
    api_origin,
    download_bulk_file,
    endpoint_url,
    parse_file_list,
    parquet_file_status,
    read_api_key,
    select_bulk_files,
)
from ingestion.acquire import _get_json

_MANIFEST_NAME = "xkdr_acquisition_manifest.json"
_LOG_NAME = "xkdr_acquisition.log"
_REQUIRED_NOTE = (
    "Monthly files contain every source and pollutant. "
    "CPCB CAAQM PM2.5 and PM10 are selected when the files are prepared. "
    "US Embassy rows in the same file are not a separate download."
)


@dataclass(frozen=True)
class MonthRequest:
    year: int
    month: int


def plan_acquisition(
    config: AppConfig,
    *,
    transport: Transport | None = None,
    api_key: str | None = None,
    months: tuple[MonthRequest, ...] | None = None,
    download: bool = False,
) -> dict[str, object]:
    """Write the acquisition manifest. Download only an explicit selection."""

    window = _window_months(config)
    selected = months if months is not None else ()
    if download and not selected:
        raise AcquisitionError(
            "Refusing to download without --month or --start/--end. "
            "Run acquire-xkdr with no --download to refresh the manifest."
        )
    _reject_months_outside_window(selected, window)
    key, key_configured = _optional_key(config, api_key)
    remote: dict[tuple[int, int], BulkFile] = {}
    listing = "not_requested"
    listing_error = None
    if key_configured:
        listing = "retrieved"
        try:
            remote = _remote_months(config, transport, key)
        except AcquisitionError as exc:
            listing = "failed"
            listing_error = str(exc)
            remote = {}
    prepared = _prepared_days(config.paths.project_root)
    records = [
        _month_record(config, item, remote, listing, prepared)
        for item in window
    ]
    outside = _outside_window(config, window)
    manifest = {
        "schema_version": 1,
        "generated_at": datetime.now(ZoneInfo(config.dataset.timezone)).isoformat(timespec="seconds"),
        "mode": "download" if download else "dry_run",
        "api_key_configured": key_configured,
        "remote_listing": listing,
        "listing_error": listing_error,
        "window": {
            "start": config.dataset.date_start.isoformat() if config.dataset.date_start else None,
            "end": config.dataset.date_end.isoformat() if config.dataset.date_end else None,
        },
        "notes": [_REQUIRED_NOTE, _key_note(key_configured, listing)],
        "months": records,
        "local_files_outside_window": outside,
    }
    if download:
        manifest = _download_selected(config, manifest, selected, remote, transport, key)
    path = _write_manifest(config, manifest)
    _log(config, f"{manifest['mode']} listing={listing} manifest={path.name} months={len(records)}")
    return manifest


def parse_month_token(token: str) -> MonthRequest:
    """Parse ``YYYY-MM`` into a month request."""

    parts = token.split("-")
    if len(parts) != 2 or len(parts[0]) != 4 or len(parts[1]) != 2:
        raise AcquisitionError(f"Month must be YYYY-MM. Received {token!r}.")
    if not parts[0].isdigit() or not parts[1].isdigit():
        raise AcquisitionError(f"Month must be YYYY-MM. Received {token!r}.")
    year = int(parts[0])
    month = int(parts[1])
    if month < 1 or month > 12:
        raise AcquisitionError(f"Month must be YYYY-MM. Received {token!r}.")
    return MonthRequest(year, month)


def months_between(start: MonthRequest, end: MonthRequest) -> tuple[MonthRequest, ...]:
    """Inclusive month range. The end may be before the start; the pair is ordered."""

    cursor = start if (start.year, start.month) <= (end.year, end.month) else end
    last = end if cursor is start else start
    found: list[MonthRequest] = []
    year, month = cursor.year, cursor.month
    while (year, month) <= (last.year, last.month):
        found.append(MonthRequest(year, month))
        month += 1
        if month == 13:
            year += 1
            month = 1
    return tuple(found)


def format_plan(manifest: dict[str, object]) -> str:
    """Text summary safe to print. It does not include an API key."""

    months = manifest.get("months")
    rows = months if isinstance(months, list) else []
    remote_available = sum(1 for item in rows if isinstance(item, dict) and item.get("remote_availability") == "available")
    local = sum(1 for item in rows if isinstance(item, dict) and item.get("local_availability") == "present")
    unknown = sum(1 for item in rows if isinstance(item, dict) and item.get("remote_availability") == "unknown")
    pending = sum(1 for item in rows if isinstance(item, dict) and item.get("download_status") == "download_pending")
    remote_bytes = [
        int(item["remote_file_bytes"])
        for item in rows
        if isinstance(item, dict) and isinstance(item.get("remote_file_bytes"), int)
    ]
    lines = [
        f"Mode: {manifest.get('mode')}",
        f"API key configured: {manifest.get('api_key_configured')}",
        f"Remote listing: {manifest.get('remote_listing')}",
        f"Months in window: {len(rows)}",
        f"Already stored locally: {local}",
        f"Available remotely and not stored: {pending}",
        f"Remote file entries with a size: {remote_available}",
        f"Remote availability not checked: {unknown}",
    ]
    if remote_bytes:
        lines.append(f"Estimated download of sized remote files not stored locally: {_format_bytes(_pending_bytes(rows))}")
        lines.append(f"Largest listed remote file: {_format_bytes(max(remote_bytes))}")
    if manifest.get("listing_error"):
        lines.append(f"Listing error: {manifest['listing_error']}")
    return "\n".join(lines)


def _pending_bytes(rows: list[object]) -> int:
    total = 0
    for item in rows:
        if not isinstance(item, dict):
            continue
        if item.get("download_status") != "download_pending":
            continue
        size = item.get("remote_file_bytes")
        if isinstance(size, int):
            total += size
    return total


def _window_months(config: AppConfig) -> tuple[MonthRequest, ...]:
    start = config.dataset.date_start
    end = config.dataset.date_end
    if start is None or end is None:
        raise AcquisitionError("acquire-xkdr requires dataset.date_start and dataset.date_end.")
    return months_between(MonthRequest(start.year, start.month), MonthRequest(end.year, end.month))


def _reject_months_outside_window(selected: tuple[MonthRequest, ...], window: tuple[MonthRequest, ...]) -> None:
    allowed = {(item.year, item.month) for item in window}
    outside = [f"{item.year:04d}-{item.month:02d}" for item in selected if (item.year, item.month) not in allowed]
    if outside:
        raise AcquisitionError(
            "Requested months are outside the configured window: " + ", ".join(outside) + "."
        )


def _optional_key(config: AppConfig, api_key: str | None) -> tuple[str, bool]:
    if api_key is not None:
        return api_key, bool(api_key.strip())
    try:
        return read_api_key(config.paths.project_root), True
    except AcquisitionError:
        return "", False


def _remote_months(config: AppConfig, transport: Transport | None, api_key: str) -> dict[tuple[int, int], BulkFile]:
    origin = api_origin(config.dataset.documentation_url)
    client = transport if transport is not None else _default_transport()
    payload = _get_json(client, endpoint_url(origin, _FILES_PATH), api_key)
    files = parse_file_list(payload)
    found: dict[tuple[int, int], BulkFile] = {}
    for item in files:
        if item.year is None or item.month is None:
            continue
        found[(item.year, item.month)] = item
    return found


def _default_transport() -> Transport:
    from ingestion.acquire import UrllibTransport

    return UrllibTransport()


def _month_record(
    config: AppConfig,
    item: MonthRequest,
    remote: dict[tuple[int, int], BulkFile],
    listing: str,
    prepared: dict[tuple[int, int], int],
) -> dict[str, object]:
    path = _measurement_path(config, item)
    partial = path.with_name(path.name + ".partial")
    local_bytes = path.stat().st_size if path.is_file() else 0
    local_present = path.is_file() and local_bytes > 0
    remote_item = remote.get((item.year, item.month))
    if listing != "retrieved":
        remote_availability = "unknown"
    elif remote_item is None:
        remote_availability = "unavailable"
    else:
        remote_availability = "available"
    if local_present:
        valid, reason = parquet_file_status(path)
        validation = "validated" if valid else "invalid"
        error = None if valid else reason
        download_status = "already_downloaded"
    elif partial.is_file():
        validation = "not_checked"
        error = "A partial download is present and the final file is absent."
        download_status = "failed"
    elif remote_availability == "unavailable":
        validation = "not_checked"
        error = None
        download_status = "unavailable_remotely"
    elif remote_availability == "available":
        validation = "not_checked"
        error = None
        download_status = "download_pending"
    else:
        validation = "not_checked"
        error = None
        download_status = "not_requested"
    observed_days = prepared.get((item.year, item.month), 0)
    length = monthrange(item.year, item.month)[1]
    if observed_days >= length:
        processing = "prepared"
    elif observed_days > 0:
        processing = "partial"
    else:
        processing = "not_prepared"
    return {
        "year": item.year,
        "month": item.month,
        "remote_key": None if remote_item is None else remote_item.key,
        "remote_availability": remote_availability,
        "local_availability": "present" if local_present else "absent",
        "remote_file_bytes": None if remote_item is None else remote_item.size_bytes,
        "local_file_bytes": local_bytes,
        "remote_row_count": None if remote_item is None else remote_item.row_count,
        "download_status": download_status,
        "validation_status": validation,
        "processing_status": processing,
        "prepared_days": observed_days,
        "days_in_month": length,
        "error": error,
    }


def _download_selected(
    config: AppConfig,
    manifest: dict[str, object],
    selected: tuple[MonthRequest, ...],
    remote: dict[tuple[int, int], BulkFile],
    transport: Transport | None,
    api_key: str,
) -> dict[str, object]:
    if not api_key.strip():
        raise AcquisitionError(
            "Set AQI_API_KEY in the environment or in an untracked .env file before downloading."
        )
    wanted = {(item.year, item.month) for item in selected}
    origin = api_origin(config.dataset.documentation_url)
    client = transport if transport is not None else _default_transport()
    months = manifest["months"]
    assert isinstance(months, list)
    for record in months:
        if not isinstance(record, dict):
            continue
        key = (int(record["year"]), int(record["month"]))
        if key not in wanted:
            continue
        remote_item = remote.get(key)
        if remote_item is None:
            record["download_status"] = "unavailable_remotely" if manifest["remote_listing"] == "retrieved" else "not_requested"
            record["error"] = "The file list did not include this month."
            continue
        result = download_bulk_file(
            client,
            origin,
            api_key,
            config.paths.raw_root,
            remote_item,
            retries=3,
            validate_parquet=True,
        )
        _log(config, f"{result.status} {result.key} bytes={result.bytes_written}")
        if result.status == "skipped":
            record["download_status"] = "already_downloaded"
        elif result.status in {"downloaded", "resumed"}:
            record["download_status"] = "downloaded"
            record["local_availability"] = "present"
            record["local_file_bytes"] = result.bytes_written
            record["validation_status"] = "validated"
            record["duplicate_timestamp_keys"] = duplicate_timestamp_count(Path(result.path))
            record["error"] = None
        else:
            record["download_status"] = "failed"
            record["error"] = result.message
    return manifest


def _prepared_days(root: Path) -> dict[tuple[int, int], int]:
    path = root / "data" / "processed" / "analytics" / "daily_by_pollutant" / "data.parquet"
    if not path.is_file():
        return {}
    import pyarrow.parquet as pq

    table = pq.read_table(path, columns=["observation_date"])
    counts: dict[tuple[int, int], set[object]] = {}
    for value in table.column("observation_date").to_pylist():
        if value is None:
            continue
        stamp = value if hasattr(value, "year") else None
        if stamp is None:
            continue
        counts.setdefault((stamp.year, stamp.month), set()).add(stamp)
    return {key: len(days) for key, days in counts.items()}


def _measurement_path(config: AppConfig, item: MonthRequest) -> Path:
    return (
        config.paths.raw_root
        / "v1"
        / "measurements"
        / f"year={item.year}"
        / f"month={item.month:02d}"
        / "data.parquet"
    )


def _outside_window(config: AppConfig, window: tuple[MonthRequest, ...]) -> list[dict[str, object]]:
    allowed = {(item.year, item.month) for item in window}
    base = config.paths.raw_root / "v1" / "measurements"
    found: list[dict[str, object]] = []
    if not base.is_dir():
        return found
    for path in sorted(base.glob("year=*/month=*/data.parquet")):
        year = int(path.parents[1].name.split("=", 1)[1])
        month = int(path.parents[0].name.split("=", 1)[1])
        if (year, month) in allowed:
            continue
        found.append(
            {
                "year": year,
                "month": month,
                "local_file_bytes": path.stat().st_size,
                "download_status": "already_downloaded",
                "note": "Present locally and outside the configured acquisition window. Not selected.",
            }
        )
    return found


def _write_manifest(config: AppConfig, manifest: dict[str, object]) -> Path:
    directory = config.paths.project_root / "data" / "interim"
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / _MANIFEST_NAME
    temporary = path.with_suffix(".json.partial")
    temporary.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    temporary.replace(path)
    return path


def _log(config: AppConfig, message: str) -> None:
    directory = config.paths.project_root / "logs"
    directory.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    with (directory / _LOG_NAME).open("a", encoding="utf-8") as handle:
        handle.write(f"{stamp} {message}\n")


def _key_note(configured: bool, listing: str) -> str:
    if not configured:
        return (
            "AQI_API_KEY is not configured. Remote availability was not requested. "
            "Add the key to an untracked .env file, then run acquire-xkdr again with no --download."
        )
    if listing == "retrieved":
        return "Remote availability comes from GET /v1/files. A month is unavailable only when that list omits it."
    return "The file list could not be retrieved. Missing local months stay unknown, not unavailable."


def _format_bytes(size: int) -> str:
    return f"{size:,} bytes"


def duplicate_timestamp_count(path: Path) -> int:
    """Count extra rows that repeat station, pollutant, and timestamp.

    The raw file is not changed. A duplicate is reported, not deleted.
    """

    import pyarrow.parquet as pq

    table = pq.read_table(path, columns=["station_id", "parameter_name", "collected_at"])
    frame = table.to_pandas()
    return int(frame.duplicated(["station_id", "parameter_name", "collected_at"]).sum())


def metadata_keys() -> tuple[str, str]:
    """Station and parameter keys that accompany a month download."""

    return _STATIONS_KEY, _PARAMETERS_KEY


def listed_measurement_keys(payload: object, selected: tuple[MonthRequest, ...]) -> tuple[str, ...]:
    """Measurement keys for an explicit selection, ignoring other archive objects."""

    files = parse_file_list(payload)
    wanted = {(item.year, item.month) for item in selected}
    plan = select_bulk_files(files, date_start=None, date_end=None)
    return tuple(
        item.key
        for item in plan.selected
        if item.year is not None and (item.year, item.month) in wanted
    )
