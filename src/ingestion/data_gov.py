"""Download current data.gov.in air-quality snapshots without touching XKDR files.

The resource is a current table, not a historical archive. Each call stores
the raw JSON pages and appends station-pollutant-timestamp rows that are
not already stored. The API key is read from ``DATA_GOV_IN_API_KEY`` or
from an untracked ``.env`` file and is never written to a report or a file.
"""

from __future__ import annotations

import json
import os
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from utils.config_loader import AppConfig

RESOURCE_ID = "3b01bcb8-0b14-4abf-b6f2-c1bfd384ba69"
ENDPOINT = f"https://api.data.gov.in/resource/{RESOURCE_ID}"
_USER_AGENT = "AirPollutionBDA/1.0 (academic research)"
_PAGE_LIMIT = 1000
_MAX_PAGES = 10000
_TIMEOUT_SECONDS = 30.0
_RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})
_IDENTITY = ("state", "city", "station", "pollutant_id", "last_update")
_COLUMNS = (
    "country",
    "state",
    "city",
    "station",
    "last_update",
    "latitude",
    "longitude",
    "pollutant_id",
    "min_value",
    "max_value",
    "avg_value",
    "retrieved_at",
)
_VALUE_ALIASES = {
    "min_value": ("min_value", "pollutant_min"),
    "max_value": ("max_value", "pollutant_max"),
    "avg_value": ("avg_value", "pollutant_avg"),
}


class DataGovError(Exception):
    """A data.gov.in snapshot could not be fetched or stored safely."""


class ResponseStream(Protocol):
    status: int
    headers: Mapping[str, str]
    body: bytes


class Transport(Protocol):
    def send(self, url: str, headers: Mapping[str, str], timeout: float) -> ResponseStream:
        """Perform one GET. Implementations must not log ``url`` or ``headers``."""


@dataclass(frozen=True)
class _UrllibResponse:
    status: int
    headers: Mapping[str, str]
    body: bytes


class UrllibTransport:
    """Stdlib client. The request URL is not included in raised messages."""

    def send(self, url: str, headers: Mapping[str, str], timeout: float) -> ResponseStream:
        request = Request(url, headers=dict(headers), method="GET")
        try:
            with urlopen(request, timeout=timeout) as response:
                return _UrllibResponse(response.status, dict(response.headers.items()), response.read())
        except HTTPError as exc:
            body = exc.read()
            return _UrllibResponse(exc.code, dict(exc.headers.items()) if exc.headers else {}, body)
        except (URLError, TimeoutError, OSError) as exc:
            raise DataGovError(
                "The data.gov.in request did not complete. Check the network and try again."
            ) from exc


@dataclass(frozen=True)
class IngestReport:
    retrieved_at: str
    endpoint: str
    pages: int
    records_received: int
    records_new: int
    records_already_stored: int
    records_stored: int
    latest_last_update: str | None
    raw_dir: str
    normalized_file: str

    @property
    def document(self) -> dict[str, object]:
        return {
            "schema_version": 1,
            "source": "data.gov.in",
            "resource_id": RESOURCE_ID,
            "endpoint": self.endpoint,
            "retrieved_at": self.retrieved_at,
            "pages": self.pages,
            "records_received": self.records_received,
            "records_new": self.records_new,
            "records_already_stored": self.records_already_stored,
            "records_stored": self.records_stored,
            "latest_last_update": self.latest_last_update,
            "raw_dir": self.raw_dir,
            "normalized_file": self.normalized_file,
            "duplicate_key": list(_IDENTITY),
            "api_key_stored": False,
            "xkdr_files_modified": False,
            "note": (
                "Each snapshot is the current data.gov.in table. "
                "A repeated station, pollutant, and last_update is not stored again. "
                "A newly observed hour is appended."
            ),
        }


def read_data_gov_api_key(project_root: Path) -> str:
    """Return ``DATA_GOV_IN_API_KEY`` from the environment or ``project_root/.env``."""

    from_env = os.environ.get("DATA_GOV_IN_API_KEY", "").strip()
    if from_env:
        return from_env
    env_file = project_root / ".env"
    if env_file.is_file():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            name, raw_value = stripped.split("=", 1)
            if name.strip() == "DATA_GOV_IN_API_KEY":
                value = raw_value.strip().strip('"').strip("'")
                if value:
                    return value
    raise DataGovError(
        "DATA_GOV_IN_API_KEY is not set. Add it to the environment or to the untracked .env file. "
        "The key is not read from source code."
    )


def raw_snapshot_root(config: AppConfig) -> Path:
    return config.paths.raw_root / "data_gov_in"


def normalized_records_file(config: AppConfig) -> Path:
    return config.paths.interim_dir / "data_gov_in" / "records" / "data.parquet"


def ingest_report_file(config: AppConfig) -> Path:
    return config.paths.interim_dir / "data_gov_in" / "ingest_report.json"


def fetch_snapshot(
    config: AppConfig,
    *,
    transport: Transport | None = None,
    api_key: str | None = None,
    page_limit: int = _PAGE_LIMIT,
    pause_seconds: float = 0.0,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], datetime] | None = None,
) -> IngestReport:
    """Fetch the current resource and append rows whose identity is new."""

    if page_limit < 1:
        raise DataGovError("page_limit must be at least 1.")
    key = api_key if api_key is not None else read_data_gov_api_key(config.paths.project_root)
    if not key.strip():
        raise DataGovError(
            "DATA_GOV_IN_API_KEY is not set. Add it to the environment or to the untracked .env file."
        )
    client = transport if transport is not None else UrllibTransport()
    moment = (clock or _utc_now)()
    retrieved_at = moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    stamp = moment.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    snapshot_dir = raw_snapshot_root(config) / "snapshots" / stamp
    suffix = 1
    while snapshot_dir.exists():
        snapshot_dir = raw_snapshot_root(config) / "snapshots" / f"{stamp}-{suffix}"
        suffix += 1
    _refuse_xkdr_path(config, snapshot_dir)
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    _write_json(
        snapshot_dir / "request.json",
        {
            "endpoint": ENDPOINT,
            "format": "json",
            "offset": 0,
            "limit": page_limit,
            "retrieved_at": retrieved_at,
            "api_key_included": False,
        },
    )

    received: list[dict[str, str | None]] = []
    offset = 0
    total: int | None = None
    page_number = 0
    while page_number < _MAX_PAGES:
        payload = _get_page(client, key, offset, page_limit, sleep)
        records = payload.get("records")
        if not isinstance(records, list):
            raise DataGovError("data.gov.in returned a page without a records list.")
        page_total = _as_int(payload.get("total"), "total")
        count = _as_int(payload.get("count"), "count") if payload.get("count") is not None else len(records)
        if total is None:
            total = page_total
        _write_json(snapshot_dir / f"page-{page_number:05d}.json", _redact(payload, key))
        for item in records:
            if not isinstance(item, dict):
                raise DataGovError("data.gov.in returned a record that is not an object.")
            received.append(_normalize_record(item, retrieved_at))
        page_number += 1
        step = count if count > 0 else len(records)
        if step <= 0 or (total is not None and offset + step >= total):
            break
        offset += step
        if pause_seconds > 0:
            sleep(pause_seconds)
    else:
        raise DataGovError("Stopped paging because the data.gov.in response did not reach its reported total.")

    new_rows, already_stored = _append_new_records(config, received)
    latest = _latest_update(received)
    report = IngestReport(
        retrieved_at=retrieved_at,
        endpoint=ENDPOINT,
        pages=page_number,
        records_received=len(received),
        records_new=new_rows,
        records_already_stored=already_stored,
        records_stored=_stored_count(config),
        latest_last_update=latest,
        raw_dir=str(snapshot_dir),
        normalized_file=str(normalized_records_file(config)),
    )
    destination = ingest_report_file(config)
    _refuse_xkdr_path(config, destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    _write_json(destination, report.document)
    return report


def format_ingest_report(report: IngestReport) -> str:
    latest = report.latest_last_update if report.latest_last_update else "none"
    return (
        f"data.gov.in snapshot: {report.retrieved_at}\n"
        f"Endpoint: {report.endpoint}\n"
        f"Pages: {report.pages}\n"
        f"Records received: {report.records_received}\n"
        f"New records stored: {report.records_new}\n"
        f"Already stored: {report.records_already_stored}\n"
        f"Records now stored: {report.records_stored}\n"
        f"Latest API last_update: {latest}\n"
        f"Raw pages: {report.raw_dir}\n"
        f"Normalized records: {report.normalized_file}"
    )


def _get_page(
    transport: Transport,
    api_key: str,
    offset: int,
    page_limit: int,
    sleep: Callable[[float], None],
) -> dict[str, object]:
    query = urlencode(
        {
            "api-key": api_key,
            "format": "json",
            "offset": str(offset),
            "limit": str(page_limit),
        }
    )
    url = f"{ENDPOINT}?{query}"
    headers = {"Accept": "application/json", "User-Agent": _USER_AGENT}
    delay = 0.5
    last_message = "data.gov.in did not return a page."
    for attempt in range(3):
        try:
            response = transport.send(url, headers, _TIMEOUT_SECONDS)
        except DataGovError as exc:
            last_message = _redact_text(str(exc), api_key)
            if attempt == 2:
                raise DataGovError(last_message) from None
            sleep(delay)
            delay *= 2
            continue
        text = _redact_text(response.body.decode("utf-8", errors="replace"), api_key)
        if response.status in _RETRY_STATUSES:
            last_message = f"data.gov.in returned HTTP {response.status}. {_snippet(text)}"
            if attempt == 2:
                raise DataGovError(last_message)
            sleep(delay)
            delay *= 2
            continue
        if response.status != 200:
            raise DataGovError(f"data.gov.in returned HTTP {response.status}. {_snippet(text)}")
        try:
            payload = json.loads(text)
        except json.JSONDecodeError as exc:
            raise DataGovError("data.gov.in returned a page that is not JSON.") from exc
        if not isinstance(payload, dict):
            raise DataGovError("data.gov.in returned a JSON value that is not an object.")
        status = payload.get("status")
        if status not in (None, "ok"):
            message = payload.get("message", "")
            raise DataGovError(f"data.gov.in rejected the page. {_snippet(str(message))}")
        return payload
    raise DataGovError(last_message)


def _normalize_record(record: Mapping[str, object], retrieved_at: str) -> dict[str, str | None]:
    row: dict[str, str | None] = {column: None for column in _COLUMNS}
    row["retrieved_at"] = retrieved_at
    for column in _COLUMNS:
        if column == "retrieved_at":
            continue
        names = _VALUE_ALIASES.get(column, (column,))
        row[column] = _text_field(record, names)
    return row


def _text_field(record: Mapping[str, object], names: tuple[str, ...]) -> str | None:
    for name in names:
        if name not in record:
            continue
        value = record.get(name)
        if value is None:
            return None
        text = str(value).strip()
        return text or None
    return None


def _append_new_records(config: AppConfig, received: list[dict[str, str | None]]) -> tuple[int, int]:
    import pyarrow as pa
    import pyarrow.parquet as pq

    destination = normalized_records_file(config)
    _refuse_xkdr_path(config, destination)
    existing = _read_records(destination)
    seen = {_identity(row) for row in existing}
    fresh: list[dict[str, str | None]] = []
    duplicate_count = 0
    batch_seen: set[tuple[str, ...]] = set()
    for row in received:
        identity = _identity(row)
        if identity in seen or identity in batch_seen:
            duplicate_count += 1
            continue
        batch_seen.add(identity)
        fresh.append(row)
    combined = existing + fresh
    destination.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(combined, schema=_record_schema())
    if destination.exists():
        destination.unlink()
    pq.write_table(table, destination, compression="snappy")
    return len(fresh), duplicate_count


def _read_records(path: Path) -> list[dict[str, str | None]]:
    if not path.is_file():
        return []
    import pyarrow.parquet as pq

    table = pq.read_table(path)
    rows: list[dict[str, str | None]] = []
    for raw in table.to_pylist():
        row = {column: None for column in _COLUMNS}
        for column in _COLUMNS:
            value = raw.get(column)
            row[column] = None if value is None else str(value)
        rows.append(row)
    return rows


def _stored_count(config: AppConfig) -> int:
    return len(_read_records(normalized_records_file(config)))


def _record_schema():
    import pyarrow as pa

    return pa.schema([(column, pa.string()) for column in _COLUMNS])


def _identity(row: Mapping[str, str | None]) -> tuple[str, ...]:
    return tuple(row.get(column) or "" for column in _IDENTITY)


def _latest_update(rows: list[dict[str, str | None]]) -> str | None:
    parsed: list[tuple[datetime, str]] = []
    for row in rows:
        text = row.get("last_update")
        if not text:
            continue
        try:
            moment = datetime.strptime(text, "%d-%m-%Y %H:%M:%S")
        except ValueError:
            moment = datetime.min
        parsed.append((moment, text))
    if not parsed:
        return None
    return max(parsed)[1]


def _as_int(value: object, label: str) -> int:
    try:
        return int(value)  # type: ignore[arg-type]
    except (TypeError, ValueError) as exc:
        raise DataGovError(f"data.gov.in returned a non-numeric {label}.") from exc


def _snippet(text: str) -> str:
    compact = " ".join(text.split())
    return compact[:240]


def _redact(value: object, api_key: str) -> object:
    if isinstance(value, str):
        return _redact_text(value, api_key)
    if isinstance(value, list):
        return [_redact(item, api_key) for item in value]
    if isinstance(value, dict):
        return {str(key): _redact(item, api_key) for key, item in value.items() if str(key).lower() != "api-key"}
    return value


def _redact_text(text: str, api_key: str) -> str:
    redacted = text
    if api_key:
        redacted = redacted.replace(api_key, "[REDACTED]")
    return redacted


def _write_json(path: Path, payload: object) -> None:
    text = json.dumps(payload, indent=2)
    if "api-key=" in text.lower() or "api_key=" in text.lower():
        raise DataGovError("Refusing to store a data.gov.in file that contains an API key.")
    path.write_text(text + "\n", encoding="utf-8")


def _refuse_xkdr_path(config: AppConfig, path: Path) -> None:
    resolved = path.resolve()
    protected = [
        (config.paths.raw_root / "v1").resolve(),
        (config.paths.processed_dir / "measurements").resolve(),
        (config.paths.processed_dir / "spikes").resolve(),
        (config.paths.processed_dir / "analytics").resolve(),
        (config.paths.processed_dir / "city_portal").resolve(),
    ]
    for root in protected:
        if resolved == root or root in resolved.parents:
            raise DataGovError(f"Refusing to write data.gov.in output under {root}.")


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)
