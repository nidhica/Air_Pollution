"""Server-side cache for the XKDR archive month.

``GET /v1/meta`` reports ``measurements.last_month``. Daily aggregate queries
must not be sent for a later month. The API key is read only while sending
the request and is not written into the cache. This module does not download
bulk files and does not write under ``data/raw`` or ``data/processed``.
"""

from __future__ import annotations

import calendar
import json
import os
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from ingestion.acquire import AcquisitionError, read_api_key
from ingestion.xkdr_query import QueryError, QueryTransport, UrllibQueryTransport, measurement_headers

_ORIGIN = "https://airquality.xkdr.org"
_META_URL = f"{_ORIGIN}/v1/meta?format=json"
_TTL = timedelta(hours=24)
_TIMEOUT_SECONDS = 60.0
_MAX_BYTES = 1024 * 1024
_MONTH = re.compile(r"^(\d{4})-(\d{2})$")
_SECRET_TEXT = re.compile(r"Bearer\s+\S+|aqi_[A-Za-z0-9_-]+", re.IGNORECASE)


class ArchiveError(Exception):
    """Archive metadata could not be stored or the cache directory is unsafe."""


@dataclass(frozen=True)
class ArchiveInfo:
    """The published archive month, or an explicit statement that it is unknown."""

    last_month: str | None
    archive_end: date | None
    available: bool
    origin: str
    detail: str


def default_meta_directory(project_root: Path) -> Path:
    """Return ``project_root/data/cache/xkdr_meta`` without creating it."""

    directory = (project_root / "data" / "cache" / "xkdr_meta").resolve()
    if _is_protected(directory):
        raise ArchiveError("Archive metadata cannot be placed under data/raw or data/processed.")
    return directory


def parse_last_month(payload: object) -> tuple[str, date, str | None]:
    """Return ``YYYY-MM``, its last calendar day, and an export timestamp if present."""

    if not isinstance(payload, dict):
        raise ArchiveError("XKDR archive metadata was not a JSON object.")
    raw = _find_last_month(payload)
    if not isinstance(raw, str):
        raise ArchiveError("XKDR archive metadata did not include measurements.last_month.")
    match = _MONTH.fullmatch(raw.strip())
    if match is None:
        raise ArchiveError("XKDR measurements.last_month was not YYYY-MM.")
    year = int(match.group(1))
    month = int(match.group(2))
    if month < 1 or month > 12:
        raise ArchiveError("XKDR measurements.last_month was not YYYY-MM.")
    last_day = calendar.monthrange(year, month)[1]
    exported = payload.get("exported_at")
    exported_at = exported.strip() if isinstance(exported, str) and exported.strip() else None
    return f"{year:04d}-{month:02d}", date(year, month, last_day), exported_at


def load_archive(
    *,
    project_root: Path,
    transport: QueryTransport | None = None,
    api_key: str | None = None,
    cache_directory: Path | None = None,
    clock: Callable[[], datetime] | None = None,
) -> ArchiveInfo:
    """Return the cached archive month, refreshing it at most once per TTL."""

    directory = cache_directory if cache_directory is not None else default_meta_directory(project_root)
    if _is_protected(directory.resolve()):
        raise ArchiveError("Archive metadata cannot be placed under data/raw or data/processed.")
    now = clock() if clock is not None else datetime.now(timezone.utc)
    cached = _read_cache(directory)
    if cached is not None and now < cached[0] + _TTL:
        last_month, archive_end, stored_at = cached[1], cached[2], cached[0]
        return ArchiveInfo(
            last_month=last_month,
            archive_end=archive_end,
            available=True,
            origin="cache",
            detail=_published_detail(last_month, archive_end, f"Cached at {stored_at.isoformat()}."),
        )
    try:
        payload = _fetch_meta(project_root, transport, api_key)
        last_month, archive_end, exported_at = parse_last_month(payload)
    except (ArchiveError, QueryError, AcquisitionError, OSError, TimeoutError) as exc:
        if cached is not None:
            last_month, archive_end = cached[1], cached[2]
            return ArchiveInfo(
                last_month=last_month,
                archive_end=archive_end,
                available=True,
                origin="stale",
                detail=_published_detail(
                    last_month,
                    archive_end,
                    "The metadata refresh failed. The stored archive month is being used. "
                    + _public(str(exc)),
                ),
            )
        return ArchiveInfo(
            last_month=None,
            archive_end=None,
            available=False,
            origin="unavailable",
            detail=(
                "XKDR archive metadata could not be read. "
                "Daily aggregate queries were not sent. "
                + _public(str(exc))
            ),
        )
    _write_cache(directory, now, last_month, archive_end, exported_at)
    note = f"Exported at {exported_at}." if exported_at else "Retrieved from /v1/meta."
    return ArchiveInfo(
        last_month=last_month,
        archive_end=archive_end,
        available=True,
        origin="api",
        detail=_published_detail(last_month, archive_end, note),
    )


def _fetch_meta(project_root: Path, transport: QueryTransport | None, api_key: str | None) -> object:
    key = api_key.strip() if api_key is not None else read_api_key(project_root)
    if not key:
        raise ArchiveError("AQI_API_KEY is empty.")
    if key in _META_URL:
        raise ArchiveError("Refusing an archive metadata URL that contains the API key.")
    headers = measurement_headers(key)
    headers["Accept"] = "application/json"
    client = transport if transport is not None else UrllibQueryTransport()
    response = client.send(_META_URL, headers, _TIMEOUT_SECONDS, _MAX_BYTES)
    try:
        if response.status != 200:
            detail = _error_detail(response.body, key)
            message = f"XKDR archive metadata failed with HTTP {response.status}."
            if detail:
                message = f"{message} {detail}"
            raise ArchiveError(message)
        try:
            payload = json.loads(response.body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ArchiveError("XKDR archive metadata was not JSON.") from exc
    finally:
        response.close()
    return payload


def _find_last_month(payload: dict[str, object]) -> object:
    measurements = payload.get("measurements")
    if isinstance(measurements, dict) and "last_month" in measurements:
        return measurements.get("last_month")
    return payload.get("last_month")


def _published_detail(last_month: str, archive_end: date, note: str) -> str:
    return (
        f"Published XKDR archive month: {last_month}. "
        f"The last published day is {archive_end.isoformat()}. "
        f"{note}"
    )


def _write_cache(
    directory: Path,
    stored_at: datetime,
    last_month: str,
    archive_end: date,
    exported_at: str | None,
) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    document = {
        "archive_end": archive_end.isoformat(),
        "exported_at": exported_at,
        "last_month": last_month,
        "stored_at": stored_at.isoformat(),
    }
    text = json.dumps(document, sort_keys=True)
    if _SECRET_TEXT.search(text):
        raise ArchiveError("Refusing to cache archive metadata that contains a credential.")
    partial = directory / ".meta.json.partial"
    target = directory / "meta.json"
    previous = target.read_text(encoding="utf-8") if target.is_file() else None
    try:
        partial.write_text(text, encoding="utf-8")
        os.replace(partial, target)
    except OSError as exc:
        partial.unlink(missing_ok=True)
        if previous is not None:
            target.write_text(previous, encoding="utf-8")
        raise ArchiveError("The archive metadata cache could not be updated.") from exc


def _read_cache(directory: Path) -> tuple[datetime, str, date] | None:
    path = directory / "meta.json"
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        stored_at = datetime.fromisoformat(str(payload["stored_at"]))
        last_month, archive_end, _exported = parse_last_month(
            {"measurements": {"last_month": payload["last_month"]}, "exported_at": payload.get("exported_at")}
        )
    except (OSError, UnicodeError, json.JSONDecodeError, KeyError, TypeError, ValueError, ArchiveError):
        return None
    if stored_at.tzinfo is None:
        stored_at = stored_at.replace(tzinfo=timezone.utc)
    if archive_end.isoformat() != str(payload.get("archive_end")):
        return None
    return stored_at, last_month, archive_end


def _error_detail(body: bytes, secret: str) -> str:
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return ""
    if not isinstance(payload, dict):
        return ""
    parts = [
        str(payload[field]).strip()
        for field in ("error", "detail")
        if isinstance(payload.get(field), str) and str(payload.get(field)).strip()
    ]
    return _public(": ".join(parts), secret)[:300]


def _public(text: str, secret: str = "") -> str:
    cleaned = text.replace(secret, "[redacted]") if secret else text
    return _SECRET_TEXT.sub("[redacted]", cleaned)


def _is_protected(path: Path) -> bool:
    parts = [part.casefold() for part in path.parts]
    for index, part in enumerate(parts[:-1]):
        if part == "data" and parts[index + 1] in {"raw", "processed", "interim"}:
            return True
    return False
