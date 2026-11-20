"""Download XKDR bulk files into ``data/raw`` without changing their bytes.

Official mechanism, from https://airquality.xkdr.org/ and its OpenAPI document:

- ``GET /v1/files`` returns JSON with a ``files`` array. Each entry has a ``key``.
- ``GET /v1/files/<key>`` returns that object and honours HTTP ``Range``.
- Every request sends ``Authorization: Bearer <key>``.
- Documented keys are ``v1/stations.parquet``, ``v1/parameters.parquet``, and
  ``v1/measurements/year=YYYY/month=MM/data.parquet``.

Monthly measurement files contain every source and pollutant. They are stored
byte-for-byte under ``data/raw/<key>``. CPCB CAAQM and the configured
pollutants are applied later, when the files are read. The measurements query
API is not used: the publisher says a single query times out after 90 seconds
and whole months should be taken from these bulk files.

The API key is read from ``AQI_API_KEY`` or from an untracked ``.env`` file.
It is never written into source, reports, or log lines.
"""

from __future__ import annotations

import json
import os
import re
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlparse
from urllib.request import Request, urlopen

from utils.config_loader import AppConfig

_API_ORIGIN = "https://airquality.xkdr.org"
_FILES_PATH = "/v1/files"
_META_PATH = "/v1/meta"
_USER_AGENT = "AirPollutionBDA/1.0 (academic research)"
_STATIONS_KEY = "v1/stations.parquet"
_PARAMETERS_KEY = "v1/parameters.parquet"
_MEASUREMENT_KEY = re.compile(
    r"^v1/measurements/year=(\d{4})/month=(0[1-9]|1[0-2])/data\.parquet$"
)
_CONTENT_RANGE = re.compile(r"bytes (\d+)-(\d+)/(\d+|\*)")
_CHUNK_BYTES = 1024 * 1024
_TIMEOUT_SECONDS = 120


class AcquisitionError(Exception):
    """The XKDR file list or a bulk object could not be downloaded safely."""


class ResponseStream(Protocol):
    status: int
    headers: Mapping[str, str]

    def iter_chunks(self) -> Iterator[bytes]:
        """Yield the body in order."""

    def close(self) -> None:
        """Release the connection."""


class Transport(Protocol):
    def send(self, method: str, url: str, headers: Mapping[str, str]) -> ResponseStream:
        """Perform one HTTP request. Implementations must not log ``headers``."""


@dataclass(frozen=True)
class BulkFile:
    key: str
    year: int | None = None
    month: int | None = None
    size_bytes: int | None = None
    row_count: int | None = None


@dataclass(frozen=True)
class FilePlan:
    selected: tuple[BulkFile, ...]
    ignored_keys: tuple[str, ...]


@dataclass(frozen=True)
class FileResult:
    key: str
    path: str
    status: str
    bytes_written: int
    message: str


@dataclass(frozen=True)
class EndpointDiagnostic:
    """Safe fields from one API response. The API key is not included."""

    method: str
    url: str
    status: int
    content_type: str
    cf_ray: str | None
    user_agent: str
    detail: str


@dataclass(frozen=True)
class AcquisitionReport:
    origin: str
    files: tuple[FileResult, ...]
    ignored_keys: tuple[str, ...]

    @property
    def ok(self) -> bool:
        measurements = [item for item in self.files if item.key.startswith("v1/measurements/")]
        return bool(measurements) and all(item.status != "failed" for item in self.files)


class UrllibTransport:
    """Stdlib client. Redirects that leave the XKDR host are refused."""

    def __init__(self, timeout: float = _TIMEOUT_SECONDS) -> None:
        self.timeout = timeout

    def send(self, method: str, url: str, headers: Mapping[str, str]) -> ResponseStream:
        request = Request(url, headers=dict(headers), method=method)
        try:
            response = urlopen(request, timeout=self.timeout)
        except HTTPError as exc:
            response = exc
        except URLError as exc:
            reason = getattr(exc, "reason", exc)
            raise AcquisitionError(f"Could not reach the XKDR API: {reason}") from exc
        if urlparse(url).netloc != urlparse(response.geturl()).netloc:
            response.close()
            raise AcquisitionError("Refusing a redirect that leaves the XKDR host.")
        return _UrllibResponse(response)


class _UrllibResponse:
    def __init__(self, response: object) -> None:
        self._response = response
        self.status = int(response.status)  # type: ignore[attr-defined]
        raw_headers = response.headers  # type: ignore[attr-defined]
        self.headers = {key: value for key, value in raw_headers.items()}

    def iter_chunks(self) -> Iterator[bytes]:
        while True:
            chunk = self._response.read(_CHUNK_BYTES)  # type: ignore[attr-defined]
            if not chunk:
                break
            yield chunk

    def close(self) -> None:
        self._response.close()  # type: ignore[attr-defined]


def acquire_dataset(
    config: AppConfig,
    *,
    transport: Transport | None = None,
    api_key: str | None = None,
) -> AcquisitionReport:
    """Download the documented bulk files for this configuration.

    A file already present at its final path is left unchanged. An interrupted
    transfer stays in a ``.partial`` file and is resumed with ``Range`` on the
    next run. The final path is published only after the body is complete.
    """

    key = api_key if api_key is not None else read_api_key(config.paths.project_root)
    if not key.strip():
        raise AcquisitionError(_missing_key_message())
    origin = api_origin(config.dataset.documentation_url)
    client = transport if transport is not None else UrllibTransport()
    payload = _get_json(client, endpoint_url(origin, _FILES_PATH), key)
    plan = select_bulk_files(
        parse_file_list(payload),
        date_start=config.dataset.date_start,
        date_end=config.dataset.date_end,
    )
    results = tuple(
        _download_one(client, origin, key, config.paths.raw_root, item, retries=1, validate_parquet=False)
        for item in plan.selected
    )
    for result in results:
        detail = f" ({result.message})" if result.message else ""
        print(f"{result.status}: {result.key} [{result.bytes_written} bytes]{detail}", flush=True)
    if plan.ignored_keys:
        print("Ignored undocumented keys: " + ", ".join(plan.ignored_keys), flush=True)
    return AcquisitionReport(origin=origin, files=results, ignored_keys=plan.ignored_keys)


def read_api_key(project_root: Path) -> str:
    """Return ``AQI_API_KEY`` from the environment or from ``project_root/.env``."""

    from_env = os.environ.get("AQI_API_KEY", "").strip()
    if from_env:
        return from_env
    env_file = project_root / ".env"
    if env_file.is_file():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            name, raw_value = stripped.split("=", 1)
            if name.strip() == "AQI_API_KEY":
                value = raw_value.strip().strip('"').strip("'")
                if value:
                    return value
    raise AcquisitionError(_missing_key_message())


def api_origin(documentation_url: str) -> str:
    parsed = urlparse(documentation_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise AcquisitionError("dataset.documentation_url must be an absolute http(s) URL.")
    origin = f"{parsed.scheme}://{parsed.netloc}"
    if origin != _API_ORIGIN:
        raise AcquisitionError(
            f"dataset.documentation_url must use {_API_ORIGIN}. The configured origin is {origin}."
        )
    return origin


def request_headers(api_key: str, *, accept: str) -> dict[str, str]:
    """Headers for every XKDR request.

    ``urllib`` sends ``User-Agent: Python-urllib/3.12`` when this header is
    omitted. Cloudflare error 1010 rejects that signature. The value below
    names this application; it is not a browser identity.
    """

    return {
        "Authorization": f"Bearer {api_key}",
        "Accept": accept,
        "User-Agent": _USER_AGENT,
    }


def probe_api(
    config: AppConfig,
    *,
    transport: Transport | None = None,
    api_key: str | None = None,
) -> tuple[EndpointDiagnostic, ...]:
    """Call ``GET /v1/meta`` and ``GET /v1/files`` with the download headers."""

    key = api_key if api_key is not None else read_api_key(config.paths.project_root)
    if not key.strip():
        raise AcquisitionError(_missing_key_message())
    origin = api_origin(config.dataset.documentation_url)
    client = transport if transport is not None else UrllibTransport()
    return tuple(
        _probe_one(client, endpoint_url(origin, path), key) for path in (_META_PATH, _FILES_PATH)
    )


def format_diagnostic(diagnostic: EndpointDiagnostic) -> str:
    ray = diagnostic.cf_ray if diagnostic.cf_ray else "absent"
    detail = f" {diagnostic.detail}" if diagnostic.detail else ""
    return (
        f"{diagnostic.method} {diagnostic.url} -> HTTP {diagnostic.status} "
        f"content-type={diagnostic.content_type} cf-ray={ray} "
        f"user-agent={diagnostic.user_agent}{detail}"
    )


def parse_file_list(payload: object) -> tuple[BulkFile, ...]:
    """Parse the documented ``GET /v1/files`` body. Only ``files[].key`` is required."""

    if not isinstance(payload, dict) or not isinstance(payload.get("files"), list):
        raise AcquisitionError("XKDR /v1/files did not return a 'files' array.")
    parsed: list[BulkFile] = []
    for item in payload["files"]:
        if not isinstance(item, dict) or not isinstance(item.get("key"), str):
            raise AcquisitionError("A bulk-file entry has no string 'key'.")
        key = item["key"]
        _reject_unsafe_key(key)
        match = _MEASUREMENT_KEY.fullmatch(key)
        size_bytes = _optional_int(item, ("size_bytes", "size", "bytes", "content_length", "file_size"))
        row_count = _optional_int(item, ("row_count", "rows", "n_rows"))
        if match:
            parsed.append(BulkFile(key, int(match.group(1)), int(match.group(2)), size_bytes, row_count))
        elif key.startswith("v1/measurements/"):
            raise AcquisitionError(f"Unrecognised measurement key: {key}")
        else:
            parsed.append(BulkFile(key, size_bytes=size_bytes, row_count=row_count))
    return tuple(parsed)


def select_bulk_files(
    files: tuple[BulkFile, ...],
    *,
    date_start: date | None,
    date_end: date | None,
) -> FilePlan:
    """Keep station and parameter metadata, plus measurement months in the window."""

    selected: list[BulkFile] = []
    ignored: list[str] = []
    for item in files:
        if item.key in {_STATIONS_KEY, _PARAMETERS_KEY} or item.year is not None:
            if item.year is None or _month_overlaps(item.year, item.month or 1, date_start, date_end):
                selected.append(item)
        else:
            ignored.append(item.key)
    selected.sort(key=lambda item: (item.year or 0, item.month or 0, _metadata_rank(item.key), item.key))
    return FilePlan(selected=tuple(selected), ignored_keys=tuple(ignored))


def format_acquisition_report(report: AcquisitionReport) -> str:
    downloaded = sum(1 for item in report.files if item.status == "downloaded")
    resumed = sum(1 for item in report.files if item.status == "resumed")
    skipped = sum(1 for item in report.files if item.status == "skipped")
    failed = sum(1 for item in report.files if item.status == "failed")
    return (
        f"Origin: {report.origin}\n"
        f"Downloaded: {downloaded}\n"
        f"Resumed: {resumed}\n"
        f"Already present: {skipped}\n"
        f"Failed: {failed}\n"
        f"Ignored undocumented keys: {len(report.ignored_keys)}"
    )


def download_bulk_file(
    transport: Transport,
    origin: str,
    api_key: str,
    raw_root: Path,
    item: BulkFile,
    *,
    retries: int = 3,
    validate_parquet: bool = True,
) -> FileResult:
    """Download one bulk object. A non-empty final file is never replaced."""

    return _download_one(
        transport,
        origin,
        api_key,
        raw_root,
        item,
        retries=retries,
        validate_parquet=validate_parquet,
    )


def _download_one(
    transport: Transport,
    origin: str,
    api_key: str,
    raw_root: Path,
    item: BulkFile,
    *,
    retries: int = 1,
    validate_parquet: bool = False,
) -> FileResult:
    attempts = max(1, retries)
    result: FileResult | None = None
    for attempt in range(attempts):
        result = _transfer_one(
            transport,
            origin,
            api_key,
            raw_root,
            item,
            validate_parquet=validate_parquet,
        )
        if result.status != "failed" or attempt == attempts - 1 or not _retriable(result.message):
            return result
    assert result is not None
    return result


def _transfer_one(
    transport: Transport,
    origin: str,
    api_key: str,
    raw_root: Path,
    item: BulkFile,
    *,
    validate_parquet: bool,
) -> FileResult:
    destination = _destination(raw_root, item.key)
    partial = destination.with_name(destination.name + ".partial")
    if destination.is_file() and destination.stat().st_size > 0:
        return FileResult(item.key, str(destination), "skipped", destination.stat().st_size, "already present")

    destination.parent.mkdir(parents=True, exist_ok=True)
    offset = partial.stat().st_size if partial.is_file() else 0
    headers = request_headers(api_key, accept="application/octet-stream")
    if offset:
        headers["Range"] = f"bytes={offset}-"
    object_url = _object_url(origin, item.key)
    try:
        response = transport.send("GET", object_url, headers)
    except AcquisitionError as exc:
        return FileResult(item.key, str(destination), "failed", offset, _redact(str(exc), api_key))

    try:
        if response.status in {401, 403, 404}:
            return FileResult(
                item.key,
                str(destination),
                "failed",
                offset,
                _diagnostic_text(object_url, response, api_key, _preview(response)),
            )
        if response.status == 416:
            if offset and partial.is_file():
                partial.replace(destination)
                return FileResult(item.key, str(destination), "resumed", offset, "partial file was already complete")
            return FileResult(item.key, str(destination), "failed", offset, "HTTP 416.")
        if response.status not in {200, 206}:
            return FileResult(
                item.key,
                str(destination),
                "failed",
                offset,
                _diagnostic_text(object_url, response, api_key, _preview(response)),
            )

        write_offset = offset
        if response.status == 200:
            write_offset = 0
        elif response.status == 206:
            content_range = _header(response.headers, "Content-Range")
            parsed_range = _parse_content_range(content_range)
            if parsed_range is None or parsed_range[0] != offset:
                return FileResult(
                    item.key,
                    str(destination),
                    "failed",
                    offset,
                    "The Range response does not continue the partial file.",
                )

        expected = _expected_total(response, write_offset)
        written = write_offset
        mode = "ab" if response.status == 206 and write_offset else "wb"
        try:
            with partial.open(mode) as handle:
                for chunk in response.iter_chunks():
                    handle.write(chunk)
                    written += len(chunk)
                    handle.flush()
        except OSError as exc:
            return FileResult(item.key, str(destination), "failed", written, f"Download interrupted: {exc.strerror}.")
        if expected is not None and written != expected:
            return FileResult(
                item.key,
                str(destination),
                "failed",
                written,
                f"Incomplete object: received {written} of {expected} bytes.",
            )
        if validate_parquet:
            ok, reason = parquet_file_status(partial)
            if not ok:
                partial.unlink(missing_ok=True)
                return FileResult(item.key, str(destination), "failed", written, reason)
        partial.replace(destination)
        status = "resumed" if response.status == 206 and offset else "downloaded"
        return FileResult(item.key, str(destination), status, written, "")
    finally:
        response.close()


def _get_json(transport: Transport, url: str, api_key: str) -> object:
    response = transport.send("GET", url, request_headers(api_key, accept="application/json"))
    try:
        body = b"".join(response.iter_chunks())
        if response.status != 200:
            raise AcquisitionError(_diagnostic_text(url, response, api_key, body[:400]))
        try:
            return json.loads(body.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise AcquisitionError(
                _diagnostic_text(url, response, api_key, b"") + " The body was not JSON."
            ) from exc
    finally:
        response.close()


def _probe_one(transport: Transport, url: str, api_key: str) -> EndpointDiagnostic:
    try:
        response = transport.send("GET", url, request_headers(api_key, accept="application/json"))
    except AcquisitionError as exc:
        return EndpointDiagnostic(
            method="GET",
            url=url,
            status=0,
            content_type="absent",
            cf_ray=None,
            user_agent=_USER_AGENT,
            detail=_redact(str(exc), api_key),
        )
    try:
        preview = _preview(response) if response.status != 200 else b""
        return _diagnostic(url, response, api_key, preview)
    finally:
        response.close()


def _diagnostic(url: str, response: ResponseStream, api_key: str, preview: bytes) -> EndpointDiagnostic:
    detail = ""
    if response.status != 200 and preview:
        detail = _redact(preview.decode("utf-8", errors="replace"), api_key)
        detail = " ".join(detail.split())[:300]
    return EndpointDiagnostic(
        method="GET",
        url=url,
        status=response.status,
        content_type=_header(response.headers, "Content-Type") or "absent",
        cf_ray=_header(response.headers, "CF-Ray"),
        user_agent=_USER_AGENT,
        detail=detail,
    )


def _diagnostic_text(url: str, response: ResponseStream, api_key: str, preview: bytes) -> str:
    return format_diagnostic(_diagnostic(url, response, api_key, preview))


def _preview(response: ResponseStream) -> bytes:
    collected = bytearray()
    for chunk in response.iter_chunks():
        collected.extend(chunk)
        if len(collected) >= 400:
            break
    return bytes(collected[:400])


def endpoint_url(origin: str, path: str) -> str:
    return origin.rstrip("/") + path


def _files_url(origin: str) -> str:
    return endpoint_url(origin, _FILES_PATH)


def _object_url(origin: str, key: str) -> str:
    return _files_url(origin) + "/" + quote(key, safe="/=")


def _destination(raw_root: Path, key: str) -> Path:
    _reject_unsafe_key(key)
    root = raw_root.resolve()
    destination = (root / key).resolve()
    if destination != root and root not in destination.parents:
        raise AcquisitionError(f"Refusing bulk file key {key!r}.")
    return destination


def _reject_unsafe_key(key: str) -> None:
    parts = key.split("/")
    if not key or key.startswith("/") or "\\" in key or ".." in parts or any(part == "" for part in parts):
        raise AcquisitionError(f"Refusing bulk file key {key!r}.")


def _month_overlaps(year: int, month: int, start: date | None, end: date | None) -> bool:
    if start is None and end is None:
        return True
    first = date(year, month, 1)
    if month == 12:
        last = date(year, 12, 31)
    else:
        last = date(year, month + 1, 1) - timedelta(days=1)
    window_start = start or date.min
    window_end = end or date.max
    return first <= window_end and last >= window_start


def _metadata_rank(key: str) -> int:
    if key == _STATIONS_KEY:
        return 0
    if key == _PARAMETERS_KEY:
        return 1
    return 2


def _header(headers: Mapping[str, str], name: str) -> str | None:
    for key, value in headers.items():
        if key.lower() == name.lower():
            return value
    return None


def _parse_content_range(value: str | None) -> tuple[int, int, int | None] | None:
    if value is None:
        return None
    match = _CONTENT_RANGE.fullmatch(value.strip())
    if match is None:
        return None
    total = None if match.group(3) == "*" else int(match.group(3))
    return int(match.group(1)), int(match.group(2)), total


def _expected_total(response: ResponseStream, write_offset: int) -> int | None:
    if response.status == 206:
        parsed = _parse_content_range(_header(response.headers, "Content-Range"))
        if parsed is not None and parsed[2] is not None:
            return parsed[2]
    length = _header(response.headers, "Content-Length")
    if length is None or not length.isdigit():
        return None
    size = int(length)
    if response.status == 206:
        return write_offset + size
    return size


def parquet_file_status(path: Path) -> tuple[bool, str]:
    """Return whether a local file is a readable measurement Parquet file.

    The check reads the footer and schema. It does not rewrite the file.
    """

    try:
        import pyarrow.parquet as pq

        parquet = pq.ParquetFile(path)
        names = set(parquet.schema_arrow.names)
    except Exception as exc:
        return False, f"Parquet validation failed: {exc.__class__.__name__}."
    required = {"station_id", "parameter_name", "collected_at", "value", "source", "unit"}
    missing = sorted(required - names)
    if missing:
        return False, "Parquet schema is missing: " + ", ".join(missing) + "."
    if parquet.metadata is None or parquet.metadata.num_rows < 1:
        return False, "Parquet file has no rows."
    return True, ""


def _optional_int(item: Mapping[str, object], names: tuple[str, ...]) -> int | None:
    for name in names:
        value = item.get(name)
        if isinstance(value, bool) or value is None:
            continue
        if isinstance(value, int):
            return value
        if isinstance(value, float) and value.is_integer():
            return int(value)
        if isinstance(value, str) and value.isdigit():
            return int(value)
        if isinstance(value, dict):
            nested = _optional_int(value, ("bytes", "size", "rows", "count"))
            if nested is not None:
                return nested
    return None


def _retriable(message: str) -> bool:
    return message.startswith("Could not reach") or "HTTP 429" in message or "HTTP 503" in message


def _redact(text: str, api_key: str) -> str:
    if api_key:
        return text.replace(api_key, "[redacted]")
    return text


def _missing_key_message() -> str:
    return (
        "Set AQI_API_KEY in the environment or in an untracked .env file. "
        "Request a key from https://airquality.xkdr.org/signup."
    )
