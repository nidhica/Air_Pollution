"""Disk cache for completed XKDR measurements Parquet bodies.

Entries live under ``data/cache/xkdr_query/`` unless a test passes another
directory. The cache key is a SHA-256 of the normalized query parameters.
The API key is not an argument and cannot appear in a filename.

A write goes to a temporary sibling and replaces the cache file only after
the temporary file is complete. A failed replacement leaves the previous
entry in place. The cache refuses a directory inside ``data/raw`` or
``data/processed``.

Lookup honours a time-to-live. ``invalidate`` removes one query and
``purge_expired`` removes entries whose stored time is outside the TTL.
This module does not call the network. The Timeline uses it for the recent
7-day window, including presets that contain those dates.
"""

from __future__ import annotations

import json
import os
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from ingestion.xkdr_query import MeasurementQuery

_DEFAULT_TTL = timedelta(days=7)


class CacheError(Exception):
    """A cache entry could not be stored or the directory is not allowed."""


@dataclass(frozen=True)
class CacheEntry:
    """One completed response. ``parameters`` are the normalized query fields."""

    path: Path
    stored_at: datetime
    parameters: dict[str, object]

    def read_bytes(self) -> bytes:
        return self.path.read_bytes()


class QueryCache:
    """Parquet cache keyed only by a measurements query."""

    def __init__(
        self,
        directory: Path,
        *,
        ttl: timedelta = _DEFAULT_TTL,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if ttl <= timedelta(0):
            raise CacheError("The query cache TTL must be positive.")
        self.directory = directory.resolve()
        if _is_protected(self.directory):
            raise CacheError("The query cache cannot be placed under data/raw or data/processed.")
        self.ttl = ttl
        self._clock = clock if clock is not None else _utc_now

    def get(self, query: MeasurementQuery) -> CacheEntry | None:
        """Return a fresh entry, or ``None`` on a miss or an expired entry."""

        path, meta_path = self._paths(query)
        if not path.is_file() or not meta_path.is_file():
            return None
        document = _read_meta(meta_path)
        if document is None or document.get("parameters") != query.normalized():
            return None
        stored_at = _stored_at(document)
        if stored_at is None or self._clock() >= stored_at + self.ttl:
            return None
        return CacheEntry(path=path, stored_at=stored_at, parameters=document["parameters"])

    def put(self, query: MeasurementQuery, payload: bytes) -> CacheEntry:
        """Store a complete Parquet body. An empty body is refused."""

        if not payload:
            raise CacheError("Refusing to cache an empty measurements body.")
        self.directory.mkdir(parents=True, exist_ok=True)
        path, meta_path = self._paths(query)
        stored_at = self._clock()
        document = {"stored_at": stored_at.isoformat(), "parameters": query.normalized()}
        partial = self.directory / f".{query.cache_token()}.parquet.partial"
        partial_meta = self.directory / f".{query.cache_token()}.json.partial"
        previous = path.read_bytes() if path.is_file() else None
        try:
            partial.write_bytes(payload)
            os.replace(partial, path)
        except OSError as exc:
            partial.unlink(missing_ok=True)
            raise CacheError(
                "The query cache could not be updated. The previous entry was left in place."
            ) from exc
        try:
            partial_meta.write_text(json.dumps(document, sort_keys=True), encoding="utf-8")
            os.replace(partial_meta, meta_path)
        except OSError as exc:
            partial_meta.unlink(missing_ok=True)
            try:
                if previous is None:
                    path.unlink(missing_ok=True)
                else:
                    path.write_bytes(previous)
            except OSError:
                pass
            raise CacheError(
                "The query cache could not be updated. The previous entry was left in place."
            ) from exc
        return CacheEntry(path=path, stored_at=stored_at, parameters=document["parameters"])

    def invalidate(self, query: MeasurementQuery) -> None:
        """Remove one query. Missing entries are ignored."""

        for path in self._paths(query):
            path.unlink(missing_ok=True)

    def purge_expired(self) -> int:
        """Delete entries whose stored time is outside the TTL."""

        if not self.directory.is_dir():
            return 0
        removed = 0
        now = self._clock()
        for meta_path in self.directory.glob("*.json"):
            token = meta_path.stem
            if len(token) != 64:
                continue
            document = _read_meta(meta_path)
            stored_at = None if document is None else _stored_at(document)
            if stored_at is not None and now < stored_at + self.ttl:
                continue
            meta_path.unlink(missing_ok=True)
            (self.directory / f"{token}.parquet").unlink(missing_ok=True)
            removed += 1
        return removed

    def clear(self) -> None:
        """Remove every hashed entry in this directory."""

        if not self.directory.is_dir():
            return
        for path in self.directory.iterdir():
            if path.is_file() and (path.suffix in {".parquet", ".json"} or path.name.endswith(".partial")):
                path.unlink()

    def _paths(self, query: MeasurementQuery) -> tuple[Path, Path]:
        token = query.cache_token()
        if len(token) != 64 or any(character not in "0123456789abcdef" for character in token):
            raise CacheError("The cache token is not a hash of the query parameters.")
        return self.directory / f"{token}.parquet", self.directory / f"{token}.json"


def default_cache_directory(project_root: Path) -> Path:
    """Return ``project_root/data/cache/xkdr_query`` without creating it."""

    directory = (project_root / "data" / "cache" / "xkdr_query").resolve()
    if _is_protected(directory):
        raise CacheError("The query cache cannot be placed under data/raw or data/processed.")
    return directory


def _is_protected(path: Path) -> bool:
    parts = [part.casefold() for part in path.parts]
    for index, part in enumerate(parts[:-1]):
        if part == "data" and parts[index + 1] in {"raw", "processed"}:
            return True
    return False


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _read_meta(path: Path) -> dict[str, object] | None:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict) or not isinstance(payload.get("parameters"), dict):
        return None
    return payload


def _stored_at(document: dict[str, object]) -> datetime | None:
    raw = document.get("stored_at")
    if not isinstance(raw, str):
        return None
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed
