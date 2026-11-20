"""Demonstration sign-in for the AEROSCOPE portal.

Passwords are checked with PBKDF2-HMAC-SHA256. The portal stores the salt
and hash in ``config/portal_users.json``. It does not store the password.
The Streamlit process keeps the signed-in email in session state for this
browser tab only. There is no account database, lockout, or server session.
"""

from __future__ import annotations

import hashlib
import hmac
import json
from dataclasses import dataclass
from pathlib import Path


class AuthenticationError(Exception):
    """The demonstration user file cannot be read."""


@dataclass(frozen=True)
class PortalUser:
    email: str
    salt: bytes
    iterations: int
    password_hash: str


def users_path(root: Path) -> Path:
    return root / "config" / "portal_users.json"


def load_users(path: Path) -> tuple[PortalUser, ...]:
    """Read demonstration accounts. The password itself is not in the file."""

    if not path.is_file():
        raise AuthenticationError(f"Portal user file not found: {path}")
    document = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(document, dict) or not isinstance(document.get("users"), list):
        raise AuthenticationError(f"Portal user file is missing a users list: {path}")
    users: list[PortalUser] = []
    for item in document["users"]:
        if not isinstance(item, dict):
            raise AuthenticationError("Each portal user must be an object.")
        email = str(item.get("email", "")).strip().lower()
        salt_hex = str(item.get("salt_hex", "")).strip()
        password_hash = str(item.get("password_hash", "")).strip()
        iterations = item.get("iterations")
        if not email or not salt_hex or not password_hash or not isinstance(iterations, int) or iterations < 1:
            raise AuthenticationError("A portal user is missing an email, salt, iteration count, or hash.")
        try:
            salt = bytes.fromhex(salt_hex)
        except ValueError as exc:
            raise AuthenticationError("A portal user salt is not hexadecimal.") from exc
        users.append(PortalUser(email, salt, iterations, password_hash.lower()))
    return tuple(users)


def verify_credentials(email: str, password: str, users: tuple[PortalUser, ...]) -> bool:
    """Return whether the email and password match one stored hash."""

    wanted = email.strip().lower()
    for user in users:
        if user.email != wanted:
            continue
        digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), user.salt, user.iterations)
        return hmac.compare_digest(digest.hex(), user.password_hash)
    return False


def begin_session(state: dict[str, object], email: str) -> None:
    """Mark a mapping as signed in and clear any previously selected city."""

    state["authenticated"] = True
    state["user_email"] = email.strip().lower()
    state["portal_view"] = "timeline"
    clear_city(state)
    clear_comparison(state)
    clear_recent(state)


def end_session(state: dict[str, object]) -> None:
    """Clear the signed-in email, the selected city, and any city comparison."""

    state["authenticated"] = False
    state["user_email"] = None
    state["portal_view"] = "explore"
    clear_city(state)
    clear_comparison(state)
    clear_recent(state)


def clear_city(state: dict[str, object]) -> None:
    state["city_selected"] = False
    state["city_name"] = None
    state["state_name"] = None


def clear_comparison(state: dict[str, object]) -> None:
    """Drop comparison choices so the next sign-in starts without them."""

    for key in (
        "comparison_error",
        "comparison_places",
        "comparison_city_labels",
        "comparison_dates",
        "comparison_trend",
        "compare_city_1",
        "compare_city_2",
        "compare_city_3",
        "compare_dates",
        "compare_trend_pollutant",
    ):
        state.pop(key, None)


def clear_recent(state: dict[str, object]) -> None:
    """Drop recent-snapshot filters so the next sign-in starts without them."""

    for key in ("recent_state", "recent_city", "recent_station", "recent_pollutant"):
        state.pop(key, None)


def select_city(state: dict[str, object], city_name: str, state_name: str | None) -> None:
    state["city_selected"] = True
    state["city_name"] = city_name
    state["state_name"] = state_name
