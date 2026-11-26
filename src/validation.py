"""
Validation rules for heart rate readings.

Kept separate from the consumer so the rules can be unit-tested without Kafka or PostgreSQL.

A reading goes through two checks:
  1. parse_message()   -> is it valid JSON?
  2. validate_reading() -> does it have the right fields, types and a believable heart rate?
If both pass, it gets a status label: "low", "normal" or "high".
"""

import json
import math
from datetime import datetime, timezone

from src import config

REQUIRED_FIELDS = ("customer_id", "timestamp", "heart_rate")


class InvalidReadingError(ValueError):
    """Raised when a message cannot be stored as a heart rate reading."""


def parse_message(raw: bytes) -> dict:
    """Turn raw Kafka message bytes into a dict, or raise InvalidReadingError."""
    if raw is None:
        raise InvalidReadingError("empty message")
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise InvalidReadingError(f"not valid JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise InvalidReadingError("message is not a JSON object")
    return data


def classify_heart_rate(bpm: int) -> str:
    """Label a (valid) heart rate."""
    if bpm < config.LOW_BPM_THRESHOLD:
        return "low"
    if bpm > config.HIGH_BPM_THRESHOLD:
        return "high"
    return "normal"


def _parse_timestamp(value) -> datetime:
    if not isinstance(value, str):
        raise InvalidReadingError("timestamp must be a string")
    try:
        ts = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise InvalidReadingError(f"bad timestamp: {value!r}") from exc
    if ts.tzinfo is None:  # no time zone given -> assume UTC
        ts = ts.replace(tzinfo=timezone.utc)
    return ts


def validate_reading(data: dict) -> dict:
    """
    Check a parsed reading and return a clean row ready for the database:
        {"customer_id": str, "recorded_at": datetime, "heart_rate": int, "status": str}
    Raises InvalidReadingError if anything is wrong.
    """
    missing = [field for field in REQUIRED_FIELDS if field not in data]
    if missing:
        raise InvalidReadingError(f"missing fields: {missing}")

    customer_id = data["customer_id"]
    if not isinstance(customer_id, str) or not customer_id.strip():
        raise InvalidReadingError("customer_id must be a non-empty string")
    if len(customer_id) > 20:
        raise InvalidReadingError("customer_id is longer than 20 characters")

    heart_rate = data["heart_rate"]
    # bool is a subclass of int in Python, so exclude it explicitly
    if isinstance(heart_rate, bool) or not isinstance(heart_rate, (int, float)):
        raise InvalidReadingError(f"heart_rate must be a number, got {heart_rate!r}")
    # Python's json module accepts NaN and Infinity, but they can't be rounded or stored
    if isinstance(heart_rate, float) and not math.isfinite(heart_rate):
        raise InvalidReadingError(f"heart_rate must be a finite number, got {heart_rate!r}")
    if isinstance(heart_rate, float) and not heart_rate.is_integer():
        heart_rate = round(heart_rate)
    heart_rate = int(heart_rate)
    if not config.VALID_MIN_BPM <= heart_rate <= config.VALID_MAX_BPM:
        raise InvalidReadingError(f"heart_rate {heart_rate} is outside the possible range")

    return {
        "customer_id": customer_id.strip(),
        "recorded_at": _parse_timestamp(data["timestamp"]),
        "heart_rate": heart_rate,
        "status": classify_heart_rate(heart_rate),
    }