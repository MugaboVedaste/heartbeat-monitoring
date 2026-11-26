"""
Unit tests for src/validation.py.

These check the rules that decide whether a reading is saved and how it is labelled.
No Kafka or PostgreSQL needed.
"""

from datetime import datetime, timezone

import pytest

from src.validation import (
    InvalidReadingError,
    classify_heart_rate,
    parse_message,
    validate_reading,
)


def make_reading(**changes) -> dict:
    """A valid reading; pass keyword arguments to change any field."""
    reading = {
        "customer_id": "CUST-001",
        "timestamp": "2026-11-24T10:00:00+00:00",
        "heart_rate": 72,
    }
    reading.update(changes)
    return reading


# ---------- Labelling: low / normal / high ----------

@pytest.mark.parametrize("bpm, expected", [
    (72, "normal"),
    (50, "normal"),   # exactly on the low threshold -> still normal
    (120, "normal"),  # exactly on the high threshold -> still normal
    (49, "low"),
    (30, "low"),
    (121, "high"),
    (180, "high"),
])
def test_heart_rate_is_labelled_correctly(bpm, expected):
    assert classify_heart_rate(bpm) == expected


# ---------- Valid readings ----------

def test_valid_reading_becomes_a_database_row():
    row = validate_reading(make_reading(heart_rate=45))

    assert row["customer_id"] == "CUST-001"
    assert row["heart_rate"] == 45
    assert row["status"] == "low"
    assert row["recorded_at"] == datetime(2026, 11, 24, 10, 0, tzinfo=timezone.utc)


@pytest.mark.parametrize("bpm", [20, 250])
def test_edges_of_possible_range_are_accepted(bpm):
    assert validate_reading(make_reading(heart_rate=bpm))["heart_rate"] == bpm


def test_timestamp_ending_in_z_is_accepted():
    row = validate_reading(make_reading(timestamp="2026-11-24T10:00:00Z"))
    assert row["recorded_at"].tzinfo is not None


def test_timestamp_without_timezone_is_treated_as_utc():
    row = validate_reading(make_reading(timestamp="2026-11-24T10:00:00"))
    assert row["recorded_at"].tzinfo == timezone.utc


@pytest.mark.parametrize("value, expected", [(72.0, 72), (72.6, 73)])
def test_decimal_heart_rate_is_rounded(value, expected):
    assert validate_reading(make_reading(heart_rate=value))["heart_rate"] == expected


# ---------- Readings that must be rejected ----------

@pytest.mark.parametrize("bpm", [0, -5, 19, 251, 400])
def test_impossible_heart_rate_is_rejected(bpm):
    with pytest.raises(InvalidReadingError):
        validate_reading(make_reading(heart_rate=bpm))


@pytest.mark.parametrize("value", ["72", None, True, [72]])
def test_heart_rate_that_is_not_a_number_is_rejected(value):
    with pytest.raises(InvalidReadingError):
        validate_reading(make_reading(heart_rate=value))


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_heart_rate_is_rejected(value):
    with pytest.raises(InvalidReadingError, match="finite"):
        validate_reading(make_reading(heart_rate=value))


@pytest.mark.parametrize("token", [b"NaN", b"Infinity", b"-Infinity"])
def test_nan_and_infinity_in_raw_json_are_rejected_not_crashed_on(token):
    # Python's json.loads accepts these tokens, so they reach validation from real Kafka messages
    raw = b'{"customer_id": "CUST-001", "timestamp": "2026-11-24T10:00:00Z", "heart_rate": ' + token + b"}"
    with pytest.raises(InvalidReadingError):
        validate_reading(parse_message(raw))


@pytest.mark.parametrize("field", ["customer_id", "timestamp", "heart_rate"])
def test_missing_field_is_rejected(field):
    reading = make_reading()
    del reading[field]
    with pytest.raises(InvalidReadingError, match="missing"):
        validate_reading(reading)


@pytest.mark.parametrize("customer_id", ["", "   ", 123, "C" * 21])
def test_bad_customer_id_is_rejected(customer_id):
    with pytest.raises(InvalidReadingError):
        validate_reading(make_reading(customer_id=customer_id))


@pytest.mark.parametrize("timestamp", ["yesterday", "2026-13-45", 12345])
def test_bad_timestamp_is_rejected(timestamp):
    with pytest.raises(InvalidReadingError):
        validate_reading(make_reading(timestamp=timestamp))


# ---------- Parsing raw Kafka messages ----------

def test_valid_json_message_is_parsed():
    assert parse_message(b'{"customer_id": "CUST-001"}') == {"customer_id": "CUST-001"}


@pytest.mark.parametrize("raw", [b"hello", b"[1, 2, 3]", b"", None, b"\xff\xfe\x00"])
def test_message_that_is_not_a_json_object_is_rejected(raw):
    with pytest.raises(InvalidReadingError):
        parse_message(raw)