"""
Unit tests for src/data_generator.py.

These check that the fake data looks like real heart rate data and has the right format.
No Kafka or PostgreSQL needed.
"""

from datetime import datetime
from itertools import islice

from src import data_generator
from src.data_generator import create_customers, generate_reading, stream_readings
from src.validation import validate_reading


def test_creates_the_requested_number_of_unique_customers():
    customers = create_customers(5)
    ids = [c.customer_id for c in customers]

    assert ids == ["CUST-001", "CUST-002", "CUST-003", "CUST-004", "CUST-005"]
    assert all(58 <= c.resting_rate <= 82 for c in customers)


def test_reading_has_the_expected_fields_and_types():
    customer = create_customers(1)[0]
    reading = generate_reading(customer, anomaly_probability=0)

    assert set(reading) == {"customer_id", "timestamp", "heart_rate"}
    assert reading["customer_id"] == "CUST-001"
    assert isinstance(reading["heart_rate"], int)
    assert datetime.fromisoformat(reading["timestamp"]).tzinfo is not None  # has a time zone


def test_without_anomalies_readings_stay_in_a_normal_range():
    customer = create_customers(1)[0]
    rates = [generate_reading(customer, anomaly_probability=0)["heart_rate"] for _ in range(1000)]

    assert all(45 <= r <= 110 for r in rates)


def test_readings_change_gradually_not_randomly():
    customer = create_customers(1)[0]
    rates = [generate_reading(customer, anomaly_probability=0)["heart_rate"] for _ in range(500)]
    biggest_jump = max(abs(a - b) for a, b in zip(rates, rates[1:]))

    assert biggest_jump < 15  # a real heart doesn't jump 50 bpm in one second


def test_with_anomalies_forced_every_reading_is_low_or_high():
    customer = create_customers(1)[0]
    rates = [generate_reading(customer, anomaly_probability=1)["heart_rate"] for _ in range(200)]

    assert all(r <= 48 or r >= 130 for r in rates)


def test_every_generated_reading_passes_validation():
    customers = create_customers(10)
    for _ in range(100):
        for customer in customers:
            validate_reading(generate_reading(customer, anomaly_probability=0.1))  # raises if invalid


def test_stream_gives_one_reading_per_customer_each_round(monkeypatch):
    monkeypatch.setattr(data_generator.time, "sleep", lambda seconds: None)  # don't actually wait

    readings = list(islice(stream_readings(num_customers=3, interval_seconds=1), 6))
    ids = [r["customer_id"] for r in readings]

    assert ids == ["CUST-001", "CUST-002", "CUST-003", "CUST-001", "CUST-002", "CUST-003"]