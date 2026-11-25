"""
End-to-end test: sends real messages through Kafka and checks what lands in PostgreSQL.

Before running:
  1. docker compose up -d        (Kafka + PostgreSQL)
  2. python -m src.consumer      (in another terminal; the producer is NOT needed)
Then:
  pytest -m integration -v

Each test uses its own made-up customer id (e.g. TEST-3fa9c1d2) so it never mixes with
real data, and deletes its rows afterwards. If Kafka or PostgreSQL is not running,
the tests are skipped instead of failing. The fixtures (db, producer, test_customer)
come from conftest.py.
"""

import pytest

from tests.helpers import reading, send, wait_for_rows

pytestmark = pytest.mark.integration


def test_normal_low_and_high_readings_are_saved_with_correct_status(producer, db, test_customer):
    send(producer, test_customer, reading(test_customer, 72, seconds_offset=0))
    send(producer, test_customer, reading(test_customer, 45, seconds_offset=1))
    send(producer, test_customer, reading(test_customer, 150, seconds_offset=2))
    producer.flush(10)

    rows = wait_for_rows(db, test_customer, expected_count=3)

    assert rows == [(72, "normal"), (45, "low"), (150, "high")]


def test_invalid_messages_are_skipped_and_pipeline_keeps_working(producer, db, test_customer):
    # Bad messages first. They use the same key as the good one, so they go to the same
    # partition and the consumer handles them BEFORE the good reading.
    send(producer, test_customer, reading(test_customer, 400))           # impossible heart rate
    send(producer, test_customer, reading(test_customer, "seventy"))     # not a number
    send(producer, test_customer, {"customer_id": test_customer})        # missing fields
    send(producer, test_customer, b"this is not json")                   # not JSON at all
    # Then one good reading
    send(producer, test_customer, reading(test_customer, 80))
    producer.flush(10)

    rows = wait_for_rows(db, test_customer, expected_count=1)

    # Only the good reading was saved, and the consumer didn't crash on the bad ones
    assert rows == [(80, "normal")]


def test_a_burst_of_readings_is_saved_completely(producer, db, test_customer):
    count = 200
    for i in range(count):
        send(producer, test_customer, reading(test_customer, 70 + i % 10, seconds_offset=i))
    producer.flush(10)

    rows = wait_for_rows(db, test_customer, expected_count=count)

    assert len(rows) == count  # nothing lost, nothing duplicated