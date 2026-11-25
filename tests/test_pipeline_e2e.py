"""
End-to-end test: sends real messages through Kafka and checks what lands in PostgreSQL.

Before running:
  1. docker compose up -d        (Kafka + PostgreSQL)
  2. python -m src.consumer      (in another terminal; the producer is NOT needed)
Then:
  pytest -m integration -v

Each test uses its own made-up customer id (e.g. TEST-3fa9c1d2) so it never mixes with
real data, and deletes its rows afterwards. If Kafka or PostgreSQL is not running,
the tests are skipped instead of failing.
"""

import json
import time
import uuid
from datetime import datetime, timedelta, timezone

import psycopg2
import pytest
from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient

from src import config

pytestmark = pytest.mark.integration

WAIT_SECONDS = 20  # how long to wait for the consumer to save readings


# ---------- Fixtures: things each test needs ----------

@pytest.fixture
def db():
    try:
        conn = psycopg2.connect(
            host=config.POSTGRES_HOST, port=config.POSTGRES_PORT, dbname=config.POSTGRES_DB,
            user=config.POSTGRES_USER, password=config.POSTGRES_PASSWORD, connect_timeout=3,
        )
    except psycopg2.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    conn.autocommit = True
    yield conn
    conn.close()


@pytest.fixture
def producer():
    try:
        AdminClient({"bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS}).list_topics(timeout=5)
    except KafkaException as exc:
        pytest.skip(f"Kafka not reachable: {exc}")
    p = Producer({"bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS})
    yield p
    p.flush(10)


@pytest.fixture
def test_customer(db):
    """A unique customer id for this test; its rows are deleted when the test ends."""
    customer_id = f"TEST-{uuid.uuid4().hex[:8]}"
    yield customer_id
    with db.cursor() as cur:
        cur.execute("DELETE FROM heartbeats WHERE customer_id = %s", (customer_id,))


# ---------- Helpers ----------

def send(producer, customer_id: str, payload) -> None:
    """Send one message. payload can be a dict (sent as JSON) or raw bytes."""
    value = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
    producer.produce(config.KAFKA_TOPIC, key=customer_id.encode(), value=value)


def reading(customer_id: str, heart_rate, seconds_offset: int = 0) -> dict:
    ts = datetime.now(timezone.utc) + timedelta(seconds=seconds_offset)
    return {"customer_id": customer_id, "timestamp": ts.isoformat(), "heart_rate": heart_rate}


def wait_for_rows(db, customer_id: str, expected_count: int) -> list[tuple]:
    """Check the database every half second until the rows appear (or time runs out)."""
    deadline = time.monotonic() + WAIT_SECONDS
    rows = []
    while time.monotonic() < deadline:
        with db.cursor() as cur:
            cur.execute(
                "SELECT heart_rate, status FROM heartbeats "
                "WHERE customer_id = %s ORDER BY recorded_at",
                (customer_id,),
            )
            rows = cur.fetchall()
        if len(rows) >= expected_count:
            return rows
        time.sleep(0.5)
    pytest.fail(
        f"Only {len(rows)} of {expected_count} readings reached PostgreSQL after {WAIT_SECONDS}s. "
        "Is the consumer running? (python -m src.consumer)"
    )


# ---------- Tests ----------

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