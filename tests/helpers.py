"""Small helper functions shared by the end-to-end tests."""

import json
import time
from datetime import datetime, timedelta, timezone

import psycopg2
import pytest

from src import config


def connect_db(timeout: int = 3):
    conn = psycopg2.connect(
        host=config.POSTGRES_HOST, port=config.POSTGRES_PORT, dbname=config.POSTGRES_DB,
        user=config.POSTGRES_USER, password=config.POSTGRES_PASSWORD, connect_timeout=timeout,
    )
    conn.autocommit = True
    return conn


def send(producer, customer_id: str, payload) -> None:
    """Send one message. payload can be a dict (sent as JSON) or raw bytes."""
    value = payload if isinstance(payload, bytes) else json.dumps(payload).encode()
    producer.produce(config.KAFKA_TOPIC, key=customer_id.encode(), value=value)


def reading(customer_id: str, heart_rate, seconds_offset: int = 0) -> dict:
    ts = datetime.now(timezone.utc) + timedelta(seconds=seconds_offset)
    return {"customer_id": customer_id, "timestamp": ts.isoformat(), "heart_rate": heart_rate}


def wait_for_rows(conn, customer_id: str, expected_count: int, timeout: float = 20) -> list[tuple]:
    """Check the database every half second until the rows appear (or time runs out)."""
    deadline = time.monotonic() + timeout
    rows = []
    while time.monotonic() < deadline:
        with conn.cursor() as cur:
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
        f"Only {len(rows)} of {expected_count} readings reached PostgreSQL after {timeout}s. "
        "Is the consumer running? (python -m src.consumer)"
    )