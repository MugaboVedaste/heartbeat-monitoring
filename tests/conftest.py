"""
Shared pytest fixtures for the end-to-end tests.

pytest loads this file automatically, so any test can ask for `db`, `producer`
or `test_customer` just by naming it as a function argument.
"""

import uuid

import psycopg2
import pytest
from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient

from src import config
from tests.helpers import connect_db


@pytest.fixture
def db():
    """A database connection. Skips the test if PostgreSQL isn't running."""
    try:
        conn = connect_db()
    except psycopg2.OperationalError as exc:
        pytest.skip(f"PostgreSQL not reachable: {exc}")
    yield conn
    conn.close()


@pytest.fixture
def producer():
    """A Kafka producer. Skips the test if Kafka isn't running."""
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
    try:
        conn = connect_db()  # fresh connection, in case the test restarted the database
    except psycopg2.OperationalError:
        return
    with conn, conn.cursor() as cur:
        cur.execute("DELETE FROM heartbeats WHERE customer_id = %s", (customer_id,))
    conn.close()