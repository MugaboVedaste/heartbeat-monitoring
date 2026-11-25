"""
Recovery test: does the pipeline survive PostgreSQL going down?

What it does:
  1. stops the PostgreSQL container
  2. sends readings to Kafka while the database is down
  3. starts PostgreSQL again
  4. checks that every reading still ends up in the database, in order

Before running:
  1. docker compose up -d
  2. python -m src.consumer      (in another terminal)
Then, from the project root:
  pytest -m recovery -v -s

This test really stops and starts your postgres container, so it takes about a minute.
Watch the consumer terminal while it runs: you'll see the retries happen.
"""

import shutil
import subprocess
import time

import psycopg2
import pytest

from tests.helpers import connect_db, reading, send, wait_for_rows

pytestmark = pytest.mark.recovery

DOWNTIME_SECONDS = 8      # how long the database stays down
STARTUP_TIMEOUT = 60      # how long to wait for PostgreSQL to accept connections again
RESULT_TIMEOUT = 60       # consumer may be in a 16s backoff when the database returns


def docker_compose(*args) -> None:
    subprocess.run(["docker", "compose", *args], check=True, capture_output=True, timeout=60)


def wait_until_postgres_accepts_connections() -> None:
    deadline = time.monotonic() + STARTUP_TIMEOUT
    while time.monotonic() < deadline:
        try:
            connect_db(timeout=2).close()
            return
        except psycopg2.OperationalError:
            time.sleep(1)
    pytest.fail("PostgreSQL did not come back up in time")


def test_no_readings_are_lost_while_database_is_down(producer, db, test_customer):
    if shutil.which("docker") is None:
        pytest.skip("docker command not found")

    readings_to_send = 10
    db.close()  # this connection would break anyway when the database stops

    print(f"\nStopping PostgreSQL for {DOWNTIME_SECONDS}s...")
    docker_compose("stop", "postgres")
    try:
        # Database is down: send readings anyway. They wait safely in Kafka.
        for i in range(readings_to_send):
            send(producer, test_customer, reading(test_customer, 70 + i, seconds_offset=i))
        producer.flush(10)
        time.sleep(DOWNTIME_SECONDS)  # the consumer should be retrying now, not crashing
    finally:
        print("Starting PostgreSQL again...")
        docker_compose("start", "postgres")  # always restart it, even if something above failed
        wait_until_postgres_accepts_connections()
        print("PostgreSQL is back. Waiting for the consumer to catch up...")

    conn = connect_db()
    try:
        rows = wait_for_rows(conn, test_customer, expected_count=readings_to_send,
                             timeout=RESULT_TIMEOUT)
        assert len(rows) == readings_to_send, "some readings were lost or duplicated"
        assert [hr for hr, _ in rows] == [70 + i for i in range(readings_to_send)]  # in order
    finally:
        conn.close()