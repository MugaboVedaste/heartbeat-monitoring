"""
Unit tests for the consumer's database retry logic.

They use a fake database that fails a chosen number of times, and a fake wait function
that records the delays instead of really sleeping, so they run instantly.
No Kafka or PostgreSQL needed.
"""

import psycopg2
import pytest

from src.consumer import backoff_delay, save_with_retry

ROWS = [{"customer_id": "CUST-001", "recorded_at": None, "heart_rate": 72, "status": "normal"}]


class FakeDatabase:
    """Pretends to be the Database class. Fails `failures` times, then works."""

    def __init__(self, failures=0, error=psycopg2.OperationalError("connection refused")):
        self.failures_left = failures
        self.error = error
        self.saved = []
        self.close_calls = 0

    def ensure_connected(self):
        pass

    def save(self, rows):
        if self.failures_left > 0:
            self.failures_left -= 1
            raise self.error
        self.saved.extend(rows)

    def save_each(self, rows):
        self.saved_one_by_one = list(rows)
        self.saved.extend(rows)
        return 0

    def close(self):
        self.close_calls += 1


class FakeWait:
    """Records how long the consumer asked to wait, without waiting."""

    def __init__(self):
        self.delays = []

    def __call__(self, seconds):
        self.delays.append(seconds)


def test_waits_grow_exponentially_and_stop_at_the_maximum():
    assert [backoff_delay(n) for n in range(1, 9)] == [1, 2, 4, 8, 16, 30, 30, 30]


def test_saves_immediately_when_database_is_fine():
    db, wait = FakeDatabase(failures=0), FakeWait()

    save_with_retry(db, ROWS, wait)

    assert db.saved == ROWS
    assert wait.delays == []


def test_retries_until_database_comes_back():
    db, wait = FakeDatabase(failures=3), FakeWait()

    save_with_retry(db, ROWS, wait)

    assert db.saved == ROWS             # eventually saved
    assert wait.delays == [1, 2, 4]     # waited longer each time
    assert db.close_calls == 3          # dropped the broken connection before each retry


def test_lost_connection_error_is_also_retried():
    db, wait = FakeDatabase(failures=1, error=psycopg2.InterfaceError("connection already closed")), FakeWait()

    save_with_retry(db, ROWS, wait)

    assert db.saved == ROWS


def test_gives_up_after_max_attempts():
    db, wait = FakeDatabase(failures=10), FakeWait()

    with pytest.raises(psycopg2.OperationalError):
        save_with_retry(db, ROWS, wait, max_attempts=3)

    assert db.saved == []
    assert wait.delays == [1, 2]        # 3 attempts = 2 waits in between


def test_errors_that_retrying_cannot_fix_are_not_retried():
    db, wait = FakeDatabase(failures=1, error=ValueError("bug in our code")), FakeWait()

    with pytest.raises(ValueError):
        save_with_retry(db, ROWS, wait)

    assert wait.delays == []            # failed straight away, no pointless retries


def test_batch_with_a_rejected_row_falls_back_to_saving_one_by_one():
    db = FakeDatabase(failures=1, error=psycopg2.DataError("value out of range"))
    wait = FakeWait()

    save_with_retry(db, ROWS, wait)

    assert db.saved_one_by_one == ROWS  # switched to row-by-row instead of failing forever
    assert wait.delays == []            # not a connection problem, so no waiting


def test_connection_lost_during_row_by_row_fallback_is_still_retried():
    class DropsDuringFallback(FakeDatabase):
        def __init__(self):
            super().__init__(failures=1, error=psycopg2.IntegrityError("check violation"))
            self.fallback_calls = 0

        def save_each(self, rows):
            self.fallback_calls += 1
            if self.fallback_calls == 1:
                raise psycopg2.OperationalError("server closed the connection")
            return super().save_each(rows)

    db, wait = DropsDuringFallback(), FakeWait()

    save_with_retry(db, ROWS, wait)

    assert db.fallback_calls == 2       # retried the fallback after the connection dropped
    assert db.saved == ROWS
    assert wait.delays == [1]


def test_empty_batch_does_not_touch_the_database():
    db, wait = FakeDatabase(failures=5), FakeWait()

    save_with_retry(db, [], wait)

    assert wait.delays == []