"""
Kafka consumer: reads heart rate readings, validates them, and saves valid ones to PostgreSQL.

How it stays reliable:
  - readings are saved in small batches (faster than one INSERT per reading)
  - Kafka offsets are committed only AFTER the batch is saved in PostgreSQL,
    so if the consumer crashes, unsaved readings are read again on restart (nothing is lost)
  - invalid messages are logged and skipped, so one bad message can't stop the pipeline
  - if PostgreSQL is down, the consumer waits and retries (1s, 2s, 4s ... up to 30s)
    instead of crashing; while it waits, it pauses reading from Kafka

Run from the project root (venv active, Kafka and PostgreSQL running):
    python -m src.consumer
Stop with Ctrl+C.
"""

import logging
import time
from typing import Callable

import psycopg2
from confluent_kafka import Consumer, KafkaError, TopicPartition
from psycopg2.extras import execute_values

from src import config
from src.validation import InvalidReadingError, parse_message, validate_reading

logger = logging.getLogger("consumer")

GROUP_ID = "heartbeat-db-writer"
BATCH_SIZE = 50                # save after this many valid readings...
FLUSH_INTERVAL_SECONDS = 2.0   # ...or after this long, whichever comes first

MAX_BACKOFF_SECONDS = 30       # longest wait between database retries
SHUTDOWN_SAVE_ATTEMPTS = 3     # when stopping, don't retry forever

# Errors that mean "the database can't be reached right now" -> worth retrying.
# Other errors (e.g. a bug in our SQL) would fail every time, so they are not retried.
RETRYABLE_DB_ERRORS = (psycopg2.OperationalError, psycopg2.InterfaceError)

# ON CONFLICT DO NOTHING: a reading Kafka delivers twice (e.g. after a crash) is skipped, not duplicated.
# This relies on the unique index on (customer_id, recorded_at) from sql/schema.sql.
INSERT_SQL = """
    INSERT INTO heartbeats (customer_id, recorded_at, heart_rate, status)
    VALUES %s
    ON CONFLICT DO NOTHING
"""
INSERT_ONE_SQL = INSERT_SQL.replace("VALUES %s", "VALUES (%s, %s, %s, %s)")

# Errors caused by the DATA, not the connection: retrying the same rows would fail forever.
REJECTED_ROW_ERRORS = (psycopg2.DataError, psycopg2.IntegrityError)


class Database:
    """Holds the PostgreSQL connection and reconnects when it has been lost."""

    def __init__(self):
        self.conn = None

    def ensure_connected(self) -> None:
        if self.conn is None or self.conn.closed:
            self.conn = psycopg2.connect(
                host=config.POSTGRES_HOST,
                port=config.POSTGRES_PORT,
                dbname=config.POSTGRES_DB,
                user=config.POSTGRES_USER,
                password=config.POSTGRES_PASSWORD,
                connect_timeout=5,
            )

    def save(self, rows: list[dict]) -> None:
        """Insert all rows in one transaction: either all are saved or none are."""
        values = [(r["customer_id"], r["recorded_at"], r["heart_rate"], r["status"]) for r in rows]
        with self.conn:  # commits on success, rolls back on error
            with self.conn.cursor() as cur:
                execute_values(cur, INSERT_SQL, values)

    def save_each(self, rows: list[dict]) -> int:
        """
        Insert rows one at a time, skipping any the database rejects. Returns how many were rejected.

        Used when a batch contains a row PostgreSQL refuses (a data error). Without this, that one
        row would make the whole batch fail on every restart and block the pipeline.
        """
        rejected = 0
        for r in rows:
            try:
                with self.conn:  # one transaction per row
                    with self.conn.cursor() as cur:
                        cur.execute(INSERT_ONE_SQL,
                                    (r["customer_id"], r["recorded_at"], r["heart_rate"], r["status"]))
            except REJECTED_ROW_ERRORS as exc:
                rejected += 1
                logger.error("Database rejected a reading and it was skipped (%s): %s", r, exc)
        return rejected

    def close(self) -> None:
        if self.conn is not None:
            try:
                self.conn.close()
            except psycopg2.Error:
                pass
        self.conn = None


def backoff_delay(attempt: int) -> int:
    """Seconds to wait before retry number `attempt`: 1, 2, 4, 8, 16, 30, 30, ..."""
    return min(2 ** (attempt - 1), MAX_BACKOFF_SECONDS)


def save_with_retry(db, rows: list[dict], wait: Callable[[float], None],
                    max_attempts: int | None = None) -> None:
    """
    Save rows, retrying with exponential backoff while the database is unavailable.

    `wait` is the function used to pause between retries. The consumer passes one that keeps
    the Kafka connection alive; tests pass a fake one so they don't really wait.
    `max_attempts=None` means keep trying until it works.
    """
    if not rows:
        return
    attempt = 0
    one_by_one = False
    while True:
        try:
            db.ensure_connected()
            if one_by_one:
                db.save_each(rows)  # rows saved before a connection drop are skipped by ON CONFLICT
            else:
                db.save(rows)
            if attempt:
                logger.info("Database is back: saved the batch after %d failed attempt(s)", attempt)
            return
        except REJECTED_ROW_ERRORS:
            # The batch contains a row the database refuses. Save the good rows one by one
            # and skip the bad ones, instead of failing (and replaying) the whole batch forever.
            one_by_one = True
        except RETRYABLE_DB_ERRORS as exc:
            attempt += 1
            db.close()  # the old connection may be broken; reconnect on the next try
            if max_attempts is not None and attempt >= max_attempts:
                raise
            delay = backoff_delay(attempt)
            reason = str(exc).strip().splitlines()[0] if str(exc).strip() else type(exc).__name__
            logger.warning("Database unavailable (%s). Retry %d in %ds...", reason, attempt, delay)
            wait(delay)


def create_consumer() -> Consumer:
    consumer = Consumer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",  # first run: start from the oldest message
        "enable.auto.commit": False,      # we commit ourselves, after saving to the database
    })
    consumer.subscribe([config.KAFKA_TOPIC])
    return consumer


def make_paused_wait(consumer: Consumer) -> Callable[[float], None]:
    """
    Return a wait function that pauses Kafka reading but keeps polling.

    Why not just time.sleep()? Kafka expects a consumer to poll regularly; one that goes
    silent for too long is considered dead and removed from its group. Pausing means
    poll() keeps us "alive" without handing us new messages.
    """
    def wait(seconds: float) -> None:
        partitions = consumer.assignment()
        consumer.pause(partitions)
        try:
            deadline = time.monotonic() + seconds
            while (remaining := deadline - time.monotonic()) > 0:
                msg = consumer.poll(min(remaining, 1.0))
                if msg is not None and not msg.error():
                    # Shouldn't happen while paused, but if it does, rewind so it isn't skipped
                    consumer.seek(TopicPartition(msg.topic(), msg.partition(), msg.offset()))
        finally:
            consumer.resume(partitions)
    return wait


def run() -> None:
    consumer = create_consumer()
    db = Database()
    wait = make_paused_wait(consumer)
    logger.info("Listening on topic '%s' as group '%s'", config.KAFKA_TOPIC, GROUP_ID)

    batch: list[dict] = []
    has_uncommitted = False  # messages read since the last offset commit (valid or not)
    last_flush = time.monotonic()
    stats = {"saved": 0, "invalid": 0, "low": 0, "high": 0}

    def flush(max_attempts: int | None = None) -> None:
        nonlocal has_uncommitted, last_flush
        if has_uncommitted:
            save_with_retry(db, batch, wait, max_attempts)  # 1. save to PostgreSQL (retries if down)
            consumer.commit(asynchronous=False)             # 2. then tell Kafka "done up to here"
            stats["saved"] += len(batch)
            if batch:
                logger.info("Saved %d readings (total saved=%d, invalid=%d, low=%d, high=%d)",
                            len(batch), stats["saved"], stats["invalid"], stats["low"], stats["high"])
            batch.clear()
            has_uncommitted = False
        last_flush = time.monotonic()

    try:
        while True:
            msg = consumer.poll(1.0)  # wait up to 1 second for a message

            if msg is not None:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        logger.error("Kafka error: %s", msg.error())
                    continue

                has_uncommitted = True
                try:
                    row = validate_reading(parse_message(msg.value()))
                except InvalidReadingError as exc:
                    stats["invalid"] += 1
                    logger.warning("Skipping invalid message (partition %d, offset %d): %s",
                                   msg.partition(), msg.offset(), exc)
                else:
                    batch.append(row)
                    if row["status"] != "normal":
                        stats[row["status"]] += 1
                        logger.warning("ANOMALY %s: %s bpm (%s)",
                                       row["customer_id"], row["heart_rate"], row["status"])

            if len(batch) >= BATCH_SIZE or time.monotonic() - last_flush >= FLUSH_INTERVAL_SECONDS:
                flush()

    except KeyboardInterrupt:
        logger.info("Stopping...")
    finally:
        try:
            flush(max_attempts=SHUTDOWN_SAVE_ATTEMPTS)  # save what's left, but don't hang forever
        except Exception as exc:
            # Offsets were NOT committed, so these readings will be read again on the next start
            logger.error("Could not save the last %d readings (%s). They will be re-read on restart.",
                         len(batch), exc)
        finally:
            consumer.close()
            db.close()
            logger.info("Consumer stopped. Saved=%d, invalid=%d", stats["saved"], stats["invalid"])


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    run()