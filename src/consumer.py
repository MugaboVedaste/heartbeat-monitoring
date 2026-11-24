"""
Kafka consumer: reads heart rate readings, validates them, and saves valid ones to PostgreSQL.

How it stays reliable:
  - readings are saved in small batches (faster than one INSERT per reading)
  - Kafka offsets are committed only AFTER the batch is saved in PostgreSQL,
    so if the consumer crashes, unsaved readings are read again on restart (nothing is lost)
  - invalid messages are logged and skipped, so one bad message can't stop the pipeline

Run from the project root (venv active, Kafka and PostgreSQL running):
    python -m src.consumer
Stop with Ctrl+C.
"""

import logging
import time

import psycopg2
from confluent_kafka import Consumer, KafkaError
from psycopg2.extras import execute_values

from src import config
from src.validation import InvalidReadingError, parse_message, validate_reading

logger = logging.getLogger("consumer")

GROUP_ID = "heartbeat-db-writer"
BATCH_SIZE = 50                # save after this many valid readings...
FLUSH_INTERVAL_SECONDS = 2.0   # ...or after this long, whichever comes first

INSERT_SQL = """
    INSERT INTO heartbeats (customer_id, recorded_at, heart_rate, status)
    VALUES %s
"""


def create_consumer() -> Consumer:
    consumer = Consumer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",  # first run: start from the oldest message
        "enable.auto.commit": False,      # we commit ourselves, after saving to the database
    })
    consumer.subscribe([config.KAFKA_TOPIC])
    return consumer


def connect_db():
    return psycopg2.connect(
        host=config.POSTGRES_HOST,
        port=config.POSTGRES_PORT,
        dbname=config.POSTGRES_DB,
        user=config.POSTGRES_USER,
        password=config.POSTGRES_PASSWORD,
    )


def save_batch(conn, rows: list[dict]) -> None:
    """Insert all rows in one transaction: either all are saved or none are."""
    if not rows:
        return
    values = [(r["customer_id"], r["recorded_at"], r["heart_rate"], r["status"]) for r in rows]
    with conn:  # commits on success, rolls back on error
        with conn.cursor() as cur:
            execute_values(cur, INSERT_SQL, values)


def run() -> None:
    consumer = create_consumer()
    conn = connect_db()
    logger.info("Listening on topic '%s' as group '%s'", config.KAFKA_TOPIC, GROUP_ID)

    batch: list[dict] = []
    has_uncommitted = False  # messages read since the last offset commit (valid or not)
    last_flush = time.monotonic()
    stats = {"saved": 0, "invalid": 0, "low": 0, "high": 0}

    def flush() -> None:
        nonlocal has_uncommitted, last_flush
        if has_uncommitted:
            save_batch(conn, batch)                   # 1. save to PostgreSQL
            consumer.commit(asynchronous=False)       # 2. then tell Kafka "done up to here"
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
            flush()  # save whatever is left before exiting
        finally:
            consumer.close()
            conn.close()
            logger.info("Consumer stopped. Saved=%d, invalid=%d", stats["saved"], stats["invalid"])


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    run()