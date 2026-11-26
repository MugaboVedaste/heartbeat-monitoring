"""
Alert service: watches the heartbeat stream and emails when a customer is in a bad condition.

It is a SECOND Kafka consumer group ("heartbeat-alerts"). It reads the same messages as the
database consumer, completely independently: Kafka gives every group every message.

For each reading it:
  1. validates it (same rules as the database consumer)
  2. passes it to the AlertDetector (see alerting.py for the rules)
  3. if an alert is raised: sends an email, saves it to the alerts table, and logs it

Run from the project root (venv active, Docker services running):
    python -m src.alert_service
Stop with Ctrl+C.
"""

import logging

import psycopg2
from confluent_kafka import Consumer, KafkaError

from src import config
from src.alerting import Alert, AlertDetector, send_email
from src.consumer import Database
from src.validation import InvalidReadingError, parse_message, validate_reading

logger = logging.getLogger("alert_service")

GROUP_ID = "heartbeat-alerts"

INSERT_ALERT_SQL = """
    INSERT INTO alerts (customer_id, alert_type, heart_rates, started_at, triggered_at, email_sent)
    VALUES (%s, %s, %s, %s, %s, %s)
"""


def create_consumer() -> Consumer:
    consumer = Consumer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        # First start: only watch NEW readings. Alerting on hours of old data would spam emails.
        "auto.offset.reset": "latest",
        # Losing or repeating an alert check on a crash is acceptable here, so auto-commit is fine
        "enable.auto.commit": True,
    })
    consumer.subscribe([config.KAFKA_TOPIC])
    return consumer


def save_alert(db: Database, alert: Alert, email_sent: bool) -> None:
    """Save the alert. If the database is down, log it and carry on: the email matters more."""
    try:
        db.ensure_connected()
        with db.conn, db.conn.cursor() as cur:
            cur.execute(INSERT_ALERT_SQL, (
                alert.customer_id, alert.alert_type, alert.heart_rates,
                alert.started_at, alert.triggered_at, email_sent,
            ))
    except psycopg2.Error as exc:
        db.close()
        logger.error("Could not save alert for %s to the database: %s", alert.customer_id, exc)


def handle_alert(db: Database, alert: Alert) -> None:
    email_sent = send_email(alert)
    save_alert(db, alert, email_sent)
    level = logging.INFO if alert.alert_type == "recovered" else logging.WARNING
    logger.log(level, "%s %s: readings %s (email %s)",
               alert.alert_type.upper(), alert.customer_id, alert.heart_rates,
               "sent" if email_sent else "FAILED")


def run() -> None:
    consumer = create_consumer()
    detector = AlertDetector()
    db = Database()
    logger.info(
        "Watching topic '%s' as group '%s': alert after %d abnormal readings in a row, "
        "emails to %s via %s:%d",
        config.KAFKA_TOPIC, GROUP_ID, detector.consecutive,
        config.ALERT_EMAIL_TO, config.SMTP_HOST, config.SMTP_PORT,
    )
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error("Kafka error: %s", msg.error())
                continue
            try:
                row = validate_reading(parse_message(msg.value()))
            except InvalidReadingError:
                continue  # the database consumer already logs invalid messages
            alert = detector.process(row)
            if alert:
                handle_alert(db, alert)
    except KeyboardInterrupt:
        logger.info("Stopping...")
    finally:
        consumer.close()
        db.close()
        logger.info("Alert service stopped")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    run()
