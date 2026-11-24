"""
Kafka producer: sends generated heart rate readings to the "heartbeats" topic, continuously.

Run from the project root (venv active, Kafka running):
    python -m src.producer
Stop with Ctrl+C.
"""

import json
import logging

from confluent_kafka import KafkaException, Producer

from src import config
from src.data_generator import stream_readings

logger = logging.getLogger("producer")

LOG_EVERY_N_MESSAGES = 50


def create_producer() -> Producer:
    return Producer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "heartbeat-producer",
        "acks": "all",                 # wait until Kafka has safely stored the message
        "enable.idempotence": True,    # retries never create duplicate messages
        "linger.ms": 10,               # wait up to 10 ms to send messages in small batches
    })


def delivery_report(err, msg) -> None:
    """Called by Kafka once per message to say whether it was stored."""
    if err is not None:
        logger.error("Delivery failed for key=%s: %s", msg.key(), err)


def send_reading(producer: Producer, reading: dict) -> None:
    payload = json.dumps(reading).encode("utf-8")
    key = reading["customer_id"].encode("utf-8")  # same customer -> same partition -> kept in order
    while True:
        try:
            producer.produce(config.KAFKA_TOPIC, key=key, value=payload, callback=delivery_report)
            break
        except BufferError:
            # The local send queue is full: let Kafka catch up, then try again
            logger.warning("Local queue full, waiting...")
            producer.poll(1)
    producer.poll(0)  # trigger delivery_report callbacks for finished messages


def run() -> None:
    producer = create_producer()
    logger.info("Sending readings to topic '%s' at %s",
                config.KAFKA_TOPIC, config.KAFKA_BOOTSTRAP_SERVERS)
    sent = 0
    try:
        for reading in stream_readings():
            send_reading(producer, reading)
            sent += 1
            if reading["heart_rate"] < config.LOW_BPM_THRESHOLD or \
               reading["heart_rate"] > config.HIGH_BPM_THRESHOLD:
                logger.info("Anomaly generated: %s", reading)
            if sent % LOG_EVERY_N_MESSAGES == 0:
                logger.info("Sent %d readings so far", sent)
    except KeyboardInterrupt:
        logger.info("Stopping...")
    except KafkaException as exc:
        logger.error("Kafka error: %s", exc)
    finally:
        remaining = producer.flush(10)  # send anything still in the local queue
        if remaining:
            logger.warning("%d messages were not delivered", remaining)
        logger.info("Producer stopped after sending %d readings", sent)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    run()