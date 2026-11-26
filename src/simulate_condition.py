"""
Simulate a heart condition for one customer, to test alerts end to end.

Sends a series of readings straight to Kafka, one per second.

Examples (from the project root, venv active):
    python -m src.simulate_condition --condition high            # tachycardia episode
    python -m src.simulate_condition --condition low --count 6   # bradycardia episode
    python -m src.simulate_condition --condition normal          # recovery
    python -m src.simulate_condition --condition high --then-recover

The default customer is CUST-999, which the normal producer never uses, so its regular
readings can't interrupt the simulated streak.
"""

import argparse
import json
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

from src import config

RANGES = {
    "high": (130, 170),
    "low": (38, 46),
    "normal": (65, 85),
}


def send_series(producer: Producer, customer_id: str, condition: str, count: int, interval: float) -> None:
    low, high = RANGES[condition]
    for i in range(count):
        reading = {
            "customer_id": customer_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "heart_rate": random.randint(low, high),
        }
        producer.produce(config.KAFKA_TOPIC, key=customer_id.encode(), value=json.dumps(reading).encode())
        producer.poll(0)
        print(f"[{condition:>6}] {i + 1}/{count}  {customer_id}  {reading['heart_rate']} bpm")
        time.sleep(interval)
    producer.flush(10)


def main() -> None:
    parser = argparse.ArgumentParser(description="Send a simulated heart condition to Kafka.")
    parser.add_argument("--condition", choices=RANGES, default="high")
    parser.add_argument("--customer", default="CUST-999")
    parser.add_argument("--count", type=int, default=5, help="number of readings to send")
    parser.add_argument("--interval", type=float, default=1.0, help="seconds between readings")
    parser.add_argument("--then-recover", action="store_true",
                        help="afterwards, send normal readings so a RECOVERED alert is raised")
    args = parser.parse_args()

    producer = Producer({"bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS})
    send_series(producer, args.customer, args.condition, args.count, args.interval)
    if args.then_recover and args.condition != "normal":
        send_series(producer, args.customer, "normal", config.ALERT_RECOVERY_READINGS + 1, args.interval)
    print("Done.")


if __name__ == "__main__":
    main()
