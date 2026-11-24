"""
Synthetic heart rate data generator.

Makes readings that behave like real heart rate monitors:
  - each customer has their own resting heart rate (e.g. 58 bpm for one, 79 bpm for another)
  - readings drift a little from second to second instead of jumping randomly
  - the rate is gently pulled back toward the customer's resting rate
  - occasionally an anomaly happens (a sudden spike or drop), so the consumer has something to detect

This module knows nothing about Kafka. The producer imports it and sends what it generates.

Run on its own to preview readings:
    python -m src.data_generator
"""

import logging
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterator

from src import config

logger = logging.getLogger(__name__)


@dataclass
class Customer:
    customer_id: str
    resting_rate: float  # this person's normal heart rate
    current_rate: float  # where their heart rate is right now


def create_customers(count: int) -> list[Customer]:
    """Create `count` fake customers, each with a personal resting heart rate."""
    customers = []
    for i in range(1, count + 1):
        resting = random.uniform(58, 82)
        customers.append(
            Customer(customer_id=f"CUST-{i:03d}", resting_rate=resting, current_rate=resting)
        )
    return customers


def next_heart_rate(customer: Customer, anomaly_probability: float) -> int:
    """
    Return the customer's next reading.

    Normal case: small random change + a pull back toward the resting rate
    (this is called a "mean-reverting random walk").
    Rare case: a one-off anomaly that does not change the customer's ongoing rate.
    """
    if random.random() < anomaly_probability:
        if random.random() < 0.5:
            return random.randint(130, 185)  # spike, e.g. tachycardia
        return random.randint(35, 48)        # drop, e.g. bradycardia

    drift = random.gauss(0, 2)                                      # small natural variation
    pull_back = 0.1 * (customer.resting_rate - customer.current_rate)  # tendency to return to normal
    customer.current_rate += drift + pull_back
    customer.current_rate = max(45, min(110, customer.current_rate))  # keep normal readings realistic
    return round(customer.current_rate)


def generate_reading(customer: Customer, anomaly_probability: float) -> dict:
    """Build one reading in the format the rest of the pipeline expects."""
    return {
        "customer_id": customer.customer_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "heart_rate": next_heart_rate(customer, anomaly_probability),
    }


def stream_readings(
    num_customers: int = config.NUM_CUSTOMERS,
    interval_seconds: float = config.SEND_INTERVAL_SECONDS,
    anomaly_probability: float = config.ANOMALY_PROBABILITY,
) -> Iterator[dict]:
    """
    Yield readings forever: one reading per customer, then wait `interval_seconds`, then repeat.

    This is a Python generator, so the caller processes each reading as it is produced:
        for reading in stream_readings():
            send(reading)
    """
    customers = create_customers(num_customers)
    logger.info("Simulating %d customers, one round every %.1fs", num_customers, interval_seconds)

    while True:
        for customer in customers:
            yield generate_reading(customer, anomaly_probability)
        time.sleep(interval_seconds)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        for reading in stream_readings():
            print(reading)
    except KeyboardInterrupt:
        logger.info("Stopped by user")