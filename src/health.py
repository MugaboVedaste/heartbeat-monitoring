"""
Pipeline health checks and the rules for when to notify.

Kept separate from health_monitor.py (the loop that runs every 30s) so the notification
rules can be unit-tested without Kafka or PostgreSQL.

Checks:
  - Kafka broker reachable and topic exists
  - PostgreSQL reachable
  - New data arriving (latest saved reading is recent)
  - Each consumer group running (has members) and keeping up (lag below a limit)
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from email.message import EmailMessage

import psycopg2
from confluent_kafka import Consumer, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient

from src import config

logger = logging.getLogger(__name__)

DB_CONSUMER_GROUP = "heartbeat-db-writer"
ALERT_GROUP = "heartbeat-alerts"


@dataclass
class CheckResult:
    name: str      # stable id, e.g. "kafka"
    label: str     # human-friendly name for emails and logs
    ok: bool
    detail: str


@dataclass
class Notification:
    kind: str      # "problem" or "resolved"
    result: CheckResult


# ---------- When to notify ----------

class HealthTracker:
    """
    Turns check results into notifications:
      - "problem" when a check has failed `failures_before_alert` times in a row
        (one failed check could be a blip; two in a row is a real problem)
      - "resolved" when a check that was reported as a problem passes again
    Each problem is reported once, not on every check.
    """

    def __init__(self, failures_before_alert: int = config.HEALTH_FAILURES_BEFORE_ALERT):
        self.failures_before_alert = failures_before_alert
        self.fail_counts: dict[str, int] = {}
        self.reported: set[str] = set()   # checks currently reported as problems

    def update(self, results: list[CheckResult]) -> list[Notification]:
        notifications = []
        for r in results:
            if r.ok:
                self.fail_counts[r.name] = 0
                if r.name in self.reported:
                    self.reported.discard(r.name)
                    notifications.append(Notification("resolved", r))
            else:
                self.fail_counts[r.name] = self.fail_counts.get(r.name, 0) + 1
                if self.fail_counts[r.name] >= self.failures_before_alert and r.name not in self.reported:
                    self.reported.add(r.name)
                    notifications.append(Notification("problem", r))
        return notifications


def build_health_email(notifications: list[Notification], all_results: list[CheckResult]) -> EmailMessage:
    problems = [n.result for n in notifications if n.kind == "problem"]
    resolved = [n.result for n in notifications if n.kind == "resolved"]

    if problems:
        subject = "PIPELINE PROBLEM: " + ", ".join(r.label for r in problems)
    else:
        subject = "PIPELINE RESOLVED: " + ", ".join(r.label for r in resolved)

    lines = []
    if problems:
        lines.append("New problems:")
        lines += [f"  ✗ {r.label}: {r.detail}" for r in problems]
        lines.append("")
    if resolved:
        lines.append("Resolved:")
        lines += [f"  ✓ {r.label}: {r.detail}" for r in resolved]
        lines.append("")
    lines.append("Current status of all checks:")
    lines += [f"  {'✓' if r.ok else '✗'} {r.label}: {r.detail}" for r in all_results]
    lines.append("")
    lines.append(f"Checked at {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = config.ALERT_EMAIL_FROM
    msg["To"] = config.ALERT_EMAIL_TO
    msg.set_content("\n".join(lines) + "\n\n-- Heartbeat Monitoring System (health monitor)\n")
    return msg


# ---------- The checks ----------

def check_kafka(admin: AdminClient) -> CheckResult:
    try:
        metadata = admin.list_topics(timeout=5)
    except KafkaException as exc:
        return CheckResult("kafka", "Kafka broker", False, f"not reachable ({exc})")
    topic = metadata.topics.get(config.KAFKA_TOPIC)
    if topic is None:
        return CheckResult("kafka", "Kafka broker", False, f"topic '{config.KAFKA_TOPIC}' does not exist")
    return CheckResult("kafka", "Kafka broker", True,
                       f"reachable, topic '{config.KAFKA_TOPIC}' has {len(topic.partitions)} partitions")


def check_postgres_and_freshness() -> list[CheckResult]:
    """One connection for both checks. If PostgreSQL is down, freshness can't be checked, so it's skipped."""
    try:
        conn = psycopg2.connect(
            host=config.POSTGRES_HOST, port=config.POSTGRES_PORT, dbname=config.POSTGRES_DB,
            user=config.POSTGRES_USER, password=config.POSTGRES_PASSWORD, connect_timeout=5,
        )
    except psycopg2.OperationalError as exc:
        reason = str(exc).strip().splitlines()[0]
        return [CheckResult("postgres", "PostgreSQL", False, f"not reachable ({reason})")]

    try:
        with conn.cursor() as cur:
            # ORDER BY id uses the primary key index, so this is fast even on a big table
            cur.execute("SELECT EXTRACT(EPOCH FROM NOW() - ingested_at) "
                        "FROM heartbeats ORDER BY id DESC LIMIT 1")
            row = cur.fetchone()
    finally:
        conn.close()

    postgres = CheckResult("postgres", "PostgreSQL", True, "reachable")
    limit = config.HEALTH_MAX_DATA_AGE_SECONDS
    if row is None:
        fresh = CheckResult("data_freshness", "New data arriving", False, "no readings saved yet")
    elif row[0] > limit:
        fresh = CheckResult("data_freshness", "New data arriving", False,
                            f"no new reading saved for {int(row[0])}s (limit {limit}s)")
    else:
        fresh = CheckResult("data_freshness", "New data arriving", True,
                            f"last reading saved {int(row[0])}s ago")
    return [postgres, fresh]


def consumer_group_lag(group: str) -> int | None:
    """
    Unread messages for a consumer group = (newest offset) - (group's bookmark), summed over partitions.
    Returns None if the group has never committed a bookmark.

    We create a Consumer with the group's id but never subscribe, so it does NOT join the group;
    it only reads the group's committed offsets.
    """
    consumer = Consumer({"bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS,
                         "group.id": group, "enable.auto.commit": False})
    try:
        metadata = consumer.list_topics(config.KAFKA_TOPIC, timeout=5)
        partitions = [TopicPartition(config.KAFKA_TOPIC, p)
                      for p in metadata.topics[config.KAFKA_TOPIC].partitions]
        committed = consumer.committed(partitions, timeout=5)
        lag, found_any = 0, False
        for tp in committed:
            if tp.offset < 0:  # no bookmark yet for this partition
                continue
            found_any = True
            _, newest = consumer.get_watermark_offsets(TopicPartition(tp.topic, tp.partition), timeout=5)
            lag += max(0, newest - tp.offset)
        return lag if found_any else None
    finally:
        consumer.close()


def check_consumer_group(admin: AdminClient, group: str, label: str) -> CheckResult:
    name = f"group:{group}"
    try:
        description = admin.describe_consumer_groups([group], request_timeout=5)[group].result()
        members = len(description.members)
        lag = consumer_group_lag(group)
    except KafkaException as exc:
        return CheckResult(name, label, False, f"could not check ({exc})")

    lag_text = "unknown" if lag is None else str(lag)
    if members == 0:
        return CheckResult(name, label, False, f"not running (no active consumers, lag {lag_text})")
    if lag is not None and lag > config.HEALTH_MAX_CONSUMER_LAG:
        return CheckResult(name, label, False,
                           f"falling behind: {lag} unread messages (limit {config.HEALTH_MAX_CONSUMER_LAG})")
    return CheckResult(name, label, True, f"running ({members} consumer(s)), lag {lag_text}")


def run_all_checks(admin: AdminClient) -> list[CheckResult]:
    results = [check_kafka(admin)]
    if results[0].ok:  # group checks need Kafka; skip them rather than report duplicate problems
        results.append(check_consumer_group(admin, DB_CONSUMER_GROUP, "Database consumer"))
        if config.HEALTH_WATCH_ALERT_SERVICE:
            results.append(check_consumer_group(admin, ALERT_GROUP, "Alert service"))
    results += check_postgres_and_freshness()
    return results
