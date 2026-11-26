"""
Pipeline health monitor: checks the pipeline every 30 seconds and emails when something breaks.

It watches:
  - Kafka           (broker reachable, topic exists)
  - PostgreSQL      (reachable)
  - Data freshness  (new readings are still being saved)
  - Database consumer and alert service (running, and keeping up with Kafka)

It emails once when a problem starts (after 2 failed checks in a row) and once when it's
resolved, using the same email settings as the alert service (Mailpit by default).

Run from the project root (venv active):
    python -m src.health_monitor
Stop with Ctrl+C.
"""

import logging
import time

from confluent_kafka.admin import AdminClient

from src import config
from src.alerting import send_message
from src.health import HealthTracker, build_health_email, run_all_checks

logger = logging.getLogger("health_monitor")


def run() -> None:
    admin = AdminClient({"bootstrap.servers": config.KAFKA_BOOTSTRAP_SERVERS})
    tracker = HealthTracker()
    logger.info("Checking pipeline health every %ds; emails to %s via %s:%d",
                config.HEALTH_CHECK_INTERVAL_SECONDS, config.ALERT_EMAIL_TO,
                config.SMTP_HOST, config.SMTP_PORT)
    try:
        while True:
            results = run_all_checks(admin)

            summary = " | ".join(f"{r.label}: {'OK' if r.ok else 'FAIL'}" for r in results)
            if all(r.ok for r in results):
                logger.info(summary)
            else:
                logger.warning(summary)
                for r in results:
                    if not r.ok:
                        logger.warning("  %s: %s", r.label, r.detail)

            notifications = tracker.update(results)
            if notifications:
                sent = send_message(build_health_email(notifications, results))
                for n in notifications:
                    logger.log(logging.WARNING if n.kind == "problem" else logging.INFO,
                               "%s: %s (email %s)", n.kind.upper(), n.result.label,
                               "sent" if sent else "FAILED")

            time.sleep(config.HEALTH_CHECK_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        logger.info("Health monitor stopped")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    # The Kafka library logs its own connection errors very loudly; our checks report them instead
    logging.getLogger("confluent_kafka").setLevel(logging.ERROR)
    run()
