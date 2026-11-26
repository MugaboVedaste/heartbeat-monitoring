"""
Alert rules and email notifications.

Kept separate from alert_service.py (which handles Kafka) so the rules can be unit-tested
without Kafka, PostgreSQL or a mail server.

The rule:
  - ALERT when a customer has ALERT_CONSECUTIVE_READINGS abnormal readings in a row
    (all "high" or all "low"). A single odd reading is NOT an alert: real sensors glitch.
  - Don't repeat the same alert for that customer until ALERT_COOLDOWN_SECONDS have passed.
  - Send a RECOVERED alert after ALERT_RECOVERY_READINGS normal readings in a row.
"""

import logging
import smtplib
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from email.message import EmailMessage

from src import config

logger = logging.getLogger(__name__)


@dataclass
class Alert:
    customer_id: str
    alert_type: str          # "high", "low" or "recovered"
    heart_rates: list[int]   # the readings that caused it
    started_at: datetime     # first reading of the streak
    triggered_at: datetime   # the reading that triggered the alert


@dataclass
class CustomerState:
    """What the detector remembers about one customer."""
    streak_status: str | None = None              # status of the current run of readings
    streak_rates: list[int] = field(default_factory=list)
    streak_started_at: datetime | None = None
    active_alert: str | None = None                # "high" / "low" while a problem is ongoing
    last_alert_at: datetime | None = None


class AlertDetector:
    """Feed it validated readings one by one; it returns an Alert when something needs attention."""

    def __init__(self,
                 consecutive: int = config.ALERT_CONSECUTIVE_READINGS,
                 cooldown_seconds: int = config.ALERT_COOLDOWN_SECONDS,
                 recovery: int = config.ALERT_RECOVERY_READINGS):
        self.consecutive = consecutive
        self.cooldown = timedelta(seconds=cooldown_seconds)
        self.recovery = recovery
        self.customers: dict[str, CustomerState] = {}

    def process(self, row: dict) -> Alert | None:
        """
        row is a validated reading:
            {"customer_id": ..., "recorded_at": datetime, "heart_rate": int, "status": ...}
        """
        state = self.customers.setdefault(row["customer_id"], CustomerState())
        status, now = row["status"], row["recorded_at"]

        # Extend the current streak, or start a new one if the status changed
        if status == state.streak_status:
            state.streak_rates.append(row["heart_rate"])
            state.streak_rates = state.streak_rates[-10:]  # only keep the latest few
        else:
            state.streak_status = status
            state.streak_rates = [row["heart_rate"]]
            state.streak_started_at = now
        count = len(state.streak_rates)

        if status in ("high", "low"):
            if count < self.consecutive:
                return None
            is_new_problem = state.active_alert != status
            cooldown_over = state.last_alert_at is None or now - state.last_alert_at >= self.cooldown
            if is_new_problem or cooldown_over:
                state.active_alert = status
                state.last_alert_at = now
                return self._make_alert(row, status, state)
            return None

        # Normal reading: has an ongoing problem ended?
        if state.active_alert and count >= self.recovery:
            state.active_alert = None
            return self._make_alert(row, "recovered", state)
        return None

    @staticmethod
    def _make_alert(row: dict, alert_type: str, state: CustomerState) -> Alert:
        return Alert(
            customer_id=row["customer_id"],
            alert_type=alert_type,
            heart_rates=list(state.streak_rates),
            started_at=state.streak_started_at,
            triggered_at=row["recorded_at"],
        )


# ---------- Email ----------

def build_email(alert: Alert) -> EmailMessage:
    rates = ", ".join(str(r) for r in alert.heart_rates)
    when = alert.triggered_at.strftime("%Y-%m-%d %H:%M:%S %Z")
    latest = alert.heart_rates[-1]

    if alert.alert_type == "recovered":
        subject = f"RECOVERED: {alert.customer_id} heart rate back to normal ({latest} bpm)"
        body = (
            f"Customer {alert.customer_id}'s heart rate is back in the normal range.\n\n"
            f"Recent readings: {rates} bpm\n"
            f"Time: {when}\n"
        )
    else:
        direction = "HIGH" if alert.alert_type == "high" else "LOW"
        limit = (f"above {config.HIGH_BPM_THRESHOLD}" if alert.alert_type == "high"
                 else f"below {config.LOW_BPM_THRESHOLD}")
        subject = f"ALERT: {direction} heart rate for {alert.customer_id} ({latest} bpm)"
        body = (
            f"Customer {alert.customer_id} has had {len(alert.heart_rates)} readings in a row "
            f"{limit} bpm.\n\n"
            f"Readings: {rates} bpm\n"
            f"Started: {alert.started_at.strftime('%Y-%m-%d %H:%M:%S %Z')}\n"
            f"Latest:  {when}\n\n"
            f"Please check on this customer.\n"
        )

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = config.ALERT_EMAIL_FROM
    msg["To"] = config.ALERT_EMAIL_TO
    msg.set_content(body + "\n-- Heartbeat Monitoring System\n")
    return msg


@dataclass(frozen=True)
class MailServer:
    """One place to deliver emails to (Mailpit, Gmail, ...)."""
    name: str
    host: str
    port: int
    use_tls: bool
    user: str
    password: str
    sender: str
    recipients: list[str]


def _split_addresses(value: str) -> list[str]:
    """'a@x.com, b@y.com' -> ['a@x.com', 'b@y.com']"""
    return [a.strip() for a in value.split(",") if a.strip()]


def configured_mail_servers() -> list[MailServer]:
    """
    Every mail server that has settings in .env:
      - "mailpit": the local test inbox (SMTP_* settings)
      - "real":    a real email account such as Gmail (REAL_SMTP_* settings), only if REAL_SMTP_HOST is set
    """
    servers = []
    if config.SMTP_HOST:
        servers.append(MailServer(
            name="mailpit", host=config.SMTP_HOST, port=config.SMTP_PORT,
            use_tls=config.SMTP_USE_TLS, user=config.SMTP_USER, password=config.SMTP_PASSWORD,
            sender=config.ALERT_EMAIL_FROM, recipients=_split_addresses(config.ALERT_EMAIL_TO),
        ))
    if config.REAL_SMTP_HOST:
        servers.append(MailServer(
            name="real", host=config.REAL_SMTP_HOST, port=config.REAL_SMTP_PORT,
            use_tls=config.REAL_SMTP_USE_TLS, user=config.REAL_SMTP_USER,
            password=config.REAL_SMTP_PASSWORD,
            # Gmail and most providers only allow sending "from" your own address
            sender=config.REAL_ALERT_EMAIL_FROM or config.REAL_SMTP_USER,
            recipients=_split_addresses(config.REAL_ALERT_EMAIL_TO),
        ))
    return servers


def _send_via(server: MailServer, msg: EmailMessage) -> bool:
    if not server.recipients:
        logger.error("[%s] no recipients configured, email not sent", server.name)
        return False
    # Same email content, but with this server's sender and recipients
    del msg["From"]
    del msg["To"]
    msg["From"] = server.sender
    msg["To"] = ", ".join(server.recipients)
    try:
        with smtplib.SMTP(server.host, server.port, timeout=10) as smtp:
            if server.use_tls:
                smtp.starttls()  # encrypt the connection (required by Gmail)
            if server.user:
                smtp.login(server.user, server.password)
            smtp.send_message(msg)
        return True
    except (smtplib.SMTPException, OSError) as exc:
        logger.error("[%s] could not send email %r: %s", server.name, msg["Subject"], exc)
        return False


def send_message(msg: EmailMessage) -> bool:
    """
    Send an email to every configured mail server.
    Returns True if at least one delivered it; each failure is logged.
    """
    servers = configured_mail_servers()
    if not servers:
        logger.error("No mail server configured; email %r not sent", msg["Subject"])
        return False
    results = [_send_via(server, msg) for server in servers]
    return any(results)


def send_email(alert: Alert) -> bool:
    """Send a customer alert email. Returns True if sent."""
    return send_message(build_email(alert))