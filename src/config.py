"""
Central place for all settings.

Values come from the .env file (loaded below), with safe defaults for local development.
Every script imports from here instead of reading environment variables itself.
"""

import os

from dotenv import load_dotenv

load_dotenv()  # reads .env from the project root into environment variables

# --- Kafka ---
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "heartbeats")

# --- PostgreSQL ---
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5433"))  # docker-compose maps 5433 -> 5432
POSTGRES_DB = os.getenv("POSTGRES_DB", "heartbeat_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "")

# --- Data generator ---
NUM_CUSTOMERS = int(os.getenv("NUM_CUSTOMERS", "10"))
SEND_INTERVAL_SECONDS = float(os.getenv("SEND_INTERVAL_SECONDS", "1.0"))
ANOMALY_PROBABILITY = float(os.getenv("ANOMALY_PROBABILITY", "0.03"))

# --- Validation rules (used by the consumer) ---
# Readings outside VALID range are rejected as sensor errors.
# Readings inside the valid range are labelled low / normal / high.
VALID_MIN_BPM = 20
VALID_MAX_BPM = 250
LOW_BPM_THRESHOLD = 50
HIGH_BPM_THRESHOLD = 120


# --- Alerting (used by alert_service.py) ---
ALERT_CONSECUTIVE_READINGS = int(os.getenv("ALERT_CONSECUTIVE_READINGS", "3"))
ALERT_COOLDOWN_SECONDS = int(os.getenv("ALERT_COOLDOWN_SECONDS", "300"))
ALERT_RECOVERY_READINGS = int(os.getenv("ALERT_RECOVERY_READINGS", "3"))

# --- Email (defaults point at Mailpit, the test mail server in docker-compose) ---
SMTP_HOST = os.getenv("SMTP_HOST", "localhost")
SMTP_PORT = int(os.getenv("SMTP_PORT", "1025"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
SMTP_USE_TLS = os.getenv("SMTP_USE_TLS", "false").lower() == "true"
ALERT_EMAIL_FROM = os.getenv("ALERT_EMAIL_FROM", "alerts@heartbeat.local")
ALERT_EMAIL_TO = os.getenv("ALERT_EMAIL_TO", "doctor@heartbeat.local")

# --- Pipeline health monitor (used by health_monitor.py) ---
HEALTH_CHECK_INTERVAL_SECONDS = int(os.getenv("HEALTH_CHECK_INTERVAL_SECONDS", "30"))
HEALTH_MAX_DATA_AGE_SECONDS = int(os.getenv("HEALTH_MAX_DATA_AGE_SECONDS", "120"))
HEALTH_MAX_CONSUMER_LAG = int(os.getenv("HEALTH_MAX_CONSUMER_LAG", "500"))
HEALTH_FAILURES_BEFORE_ALERT = int(os.getenv("HEALTH_FAILURES_BEFORE_ALERT", "2"))
HEALTH_WATCH_ALERT_SERVICE = os.getenv("HEALTH_WATCH_ALERT_SERVICE", "true").lower() == "true"

# --- Real email (optional): also send alerts to a real inbox, e.g. Gmail ---
# Leave REAL_SMTP_HOST empty to only use Mailpit.
REAL_SMTP_HOST = os.getenv("REAL_SMTP_HOST", "")
REAL_SMTP_PORT = int(os.getenv("REAL_SMTP_PORT", "587"))
REAL_SMTP_USE_TLS = os.getenv("REAL_SMTP_USE_TLS", "true").lower() == "true"
REAL_SMTP_USER = os.getenv("REAL_SMTP_USER", "")
REAL_SMTP_PASSWORD = os.getenv("REAL_SMTP_PASSWORD", "")
REAL_ALERT_EMAIL_FROM = os.getenv("REAL_ALERT_EMAIL_FROM", "")   # defaults to REAL_SMTP_USER
REAL_ALERT_EMAIL_TO = os.getenv("REAL_ALERT_EMAIL_TO", "")       # comma-separated for several people