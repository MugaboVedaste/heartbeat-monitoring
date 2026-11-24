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
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
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