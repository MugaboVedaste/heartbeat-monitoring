-- Alerts raised by alert_service.py
-- Runs automatically on a FRESH database. For an existing database, run it once by hand:
--   docker exec -i postgres psql -U postgres -d heartbeat_db < sql/002_alerts.sql

CREATE TABLE IF NOT EXISTS alerts (
    id            BIGSERIAL    PRIMARY KEY,
    customer_id   VARCHAR(20)  NOT NULL,
    alert_type    VARCHAR(10)  NOT NULL
                  CHECK (alert_type IN ('high', 'low', 'recovered')),
    heart_rates   SMALLINT[]   NOT NULL,          -- the readings that caused the alert
    started_at    TIMESTAMPTZ  NOT NULL,          -- first reading of the streak
    triggered_at  TIMESTAMPTZ  NOT NULL,          -- reading that triggered the alert
    email_sent    BOOLEAN      NOT NULL DEFAULT FALSE,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alerts_triggered_at ON alerts (triggered_at);
CREATE INDEX IF NOT EXISTS idx_alerts_customer_time ON alerts (customer_id, triggered_at);

-- Let Grafana's read-only user see alerts (skipped quietly if that user doesn't exist yet)
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grafana_reader') THEN
        GRANT SELECT ON alerts TO grafana_reader;
    END IF;
END
$$;
