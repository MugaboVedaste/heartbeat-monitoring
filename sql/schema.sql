-- Heartbeat monitoring schema
-- Runs automatically the first time the PostgreSQL container starts.

CREATE TABLE IF NOT EXISTS heartbeats (
    id           BIGSERIAL    PRIMARY KEY,
    customer_id  VARCHAR(20)  NOT NULL,
    recorded_at  TIMESTAMPTZ  NOT NULL,                 -- when the sensor took the reading
    heart_rate   SMALLINT     NOT NULL
                 CHECK (heart_rate BETWEEN 20 AND 250),  -- safety net: physically impossible values are rejected
    status       VARCHAR(10)  NOT NULL
                 CHECK (status IN ('low', 'normal', 'high')),
    ingested_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()     -- when our pipeline saved it
);

-- Required by the project: fast queries by time range ("readings from the last hour")
CREATE INDEX IF NOT EXISTS idx_heartbeats_recorded_at
    ON heartbeats (recorded_at);

-- Fast queries for one customer over time ("CUST-007's readings today").
-- UNIQUE also makes writes idempotent: if Kafka re-delivers a reading after a crash,
-- the consumer's INSERT ... ON CONFLICT DO NOTHING skips it instead of saving a duplicate.
CREATE UNIQUE INDEX IF NOT EXISTS idx_heartbeats_customer_time
    ON heartbeats (customer_id, recorded_at);

-- Fast lookup of anomalies ("all high readings this week")
CREATE INDEX IF NOT EXISTS idx_heartbeats_status_time
    ON heartbeats (status, recorded_at)
    WHERE status <> 'normal';