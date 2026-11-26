#!/bin/bash
# Creates Grafana's read-only database user the first time the PostgreSQL container starts.
# Runs after schema.sql and 002_alerts.sql (the init scripts run in alphabetical order).
# GRAFANA_DB_USER / GRAFANA_DB_PASSWORD come from .env via docker-compose.yml.
#
# For a database that already exists, this does not re-run: use the manual SQL in the README.

if [ -z "$GRAFANA_DB_PASSWORD" ]; then
    echo "GRAFANA_DB_PASSWORD is not set: skipping creation of the Grafana read-only user"
else
    # psql variables (:"name" = identifier, :'name' = quoted value) avoid SQL injection and quoting bugs
    psql -v ON_ERROR_STOP=1 \
         -v gf_user="${GRAFANA_DB_USER:-grafana_reader}" \
         -v gf_pass="$GRAFANA_DB_PASSWORD" \
         -v db_name="$POSTGRES_DB" \
         --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<'EOSQL'
CREATE USER :"gf_user" WITH PASSWORD :'gf_pass';
GRANT CONNECT ON DATABASE :"db_name" TO :"gf_user";
GRANT USAGE ON SCHEMA public TO :"gf_user";
GRANT SELECT ON heartbeats, alerts TO :"gf_user";
EOSQL
fi
