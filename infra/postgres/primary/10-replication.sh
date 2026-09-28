#!/bin/sh
set -eu

: "${REPLICATION_USER:?REPLICATION_USER is required}"
: "${REPLICATION_PASSWORD:?REPLICATION_PASSWORD is required}"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
    -v replication_user="$REPLICATION_USER" \
    -v replication_password="$REPLICATION_PASSWORD" <<-'EOSQL'
    DO $$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = :'replication_user') THEN
            EXECUTE format(
                'CREATE ROLE %I WITH REPLICATION LOGIN PASSWORD %L',
                :'replication_user',
                :'replication_password'
            );
        END IF;
    END
    $$;
EOSQL

printf '%s\n' "host replication ${REPLICATION_USER} all scram-sha-256" >> "$PGDATA/pg_hba.conf"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
    -c 'SELECT pg_reload_conf()'
