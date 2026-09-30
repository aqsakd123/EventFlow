#!/bin/sh
set -eu

: "${REPLICATION_USER:?REPLICATION_USER is required}"
: "${REPLICATION_PASSWORD:?REPLICATION_PASSWORD is required}"

sql_escape_literal() {
    printf '%s' "$1" | sed "s/'/''/g"
}

sql_escape_identifier() {
    printf '%s' "$1" | sed 's/"/""/g'
}

escaped_user="$(sql_escape_literal "$REPLICATION_USER")"
escaped_identifier="$(sql_escape_identifier "$REPLICATION_USER")"
escaped_password="$(sql_escape_literal "$REPLICATION_PASSWORD")"
existing_role="$(psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
    -Atc "SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = '$escaped_user'")"

if [ "$existing_role" != "1" ]; then
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE ROLE "$escaped_identifier" WITH REPLICATION LOGIN PASSWORD '$escaped_password';
EOSQL
fi

printf '%s\n' "host replication ${REPLICATION_USER} all scram-sha-256" >> "$PGDATA/pg_hba.conf"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
    -c 'SELECT pg_reload_conf()'
