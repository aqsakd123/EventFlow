#!/bin/sh
set -eu

: "${PRIMARY_HOST:?PRIMARY_HOST is required}"
: "${REPLICATION_USER:?REPLICATION_USER is required}"
: "${REPLICATION_PASSWORD:?REPLICATION_PASSWORD is required}"

mkdir -p "$PGDATA"
chown -R postgres:postgres "$PGDATA"
chmod 0700 "$PGDATA"

until pg_isready -h "$PRIMARY_HOST" -p "${PRIMARY_PORT:-5432}" -U "$REPLICATION_USER"; do
    sleep 1
done

if [ ! -s "$PGDATA/PG_VERSION" ]; then
    find "$PGDATA" -mindepth 1 -maxdepth 1 -exec rm -rf {} +
    gosu postgres env PGPASSWORD="$REPLICATION_PASSWORD" pg_basebackup \
        -h "$PRIMARY_HOST" -p "${PRIMARY_PORT:-5432}" -U "$REPLICATION_USER" \
        -D "$PGDATA" -Fp -Xs -P -R
fi

exec docker-entrypoint.sh postgres -c hot_standby=on
