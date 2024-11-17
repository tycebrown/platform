#!/bin/bash

psql --dbname "$POSTGRES_DB" --username "$POSTGRES_USER" -f /db/schema.sql;
psql --dbname "$POSTGRES_DB" --username "$POSTGRES_USER" -f /db/migrations/11-03-24-gloss-events-sync-state.sql;
pg_restore -Fc --dbname "$POSTGRES_DB" --username "$POSTGRES_USER" /db/data.dump;
