#!/bin/sh
set -eu
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  --set=app_password="$SINK_PASSWORD" <<'SQL'
CREATE ROLE projector LOGIN PASSWORD :'app_password';
CREATE TABLE order_view (
  id bigint PRIMARY KEY, revision bigint NOT NULL, status varchar(32), deleted boolean NOT NULL,
  CHECK (deleted = (status IS NULL))
);
GRANT SELECT, INSERT, UPDATE ON order_view TO projector;
SQL
