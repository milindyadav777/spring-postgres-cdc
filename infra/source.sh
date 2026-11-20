#!/bin/sh
set -eu
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
  --set=capture_password="$CAPTURE_PASSWORD" <<'SQL'
CREATE ROLE cdc_reader WITH LOGIN REPLICATION PASSWORD :'capture_password';
CREATE SEQUENCE order_revision_seq;
CREATE TABLE orders (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  status varchar(32) NOT NULL CHECK (status IN ('CREATED','PAID','CANCELLED')),
  revision bigint NOT NULL
);
CREATE FUNCTION assign_revision() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'UPDATE' AND NEW.id <> OLD.id THEN
    RAISE EXCEPTION 'order identity is immutable';
  END IF;
  NEW.revision := nextval('order_revision_seq');
  RETURN NEW;
END $$;
CREATE TRIGGER order_revision BEFORE INSERT OR UPDATE ON orders
FOR EACH ROW EXECUTE FUNCTION assign_revision();
ALTER TABLE orders REPLICA IDENTITY FULL;
CREATE PUBLICATION orders_pub FOR TABLE orders WITH (publish = 'insert,update,delete');
GRANT SELECT ON orders TO cdc_reader;
INSERT INTO orders(status) VALUES ('CREATED');
SQL
