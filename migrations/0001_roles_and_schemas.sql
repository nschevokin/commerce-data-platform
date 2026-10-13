DO $$
DECLARE r text;
BEGIN
  FOREACH r IN ARRAY ARRAY['commerce_ingest', 'commerce_transform', 'commerce_publish',
                           'commerce_dashboard']
  LOOP
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = r) THEN
      EXECUTE format('CREATE ROLE %I NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT', r);
    END IF;
  END LOOP;
END $$;

CREATE SCHEMA IF NOT EXISTS control;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS serving;

DO $$
BEGIN
  EXECUTE format('REVOKE ALL ON DATABASE %I FROM PUBLIC', current_database());
  EXECUTE format('GRANT CONNECT ON DATABASE %I TO PUBLIC', current_database());
  EXECUTE format('GRANT TEMPORARY ON DATABASE %I TO commerce_ingest, commerce_transform',
                 current_database());
  EXECUTE format('GRANT CREATE ON DATABASE %I TO commerce_transform', current_database());
END $$;

REVOKE ALL ON SCHEMA public FROM PUBLIC;
REVOKE ALL ON SCHEMA control, bronze, serving FROM PUBLIC;

GRANT USAGE ON SCHEMA control TO commerce_ingest, commerce_transform, commerce_publish;
GRANT USAGE ON SCHEMA bronze  TO commerce_ingest, commerce_transform;
GRANT USAGE ON SCHEMA serving TO commerce_publish, commerce_dashboard;
GRANT CREATE ON SCHEMA serving TO commerce_publish;

ALTER DEFAULT PRIVILEGES FOR ROLE commerce_publish IN SCHEMA serving
  GRANT SELECT ON TABLES TO commerce_dashboard;
