CREATE TABLE bronze.source_events (
  replay_version          text NOT NULL,
  source_name             text NOT NULL CHECK (source_name IN (
                            'orders', 'customers', 'order_items', 'order_payments',
                            'products', 'sellers', 'category_translation')),
  entity_key              text NOT NULL,
  revision                integer NOT NULL CHECK (revision > 0),
  available_at            timestamptz NOT NULL,
  operation               text NOT NULL DEFAULT 'UPSERT' CHECK (operation = 'UPSERT'),
  payload                 jsonb NOT NULL,
  payload_sha256          text NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
  parser_contract_version integer NOT NULL,
  source_file_sha256      text NOT NULL CHECK (source_file_sha256 ~ '^[0-9a-f]{64}$'),
  ingested_at             timestamptz NOT NULL DEFAULT now(),
  first_run_id            uuid NOT NULL REFERENCES control.runs (run_id),
  PRIMARY KEY (replay_version, source_name, entity_key, revision)
);

CREATE INDEX source_events_frontier ON bronze.source_events (replay_version, source_name, available_at);

GRANT SELECT, INSERT ON bronze.source_events TO commerce_ingest;
GRANT SELECT ON bronze.source_events TO commerce_transform;
