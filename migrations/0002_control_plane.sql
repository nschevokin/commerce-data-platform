CREATE TABLE control.runs (
  run_id            uuid PRIMARY KEY,
  mode              text NOT NULL CHECK (mode IN ('scheduled', 'manual', 'backfill', 'audit')),
  replay_version    text NOT NULL,
  interval_start    timestamptz NOT NULL,
  interval_end      timestamptz NOT NULL,
  manifest_sha256s  text[] NOT NULL DEFAULT '{}',
  code_sha          text NOT NULL,
  dependency_lock_sha text NOT NULL,
  status            text NOT NULL CHECK (status IN (
                      'CREATED', 'INPUTS_VERIFIED', 'INGESTED', 'CANDIDATE_BUILT', 'VALIDATED',
                      'PUBLISHED', 'VERIFIED', 'SUCCEEDED', 'NO_CHANGE',
                      'INPUT_NOT_READY', 'FAILED_CONTRACT', 'FAILED_QUALITY',
                      'FAILED_TRANSIENT', 'FAILED_VERIFICATION')),
  airflow_dag_run_id text,
  created_at        timestamptz NOT NULL DEFAULT now(),
  updated_at        timestamptz NOT NULL DEFAULT now(),
  CHECK (interval_end > interval_start),
  CHECK (interval_end - interval_start <= interval '7 days')
);

CREATE TABLE control.stage_results (
  run_id        uuid NOT NULL REFERENCES control.runs (run_id),
  stage         text NOT NULL,
  source_name   text NOT NULL DEFAULT '',
  attempt       integer NOT NULL CHECK (attempt > 0),
  status        text NOT NULL CHECK (status IN ('RUNNING', 'SUCCEEDED', 'FAILED', 'SKIPPED')),
  error_code    text,
  artifact_uri  text,
  started_at    timestamptz NOT NULL DEFAULT now(),
  finished_at   timestamptz,
  PRIMARY KEY (run_id, stage, source_name, attempt),
  CHECK (status <> 'FAILED' OR error_code IS NOT NULL)
);

CREATE TABLE control.file_receipts (
  replay_version        text NOT NULL,
  source_name           text NOT NULL,
  file_sha256           text NOT NULL CHECK (file_sha256 ~ '^[0-9a-f]{64}$'),
  interval_start        timestamptz NOT NULL,
  manifest_sha256       text NOT NULL,
  input_rows            bigint NOT NULL CHECK (input_rows >= 0),
  accepted_new_rows     bigint NOT NULL CHECK (accepted_new_rows >= 0),
  exact_duplicate_rows  bigint NOT NULL CHECK (exact_duplicate_rows >= 0),
  rejected_rows         bigint NOT NULL DEFAULT 0 CHECK (rejected_rows = 0),
  run_id                uuid NOT NULL REFERENCES control.runs (run_id),
  fencing_generation    bigint NOT NULL,
  committed_at          timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (replay_version, source_name, file_sha256),
  CHECK (input_rows = accepted_new_rows + exact_duplicate_rows + rejected_rows)
);

CREATE TABLE control.intervals (
  replay_version   text NOT NULL,
  interval_start   timestamptz NOT NULL,
  interval_end     timestamptz NOT NULL,
  manifest_sha256  text NOT NULL,
  completeness     text NOT NULL CHECK (completeness IN ('INCOMPLETE', 'COMPLETE')),
  total_rows       bigint NOT NULL DEFAULT 0,
  run_id           uuid NOT NULL REFERENCES control.runs (run_id),
  updated_at       timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (replay_version, interval_start),
  CHECK (interval_end > interval_start)
);

CREATE TABLE control.releases (
  release_id            uuid PRIMARY KEY,
  schema_name           text NOT NULL UNIQUE CHECK (schema_name ~ '^cand_[0-9a-f]{32}$'),
  run_id                uuid NOT NULL REFERENCES control.runs (run_id),
  replay_version        text NOT NULL,
  frontier_end          timestamptz NOT NULL,
  input_digest          text NOT NULL,
  code_sha              text NOT NULL,
  dependency_lock_sha   text NOT NULL,
  parser_contract_version integer NOT NULL,
  serving_contract_version integer NOT NULL,
  status                text NOT NULL CHECK (status IN (
                          'BUILDING', 'BUILT', 'VALIDATED', 'REJECTED', 'DROPPED')),
  blocking_failures     integer,
  warnings              integer,
  artifact_uri          text,
  created_at            timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE control.quality_results (
  release_id   uuid NOT NULL REFERENCES control.releases (release_id),
  check_id     text NOT NULL,
  run_id       uuid NOT NULL REFERENCES control.runs (run_id),
  severity     text NOT NULL CHECK (severity IN ('BLOCK', 'WARN')),
  passed       boolean NOT NULL,
  observed     numeric,
  threshold    numeric,
  detail       jsonb,
  recorded_at  timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (release_id, check_id)
);

CREATE TABLE control.publications (
  generation          bigint PRIMARY KEY CHECK (generation > 0),
  release_id          uuid NOT NULL REFERENCES control.releases (release_id),
  previous_release_id uuid REFERENCES control.releases (release_id),
  kind                text NOT NULL CHECK (kind IN ('PUBLISH', 'ROLLBACK')),
  run_id              uuid REFERENCES control.runs (run_id),
  operator            text,
  reason              text,
  incident_id         text,
  published_at        timestamptz NOT NULL DEFAULT now(),
  CHECK (kind = 'PUBLISH' OR (operator IS NOT NULL AND reason IS NOT NULL))
);
CREATE UNIQUE INDEX publications_one_publish_per_release
  ON control.publications (release_id) WHERE kind = 'PUBLISH';

CREATE TABLE control.current_release (
  singleton_id integer PRIMARY KEY CHECK (singleton_id = 1),
  release_id   uuid REFERENCES control.releases (release_id),
  generation   bigint NOT NULL DEFAULT 0 CHECK (generation >= 0)
);

CREATE TABLE control.run_guard (
  singleton_id       integer PRIMARY KEY CHECK (singleton_id = 1),
  owner_run_id       uuid REFERENCES control.runs (run_id),
  fencing_generation bigint NOT NULL DEFAULT 0 CHECK (fencing_generation >= 0)
);

INSERT INTO control.current_release (singleton_id, release_id, generation) VALUES (1, NULL, 0);
INSERT INTO control.run_guard (singleton_id, owner_run_id, fencing_generation) VALUES (1, NULL, 0);

CREATE INDEX releases_status_created ON control.releases (status, created_at);
CREATE INDEX publications_published_at ON control.publications (published_at);

GRANT SELECT, INSERT, UPDATE ON control.runs, control.stage_results TO commerce_ingest,
  commerce_transform, commerce_publish;
GRANT SELECT, INSERT ON control.file_receipts TO commerce_ingest;
GRANT SELECT, INSERT, UPDATE ON control.intervals TO commerce_ingest;
GRANT SELECT ON control.file_receipts, control.intervals TO commerce_transform, commerce_publish;
GRANT SELECT, INSERT, UPDATE ON control.releases, control.quality_results TO commerce_transform;
GRANT SELECT ON control.releases, control.quality_results, control.publications,
  control.current_release, control.run_guard TO commerce_publish;
GRANT SELECT ON control.run_guard TO commerce_ingest, commerce_transform;
