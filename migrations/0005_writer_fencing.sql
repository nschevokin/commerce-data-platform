CREATE TABLE control.guard_log (
  log_id      bigserial PRIMARY KEY,
  at          timestamptz NOT NULL DEFAULT now(),
  action      text NOT NULL CHECK (action IN ('ACQUIRE', 'RELEASE', 'TAKEOVER')),
  run_id      uuid,
  generation  bigint NOT NULL,
  operator    text,
  reason      text
);
GRANT SELECT ON control.guard_log TO commerce_ingest, commerce_transform, commerce_publish;

CREATE FUNCTION control.acquire_guard(p_run uuid) RETURNS bigint
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, control AS $$
DECLARE g control.run_guard%ROWTYPE;
BEGIN
  SELECT * INTO g FROM control.run_guard WHERE singleton_id = 1 FOR UPDATE;
  IF g.owner_run_id IS NOT NULL AND g.owner_run_id <> p_run THEN
    RAISE EXCEPTION 'writer guard is held by run %', g.owner_run_id USING ERRCODE = 'CG001';
  END IF;
  IF g.owner_run_id = p_run THEN
    RETURN g.fencing_generation;
  END IF;
  UPDATE control.run_guard SET owner_run_id = p_run, fencing_generation = g.fencing_generation + 1
   WHERE singleton_id = 1;
  INSERT INTO control.guard_log (action, run_id, generation)
    VALUES ('ACQUIRE', p_run, g.fencing_generation + 1);
  RETURN g.fencing_generation + 1;
END $$;

CREATE FUNCTION control.release_guard(p_run uuid, p_generation bigint) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, control AS $$
DECLARE g control.run_guard%ROWTYPE;
BEGIN
  SELECT * INTO g FROM control.run_guard WHERE singleton_id = 1 FOR UPDATE;
  IF g.owner_run_id IS DISTINCT FROM p_run OR g.fencing_generation <> p_generation THEN
    RAISE EXCEPTION 'run % does not own the writer guard at generation %', p_run, p_generation
      USING ERRCODE = 'CG002';
  END IF;
  UPDATE control.run_guard SET owner_run_id = NULL WHERE singleton_id = 1;
  INSERT INTO control.guard_log (action, run_id, generation) VALUES ('RELEASE', p_run, p_generation);
END $$;

CREATE FUNCTION control.takeover_guard(p_new_run uuid, p_operator text, p_reason text) RETURNS bigint
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, control AS $$
DECLARE g control.run_guard%ROWTYPE;
BEGIN
  IF coalesce(btrim(p_operator), '') = '' OR coalesce(btrim(p_reason), '') = '' THEN
    RAISE EXCEPTION 'takeover needs an operator and a reason' USING ERRCODE = '22023';
  END IF;
  SELECT * INTO g FROM control.run_guard WHERE singleton_id = 1 FOR UPDATE;
  UPDATE control.run_guard SET owner_run_id = p_new_run, fencing_generation = g.fencing_generation + 1
   WHERE singleton_id = 1;
  INSERT INTO control.guard_log (action, run_id, generation, operator, reason)
    VALUES ('TAKEOVER', p_new_run, g.fencing_generation + 1, p_operator, p_reason);
  RETURN g.fencing_generation + 1;
END $$;

CREATE FUNCTION control.assert_writer(p_run uuid, p_generation bigint) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, control AS $$
DECLARE g control.run_guard%ROWTYPE;
BEGIN
  SELECT * INTO g FROM control.run_guard WHERE singleton_id = 1 FOR SHARE;
  IF g.owner_run_id IS DISTINCT FROM p_run OR g.fencing_generation <> p_generation THEN
    RAISE EXCEPTION 'stale writer: run % generation % (current owner %, generation %)',
      p_run, p_generation, g.owner_run_id, g.fencing_generation USING ERRCODE = 'CG002';
  END IF;
END $$;

REVOKE ALL ON FUNCTION control.acquire_guard(uuid), control.release_guard(uuid, bigint),
  control.takeover_guard(uuid, text, text), control.assert_writer(uuid, bigint) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION control.acquire_guard(uuid), control.release_guard(uuid, bigint),
  control.assert_writer(uuid, bigint) TO commerce_ingest, commerce_transform, commerce_publish;
