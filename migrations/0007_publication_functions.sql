CREATE FUNCTION control.lock_for_publication(p_run uuid, p_generation bigint)
RETURNS TABLE (current_release_id uuid, current_generation bigint)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, control AS $$
DECLARE g control.run_guard%ROWTYPE; c control.current_release%ROWTYPE;
BEGIN
  SELECT * INTO g FROM control.run_guard WHERE singleton_id = 1 FOR UPDATE;
  IF g.owner_run_id IS DISTINCT FROM p_run OR g.fencing_generation <> p_generation THEN
    RAISE EXCEPTION 'stale writer: run % generation % (current owner %, generation %)',
      p_run, p_generation, g.owner_run_id, g.fencing_generation USING ERRCODE = 'CG002';
  END IF;
  SELECT * INTO c FROM control.current_release WHERE singleton_id = 1 FOR UPDATE;
  RETURN QUERY SELECT c.release_id, c.generation;
END $$;

CREATE FUNCTION control.record_publication(
  p_run uuid, p_run_generation bigint, p_release uuid, p_expected_generation bigint,
  p_kind text, p_operator text, p_reason text, p_incident text) RETURNS bigint
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, control AS $$
DECLARE g control.run_guard%ROWTYPE; c control.current_release%ROWTYPE; new_gen bigint;
BEGIN
  SELECT * INTO g FROM control.run_guard WHERE singleton_id = 1 FOR UPDATE;
  IF g.owner_run_id IS DISTINCT FROM p_run OR g.fencing_generation <> p_run_generation THEN
    RAISE EXCEPTION 'stale writer at record_publication: run %', p_run USING ERRCODE = 'CG002';
  END IF;
  SELECT * INTO c FROM control.current_release WHERE singleton_id = 1 FOR UPDATE;
  IF c.generation <> p_expected_generation THEN
    RAISE EXCEPTION 'serving generation moved: expected %, found %', p_expected_generation, c.generation
      USING ERRCODE = 'CG003';
  END IF;
  new_gen := c.generation + 1;
  INSERT INTO control.publications (generation, release_id, previous_release_id, kind, run_id, operator,
                                    reason, incident_id)
    VALUES (new_gen, p_release, c.release_id, p_kind, p_run, p_operator, p_reason, p_incident);
  UPDATE control.current_release SET release_id = p_release, generation = new_gen WHERE singleton_id = 1;
  RETURN new_gen;
END $$;

REVOKE ALL ON FUNCTION control.lock_for_publication(uuid, bigint),
  control.record_publication(uuid, bigint, uuid, bigint, text, text, text, text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION control.lock_for_publication(uuid, bigint),
  control.record_publication(uuid, bigint, uuid, bigint, text, text, text, text) TO commerce_publish;
GRANT SELECT ON control.current_release, control.publications TO commerce_publish, commerce_transform;
