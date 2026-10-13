CREATE VIEW serving.release_info AS
SELECT
  NULL::uuid        AS release_id,
  'NOT_PUBLISHED'::text AS status,
  NULL::text        AS replay_version,
  NULL::timestamptz AS frontier_end,
  NULL::text        AS code_sha,
  NULL::timestamptz AS published_at,
  0::bigint         AS generation,
  1::integer        AS serving_contract_version;

ALTER VIEW serving.release_info OWNER TO commerce_publish;
GRANT SELECT ON serving.release_info TO commerce_dashboard;
