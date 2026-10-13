from __future__ import annotations

from datetime import timedelta

from airflow.sdk import DAG, CronDataIntervalTimetable, Param, TriggerRule, get_current_context, task
from airflow.sdk.bases.sensor import PokeReturnValue
from airflow.sdk.exceptions import AirflowFailException

from commerce.orchestration import ledger, stages, window
from commerce.orchestration.errors import InputNotReady, is_retryable
from commerce.orchestration.wiring import Wiring

RETRIES = dict(retries=2, retry_delay=timedelta(seconds=30), retry_exponential_backoff=True,
               max_retry_delay=timedelta(minutes=5))


def _wiring(params: dict) -> Wiring:
    return Wiring.from_env(replay_version=params.get("replay_version") or None)


def _guarded(fn, *args):
    try:
        return fn(*args)
    except Exception as exc:
        if is_retryable(exc):
            raise
        raise AirflowFailException(f"{type(exc).__name__}: {exc}") from exc


def _run_id(w: Wiring) -> str:
    return str(ledger.run_id_for(w.replay_version, get_current_context()["dag_run"].run_id))


with DAG(
    dag_id="commerce_pipeline",
    schedule=CronDataIntervalTimetable("@daily", timezone="UTC"),
    start_date=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    dagrun_timeout=timedelta(minutes=45),
    params={"interval_start": Param(None, type=["null", "string"], description="UTC, ends in Z (manual)"),
            "interval_end": Param(None, type=["null", "string"], description="UTC, ends in Z (manual)"),
            "replay_version": Param(None, type=["null", "string"])},
    default_args=RETRIES,
    tags=["commerce"],
) as dag:

    @task(execution_timeout=timedelta(minutes=2))
    def resolve_run() -> str:
        ctx = get_current_context()
        run, params = ctx["dag_run"], ctx["params"]
        w = _wiring(params)

        def work():
            mode, start, end = window.run_window(
                run_type=run.run_type.value, data_interval_start=run.data_interval_start,
                data_interval_end=run.data_interval_end, interval_start=params.get("interval_start"),
                interval_end=params.get("interval_end"))
            return stages.resolve_run(w, dag_run_id=run.run_id, mode=mode, interval_start=start,
                                      interval_end=end)
        return _guarded(work)

    @task(execution_timeout=timedelta(minutes=1))
    def claim_writer(rid: str) -> int:
        return _guarded(stages.claim_writer, _wiring(get_current_context()["params"]), rid)

    @task.sensor(poke_interval=30, timeout=600, mode="reschedule", retries=0)
    def wait_for_manifests(rid: str) -> PokeReturnValue:
        ctx = get_current_context()
        w = _wiring(ctx["params"])
        ready = stages.manifests_ready(w, rid)
        if not ready and ctx["dag_run"].run_type.value == "manual":
            raise AirflowFailException(str(InputNotReady("manifest missing for a manual window")))
        return PokeReturnValue(is_done=ready)

    @task(execution_timeout=timedelta(minutes=2))
    def resolve_manifest(rid: str) -> dict:
        return _guarded(stages.resolve_manifest, _wiring(get_current_context()["params"]), rid)

    @task(execution_timeout=timedelta(minutes=5))
    def verify_manifest(rid: str) -> dict:
        return _guarded(stages.verify_manifest, _wiring(get_current_context()["params"]), rid)

    @task(pool="commerce_ingest", execution_timeout=timedelta(minutes=5), max_active_tis_per_dagrun=2)
    def ingest_source(rid: str, source: str) -> dict:
        return _guarded(stages.ingest_source, _wiring(get_current_context()["params"]), rid, source)

    @task(execution_timeout=timedelta(minutes=2))
    def reconcile_ingestion(rid: str) -> dict:
        return _guarded(stages.reconcile_ingestion, _wiring(get_current_context()["params"]), rid)

    @task(execution_timeout=timedelta(minutes=2))
    def choose_frontier(rid: str) -> dict:
        return _guarded(stages.choose_frontier, _wiring(get_current_context()["params"]), rid)

    @task(execution_timeout=timedelta(minutes=2))
    def prepare_candidate(rid: str) -> dict:
        return _guarded(stages.prepare_candidate, _wiring(get_current_context()["params"]), rid)

    @task(pool="commerce_transform", execution_timeout=timedelta(minutes=15))
    def build_candidate(rid: str) -> dict:
        return _guarded(stages.build_candidate_stage, _wiring(get_current_context()["params"]), rid)

    @task(execution_timeout=timedelta(minutes=5))
    def validate_candidate(rid: str) -> dict:
        return _guarded(stages.validate_candidate, _wiring(get_current_context()["params"]), rid)

    @task(execution_timeout=timedelta(seconds=60))
    def publish_release(rid: str) -> dict:
        return _guarded(stages.publish_release, _wiring(get_current_context()["params"]), rid)

    @task(execution_timeout=timedelta(minutes=2))
    def verify_serving(rid: str) -> dict:
        return _guarded(stages.verify_serving, _wiring(get_current_context()["params"]), rid)

    @task(trigger_rule=TriggerRule.ALL_DONE, retries=0, execution_timeout=timedelta(minutes=2))
    def pipeline_result() -> dict:
        w = _wiring(get_current_context()["params"])
        return stages.pipeline_result(w, _run_id(w))

    run = resolve_run()
    claimed = claim_writer(run)
    waited = wait_for_manifests(run)
    resolved = resolve_manifest(run)
    verified = verify_manifest(run)
    ingested = ingest_source.partial(rid=run).expand(source=list(stages.SOURCES))
    reconciled = reconcile_ingestion(run)
    frontier = choose_frontier(run)
    prepared = prepare_candidate(run)
    built = build_candidate(run)
    validated = validate_candidate(run)
    published = publish_release(run)
    served = verify_serving(run)
    final = pipeline_result()

    (run >> claimed >> waited >> resolved >> verified >> ingested >> reconciled >> frontier >> prepared
     >> built >> validated >> published >> served >> final)
    for upstream in (claimed, waited, resolved, verified, ingested, reconciled, frontier, prepared, built,
                     validated, published, served):
        upstream >> final
