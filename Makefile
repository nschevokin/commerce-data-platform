SHELL := /bin/bash
.DEFAULT_GOAL := help
UV ?= uv
PY := .venv/bin/python
DBT := envs/dbt/.venv/bin/dbt

help:  ## list targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-16s %s\n", $$1, $$2}'

setup:  ## create both locked environments (app + isolated dbt)
	$(UV) sync --frozen --python 3.12
	cd envs/dbt && $(UV) sync --frozen --python 3.12

lint:  ## static checks
	.venv/bin/ruff check src tests dags dashboard
	$(DBT) parse --project-dir dbt --profiles-dir dbt --vars '{candidate_schema: cand_00000000000000000000000000000000, source_schema: x, run_id: x}' --target-path artifacts/parse >/dev/null

airflow-env:  ## locked Airflow 3.3.2 environment for the DAG tests (envs/airflow, not committed)
	$(UV) venv --python 3.12 envs/airflow/.venv
	$(UV) pip install --python envs/airflow/.venv/bin/python -r docker/airflow/requirements.lock pytest
	AIRFLOW_HOME=$(CURDIR)/artifacts/airflow_home envs/airflow/.venv/bin/airflow db migrate

test-airflow:  ## execute the real DAG with Airflow (needs airflow-env and COMMERCE_TEST_SERVER_DSN)
	@test -n "$$COMMERCE_TEST_SERVER_DSN" || (echo "set COMMERCE_TEST_SERVER_DSN"; exit 2)
	AIRFLOW_HOME=$(CURDIR)/artifacts/airflow_home AIRFLOW__CORE__LOAD_EXAMPLES=False \
	PYTHONPATH=$(CURDIR)/src:$(CURDIR)/tests/unit envs/airflow/.venv/bin/python -W ignore -m pytest -q tests/dag

airflow-scheduler-check:  ## real scheduler/dag-processor/api-server run of the DAG (needs airflow-env + COMMERCE_TEST_SERVER_DSN)
	@test -n "$$COMMERCE_TEST_SERVER_DSN" || (echo "set COMMERCE_TEST_SERVER_DSN"; exit 2)
	PYTHONPATH=$(CURDIR)/src envs/airflow/.venv/bin/python scripts/airflow_scheduler_check.py

dashboard-up:  ## start the read-only dashboard container on 127.0.0.1:8501 (needs a published release)
	docker compose --profile dashboard up -d --build --wait dashboard

airflow-up:  ## start Airflow (api-server, scheduler, dag-processor); needs COMMERCE_DATASET_SHA256 in .env
	docker compose --profile airflow up -d --build --wait airflow-api-server airflow-scheduler airflow-dag-processor

dashboard-env:  ## locked Streamlit environment (envs/dashboard, not committed)
	$(UV) venv --python 3.12 envs/dashboard/.venv
	$(UV) pip install --python envs/dashboard/.venv/bin/python -r envs/dashboard/requirements.lock

test-dashboard:  ## run the real Streamlit page headless (needs dashboard-env and COMMERCE_TEST_SERVER_DSN)
	@test -n "$$COMMERCE_TEST_SERVER_DSN" || (echo "set COMMERCE_TEST_SERVER_DSN"; exit 2)
	PYTHONPATH=$(CURDIR)/src envs/dashboard/.venv/bin/python -m pytest -q -p no:cacheprovider tests/dashboard

dashboard:  ## serve the dashboard on http://127.0.0.1:8501 (reads COMMERCE_* settings; see README)
	COMMERCE_DASHBOARD_PASSWORD_FILE=$${COMMERCE_DASHBOARD_PASSWORD_FILE:-secrets/commerce_dashboard_password} \
	COMMERCE_WAREHOUSE_PORT=$${COMMERCE_WAREHOUSE_PORT:-5432} PYTHONPATH=$(CURDIR)/src \
	envs/dashboard/.venv/bin/streamlit run dashboard/app.py --server.address 127.0.0.1 --server.port 8501 \
	--browser.gatherUsageStats false

backup:  ## consistent dump + manifest into artifacts/backups (host needs pg_dump 16: brew install postgresql@16)
	COMMERCE_ADMIN_DSN_FILE=$${COMMERCE_ADMIN_DSN_FILE:-secrets/warehouse_admin_dsn_host} \
	.venv/bin/commerce backup --out artifacts/backups

restore-drill:  ## restore a dump into a throwaway database and verify it: make restore-drill DUMP=... MANIFEST=...
	@test -n "$(DUMP)" -a -n "$(MANIFEST)" -a -n "$$COMMERCE_TEST_SERVER_DSN" || (echo 'usage: COMMERCE_TEST_SERVER_DSN=<superuser dsn without dbname> make restore-drill DUMP=x.dump MANIFEST=x.manifest.json'; exit 2)
	.venv/bin/commerce restore-drill --dump "$(DUMP)" --manifest "$(MANIFEST)" --server-dsn "$$COMMERCE_TEST_SERVER_DSN"

parity:  ## replay the golden fixture and compare checksums with the committed reference (needs COMMERCE_TEST_SERVER_DSN)
	@test -n "$$COMMERCE_TEST_SERVER_DSN" || (echo "set COMMERCE_TEST_SERVER_DSN"; exit 2)
	.venv/bin/commerce parity --server-dsn "$$COMMERCE_TEST_SERVER_DSN"

candidates-gc:  ## list old candidate schemas that would be dropped (add APPLY=1 to drop them)
	COMMERCE_ADMIN_DSN_FILE=$${COMMERCE_ADMIN_DSN_FILE:-secrets/warehouse_admin_dsn_host} \
	.venv/bin/commerce candidates-gc $(if $(APPLY),--apply,)

test-unit:  ## unit tests (no database)
	.venv/bin/pytest -q tests/unit

test:  ## unit + integration tests (needs COMMERCE_TEST_SERVER_DSN)
	@test -n "$$COMMERCE_TEST_SERVER_DSN" || (echo "set COMMERCE_TEST_SERVER_DSN (see .env.example)"; exit 2)
	.venv/bin/pytest -q

bootstrap:  ## secrets, databases, migrations, role logins (Docker)
	./scripts/init_local_secrets.sh
	docker compose up -d --wait warehouse-db airflow-db
	docker compose run --rm --build migrate

fixture-slice:  ## build the golden fixture candidate and compare with expected.json
	COMMERCE_ADMIN_DSN_FILE=$${COMMERCE_ADMIN_DSN_FILE:-secrets/warehouse_admin_dsn_host} \
	COMMERCE_TRANSFORM_PASSWORD_FILE=$${COMMERCE_TRANSFORM_PASSWORD_FILE:-secrets/commerce_transform_password} \
	COMMERCE_WAREHOUSE_PORT=$${COMMERCE_WAREHOUSE_PORT:-5432} \
	.venv/bin/commerce fixture-slice tests/fixtures/golden_v1

stop:  ## stop services, keep volumes
	docker compose stop

import-data:  ## inventory+profile the Olist zip: make import-data ARCHIVE=... RETRIEVED_ON=YYYY-MM-DD LICENCE="..."
	@test -n "$(ARCHIVE)" -a -n "$(RETRIEVED_ON)" -a -n "$(LICENCE)" || (echo 'usage: make import-data ARCHIVE=path.zip RETRIEVED_ON=YYYY-MM-DD LICENCE="text from the dataset page"'; exit 2)
	.venv/bin/commerce import-data --archive "$(ARCHIVE)" --retrieved-on "$(RETRIEVED_ON)" --licence "$(LICENCE)" --store data/store

replay:  ## baseline replay of SOURCE (zip or dir; default: the fixture) into data/store
	.venv/bin/commerce replay baseline --source $${SOURCE:-tests/fixtures/golden_v1} --store data/store

demo verify evidence:  ## planned (blueprint §30), not implemented yet
	@echo "'$@' is planned for a later ticket (see PROGRESS.md)"; exit 3
