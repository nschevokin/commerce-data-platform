# Running it, and what it is not

## Quick start

```bash
make setup
make bootstrap
make fixture-slice
make test
make parity
make dashboard
```

`make test` needs `COMMERCE_TEST_SERVER_DSN`, a PostgreSQL 16 superuser DSN on a server where throwaway databases may
be created. `make fixture-slice` prints eligible orders 2, merchandise BRL 130.00, freight 13.00, recorded payments
143.00, AOV 65.00, on-time rate 50%. Airflow: `make airflow-up` (Compose) or `make airflow-scheduler-check` (native).
Walk-through: `docs/demo.md`. Failure procedures: `docs/runbooks.md`.

## Limits

* No cloud deployment. Raw storage is a local directory; there is no object-store adapter.
* Small data (about 450k events) on one PostgreSQL engine. Measured figures describe one 2-vCPU VM only.
* The stream is a replayed historical snapshot with simulated late records and corrections, not real CDC.
* Not verified: the dashboard Docker image, a pipeline run inside the Airflow containers, the CI workflow on GitHub,
  sensor rescheduling under a real scheduler.
* The dataset (Olist, CC BY-NC-SA 4.0) is not redistributed; `evidence/replay/olist_inventory.json` records its
  checksums and retrieval date.
