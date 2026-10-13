#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p secrets && chmod 700 secrets
gen() { [ -s "secrets/$1" ] || (umask 077; python3 -c 'import secrets;print(secrets.token_urlsafe(24))' > "secrets/$1"); }
for s in warehouse_admin_password airflow_db_password commerce_ingest_password \
         commerce_transform_password commerce_publish_password commerce_dashboard_password \
         airflow_jwt_secret; do gen "$s"; done
[ -s secrets/warehouse_admin_dsn ] || (umask 077; printf 'host=warehouse-db port=5432 dbname=commerce user=commerce_admin password=%s\n' \
  "$(cat secrets/warehouse_admin_password)" > secrets/warehouse_admin_dsn)
[ -s secrets/warehouse_admin_dsn_host ] || (umask 077; printf 'host=127.0.0.1 port=%s dbname=commerce user=commerce_admin password=%s\n' \
  "${WAREHOUSE_PORT:-5432}" "$(cat secrets/warehouse_admin_password)" > secrets/warehouse_admin_dsn_host)
[ -s secrets/airflow_fernet_key ] || (umask 077; python3 -c 'import base64,os;print(base64.urlsafe_b64encode(os.urandom(32)).decode())' > secrets/airflow_fernet_key)
echo "local secrets ready in ./secrets (git-ignored)"
