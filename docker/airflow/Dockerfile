FROM apache/airflow:3.3.2-python3.12
USER root
RUN mkdir -p /opt/commerce /data/store /data/artifacts && chown -R airflow /opt/commerce /data
USER airflow
WORKDIR /opt/commerce
COPY docker/airflow/constraints-3.3.2-py3.12.txt /tmp/constraints.txt
RUN pip install --no-cache-dir "apache-airflow==3.3.2" "psycopg[binary]>=3.3.5,<3.4" \
    --constraint /tmp/constraints.txt
RUN pip install --no-cache-dir "uv==0.11.32"
COPY --chown=airflow envs/dbt/ envs/dbt/
RUN cd envs/dbt && uv sync --frozen --python 3.12
COPY --chown=airflow src/ src/
COPY --chown=airflow migrations/ migrations/
COPY --chown=airflow dbt/ dbt/
COPY --chown=airflow dags/ /opt/airflow/dags/
ENV PYTHONPATH=/opt/commerce/src COMMERCE_DBT_EXE=/opt/commerce/envs/dbt/.venv/bin/dbt
