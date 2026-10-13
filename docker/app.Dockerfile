FROM python:3.12-slim-bookworm
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
RUN pip install --no-cache-dir "uv==0.11.32"
WORKDIR /opt/commerce
COPY pyproject.toml uv.lock README.md ./
COPY envs/dbt/ envs/dbt/
RUN uv sync --frozen --no-dev --no-install-project \
 && cd envs/dbt && uv sync --frozen
COPY src/ src/
COPY migrations/ migrations/
COPY dbt/ dbt/
RUN uv sync --frozen --no-dev
ENV PATH="/opt/commerce/.venv/bin:$PATH" \
    COMMERCE_DBT_EXE=/opt/commerce/envs/dbt/.venv/bin/dbt
RUN useradd --uid 50000 --create-home commerce && chown -R commerce /opt/commerce
USER commerce
