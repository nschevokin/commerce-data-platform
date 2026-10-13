FROM python:3.12-slim-bookworm
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/opt/commerce/src
WORKDIR /opt/commerce
COPY envs/dashboard/requirements.lock /tmp/requirements.lock
RUN pip install --no-cache-dir -r /tmp/requirements.lock
COPY src/ src/
COPY dashboard/ dashboard/
COPY .streamlit/ .streamlit/
RUN useradd --uid 50001 --create-home viewer && chown -R viewer /opt/commerce
USER viewer
EXPOSE 8501
CMD ["streamlit", "run", "dashboard/app.py", "--server.address", "0.0.0.0", "--server.port", "8501", \
     "--browser.gatherUsageStats", "false"]
