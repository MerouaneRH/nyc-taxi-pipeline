FROM apache/airflow:2.10.5-python3.12

# Project deps + dbt live in a dedicated venv so they cannot clash with
# Airflow's constrained dependency set. DAG tasks call /opt/venv/bin/*.
USER root
RUN python -m venv /opt/venv && chown -R airflow:0 /opt/venv
USER airflow

# NOTE: ingestion deps below mirror pyproject.toml runtime deps; boto3 is
# pulled in by dbt-athena. Keep in sync when ingestion/ adds libraries.
RUN /opt/venv/bin/pip install --no-cache-dir \
      "dbt-core==1.12.6" \
      "dbt-athena==1.11.1" \
      "pandas<3" \
      "pyarrow>=26.0.0" \
      "requests>=2.34.2" \
      "boto3"

ENV PYTHONPATH=/opt/project
