FROM apache/airflow:slim-2.11.0-python3.10

ARG AIRFLOW_VAR_GITHUB_TOKEN
ENV AIRFLOW_VAR_GITHUB_TOKEN=${AIRFLOW_VAR_GITHUB_TOKEN}

USER root

WORKDIR /opt/airflow/dags/etl_github

COPY --chown=airflow:root . /opt/airflow/dags/etl_github

RUN mkdir -p data && \
    chown -R airflow:root /opt/airflow/dags/etl_github

USER airflow

RUN uv pip compile pyproject.toml > requirement.txt && uv pip install -r requirement.txt