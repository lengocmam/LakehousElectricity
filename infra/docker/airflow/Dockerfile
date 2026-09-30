ARG AIRFLOW_IMAGE=apache/airflow:3.3.1-python3.10
FROM ${AIRFLOW_IMAGE}

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps ca-certificates \
    && update-ca-certificates \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
COPY infra/docker/airflow/requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
