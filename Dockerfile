FROM apache/airflow:2.5.0

USER root
RUN apt-get update && apt-get install -y --no-install-recommends chromium

USER airflow
COPY requirements.txt /
RUN pip install --upgrade pip && pip install --no-cache-dir -r /requirements.txt