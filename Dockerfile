FROM apache/airflow:3.3.1

# Dependencies before source: DAG edits shouldn't invalidate the pip layer.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
