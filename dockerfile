FROM apache/airflow:2.7.2-python3.10
RUN pip install --no-cache-dir exchange-calendars
