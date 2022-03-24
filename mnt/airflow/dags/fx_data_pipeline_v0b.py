from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from datetime import datetime, timedelta

default_args = {
    "owner": "Kyle Binder",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 1,
    "tetry_delay": timedelta(minutes=5),
}

with DAG(
    "fx_data_pipeline_v0b",
    start_date=datetime(2022, 3, 10),
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
) as dag:

    is_forex_rates_available = HttpSensor(
        task_id="is_forex_rates_available",
        http_conn_id="forex_api",  # to be specified as GIST URL
        endpoint="marclamberti/f45f872dea4dfd3eaa015a4a1af4b39b",
        response_check=lambda response: "rates" in response.text,
        poke_interval=5,  # seconds
        timeout=20,  # seconds
    )
