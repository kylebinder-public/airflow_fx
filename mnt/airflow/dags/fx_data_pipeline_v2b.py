from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from datetime import datetime, timedelta
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator

import json
import requests
import csv


default_args = {
    "owner": "Kyle Binder",
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "admin@localhost.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def downloading_rates():
    BASE_URL = "https://gist.githubusercontent.com/marclamberti/f45f872dea4dfd3eaa015a4a1af4b39b/raw/"
    ENDPOINTS = {
        "USD": "api_forex_exchange_usd.json",
        "EUR": "api_forex_exchange_eur.json",
    }
    with open("/opt/airflow/dags/files/forex_currencies.csv") as forex_currencies:
        reader = csv.DictReader(forex_currencies, delimiter=";")
        for idx, row in enumerate(reader):
            base = row["base"]
            with_pairs = row["with_pairs"].split(" ")
            indata = requests.get(f"{BASE_URL}{ENDPOINTS[base]}").json()
            outdata = {"base": base, "rates": {}, "last_update": indata["date"]}
            for pair in with_pairs:
                outdata["rates"][pair] = indata["rates"][pair]
            with open("/opt/airflow/dags/files/forex_rates.json", "a") as outfile:
                json.dump(outdata, outfile)
                outfile.write("\n")


with DAG(
    "fx_data_pipeline_v2b",
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

    is_fx_csv_available = FileSensor(
        task_id="is_fx_csv_available",
        fs_conn_id="fx_path",
        filepath="forex_currencies.csv",
    )

    call_download_rates = PythonOperator(
        task_id="call_download_rates", python_callable=downloading_rates,
    )

    saving_rates = BashOperator(
        task_id="saving_rates",
        # bash command to be executed when task is reached
        bash_command="""
            hdfs dfs -mkdir -p /forex && \
            hdfs dfs -put -f $AIRFLOW_HOME/dags/files/fx_rates.json /forex
        """,
    )

    creating_forex_rates_table = HiveOperator(
        task_id="creating_forex_rates_table",
        hive_cli_conn_id="hive_conn",
        hql="""
                CREATE EXTERNAL TABLE IF NOT EXISTS forex_rates(
                    base STRING,
                    last_update DATE,
                    eur DOUBLE,
                    usd DOUBLE,
                    nzd DOUBLE,
                    gbp DOUBLE,
                    jpy DOUBLE,
                    cad DOUBLE
                    )
                ROW FORMAT DELIMITED
                FIELDS TERMINATED BY ','
                STORED AS TEXTFILE
            """,
    )
