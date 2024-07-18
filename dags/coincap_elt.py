import csv
import os
from datetime import datetime, timedelta

import requests
from cuallee import Check, CheckLevel
import polars as pl

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

API_URL = "https://api.coincap.io/v2/exchanges"
file_path = f'{os.getenv("AIRFLOW_HOME")}/data/coincap_exchanges.csv'

default_args = {
    'owner': 'user1',
}

@task
def fetch_coincap_exchanges(url, file_path):
    response = requests.get(url)
    data = response.json()
    exchanges = data['data']
    if exchanges:
        keys = exchanges[0].keys()
        with open(file_path, 'w') as f:
            dict_writer = csv.DictWriter(f, fieldnames=keys)
            dict_writer.writeheader()
            dict_writer.writerows(exchanges)

def check_completeness(pl_df, column_name):
    check = Check(CheckLevel.ERROR, "Completeness")
    validation_results_df = (
        check.is_complete(column_name).validate(pl_df)
    )
    return validation_results_df["status"].to_list()

@task.branch
def check_data_quality(validation_results):
    if "FAIL" not in validation_results:
        return ['generate_dashboard']
    return ['stop_pipeline']


@dag(
    default_args=default_args,
    description="A simple DAG to fetch data \
    from CoinCap Exchanges API and write to a file",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
     )
def coincap_load_dag():

    check_data_quality_instance = check_data_quality(check_completeness(pl.read_csv(file_path), "name"))

    stop_pipeline = DummyOperator(task_id='stop_pipeline')

    markdown_path = f'{os.getenv("AIRFLOW_HOME")}/visualization/'
    q_cmd = (
        f'cd {markdown_path} && quarto render {markdown_path}/dashboard.qmd'
    )
    gen_dashboard = BashOperator(
        task_id="generate_dashboard", bash_command=q_cmd
    )

    #fetch_coincap_exchanges(API_URL, file_path) >> check_data_quality_instance >> gen_dashboard
    #check_data_quality_instance >> stop_pipeline

    test1 = DummyOperator(task_id='test1')
    test2 = DummyOperator(task_id='test2')

    test1>>test2

coincap_load_dag()
