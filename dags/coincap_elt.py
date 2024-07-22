"""
main airflow dag file to load data from coincap api, save it into csv file, and create visualization of it
"""
import csv
import os
from datetime import datetime, timedelta

import requests
from cuallee import Check, CheckLevel
import polars as pl

from airflow import DAG
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

# varialbes
API_URL = "https://api.coincap.io/v2/exchanges"
file_path = f'{os.getenv("AIRFLOW_HOME")}/data/'

default_args = {
    'owner': 'user1',
}

# airflow task and dag 
@task
def fetch_coincap_exchanges(url: str, file_path: str):
    """
    load data from api and save it as a csv file
    """
    # load data from json to polars.DataFrame
    response = requests.get(url)
    temp_data = response.json()
    data = temp_data['data']
    df = pl.from_dicts(data)

    # transform data
    for  column_name in df.columns:
        df = fill_missing_value(df, column_name)
    df = convert_to_datetime(df)
    df = change_specific_str_col_to_num(df)
    #print(df.null_count().sum_horizontal())

    # write data
    save_csv_polars(df, file_path)
    #save_parquet_polars(df, file_path)

def fill_missing_value(df: pl.DataFrame, column_name: str) -> pl.DataFrame:
    """
    fill missing values based on data type
    """
    if df.schema[column_name] == pl.String:
        df = df.with_columns(pl.col(column_name).fill_null("0"))
    elif df.schema[column_name] == pl.Boolean:
        df = df.with_columns(pl.col(column_name).fill_null(False))
    else:
        df = df.with_columns(pl.col(column_name).fill_null(strategy="zero"))

    #print("after")
    #print(df.select(column_name).null_count())
    return df

def convert_to_datetime(df: pl.DataFrame) -> pl.DataFrame:
    """
    convert timestamp (milliseconds) to datetime
    """
    dt = df.with_columns(
        pl.from_epoch(pl.col('updated'), time_unit="ms").alias('datetime')
    )
    return dt

def change_specific_str_col_to_num(df: pl.DataFrame) -> pl.DataFrame:
    """
    convert string to int/float
    """
    try:
        df = df.with_columns(
            pl.col("percentTotalVolume").cast(pl.Float64).alias("percentTotalVolume"),
            pl.col("volumeUsd").cast(pl.Float64).alias("volumeUsd"),
            pl.col("tradingPairs").cast(pl.Int32).alias("tradingPairs"),
            )
    except Exception as e:
        print(e)
    return df

def save_csv(data, file_path):
    """
    from the original template, to remind me how to use Dictwriter
    """
    exchanges = data['data']
    if exchanges:
        keys = exchanges[0].keys()
        with open(file_path, 'w') as f:
            dict_writer = csv.DictWriter(f, fieldnames=keys)
            dict_writer.writeheader()
            dict_writer.writerows(exchanges)

def save_csv_polars(df: pl.DataFrame, file_path: str):
    """
    write data into csv
    """
    now = datetime.now()
    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")
    file_name = f"coincap_exchanges_{day}_{month}_{year}.csv"
    
    df.write_csv(file_path+file_name)

def save_parquet_polars(df: pl.DataFrame, file_path: str):
    """
    write data into parquet
    """
    now = datetime.now()
    year = now.strftime("%Y")
    month = now.strftime("%m")
    day = now.strftime("%d")
    file_name = f"coincap_exchanges_{day}_{month}_{year}.parquet"

    df.write_parquet(file_path+file_name)

def check_completeness(pl_df: pl.DataFrame, column_name: str):
    """
    ensure that the file has no null value
    """
    check = Check(CheckLevel.ERROR, "Completeness")
    validation_results_df = (
        check.is_complete(column_name).validate(pl_df)
    )
    return validation_results_df["status"].to_list()

#@dag(default_args=default_args, description="A simple DAG to fetch data from CoinCap Exchanges API and write to a file", schedule="@daily", start_date=datetime(2024, 1, 1), catchup=False,)
@dag(default_args=default_args, schedule="@once", start_date=days_ago(1))
def coincap_load_dag():
    """
    airflow dag function
    """
    @task.branch
    def check_data_quality(validation_results):
        """
        check data before creating dashboard
        """
        if "FAIL" not in validation_results:
            return ['generate_dashboard']
        return ['stop_pipeline']

    # check if "name" column has null or not 
    #check_data_quality_instance = check_data_quality(check_completeness(pl.read_csv(file_path), "name"))

    # dummy for stopping after find null in data
    stop_pipeline = DummyOperator(task_id='stop_pipeline')

    # create visualization
    #markdown_path = f'{os.getenv("AIRFLOW_HOME")}/visualization/'
    #q_cmd = ( f'cd {markdown_path} && quarto render {markdown_path}/dashboard.qmd' )
    #gen_dashboard = BashOperator( task_id="generate_dashboard", bash_command=q_cmd )

    #fetch_coincap_exchanges(API_URL, file_path) >> check_data_quality_instance >> gen_dashboard
    #check_data_quality_instance >> stop_pipeline

    fetch_coincap_exchanges(API_URL, file_path)
    

coincap_load_dag()
