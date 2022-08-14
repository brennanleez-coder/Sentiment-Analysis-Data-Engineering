from airflow import DAG
from datalab.context import Context
import time
import yfinance as yf
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from datetime import date
from pandas_gbq import schema
from google.cloud import bigquery
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryCreateExternalTableOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryDeleteTableOperator,
    BigQueryGetDatasetOperator,
    BigQueryGetDatasetTablesOperator,
    BigQueryUpdateDatasetOperator,
    BigQueryUpdateTableOperator,
    BigQueryUpdateTableSchemaOperator,
    BigQueryUpsertTableOperator,
    BigQueryInsertJobOperator,
)
from airflow.decorators import task
import os
import requests
import pandas as pd
from functools import reduce


tickers_For_YF = ['D05.SI', 'U11.SI', 'O39.SI','Z74.SI', 'F34.SI', 'C38U.SI', 'C6L.SI', 'V03.SI', 'BN4.SI']
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/airflow/airflow/secure/is3107-344108-724c758a845b.json"                                             
###########################################
#2. DEFINE AIRFLOW DAG (SETTINGS + SCHEDULE)
############################################
with DAG( 'daily_dag',
    description='Collect Stock Prices For Analysis',
    catchup=False, 
    start_date= datetime(2020, 12, 23), 
    schedule_interval= '0 0 * * *'  
) as dag:

##########################################
#3. DEFINE AIRFLOW OPERATORS
##########################################
    def get_macro_daily_data():
        #XAU is gold ticker 
        metal_api = "https://api.metalpriceapi.com/v1/latest?api_key=7ad73b7e233df83cf184d68c371b52ce&base=XAU&currencies=SGD"
        oil_api_url = 'https://api.oilpriceapi.com/v1/prices/latest'
        headers = {
            'Authorization': 'Token 9515339960308ebca14b83f114a542e0',
            'Content-Type': 'application/json'
        }
        gold_data = requests.get(url = metal_api).json()
        oil_data = requests.get(url = oil_api_url, headers = headers).json()
        today_date = date.today()
        df = pd.DataFrame({
            "date": [today_date,],
            "oil_price" : [oil_data['data']['price'],],
            "gold_price": [gold_data['rates']['SGD'],],
        })
        df['date'] = df['date'].astype('datetime64[ns]')
        df['year'] = df['date'].dt.year 
        df['month'] = df['date'].dt.month
        df['day'] = df['date'].dt.day
        df.drop('date', axis = 1, inplace = True)

        #Cast type according to schema
        df['year'] = df['year'].astype(int) 
        df['month'] = df['month'].astype(int)
        df['day'] = df['day'].astype(int)
        df['oil_price'] = df['oil_price'].astype(float)
        df['gold_price'] = df['gold_price'].astype(float)

        today = date.today()
        year = today.year
        month = today.month
        day = today.day

        query_string = f"SELECT * FROM macro_table.macro_data_daily WHERE year = {year} and month = {month} and day = {day}" 
        pulled_df = pd.read_gbq(query_string)
        df = df[(df['year'] == year) & (df['month'] == month) & (df['day'] == day)].reset_index(drop = True)

        if len(pulled_df) == 0 and len(df) == 1: #Append
            df.to_gbq('macro_table.macro_data_daily',  if_exists='append')

    

    def get_stock_daily_data():
        df_all = []
        for ticker in tickers_For_YF:
            temp = yf.Ticker(ticker).history(period = 'max')
            temp['ticker'] = [ticker,] * len(temp)
            df_all.append(temp)

        df = pd.concat(df_all)
        df.reset_index(inplace = True)
        df['year'] = df['Date'].dt.year
        df['month'] = df['Date'].dt.month
        df['day'] = df['Date'].dt.day

        df.sort_values('Date', ascending = True, inplace = True)
        df.drop('Date', axis = 1, inplace = True)
        new_names = ['open', 'high', 'low', 'close', 'volume', 'dividends', 'stock_splits', 'ticker', 'year', 'month', 'day']
        df.columns = new_names

        df['open'] = df['open'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['close'] = df['close'].astype(float)
        df['volume'] = df['volume'].astype(float)
        df['dividends'] = df['dividends'].astype(float)
        df['stock_splits'] = df['stock_splits'].astype(float)
        df['year'] = df['year'].astype(int)
        df['month'] = df['month'].astype(int)
        df['day'] = df['day'].astype(int)
        df.to_gbq('macro_table.fact_table',  if_exists='append')

    create_dataset_task = BigQueryCreateEmptyDatasetOperator(task_id="create-dataset", dataset_id="macro_table")
    
    create_table_task_daily = BigQueryCreateEmptyTableOperator(
        task_id="create_macro_table_daily",
        dataset_id="macro_table",
        table_id="macro_data_daily",
        schema_fields=[
            {"name": "year", "type":"INTEGER", "mode":"REQUIRED"},
            {"name": "month", "type":"INTEGER", "mode":"REQUIRED"},
            {"name": "day", "type":"INTEGER", "mode":"REQUIRED"},
            {"name": "oil_price", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "gold_price", "type": "FLOAT", "mode": "NULLABLE"}
        ],
    )

    create_fact_table = BigQueryCreateEmptyTableOperator(
        task_id = "create_fact_table",
        dataset_id = 'macro_table',
        table_id = 'fact_table',
        schema_fields = [
            {"name": "open", "type": "FLOAT", "mode": "REQUIRED"},
            {"name": "high", "type": "FLOAT", "mode": "REQUIRED"},
            {"name": "low", "type": "FLOAT", "mode": "REQUIRED"},
            {"name": "close", "type": "FLOAT", "mode": "REQUIRED"},
            {"name": "volume", "type": "FLOAT", "mode": "REQUIRED"},
            {"name": "dividends", "type": "FLOAT", "mode": "REQUIRED"},
            {"name": "stock_splits", "type": "FLOAT", "mode": "REQUIRED"},
            {"name": "ticker", "type": "STRING", "mode": "REQUIRED"},
            {"name": "year", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "month", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "day", "type": "INTEGER", "mode": "REQUIRED"},
        ]
    )


    scrape_macro_daily_data = PythonOperator(
        task_id = "get_macro_daily_data", 
        python_callable = get_macro_daily_data,
        dag = dag)



    scrape_daily_stock_info = PythonOperator(
        task_id = "get_stock_daily_data",
        python_callable = get_stock_daily_data,
        dag = dag
    )


##########################################
#4. DEFINE OPERATORS HIERARCHY
##########################################

create_dataset_task >> [create_table_task_daily,  create_fact_table] 

create_table_task_daily >> scrape_macro_daily_data
create_fact_table >> scrape_daily_stock_info
# >> [ scrape_macro_monthly_data, scrape_macro_quarterly_data]
