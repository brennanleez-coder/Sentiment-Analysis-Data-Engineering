from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta, date  
import datetime as dt
import pandas as pd
from airflow.models import DAG

from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator

import json
from datetime import datetime
import time
import yfinance as yf
import numpy as np
import requests
import lxml
from pandas_gbq import schema
from functools import reduce
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

import yahoo_fin.stock_info as si

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/airflow/airflow/secure/is3107-344108-724c758a845b.json"



with DAG( 'weekly_dag',
    description='Weekly collection of financial data and load into data warehouse big query',
    catchup=False, 
    start_date= datetime(2020, 12, 23), 
    schedule_interval= '0 0 * * 0'  
) as dag:   
    tickers_For_YF = ['D05.SI', 'U11.SI', 'O39.SI','Z74.SI', 'F34.SI', 'C38U.SI', 'C6L.SI', 'V03.SI', 'BN4.SI']

############################################
#1. DEFINE PYTHON FUNCTIONS 
############################################

    def fit_into_financial_ratio(**kwargs):
        fundamental_data= {}
        for ticker in tickers_For_YF:
            fundamental_data[ticker] = si.get_quote_table(ticker, dict_result=False)

        # statsValuation = {}
        # for ticker in tickers_For_YF:
        #     statsValuation[ticker] = si.get_stats_valuation(ticker)

        miscellaneousStats = {}
        for ticker in tickers_For_YF:
            miscellaneousStats[ticker] = yf.Ticker(ticker).stats()

        stats = {}

        for ticker in tickers_For_YF:
            stats[ticker] = si.get_stats(ticker)

        dict = {"year": [], "month": [], "day": [], "company":[], "value_PE_RATIO":[], "value_PEG_RATIO":[], "value_PRICE_SALES_RATIO":[], "value_PRICE_BOOK_RATIO":[], "value_PROFIT_MARGIN":[], "value_PAYOUT_RATIO":[], "value_ROE":[], "value_ROA":[]}

        for ticker in tickers_For_YF:
            now = datetime.now() # current date and time
            dict["year"].append(int(now.year))
            dict["month"].append(int(now.month))
            dict["day"].append(int(now.day))
            dict["company"].append(str(ticker))
            dict["value_PE_RATIO"].append(fundamental_data[ticker].loc[13,"value"]if fundamental_data[ticker].loc[13,"value"] != None else np.NaN)
            dict["value_PEG_RATIO"].append(miscellaneousStats[ticker]['defaultKeyStatistics']['priceToBook'] if miscellaneousStats[ticker]['defaultKeyStatistics']['priceToBook'] != None else np.NaN)
            dict["value_PRICE_SALES_RATIO"].append(miscellaneousStats[ticker]['defaultKeyStatistics']['pegRatio'] if miscellaneousStats[ticker]['defaultKeyStatistics']['pegRatio'] != None else np.NaN)
            dict["value_PRICE_BOOK_RATIO"].append(miscellaneousStats[ticker]['defaultKeyStatistics']['priceToSalesTrailing12Months'] if miscellaneousStats[ticker]['defaultKeyStatistics']['priceToSalesTrailing12Months'] != None else np.NaN)
            dict["value_PROFIT_MARGIN"].append(miscellaneousStats[ticker]["defaultKeyStatistics"]["profitMargins"] if miscellaneousStats[ticker]["defaultKeyStatistics"]["profitMargins"] != None else np.NaN)
            dict["value_PAYOUT_RATIO"].append(stats[ticker].loc[24,"Value"]if stats[ticker].loc[24,"Value"] != None else np.NaN)
            dict["value_ROE"].append(miscellaneousStats[ticker]["financialData"]["returnOnEquity"] if miscellaneousStats[ticker]["financialData"]["returnOnEquity"] != None else np.NaN)
            dict["value_ROA"].append(miscellaneousStats[ticker]["financialData"]["returnOnAssets"] if miscellaneousStats[ticker]["financialData"]["returnOnAssets"] != None else np.NaN)
        
  
        financialRatio = pd.DataFrame.from_dict(dict).astype(str)

        financialRatio["year"] = financialRatio["year"].astype(int)
        financialRatio["month"] = financialRatio["month"].astype(int)
        financialRatio["day"] = financialRatio["day"].astype(int)
        financialRatio["company"] = financialRatio["company"].astype(str)
        financialRatio["value_PE_RATIO"] = financialRatio["value_PE_RATIO"].astype(float)
        financialRatio["value_PEG_RATIO"] = financialRatio["value_PEG_RATIO"].astype(float)
        financialRatio["value_PRICE_SALES_RATIO"] = financialRatio["value_PRICE_SALES_RATIO"].astype(float)
        financialRatio["value_PRICE_BOOK_RATIO"] = financialRatio["value_PRICE_BOOK_RATIO"].astype(float)
        financialRatio["value_PROFIT_MARGIN"] = financialRatio["value_PROFIT_MARGIN"].astype(float)
        financialRatio["value_PAYOUT_RATIO"] = financialRatio["value_PAYOUT_RATIO"].astype(str)
        financialRatio["value_ROE"] = financialRatio["value_ROE"].astype(float)
        financialRatio["value_ROA"] = financialRatio["value_ROA"].astype(float)


        today = date.today()
        year = today.year
        month = today.month


        query_string = f"SELECT * FROM financial_ratio_table.financial_ratio_table WHERE year = {year} and month = {month}" 
        pulled_df = pd.read_gbq(query_string)

        if len(pulled_df) < 1 :
            financialRatio.to_gbq('financial_ratio_table.financial_ratio_table', if_exists='append', table_schema=[
                {"name": "year", "type":"INTEGER"},
                {"name": "month", "type":"INTEGER"},
                {"name": "day", "type":"INTEGER"},
                {"name": "company", "type": "STRING"},
                {"name": "value_PE_RATIO", "type":"FLOAT"},
                {"name": "value_PEG_RATIO", "type":"FLOAT"},
                {"name": "value_PRICE_SALES_RATIO", "type":"FLOAT"},
                {"name": "value_PRICE_BOOK_RATIO", "type":"FLOAT"},
                {"name": "value_PROFIT_MARGIN", "type":"FLOAT"},
                {"name": "value_PAYOUT_RATIO", "type":"STRING"},
                {"name": "value_ROE", "type":"FLOAT"},
                {"name": "value_ROA", "type":"FLOAT"},
            ])    

        elif len(pulled_df) == 9 and pulled_df.reset_index().equals(financialRatio.reset_index()): #Means that this year and month already has values, but new values are detected
            query_string = f"DELETE FROM financial_ratio_table.financial_ratio_table WHERE year = {year} and month = {month}" 
            pd.read_gbq(query_string)
            financialRatio.to_gbq('financial_ratio_table.financial_ratio_table', if_exists='append', table_schema=[
                {"name": "year", "type":"INTEGER"},
                {"name": "month", "type":"INTEGER"},
                {"name": "day", "type":"INTEGER"},
                {"name": "company", "type": "STRING"},
                {"name": "value_PE_RATIO", "type":"FLOAT"},
                {"name": "value_PEG_RATIO", "type":"FLOAT"},
                {"name": "value_PRICE_SALES_RATIO", "type":"FLOAT"},
                {"name": "value_PRICE_BOOK_RATIO", "type":"FLOAT"},
                {"name": "value_PROFIT_MARGIN", "type":"FLOAT"},
                {"name": "value_PAYOUT_RATIO", "type":"STRING"},
                {"name": "value_ROE", "type":"FLOAT"},
                {"name": "value_ROA", "type":"FLOAT"},
            ])
    



    #monthly
    def fit_into_key_metrics(**kwargs):


        fundamental_data= {}
        for ticker in tickers_For_YF:
            fundamental_data[ticker] = si.get_quote_table(ticker, dict_result=False)

            stats = {}

        for ticker in tickers_For_YF:
            stats[ticker] = si.get_stats(ticker)        


        dict = {"year": [], "month": [], "day": [], "company": [], "value_REVENUE_PER_SHARE_TTM": [], "value_FREE_CASHFLOW_TTM":[], "value_CASH_PER_SHARE_TTM":[], "value_TOTAL_NET_INCOME_TTM":[], "value_EARNINGS_PER_SHARE_TTM":[]}
        for ticker in tickers_For_YF:
            now = datetime.now() # current date and time
            dict["year"].append(int(now.year))
            dict["month"].append(int(now.month))
            dict["day"].append(int(now.day))
            dict["company"].append(str(ticker))
            dict["value_REVENUE_PER_SHARE_TTM"].append(stats[ticker].loc[36,"Value"] if stats[ticker].loc[36,"Value"] != None else np.NaN)
            dict["value_FREE_CASHFLOW_TTM"].append(stats[ticker].loc[0,"Value"] if stats[ticker].loc[0,"Value"] != None else np.NaN)
            dict["value_CASH_PER_SHARE_TTM"].append(stats[ticker].loc[37,"Value"] if stats[ticker].loc[37,"Value"] != None else np.NaN)
            dict["value_TOTAL_NET_INCOME_TTM"].append(stats[ticker].loc[40,"Value"]if stats[ticker].loc[40,"Value"] != None else np.NaN)
            dict["value_EARNINGS_PER_SHARE_TTM"].append(fundamental_data[ticker].loc[7,"value"]if fundamental_data[ticker].loc[7,"value"] != None else np.NaN)
        
            
        keymetrics = pd.DataFrame.from_dict(dict).astype(str)

        keymetrics["year"] = keymetrics["year"].astype(int)
        keymetrics["month"] = keymetrics["month"].astype(int)
        keymetrics["day"] = keymetrics["day"].astype(int)
        keymetrics["company"] = keymetrics["company"].astype(str)
        keymetrics["value_REVENUE_PER_SHARE_TTM"] = keymetrics["value_REVENUE_PER_SHARE_TTM"].astype(str)
        keymetrics["value_FREE_CASHFLOW_TTM"] = keymetrics["value_FREE_CASHFLOW_TTM"].astype(str)
        keymetrics["value_CASH_PER_SHARE_TTM"] = keymetrics["value_CASH_PER_SHARE_TTM"].astype(str)
        keymetrics["value_TOTAL_NET_INCOME_TTM"] = keymetrics["value_TOTAL_NET_INCOME_TTM"].astype(str)
        keymetrics["value_EARNINGS_PER_SHARE_TTM"] = keymetrics["value_EARNINGS_PER_SHARE_TTM"].astype(str)


        today = date.today()
        year = today.year
        month = today.month


        query_string = f"SELECT * FROM key_metrics_table.key_metrics_table WHERE year = {year} and month = {month}" 
        pulled_df = pd.read_gbq(query_string)

        if len(pulled_df) < 1 : 
            keymetrics.to_gbq('key_metrics_table.key_metrics_table', if_exists='append', table_schema=[
                {"name": "year", "type":"INTEGER"},
                {"name": "month", "type":"INTEGER"},
                {"name": "day", "type":"INTEGER"},
                {"name": "company", "type":"STRING"},
                {"name": "value_REVENUE_PER_SHARE_TTM", "type":"STRING"},
                {"name": "value_FREE_CASHFLOW_TTM", "type":"STRING"},
                {"name": "value_CASH_PER_SHARE_TTM", "type":"STRING"},
                {"name": "value_TOTAL_NET_INCOME_TTM", "type":"STRING"},
                {"name": "value_EARNINGS_PER_SHARE_TTM", "type":"STRING"},
            ])
        elif len(pulled_df) == 9 and pulled_df.reset_index().equals(keymetrics.reset_index()): #Means that this year and month already has values, but new values are detected
            query_string = f"DELETE FROM key_metrics_table.key_metrics_table WHERE year = {year} and month = {month}" 
            pd.read_gbq(query_string)
            keymetrics.to_gbq('company_group_table.company_group_table', if_exists='append', table_schema=[
                {"name": "year", "type":"INTEGER"},
                {"name": "month", "type":"INTEGER"},
                {"name": "day", "type":"INTEGER"},
                {"name": "company", "type":"STRING"},
                {"name": "value_REVENUE_PER_SHARE_TTM", "type":"STRING"},
                {"name": "value_FREE_CASHFLOW_TTM", "type":"STRING"},
                {"name": "value_CASH_PER_SHARE_TTM", "type":"STRING"},
                {"name": "value_TOTAL_NET_INCOME_TTM", "type":"STRING"},
                {"name": "value_EARNINGS_PER_SHARE_TTM", "type":"STRING"},
            ])


    def fit_into_company_esg(**kwargs):
        miscellaneousStats = {}
        for ticker in tickers_For_YF:
            miscellaneousStats[ticker] = yf.Ticker(ticker).stats()

        dict = {"year": [], "month": [], "day": [], "company": [],  "value_ENVIRONMENTAL_RATING": [], "value_GOVERNMENT_RATING":[], "value_CONTROVERSY_LEVEL":[], "value_SOCIAL_RATING":[]}
        for ticker in tickers_For_YF:
            now = datetime.now()
            dict["year"].append(int(now.year))
            dict["month"].append(int(now.month))
            dict["day"].append(int(now.day))
            dict["company"].append(str(ticker))
            dict["value_ENVIRONMENTAL_RATING"].append((miscellaneousStats[ticker]["esgScores"]["environmentScore"] if miscellaneousStats[ticker]["esgScores"] != None else np.NaN))
            dict["value_GOVERNMENT_RATING"].append((miscellaneousStats[ticker]["esgScores"]["governanceScore"] if miscellaneousStats[ticker]["esgScores"] != None else np.NaN))
            dict["value_CONTROVERSY_LEVEL"].append((miscellaneousStats[ticker]["esgScores"]["peerHighestControversyPerformance"]["avg"] if miscellaneousStats[ticker]["esgScores"] != None else np.NaN))
            dict["value_SOCIAL_RATING"].append((miscellaneousStats[ticker]["esgScores"]['socialScore'] if miscellaneousStats[ticker]["esgScores"] != None else np.NaN))
        
        
        company_grp = pd.DataFrame.from_dict(dict).astype(str)
        company_grp["year"] = company_grp["year"].astype(int)
        company_grp["month"] = company_grp["month"].astype(int)
        company_grp["day"] = company_grp["day"].astype(int)
        company_grp["company"] = company_grp["company"].astype(str)
        company_grp["value_ENVIRONMENTAL_RATING"] = company_grp["value_ENVIRONMENTAL_RATING"].astype(float)
        company_grp["value_GOVERNMENT_RATING"] = company_grp["value_GOVERNMENT_RATING"].astype(float)
        company_grp["value_CONTROVERSY_LEVEL"] = company_grp["value_CONTROVERSY_LEVEL"].astype(float)
        company_grp["value_SOCIAL_RATING"] = company_grp["value_SOCIAL_RATING"].astype(float)

        today = date.today()
        year = today.year
        month = today.month


        query_string = f"SELECT * FROM company_esg_table.company_esg_table WHERE year = {year} and month = {month}" 
        pulled_df = pd.read_gbq(query_string)

        if len(pulled_df) < 1 :
            company_grp.to_gbq('company_esg_table.company_esg_table', if_exists='append', table_schema=[
            {"name": "year", "type":"INTEGER"},
            {"name": "month", "type":"INTEGER"},
            {"name": "day", "type":"INTEGER"},
            {"name": "company", "type":"STRING"},
            {"name": "value_ENVIRONMENTAL_RATING", "type":"FLOAT"},
            {"name": "value_GOVERNMENT_RATING", "type":"FLOAT"},
            {"name": "value_CONTROVERSY_LEVEL", "type":"FLOAT"},
            {"name": "value_SOCIAL_LEVEL", "type":"FLOAT"},
        ])
        elif len(pulled_df) == 9 and pulled_df.reset_index().equals(company_grp.reset_index()): #Means that this year and month already has values, but new values are detected
            query_string = f"DELETE FROM company_esg_table.company_esg_table WHERE year = {year} and month = {month}" 
            pd.read_gbq(query_string)
            company_grp.to_gbq('company_group_table.company_group_table', if_exists='append', table_schema=[
                {"name": "year", "type":"INTEGER"},
                {"name": "month", "type":"INTEGER"},
                {"name": "day", "type":"INTEGER"},
                {"name": "company", "type":"STRING"},
                {"name": "value_ENVIRONMENTAL_RATING", "type":"FLOAT"},
                {"name": "value_GOVERNMENT_RATING", "type":"FLOAT"},
                {"name": "value_CONTROVERSY_LEVEL", "type":"FLOAT"},
                {"name": "value_SOCIAL_LEVEL", "type":"FLOAT"},
            ])



    def fit_into_company_group(**kwargs):
        miscellaneousStats = {}
        for ticker in tickers_For_YF:
            miscellaneousStats[ticker] = yf.Ticker(ticker).stats()

        fundamental_data = {}
        for ticker in tickers_For_YF:
            fundamental_data[ticker] = si.get_quote_table(ticker, dict_result=False)

        dic = {"year": [], "month": [], "day": [],"company": [], "CompanyName": [],"Industry": [], "value_EMPLOYEES":[], "value_NO_OF_STOCKS": [], "value_MARKET_CAP": [], "value_ENTERPRISE_VALUE":[]}
        for ticker in tickers_For_YF:

            now = datetime.now()
            dic["year"].append(int(now.year))
            dic["month"].append(int(now.month))
            dic["day"].append(int(now.day))
            dic["company"].append(str(ticker))
            dic["CompanyName"].append(miscellaneousStats[ticker]["price"]["longName"])
            dic["Industry"].append(miscellaneousStats[ticker]["summaryProfile"]["industry"])
            dic["value_EMPLOYEES"].append(miscellaneousStats[ticker]["summaryProfile"]["fullTimeEmployees"] if miscellaneousStats[ticker].get("fullTimeEmployees") != None else 0)
            dic["value_NO_OF_STOCKS"].append(float(miscellaneousStats[ticker]['defaultKeyStatistics']['sharesOutstanding']))
            dic["value_MARKET_CAP"].append(float(fundamental_data[ticker].loc[11,"value"][:-1]) * 10 ** 9)
            dic["value_ENTERPRISE_VALUE"].append(miscellaneousStats[ticker]["defaultKeyStatistics"]["enterpriseValue"])
        
        company_grp = pd.DataFrame.from_dict(dic).astype(str)
        company_grp["year"] = company_grp["year"].astype(int)
        company_grp["month"] = company_grp["month"].astype(int)
        company_grp["day"] = company_grp["day"].astype(int)
        company_grp["company"] = company_grp["company"].astype(str)
        company_grp["CompanyName"] = company_grp["CompanyName"].astype(str)
        company_grp["Industry"] = company_grp["Industry"].astype(str)
        company_grp["value_EMPLOYEES"] = company_grp["value_EMPLOYEES"].astype(float)
        company_grp["value_NO_OF_STOCKS"] = company_grp["value_NO_OF_STOCKS"].astype(float)
        company_grp["value_MARKET_CAP"] = company_grp["value_MARKET_CAP"].astype(float)
        company_grp["value_ENTERPRISE_VALUE"] = company_grp["value_ENTERPRISE_VALUE"].astype(float)


        today = date.today()
        year = today.year
        month = today.month

        query_string = f"SELECT * FROM company_group_table.company_group_table WHERE year = {year} and month = {month}" 
        pulled_df = pd.read_gbq(query_string)

        if len(pulled_df) < 1 :
            company_grp.to_gbq('company_group_table.company_group_table', if_exists='append', table_schema=[
                {"name": "year", "type":"INTEGER"},
                {"name": "month", "type":"INTEGER"},
                {"name": "day", "type":"INTEGER"},
                {"name": "company", "type":"STRING"},
                {"name": "CompanyName", "type":"STRING"},
                {"name": "Industry", "type":"STRING"},
                {"name": "value_EMPLOYEES", "type":"FLOAT"},
                {"name": "value_NO_OF_STOCKS", "type":"FLOAT"},
                {"name": "value_MARKET_CAP", "type":"FLOAT"},
                {"name": "value_ENTERPRISE_VALUE", "type":"FLOAT"},
            ])
        elif len(pulled_df) == 9 and pulled_df.reset_index().equals(company_grp.reset_index()): #Means that this year and month already has values, but new values are detected
            query_string = f"DELETE FROM company_group_table.company_group_table WHERE year = {year} and month = {month}" 
            pd.read_gbq(query_string)
            company_grp.to_gbq('company_group_table.company_group_table', if_exists='append', table_schema=[
                {"name": "year", "type":"INTEGER"},
                {"name": "month", "type":"INTEGER"},
                {"name": "day", "type":"INTEGER"},
                {"name": "company", "type":"STRING"},
                {"name": "CompanyName", "type":"STRING"},
                {"name": "Industry", "type":"STRING"},
                {"name": "value_EMPLOYEES", "type":"FLOAT"},
                {"name": "value_NO_OF_STOCKS", "type":"FLOAT"},
                {"name": "value_MARKET_CAP", "type":"FLOAT"},
                {"name": "value_ENTERPRISE_VALUE", "type":"FLOAT"},
            ])


    def get_macro_monthly_data(**kwargs):
        macro_list = ['INFLATION_EXPECTATION','UNEMPLOYMENT', 'CONSUMER_SENTIMENT', 'RETAIL_SALES', 'DURABLES','NONFARM_PAYROLL','TREASURY_YIELD', 'FEDERAL_FUNDS_RATE', 'CPI']
        interval = ['TREASURY_YIELD', 'FEDERAL_FUNDS_RATE', 'CPI']
        all_dict = []
        all_df = []
        API_KEY = 'K9JXNKMP22D4AK5D'
    
        for var in macro_list:
            if var in interval:
                url =  f'https://www.alphavantage.co/query?function={var}&interval=monthly&apikey={API_KEY}'
            else:
                url = f'https://www.alphavantage.co/query?function={var}&apikey={API_KEY}'
            r = requests.get(url)
            data = r.json()
            all_dict.append(data)
            time.sleep(18)
        for i in range(len(macro_list)):
            temp = {}
            temp['date'] =[]
            temp[f"value_{macro_list[i]}"] = []
            values = all_dict[i]['data']
            for d in values:
                temp["date"].append(d['date'])
                temp[f"value_{macro_list[i]}"].append(d['value'])

            all_df.append(pd.DataFrame(temp))

        df_merged = reduce(lambda  left,right: pd.merge(left,right,on=['date'],
                                            how='inner'), all_df)
        convert_to_float =  ['value_INFLATION_EXPECTATION','value_UNEMPLOYMENT', 'value_CONSUMER_SENTIMENT', 'value_TREASURY_YIELD', 'value_FEDERAL_FUNDS_RATE', 'value_CPI']
        convert_to_int = ['value_RETAIL_SALES', 'value_DURABLES','value_NONFARM_PAYROLL']
        convert_to_date = ['date']
        for col in convert_to_float:
            df_merged[col] = df_merged[col].astype(float)
           
        for col in convert_to_int:
            df_merged[col] = df_merged[col].astype(int)

        for col in convert_to_date:
            df_merged[col] = df_merged[col].astype('datetime64[ns]')

        df_merged['year'] = df_merged['date'].dt.year 
        df_merged['month'] = df_merged['date'].dt.month
        df_merged.sort_values('date', ascending = True, inplace = True)
        df_merged.drop('date', axis = 1, inplace = True)

        df_merged['year'] = df_merged['year'].astype(int)   
        df_merged['month'] = df_merged['month'].astype(int)
        
        year_index = df_merged.columns.get_loc('year') + 1
        month_index = df_merged.columns.get_loc('month') + 1
        df_merged = df_merged[df_merged['year'] > 2020]

        df_all = []       
        for row in df_merged.itertuples():
            year = row[year_index]
            month = row[month_index]
            query_string = f" SELECT * FROM macro_table.macro_data_monthly WHERE year = {year} and month = {month}" #This is super slow 
            df_pulled = pd.read_gbq(query_string)
            if len(df_pulled) == 0: #Means that this year/month indicator doesnt already exist 
                temp = df_merged[(df_merged['year'] == year) & (df_merged['month'] == month)]
                df_all.append(temp.copy())
        if len(df_all) > 1:
            df_add = pd.concat(df_all)
            df_add.to_gbq('macro_table.macro_data_monthly',  if_exists='append') 
        elif len(df_all) == 1:
            df_all[0].to_gbq('macro_table.macro_data_monthly',  if_exists='append')

    def get_macro_quarterly_data():
        macro_list = ['REAL_GDP', 'REAL_GDP_PER_CAPITA',]
        all_dict = []
        all_df = []
        API_KEY = 'K9JXNKMP22D4AK5D'
    
        for var in macro_list:
            url =  f'https://www.alphavantage.co/query?function={var}&interval=quarterly&apikey=f{API_KEY}'
            r = requests.get(url)
            data = r.json()
            all_dict.append(data)
            time.sleep(18)

        for i in range(len(macro_list)):
            temp = {}
            temp['date'] =[]
            temp[f"value_{macro_list[i]}"] = []
            values = all_dict[i]['data']
            for d in values:
                temp["date"].append(d['date'])
                temp[f"value_{macro_list[i]}"].append(d['value'])

            all_df.append(pd.DataFrame(temp))

        df_merged = reduce(lambda  left,right: pd.merge(left,right,on=['date'],
                                            how='inner'), all_df)
                                            
        convert_to_float =  ['value_REAL_GDP','value_REAL_GDP_PER_CAPITA']
       
        for col in convert_to_float:
            df_merged[col] = df_merged[col].astype(float)
        
        df_merged['date'] = df_merged['date'].astype('datetime64[ns]')
        df_merged.sort_values('date', inplace = True)
        df_merged['year'] = df_merged['date'].dt.year
        df_merged['month'] = df_merged['date'].dt.month
        df_merged.drop('date', axis = 1, inplace = True)
        df_merged['year'] = df_merged['year'].astype(int)
        df_merged['month'] = df_merged['month'].astype(int)
        df_merged = df_merged[df_merged['year'] > 2020]

        year_index = df_merged.columns.get_loc('year') + 1
        month_index = df_merged.columns.get_loc('month') + 1

        df_all = []       
        for row in df_merged.itertuples():
            year = row[year_index]
            month = row[month_index]
            query_string = f" SELECT * FROM macro_table.macro_data_quarterly WHERE year = {year} and month = {month}" #This is super slow 
            df_pulled = pd.read_gbq(query_string)
            temp = df_merged[(df_merged['year'] == year) & (df_merged['month'] == month)]
            if len(df_pulled) == 0: #Means that this year/month indicator doesnt already exist 
                df_all.append(temp.copy())

        if len(df_all) > 1:
            df_add = pd.concat(df_all)
            df_add.to_gbq('macro_table.macro_data_quarterly',  if_exists='append') 
        elif len(df_all) == 1:
            df_all[0].to_gbq('macro_table.macro_data_quarterly',  if_exists='append')  


############################################
#2. Table Creation
############################################


    create_financial_dataset_task = BigQueryCreateEmptyDatasetOperator(task_id="create-financial-dataset", dataset_id="financial_ratio_table")
    create_key_metrics_dataset_task = BigQueryCreateEmptyDatasetOperator(task_id="create-key_metrics-dataset", dataset_id="key_metrics_table")
    create_company_esg_dataset_task = BigQueryCreateEmptyDatasetOperator(task_id="create-company-esg-dataset", dataset_id="company_esg_table")
    create_company_group_dataset_task = BigQueryCreateEmptyDatasetOperator(task_id='create-company-group-dataset', dataset_id="company_group_table")
    create_macro_dataset_task = BigQueryCreateEmptyDatasetOperator(task_id="create-macro-dataset", dataset_id="macro_table")


    create_financial_table_task_ = BigQueryCreateEmptyTableOperator(
        task_id="create_financial_table",
        dataset_id="financial_ratio_table",
        table_id="financial_ratio_table",
        schema_fields=[
            {"name": "year", "type":"INTEGER", "mode":"REQUIRED"},
            {"name": "month", "type":"INTEGER", "mode":"REQUIRED"},
            {"name": "day", "type":"INTEGER", "mode":"REQUIRED"},
            {"name": "company", "type":"STRING", "mode":"REQUIRED"},
            {"name": "value_PE_RATIO", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "value_PEG_RATIO", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "value_PRICE_SALES_RATIO", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "value_PRICE_BOOK_RATIO", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "value_PROFIT_MARGIN", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "value_PAYOUT_RATIO", "type": "STRING", "mode": "NULLABLE"},
            {"name": "value_ROE", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "value_ROA", "type": "FLOAT", "mode": "NULLABLE"},
        ],
    )

    create_key_metrics_table_task = BigQueryCreateEmptyTableOperator(
        task_id="create_key_metrics_table",
        dataset_id="key_metrics_table",
        table_id="key_metrics_table",
        schema_fields=[
            {"name": "year", "type":"INTEGER", "mode":"REQUIRED"},
            {"name": "month", "type":"INTEGER", "mode":"REQUIRED"},
            {"name": "day", "type":"INTEGER", "mode":"REQUIRED"},
            {"name": "company", "type":"STRING", "mode":"REQUIRED"},
            {"name": "value_REVENUE_PER_SHARE_TTM", "type":"STRING", "mode":"NULLABLE"},
            {"name": "value_FREE_CASHFLOW_TTM", "type":"STRING", "mode":"NULLABLE"},
            {"name": "value_CASH_PER_SHARE_TTM", "type":"STRING", "mode":"NULLABLE"},
            {"name": "value_TOTAL_NET_INCOME_TTM", "type":"STRING", "mode":"NULLABLE"},
            {"name": "value_EARNINGS_PER_SHARE_TTM", "type":"STRING", "mode":"NULLABLE"},
        ],
    )

    create_company_esg_table_task = BigQueryCreateEmptyTableOperator(
        task_id="create_company_esg_table",
        dataset_id="company_esg_table",
        table_id="company_esg_table",
        schema_fields=[
            {"name": "year", "type":"INTEGER", "mode":"REQUIRED"},
            {"name": "month", "type":"INTEGER", "mode":"REQUIRED"},
            {"name": "day", "type":"INTEGER", "mode":"REQUIRED"},
            {"name": "company", "type": "STRING", "mode": "NULLABLE"},
            {"name": "value_ENVIRONMENTAL_RATING", "type":"FLOAT", "mode":"NULLABLE"},
            {"name": "value_GOVERNMENT_RATING", "type":"FLOAT", "mode":"NULLABLE"},
            {"name": "value_CONTROVERSY_LEVEL", "type":"FLOAT", "mode":"NULLABLE"},
            {"name": "value_SOCIAL_RATING", "type":"FLOAT", "mode":"NULLABLE"},
        ],
    )

    create_table_task_monthly = BigQueryCreateEmptyTableOperator(
        task_id="create_macro_table_monthly",
        dataset_id="macro_table",
        table_id="macro_data_monthly",
        schema_fields=[
            {"name": "year", "type":"INTEGER", "mode":"REQUIRED"},
            {"name": "month", "type":"INTEGER", "mode":"REQUIRED"},
            {"name": "value_INFLATION_EXPECTATION", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "value_UNEMPLOYMENT", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "value_CONSUMER_SENTIMENT", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "value_RETAIL_SALES", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "value_DURABLES", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "value_NONFARM_PAYROLL", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "value_TREASURY_YIELD", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "value_FEDERAL_FUNDS_RATE", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "value_CPI", "type": "FLOAT", "mode": "NULLABLE"},
        ],
    )

    create_table_task_quarterly = BigQueryCreateEmptyTableOperator(
        task_id="create_macro_table_quarterly",
        dataset_id="macro_table",
        table_id="macro_data_quarterly",
        schema_fields=[
            {"name": "year", "type":"INTEGER", "mode":"REQUIRED"},
            {"name": "month", "type":"INTEGER", "mode":"REQUIRED"},
            {"name": "value_REAL_GDP", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "value_REAL_GDP_PER_CAPITA", "type": "FLOAT", "mode": "NULLABLE"}
        ],
    )


    create_company_group_table_task = BigQueryCreateEmptyTableOperator(
        task_id="create_company_group_table",
        dataset_id="company_group_table",
        table_id="company_group_table",
        schema_fields=[
            {"name": "year", "type":"INTEGER", "mode":"REQUIRED"},
            {"name": "month", "type":"INTEGER", "mode":"REQUIRED"},
            {"name": "day", "type":"INTEGER", "mode":"REQUIRED"},
            {"name": "company", "type":"STRING", "mode":"REQUIRED"},
            {"name": "CompanyName", "type":"STRING", "mode":"REQUIRED"},
            {"name": "Industry", "type":"STRING", "mode":"NULLABLE"},
            {"name": "value_EMPLOYEES", "type":"FLOAT", "mode":"NULLABLE"},
            {"name": "value_NO_OF_STOCKS", "type":"FLOAT", "mode":"NULLABLE"},
            {"name": "value_MARKET_CAP", "type":"FLOAT", "mode":"NULLABLE"},
            {"name": "value_ENTERPRISE_VALUE", "type":"FLOAT", "mode":"NULLABLE"},
        ],
    )

##########################################
#3. DEFINE AIRFLOW OPERATORS
##########################################

    fit_into_financial_ratio = PythonOperator(task_id='fit_into_financial_ratio', python_callable= fit_into_financial_ratio, provide_context = True, dag = dag)
    fit_into_key_metrics = PythonOperator(task_id='fit_into_key_metrics', python_callable= fit_into_key_metrics, provide_context = True, dag = dag)
    fit_into_company_esg = PythonOperator(task_id='fit_into_company_esg', python_callable= fit_into_company_esg, provide_context = True, dag = dag)
    fit_into_company_group = PythonOperator(task_id='fit_into_company_group', python_callable= fit_into_company_group, provide_context = True, dag = dag)
    scrape_macro_monthly_data = PythonOperator(task_id = "get_macro_monthly_data", python_callable = get_macro_monthly_data, dag = dag)
    scrape_macro_quarterly_data = PythonOperator(task_id = "get_macro_quarterly_data", python_callable = get_macro_quarterly_data,dag = dag)

##########################################
#4. DEFINE OPERATIONS HIERARCHY
##########################################

[create_financial_dataset_task, create_key_metrics_dataset_task, create_company_esg_dataset_task, create_company_group_dataset_task, create_macro_dataset_task]
create_macro_dataset_task >> [create_table_task_monthly, create_table_task_quarterly]
create_financial_dataset_task >> create_financial_table_task_ >> fit_into_financial_ratio
create_key_metrics_dataset_task >> create_key_metrics_table_task >> fit_into_key_metrics
create_company_esg_dataset_task >> create_company_esg_table_task >> fit_into_company_esg
create_company_group_dataset_task >> create_company_group_table_task >> fit_into_company_group
create_table_task_monthly >> scrape_macro_monthly_data
create_table_task_quarterly >> scrape_macro_quarterly_data


