import yfinance as yf
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import os
import time
import pandas as pd
import numpy as np

USER = "elastic"
PASS = "bitnami"
es = Elasticsearch('http://172.18.0.2:9200', basic_auth=(USER, PASS), verify_certs=False)

cwd = os.getcwd()

f = open(cwd +"/yfinance/ticker-tracklist", "r")
ticker_file_list = f.read()

ticker_file_list = ticker_file_list.splitlines()
print (ticker_file_list)

for ticker_name in ticker_file_list:
    print( "Ticker asking to Yahoo: " + ticker_name)
    ticker = yf.Ticker(ticker_name)
    now=datetime.now()



    q_income_doc = ticker.quarterly_income_stmt
    
    
    q_income_doc = q_income_doc.fillna(0)
    q_income_doc = q_income_doc.to_dict()
    for record in q_income_doc:
        q_income_doc[record]["timestamp"] = str(record)
        q_income_doc[record]["symbol"] = ticker_name
        
        resp = es.index(index="borsa_yfinance_ticker_quarterly_income_stmt", document=q_income_doc[record])
        print(resp.body)
        time.sleep(1)

   
    quarterly_balance_sheet = ticker.quarterly_balance_sheet
    quarterly_balance_sheet = quarterly_balance_sheet.fillna(0)
    quarterly_balance_sheet = quarterly_balance_sheet.to_dict()
    for record in quarterly_balance_sheet:
        quarterly_balance_sheet[record]["timestamp"] = str(record)
        quarterly_balance_sheet[record]["symbol"] = ticker_name
        
        resp = es.index(index="borsa_yfinance_ticker_quarterly_balance_sheet", document=quarterly_balance_sheet[record])
        print(resp.body)
        time.sleep(1)
    

    quarterly_cashflow = ticker.quarterly_cashflow
    quarterly_cashflow = quarterly_cashflow.fillna(0)
    quarterly_cashflow = quarterly_cashflow.to_dict()
    for record in quarterly_cashflow:
        quarterly_cashflow[record]["timestamp"] = str(record)
        quarterly_cashflow[record]["symbol"] = ticker_name
        
        resp = es.index(index="borsa_yfinance_ticker_quarterly_cashflow", document=quarterly_cashflow[record])
        print(resp.body)
        time.sleep(1)
      


time.sleep(5)

