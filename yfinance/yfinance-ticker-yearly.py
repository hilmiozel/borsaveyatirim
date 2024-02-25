import yfinance as yf
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta
import os
import time
import pandas as pd

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


    income_doc = ticker.income_stmt
    balance_doc = ticker.balance_sheet
    cashflow = ticker.cashflow
    
    
    
    income_doc = income_doc.fillna(0)
    income_doc = income_doc.to_dict()
    for record in income_doc:
        income_doc[record]["timestamp"] = str(record)
        income_doc[record]["symbol"] = ticker_name
        
        resp = es.index(index="borsa_yfinance_ticker_quarterly_income_stmt", document=income_doc[record])
        print(resp.body)
        time.sleep(1)

    
    
    balance_doc = balance_doc.fillna(0)
    balance_doc = balance_doc.to_dict()
    for record in balance_doc:
        balance_doc[record]["timestamp"] = str(record)
        balance_doc[record]["symbol"] = ticker_name
        
        resp = es.index(index="borsa_yfinance_ticker_quarterly_income_stmt", document=balance_doc[record])
        print(resp.body)
        time.sleep(1)
    
    
    
    cashflow = cashflow.fillna(0)
    cashflow = cashflow.to_dict()
    for record in cashflow:
        cashflow[record]["timestamp"] = str(record)
        cashflow[record]["symbol"] = ticker_name
        
        resp = es.index(index="borsa_yfinance_ticker_quarterly_income_stmt", document=cashflow[record])
        print(resp.body)
        time.sleep(1)   
 

time.sleep(5)

