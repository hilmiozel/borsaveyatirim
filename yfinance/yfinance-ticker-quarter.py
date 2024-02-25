import yfinance as yf
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta
import os
import time
import pandas as pd

USER = "elastic"
PASS = "bitnami"
es = Elasticsearch('http://192.168.1.35:9200', basic_auth=(USER, PASS), verify_certs=False)

cwd = os.getcwd()

f = open(cwd +"\\yfinance\\ticker-tracklist", "r")
ticker_file_list = f.read()

ticker_file_list = ticker_file_list.splitlines()
print (ticker_file_list)

for ticker in ticker_file_list:
    print( "Ticker asking to Yahoo: " + ticker)
    ticker = yf.Ticker(ticker)

    # get all stock info
    # Daily
    doc = ticker.info
    now=datetime.now()

    doc["timestamp"]= now.isoformat()

    resp = es.index(index="borsa_yfinance_ticker_info", document=doc)

    print(resp.body)
    time.sleep(1)

    income_doc = ticker.income_stmt
    income_doc = pd.DataFrame(income_doc)
    resp = es.index(index="borsa_yfinance_ticker_income_stmt", document=income_doc)
    time.sleep(1)

    q_income_doc = ticker.quarterly_income_stmt
    q_income_doc["timestamp"]= now.isoformat()
    resp = es.index(index="borsa_yfinance_ticker_quarterly_income_stmt", document=q_income_doc)
    print(resp.body)
    time.sleep(1)

    balance_doc = ticker.balance_sheet
    balance_doc["timestamp"]= now.isoformat()
    resp = es.index(index="borsa_yfinance_ticker_balance_sheet", document=balance_doc)
    print(resp.body)
    time.sleep(1)
    
    quarterly_balance_sheet = ticker.quarterly_balance_sheet
    quarterly_balance_sheet["timestamp"]= now.isoformat()
    resp = es.index(index="borsa_yfinance_ticker_quarterly_balance_sheet", document=quarterly_balance_sheet)
    print(resp.body)
    time.sleep(1)
    
    cashflow = ticker.cashflow
    cashflow["timestamp"]= now.isoformat()
    resp = es.index(index="borsa_yfinance_ticker_cashflow", document=cashflow)
    print(resp.body)
    time.sleep(1)
    
    quarterly_cashflow = ticker.cashflow
    quarterly_cashflow["timestamp"]= now.isoformat()
    resp = es.index(index="borsa_yfinance_ticker_quarterly_cashflow", document=quarterly_cashflow)
    print(resp.body)
    time.sleep(1)

time.sleep(5)

