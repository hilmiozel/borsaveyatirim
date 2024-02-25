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
    time.sleep(2)


