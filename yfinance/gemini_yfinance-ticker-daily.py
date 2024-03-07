import yfinance as yf
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta
import wbgapi as wb
import os
import re
import time
import pandas as pd
import requests
from bs4 import BeautifulSoup
import json
import google.generativeai as genai
pd.set_option('future.no_silent_downcasting', True)

# Elasticsearch credentials
USER = "elastic"
PASS = "bitnami"
es = Elasticsearch('http://172.18.0.2:9200', basic_auth=(USER, PASS), verify_certs=False)
# GEMINI API Key
api_key = os.environ.get("GEMINI_API_KEY")
genai.configure(api_key=api_key)

cwd = os.getcwd()

print("Ticker list has been read from the list")
f = open(cwd + "/yfinance/hilmi-ticker-tracklist", "r")
ticker_file_list = f.read()

ticker_file_list = ticker_file_list.splitlines()
print(ticker_file_list)

# Define constants
TARGETPRICE_URL = "https://www.borsaveyatirim.com/bist-hisse-onerileri-ve-hedef-fiyatlari"
URL1_TEMP = 'https://www.isyatirim.com.tr/tr-tr/analiz/hisse/Sayfalar/sirket-karti.aspx?hisse={hisse}'

# Get page content from URL
response = requests.get(TARGETPRICE_URL)
html_content = response.text

# Parse HTML content using BeautifulSoup
soup = BeautifulSoup(html_content, "html.parser")

# Find all tables
tables = soup.find_all("table")

# Find target table
targetprice_table = None
for table in tables:
    for content in table.contents:
        if 'Güncelleme' in content: 
            targetprice_table = table
            break

MODEL_PORFOLIO_URL = "https://www.borsaveyatirim.com/araci-kurum-hisse-onerileri"
SEARCH_TABLE_ID = "myTable2"

# Get page content from URL
response = requests.get(MODEL_PORFOLIO_URL)
html_content = response.text

# Parse HTML content using BeautifulSoup
soup = BeautifulSoup(html_content, "html.parser")

# Find all tables
tables = soup.find_all("table")

# Find model portfolio table
model_portfolio_table = None
for table in tables:
    attrs = table.attrs
    if 'id' in attrs and 'class' in attrs:
        if attrs['id'] == SEARCH_TABLE_ID and 'tablesorter' in attrs['class']:
            model_portfolio_table = table

# Macroeconomical indicators for Turkey
macro_economic_list = []

for report in ['NY.GDP.MKTP.CD', 'NY.GDP.PCAP.CD', 'NY.GDP.MKTP.KD.ZG', 'SL.UEM.TOTL.ZS', 'FP.CPI.TOTL.ZG', 'NE.TRD.GNFS.ZS', 'BN.CAB.XOKA.GD.ZS', 'NE.CON.PRVT.KD.ZG','NE.EXP.GNFS.ZS', 'NE.IMP.GNFS.ZS', 'BN.CAB.XOKA.GD.ZS']:
    data_list = list(wb.data.fetch(report, 'TUR', mrv=5))
    series_info = wb.series.info(report)

    # Her bir sözlükte 'TUR' anahtarını 'Türkiye' ile değiştir ve güncellenmiş sözlüğü yeni listeye ekle
    for row in data_list:
        row['economy'] = 'Türkiye'
        row['series'] = series_info.items[0]['value']
        macro_economic_list.append(row)
time.sleep(5)

for stock_code in ticker_file_list:
    ticker = stock_code + '.IS'
    print("Requesting Ticker info from Yahoo: " + ticker)
    ticker = yf.Ticker(ticker)

    # Get all stock info
    # Daily
    doc = ticker.info
    
    quarterly_income_stmt = ticker.quarterly_income_stmt
    quarterly_income_stmt = quarterly_income_stmt.to_string(index=False)
    
    quarterly_balance_sheet = ticker.quarterly_balance_sheet
    quarterly_balance_sheet = ticker.quarterly_balance_sheet.to_string(index=False)
    
    quarterly_cashflow = ticker.quarterly_cashflow
    quarterly_cashflow = quarterly_cashflow.to_string(index=False)
    
    yearly_income_stmt = ticker.income_stmt
    yearly_income_stmt = yearly_income_stmt.to_string(index=False)
    
    yearly_balance_sheet = ticker.balance_sheet
    yearly_balance_sheet = yearly_balance_sheet.to_string(index=False)
    
    yearly_cashflow = ticker.cashflow
    yearly_cashflow = yearly_cashflow.to_string(index=False)
    
    hist = ticker.history(period="3mo")
    hist = hist.to_string(index=False)
    
    print("Requesting Ticker info from BorsaveYatirim.com Tables:" + stock_code)
    count_of_target_price_calculation = ''
    avarage_target_price = ''
    distance_to_target_price = ''
    count_investcomp_model_portfolio_added = ''
    
    if targetprice_table:
        rows = targetprice_table.find_all('tr')
        for row in rows[1:]:  # Skip the first row as it contains headers
            columns = row.find_all('td')
            kod = columns[1].text.strip()  # Code column
            if kod == stock_code:
                count_of_target_price_calculation = int(columns[3].text.strip())  # Recommendation firm count column
                avarage_target_price = float(columns[5].text.strip())  # Average Target column
                distance_to_target_price = float(columns[6].text.strip())  # Average Target column
                break
        
    if model_portfolio_table:
        rows = model_portfolio_table.find_all('tr')
        for row in rows[1:]:  # Skip the first row as it contains headers
            columns = row.find_all('td')
            kod = columns[1].text.strip()  # Code column
            if kod == stock_code:
                count_investcomp_model_portfolio_added = int(columns[3].text.strip())  # Recommendation firm count column
                break
    
    now = datetime.now()
    doc["timestamp"] = now.isoformat()
    doc["stock_code"] = stock_code
    doc["count_of_target_price_calculation"] = count_of_target_price_calculation
    doc["avarage_target_price"] = avarage_target_price
    doc["distance_to_target_price"] = distance_to_target_price
    doc["count_investcomp_model_portfolio_added"] = count_investcomp_model_portfolio_added
    
    print("Requesting Ticker info from Isyatirim.com Tables:" + stock_code)
    URL_ISYATIRIM = URL1_TEMP.format(hisse=stock_code)
    # URL'den sayfa içeriğini getirin
    response = requests.get(URL_ISYATIRIM)
    html_content = response.text

    # BeautifulSoup kullanarak HTML içeriğini parse edin
    soup = BeautifulSoup(html_content, "html.parser")

    # Tüm tabloları bulun
    tables = soup.find_all("table")

    # Hedef tabloyu bulun
    finansaloranlartahmin_table = None
    yurtdisiiskonto_table = None
    tarihselortalamalar_table = None
    
    for table in tables:
        attrs = table.attrs
        if 'data-csvname' in attrs and 'class' in attrs:
            if attrs['data-csvname'] == 'yurtdisiiskonto' and 'excelexport' in attrs['class']:
                yurtdisiiskonto_table = table
            if attrs['data-csvname'] == 'tarihselortalamalar' and 'excelexport' in attrs['class']:
                tarihselortalamalar_table = table
    
    if yurtdisiiskonto_table:
        rows = yurtdisiiskonto_table.find_all('tr')
        for row in rows[2:]:  # İlk satır başlıkları içerdiğinden atlanır
            columns = row.find_all('td')
            if len(columns)>3:
                kod = columns[0].text.strip()  # Kod sütunu
                son_fiyat = float(columns[1].text.strip().replace('.', '').replace(',', '.')) if columns[1].text.strip().startswith(('-','0', '1', '2', '3', '4', '5', '6', '7', '8', '9')) else 0.0
                fk = float(columns[2].text.strip().replace('.', '').replace(',', '.')) if columns[2].text.strip().startswith(('-','0', '1', '2', '3', '4', '5', '6', '7', '8', '9')) else 0.0
                yurtdisi_fk_prim_oran = float(columns[3].text.strip().replace('.', '').replace(',', '.')) if columns[3].text.strip().startswith(('-','0', '1', '2', '3', '4', '5', '6', '7', '8', '9')) else 0.0
            if len(columns)==6:
                pddd = float(columns[4].text.strip().replace('.', '').replace(',', '.')) if columns[4].text.strip().startswith(('-','0', '1', '2', '3', '4', '5', '6', '7', '8', '9')) else 0.0
                yurtdisi_pddd_prim_oran = float(columns[5].text.strip().replace('.', '').replace(',', '.')) if columns[5].text.strip().startswith(('-','0', '1', '2', '3', '4', '5', '6', '7', '8', '9')) else 0.0
            elif len(columns)==8:
                fdfavök = float(columns[4].text.strip().replace('.', '').replace(',', '.')) if columns[4].text.strip().startswith(('-','0', '1', '2', '3', '4', '5', '6', '7', '8', '9')) else 0.0
                yurtdisi_fdfavök_prim_oran = float(columns[5].text.strip().replace('.', '').replace(',', '.')) if columns[5].text.strip().startswith(('-','0', '1', '2', '3', '4', '5', '6', '7', '8', '9')) else 0.0
                pddd = float(columns[6].text.strip().replace('.', '').replace(',', '.')) if columns[6].text.strip().startswith(('-','0', '1', '2', '3', '4', '5', '6', '7', '8', '9')) else 0.0
                yurtdisi_pddd_prim_oran = float(columns[7].text.strip().replace('.', '').replace(',', '.')) if columns[7].text.strip().startswith(('-','0', '1', '2', '3', '4', '5', '6', '7', '8', '9')) else 0.0
    
    if tarihselortalamalar_table:
        rows = tarihselortalamalar_table.find_all('tr')
        for row in rows[2:]:  # İlk satır başlıkları içerdiğinden atlanır
            columns = row.find_all('td')
            if len(columns)>2:
                kod = columns[0].text.strip()  # Kod sütunu
                fk = float(columns[1].text.strip().replace('.', '').replace(',', '.')) if columns[1].text.strip().startswith(('-','0', '1', '2', '3', '4', '5', '6', '7', '8', '9')) else 0.0
                tarihsel_fk_prim_oran = float(columns[2].text.strip().replace('.', '').replace(',', '.')) if columns[2].text.strip().startswith(('-','0', '1', '2', '3', '4', '5', '6', '7', '8', '9')) else 0.0
            if len(columns)==5:
                pddd = float(columns[3].text.strip().replace('.', '').replace(',', '.')) if columns[3].text.strip().startswith(('-','0', '1', '2', '3', '4', '5', '6', '7', '8', '9')) else 0.0
                tarihsel_pddd_prim_oran = float(columns[4].text.strip().replace('.', '').replace(',', '.')) if columns[4].text.strip().startswith(('-','0', '1', '2', '3', '4', '5', '6', '7', '8', '9')) else 0.0
            elif len(columns)==7:
                fdfavök = float(columns[3].text.strip().replace('.', '').replace(',', '.')) if columns[3].text.strip().startswith(('-','0', '1', '2', '3', '4', '5', '6', '7', '8', '9')) else 0.0
                tarihsel_fdfavök_prim_oran = float(columns[4].text.strip().replace('.', '').replace(',', '.')) if columns[4].text.strip().startswith(('-','0', '1', '2', '3', '4', '5', '6', '7', '8', '9')) else 0.0
                pddd = float(columns[5].text.strip().replace('.', '').replace(',', '.')) if columns[5].text.strip().startswith(('-','0', '1', '2', '3', '4', '5', '6', '7', '8', '9')) else 0.0
                tarihsel_pddd_prim_oran = float(columns[6].text.strip().replace('.', '').replace(',', '.')) if columns[6].text.strip().startswith(('-','0', '1', '2', '3', '4', '5', '6', '7', '8', '9')) else 0.0
    
    if tarihselortalamalar_table or yurtdisiiskonto_table:
    
        doc["estimated_pe_ratio"] = fk
        doc["estimated_pe_discount_foreign_comprasion"] = yurtdisi_fk_prim_oran
        doc["estimated_pe_discount_historaical_comprasion"] = tarihsel_fk_prim_oran
        doc["estimated_pb_ratio"] = pddd
        doc["estimated_pb_discount_foreign_comprasion"] = yurtdisi_pddd_prim_oran
        doc["estimated_pb_discount_historaical_comprasion"] = tarihsel_pddd_prim_oran
        doc["estimated_ebitda_ratio"] = fdfavök
        doc["estimated_ebitda_discount_foreign_comprasion"] = yurtdisi_fdfavök_prim_oran
        doc["estimated_ebitda_discount_historaical_comprasion"] = tarihsel_fdfavök_prim_oran
    
    # Set up the model
    generation_config = {
    "temperature": 0.4,
    "top_p": 0.0,
    "top_k": 1,
    "max_output_tokens": 512,
    }

    safety_settings = [
    
    ]

    model = genai.GenerativeModel(model_name="gemini-1.0-pro",
                                generation_config=generation_config,
                                safety_settings=safety_settings)

    print("Gemini Chat is starting for:" + stock_code)
    last_long_term_report = ''
    last_short_term_report = ''
    max_retry_attempts = 10
    retry_delay = 120  # seconds
    
    for _ in range(max_retry_attempts):
        try:
            convo = model.start_chat(history=[
            {
                "role": "user",
                "parts": [
                    "Hi, I'm looking for an AI investment advisor. My goal is to beat inflation in Turkey with an aggressive investment strategy for roughly one year. I plan to make regular contributions from my salary, but I want to ensure I'm buying at the right time. Turkey macro economical reports are below:\n" + json.dumps(macro_economic_list,indent=4)
                ]
            },
            {
                "role": "model",
                "parts": [
                    "I understand you're aiming for aggressive returns within a year in the Turkish market. Based on historical trends, many users with similar goals have found X, Y, and Z strategies effective. However, it's crucial to remember that past performance is not indicative of future results.\n\n"
                    "I can provide you with:"
                    "* **Strategic guidance:** Develop an aggressive investment strategy considering your risk tolerance and time horizon.\n"
                    "* **Market insights:** Analyze market trends and identify potential investment opportunities.\n"
                    "* **Technical analysis:** Utilize technical indicators to help you determine entry and exit points for your investments.\n"
                    "By combining my expertise with your financial knowledge and goals, we can work together to create a personalized investment plan that helps you achieve your financial objectives."
                ]
            },
            {
                "role": "user",
                "parts": [
                    "I am unsure whether to invest in " + stock_code,
                    "\nYour previous long term report about this stock is below: \n" + last_long_term_report,
                    "\nYour previous short term report about this stock is below: \n" + last_short_term_report,
                    "\nKeep in your mind all values are given by Turkish liras, negative comprasion values means undervalued. Recent informations about this stock are below: \n" +  json.dumps(doc, indent=4),
                    "\nGive the recent quarterly&yearly income statements, cash flows, balance sheet for this stock?"
                    ]
            },
            {
                "role": "model",
                "parts": [
                    "\nQuarterly income statements for this stock are below: \n" + quarterly_income_stmt,
                    "\nYear by year, income statements for this stock are below: \n" +  yearly_income_stmt,
                    "\nQuarterly cash flows for this stock are below: \n" + quarterly_cashflow,
                    "\nYear by year, cash flows for this stock are below: \n" +  yearly_cashflow,
                    "\nQuarterly balance sheets for this stock are below: \n" + quarterly_balance_sheet,
                    "\nYear by year, balance sheets for this stock are below: \n" +  yearly_balance_sheet
                    ]
            },
            {
                "role": "user",
                "parts": [
                    "Give the stock values history for last 90 days?"
                ]
            },
            {
                "role": "model",
                "parts": [
                    "90 days stock price history are given below \n:" + hist
                    ]
            }
            ])
            break 
        except Exception as e:
            print("GEMINI timeout hatası alındı, yeniden denenecek.")
            time.sleep(retry_delay)  # Bekleme süresi
            
    time.sleep(60)
    
    for _ in range(max_retry_attempts):
        try:
            text="Analyze this stock's long-term investment potential (considering fundamentals, value, and growth) Compared to other Turkish stocks, how attractive is it for long-term profitability (1-100 score)?"
            #ext="Please prepare a report analyzing this stock from the perspectives of fundamental analysis, value investing, growth investing, and long-term investing. Additionally, could you include a score (1-100) indicating its quality for long-term investment compared with other Turkish stocks?"
            convo.send_message(text)
            gemini_long_term_report=convo.last.text
            break 
        except Exception as e:
            print("GEMINI timeout hatası alındı, yeniden denenecek.")
            time.sleep(retry_delay)  # Bekleme süresi
    else:
        print("GEMINI timeout hatası ve maksimum yeniden deneme sayısına ulaşıldı.")
    
    convo.rewind
    time.sleep(60)
     
    for _ in range(max_retry_attempts):
        try:
            text2="Please provide a short-term analysis from 90 days stock price history; focusing on trend, momentum, and technical indicators. Include weekly support and resistance levels, and analysis using RSI, MACD, ATR, and Bollinger Bands. Additionally, provide a buying score (1-100) based on this analysis to gauge potential entry points. A higher score suggests a potential uptrend and better entry, while a lower score suggests a downtrend and weaker entry."
            #text2="Please provide the weekly support and resistance levels based on the past 3 months' stock price history? Additionally, could you analyze the same data using technical indicators like RSI, MACD, ATR, and Bollinger Bands? Please summarize the results in a short-term analysis text report focusing on trend, momentum, and technical analysis. I need to understand trend and whther is it a near a good entry point?"
            convo.send_message(text2)
            gemini_short_term_report=convo.last.text
            break 
        except Exception as e:
            print("GEMINI timeout hatası alındı, yeniden denenecek.")
            time.sleep(retry_delay)  # Bekleme süresi
    else:
        print("GEMINI timeout hatası ve maksimum yeniden deneme sayısına ulaşıldı.")
        
    doc["gemini_long_term_report"] = gemini_long_term_report
    doc["gemini_short_term_report"] = gemini_short_term_report
    print(doc)
    #resp = es.index(index="gemini_yfinance_ticker_info", document=doc)
    #print(resp.body)
    time.sleep(120)
    