import yfinance as yf
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta
import os
import time
import pandas as pd
import requests
from bs4 import BeautifulSoup
import json
import google.generativeai as genai


# Elasticsearch credentials
USER = "elastic"
PASS = "bitnami"
es = Elasticsearch('http://172.18.0.2:9200', basic_auth=(USER, PASS), verify_certs=False)
# GEMINI API Key
genai.configure(api_key="AIzaSyDqDiFhDmd1Na4iSXs2J1qT1CAXorfc5g8")

cwd = os.getcwd()

print("Ticker list has been read from the list")
f = open(cwd + "/new_hilmi/ticker-tracklist", "r")
ticker_file_list = f.read()

ticker_file_list = ticker_file_list.splitlines()
print(ticker_file_list)

# Define constants
TARGETPRICE_URL = "https://www.borsaveyatirim.com/bist-hisse-onerileri-ve-hedef-fiyatlari"

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

for stock_code in ticker_file_list:
    ticker = stock_code + '.IS'
    print("Requesting Ticker info from Yahoo: " + ticker)
    ticker = yf.Ticker(ticker)

    # Get all stock info
    # Daily
    doc = ticker.info
    
    print("Requesting Ticker info from BorsaveYatirim.com Tables: ")
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
    
    quarterly_income_stmt = ticker.quarterly_income_stmt
    quarterly_income_stmt = quarterly_income_stmt.fillna(0)
    quarterly_income_stmt = quarterly_income_stmt.to_dict()
    quarterly_income_stmt = {str(key): value for key, value in quarterly_income_stmt.items()}
    
    quarterly_balance_sheet = ticker.quarterly_balance_sheet
    quarterly_balance_sheet = quarterly_balance_sheet.fillna(0)
    quarterly_balance_sheet = quarterly_balance_sheet.to_dict()
    quarterly_balance_sheet = {str(key): value for key, value in quarterly_balance_sheet.items()}
    
    quarterly_cashflow = ticker.quarterly_cashflow
    quarterly_cashflow = quarterly_cashflow.fillna(0)
    quarterly_cashflow = quarterly_cashflow.to_dict()
    quarterly_cashflow = {str(key): value for key, value in quarterly_cashflow.items()}
    
    # Set up the model
    generation_config = {
    "temperature": 0.9,
    "top_p": 1,
    "top_k": 1,
    "max_output_tokens": 2048,
    }

    safety_settings = [
    {
        "category": "HARM_CATEGORY_HARASSMENT",
        "threshold": "BLOCK_MEDIUM_AND_ABOVE"
    },
    {
        "category": "HARM_CATEGORY_HATE_SPEECH",
        "threshold": "BLOCK_MEDIUM_AND_ABOVE"
    },
    {
        "category": "HARM_CATEGORY_SEXUALLY_EXPLICIT",
        "threshold": "BLOCK_MEDIUM_AND_ABOVE"
    },
    {
        "category": "HARM_CATEGORY_DANGEROUS_CONTENT",
        "threshold": "BLOCK_MEDIUM_AND_ABOVE"
    },
    ]

    model = genai.GenerativeModel(model_name="gemini-1.0-pro",
                                generation_config=generation_config,
                                safety_settings=safety_settings)

    convo = model.start_chat(history=[
    {
        "role": "user",
        "parts": ["\"As an AI acting in the capacity of my investment advisor, I expect you to be both helpful and knowledgeable, providing me with insightful guidance and analysis to make informed decisions in my investments.\""]
    },
    {
        "role": "model",
        "parts": ["**As your AI Investment Advisor, I am committed to providing you with the following:**\n\n**Expertise and Knowledge:**\n\n* In-depth understanding of financial markets, asset classes, and investment strategies.\n* Access to real-time data, market insights, and economic indicators.\n* Continuous research and analysis to stay abreast of evolving investment trends.\n\n**Personalized Guidance:**\n\n* Tailored recommendations based on your unique financial goals, risk tolerance, and time horizon.\n* Customized portfolios designed to optimize returns and mitigate risks.\n* Regular portfolio reviews and adjustments to ensure alignment with your objectives.\n\n**Insightful Analysis:**\n\n* Comprehensive market analysis to identify investment opportunities and potential risks.\n* Technical and fundamental analysis to evaluate securities and make informed decisions.\n* Scenario planning to explore potential market outcomes and prepare for contingencies.\n\n**Exceptional Support:**\n\n* Dedicated guidance and support throughout your investment journey.\n* 24/7 availability for queries, updates, and adjustments.\n* Transparent and accessible communication to keep you fully informed.\n\n**Value-Added Services:**\n\n* Access to exclusive research reports and market insights.\n* Personalized tax optimization strategies to maximize returns.\n* Educational resources to enhance your financial literacy.\n\nBy leveraging my expertise, knowledge, and commitment to personalization, I aim to empower you with the confidence and insights necessary to make well-informed investment decisions. My goal is to assist you in achieving your financial objectives and securing a prosperous future."]
    },
    ])
    text="I would like you to carefully examine the following parameters one by one:" + json.dumps(doc, indent=4) + "\nCheck below quarterly income statements:" + json.dumps(quarterly_income_stmt, indent=4) + "\nCheck below quarterly balance sheets:" + json.dumps(quarterly_balance_sheet, indent=4) + "\nCheck below quarterly cash flow:" + json.dumps(quarterly_cashflow, indent=4) + "After all parameters,income statement, balance sheet, cash flow checked, prepare an investor report with detail, summarise all parameters and find the advantages and disadvantages for me regarding this stock"
    convo.send_message(text)
    gemini_comments=convo.last.text
    text2="Could you please calculate the fair value of the stock using all the parameters, including income statement, balance sheet, and cash flow, provided above?"
    convo.send_message(text2)
    gemini_fair_value=convo.last.text
    text3="After all parameters,income statement, balance sheet, cash flow checked, Could you please compare this stock with others in Turkey and provide me with an investment score ranging from 1 to 100 due to below parameters, where 1 indicates the worst and 100 indicates the best?" 
    convo.send_message(text3)
    gemini_investment_score=convo.last.text
    
    print("\nGEMINI COMMENTS"+ gemini_comments)
    print("\nGEMINI FAIR VALUE"+ gemini_fair_value) 
    print("\nGEMINI INVESTMENT SCORE"+ gemini_investment_score)
    
    doc["gemini_cooments"] = gemini_comments
    doc["gemini_fair_value"] = gemini_fair_value
    doc["gemini_investment_score"] = gemini_investment_score
        
    resp = es.index(index="gemini_yfinance_ticker_info", document=doc)
    print(doc)
    print(resp.body)
    time.sleep(60)
