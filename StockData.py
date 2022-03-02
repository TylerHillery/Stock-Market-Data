#Importing Packages
from datetime import datetime
import pandas as pd
import yfinance as yf
import requests
from bs4 import BeautifulSoup
import pandas_datareader as pdr 
from google.cloud import storage
import os

#Get a list of the active S&P 500 Companies
wikiurl = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
response=requests.get(wikiurl)

#Parse data from html into a beautifulsoup object
soup = BeautifulSoup(response.text, 'html.parser')
snp500tbl = soup.find('table',{'class':"wikitable"},{'id':'constituents'})

#Convert wiki table into python data frame
snp500df = pd.read_html((str(snp500tbl)))
snp500df = pd.DataFrame(snp500df[0])

#replace . in symbols with - to match yfinance format
snp500list = snp500df['Symbol'].tolist()
snp500list = [ticker.replace('.','-') if '.' in ticker else ticker for ticker in snp500list ]

priceDataList = list()
quoteDataList = list() 
for ticker in snp500list:
    #Getting YTD price data for each ticker in the list of snp500 tickers
    priceData = yf.download(ticker, period="ytd",interval='1d', group_by="Ticker")
    priceData['ticker'] = ticker  # add this column because the dataframe doesn't contain a column with the ticker
    priceDataList.append(priceData)
    
    #Getting current yahoo quote data from pandas data reader to bring in additionally data about the company
    quoteData = pdr.get_quote_yahoo(ticker)
    quoteData['AsOfDataTime'] = datetime.now()
    quoteDataList.append(quoteData)

# combine all dataframes into a single dataframe
stockPriceDataDf = pd.concat(priceDataList)
quoteDataDf = pd.concat(quoteDataList)

#adding name data frame index
quoteDataDf = quoteDataDf.rename_axis('ticker') 

#Export dataframes as csv to google cloud storage
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'credentials\airbyte_credentials.json' # Only need this if you're running this code locally.

client = storage.Client()
bucket = client.get_bucket('yfinance_stock_data')
    
bucket.blob('data_sync/SnP500Companies.csv').upload_from_string(snp500df.to_csv(index=False,sep=';',header=True), 'text/csv')
bucket.blob('data_sync/PriceData.csv').upload_from_string(stockPriceDataDf.to_csv(index=True,sep=';',header=True), 'text/csv')
bucket.blob('data_sync/QuoteData.csv').upload_from_string(stockPriceDataDf.to_csv(index=True,sep=';',header=True), 'text/csv')

