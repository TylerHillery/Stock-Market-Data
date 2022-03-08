#Importing Packages
import pandas as pd
import yfinance as yf
import requests
import os
import pandas_datareader as pdr 
from bs4 import BeautifulSoup
from datetime import datetime
from google.cloud import storage
from dagster import job, op, schedule, repository, RunRequest, ScheduleEvaluationContext,ScheduleDefinition
from dagster_airbyte import airbyte_resource, airbyte_sync_op

#Set Global google cloud variables
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"C:\Users\Tyler\Documents\Projects\yFinance\credentials\python_credentials.json" # Only need this if you're running this code locally.
client = storage.Client()
bucket = client.get_bucket('yfinance_stock_data')


#Get a list of the active S&P 500 Companies
@op
def download_active_snp500_stocks():
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

    #export data frame as csv to google could storage
    bucket.blob('data_sync/SnP500Companies.csv').upload_from_string(snp500df.to_csv(index=False,sep=';',header=True), 'text/csv')

    #return list of tickers 
    return snp500list

@op
def download_price_data(snp500list):
    priceDataList = list()
    for ticker in snp500list:
        #Getting YTD price data for each ticker in the list of snp500 tickers
        priceData = yf.download(ticker, period="ytd",interval='1d', group_by="Ticker")
        priceData['ticker'] = ticker  # add this column because the dataframe doesn't contain a column with the ticker
        priceDataList.append(priceData)

    # combine all dataframes into a single dataframe
    stockPriceDataDf = pd.concat(priceDataList)

    #Export dataframes as csv to google cloud storage      
    bucket.blob('data_sync/PriceData.csv').upload_from_string(stockPriceDataDf.to_csv(index=True,sep=';',header=True), 'text/csv')

@op
def download_quote_data(snp500list):
    quoteDataList = list() 
    for ticker in snp500list:        
        #Getting current yahoo quote data from pandas data reader to bring in additionally data about the company
        quoteData = pdr.get_quote_yahoo(ticker)
        quoteData['AsOfDataTime'] = datetime.now()
        quoteDataList.append(quoteData)

    # combine all dataframes into a single dataframe
    quoteDataDf = pd.concat(quoteDataList)

    #adding name data frame index
    quoteDataDf = quoteDataDf.rename_axis('ticker') 

    #Export dataframes as csv to google cloud storage      
    bucket.blob('data_sync/QuoteData.csv').upload_from_string(quoteDataDf.to_csv(index=True,sep=';',header=True), 'text/csv')

#Implementing Dagster & Airbyte plugin
my_airbyte_resource = airbyte_resource.configured(
    {
        "host": "localhost", 
        "port": "8000"
    }
)
#Setting up connections
sync_snp500_companies = airbyte_sync_op.configured({"connection_id": "735bbf5c-d2de-4543-ba4d-62157158a911"}, name="sync_snp500_companies")
sync_price_data = airbyte_sync_op.configured({"connection_id": "7f0f03ce-7372-4367-a879-a788209dca69"}, name="sync_price_data")
sync_quote_data = airbyte_sync_op.configured({"connection_id": "42ccc796-fd44-48e1-b7a1-eb640e5b1ff1"}, name="sync_quote_data")

@job(resource_defs={"airbyte": my_airbyte_resource})
def stock_market_data_job():
    snp500list = download_active_snp500_stocks()
    sync_snp500_companies(start_after=snp500list) 
    sync_quote_data(start_after=download_quote_data(snp500list))
    sync_price_data(start_after=download_price_data(snp500list))

#create schedule
stock_market_data_job_schedule = ScheduleDefinition(
    cron_schedule="30 15 * * 1-5",
    job=stock_market_data_job,
    execution_timezone="US/Central",
)
#creating repository stock_market_data_repository
@repository
def stock_market_data_repository():
    return [stock_market_data_job,stock_market_data_job_schedule]