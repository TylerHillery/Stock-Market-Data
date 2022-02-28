#Importing Packages
from datetime import datetime
import pandas as pd
import yfinance as yf
import requests
from bs4 import BeautifulSoup
import pandas_datareader as pdr 

#Get a list of the active S&P 500 Companies
wikiurl = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
response=requests.get(wikiurl)

#Parse data from html into a beautifulsoup object
soup = BeautifulSoup(response.text, 'html.parser')
SnP500tbl = soup.find('table',{'class':"wikitable"},{'id':'constituents'})

#Convert wiki table into python data frame
SnP500df = pd.read_html((str(SnP500tbl)))
SnP500df = pd.DataFrame(SnP500df[0])

#Clean up periods in symbols to match yfinance format
SnP500list = SnP500df['Symbol'].tolist()
SnP500list = [ticker.replace('.','-') if '.' in ticker else ticker for ticker in SnP500list ]

df_list = list()
for ticker in SnP500list:
    data = yf.download(ticker, period="ytd",interval='1d', group_by="Ticker")
    data['ticker'] = ticker  # add this column because the dataframe doesn't contain a column with the ticker
    df_list.append(data)

# combine all dataframes into a single dataframe
stockPriceData = pd.concat(df_list)
print(stockPriceData)

#grab market cap data
df_list = list()
for ticker in SnP500list:
    data = pdr.get_quote_yahoo(ticker)
    data['AsOfDataTime'] = datetime.now()
    df_list.append(data)

# combine all dataframes into a single dataframe
yahooQuoteData = pd.concat(df_list)
yahooQuoteData = yahooQuoteData.rename_axis('ticker')
print(yahooQuoteData)

#Export dataframes to csv
yahooQuoteData.to_csv(r'C:\Users\Tyler\Documents\Projects\yFinance\StockMarketCapData.CSV', index = True, header=True, sep=";")
stockPriceData.to_csv(r'C:\Users\Tyler\Documents\Projects\yFinance\StockPriceData.CSV', index = True, header=True, sep=";")
SnP500df.to_csv(r'C:\Users\Tyler\Documents\Projects\yFinance\SnP500Stocks.CSV', index = False, header=True, sep=";")

