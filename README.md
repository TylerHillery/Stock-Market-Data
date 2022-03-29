# Project Overview
I have built a data pipeline to ingest, store, transform & visualize stock market data. Purpose of this project was to get more experience designing & building an end to end data pipeline. I also used it as an opportunity to learn about different tooling in the Modern Data Stack. 

Currently the pipeline is set up to retrieve data from companies in the S&P 500 Index. To get an active list of those companies I used [Beautiful Soup](https://beautiful-soup-4.readthedocs.io/en/latest/) python package to scrape a table from this [wiki page](https://en.wikipedia.org/wiki/List_of_S%26P_500_companies).

After I get an active list of S&P500 companies I used [yfinance](https://github.com/ranaroussi/yfinance ) to retrieve YTD price information & [pandas_datareader](https://pandas-datareader.readthedocs.io/en/latest/readers/yahoo.html) to get current quote information which provided me with additional data about the company like market cap. Originally I tried to use yfiance's .info method but was having issues when retrieving data for a large list of stocks. 

After the data is retrieved I export the pandas data frames to CSV files on Google Cloud Storage. [Dagster](https://github.com/dagster-io/dagster) then triggers my [Airbyte](https://github.com/airbytehq/airbyte) connection to load the files into BigQuery. Once the files are loaded my dbt models are then ran to transform the data. After dbt is done my dashboard is refreshed & the pipeline is complete!

### **[Dashboard](https://0b99782f.us2a.app.preset.io:443/r/3)**

<a href="https://0b99782f.us2a.app.preset.io:443/r/3" target="_blank"> <img src="Assets\Dashboard.jpg" alt="Dashboard"  width="650" height ="400"/> </a>

### **Data Pipeline Overview**
  <img src="Assets\stock-market-data-pipeline.png" alt="pipeline"  width="650" height="400"/>

### **Dagster UI** 
  <img src="Assets\dagster_UI.jpg" alt="pipeline"  width="650" height="400"/>

### **Airbyte UI** 
<img src="Assets\airbyte_UI.jpg" alt="pipeline"  width="650" height="400"/>

