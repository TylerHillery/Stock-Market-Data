# Project Overview
I have built a data pipeline to ingest, store, transform & visualize stock market data. Purpose of this project was to get more experience designing & building an end to end data pipeline. I also used it as an opportunity to learn about different tooling in the Modern Data Stack. 

Currently the pipeline is set up to only retrieve data from companies in the S&P 500 Index. To get an active list of those companies I used [Beautiful Soup](https://beautiful-soup-4.readthedocs.io/en/latest/) python package to scrape this table from this [wiki page](https://en.wikipedia.org/wiki/List_of_S%26P_500_companies).

After I get an active list of S&P500 companies I used [yfinance](https://github.com/ranaroussi/yfinance ) to retrieve YTD price information & [pandas_datareader](https://pandas-datareader.readthedocs.io/en/latest/readers/yahoo.html) to get current quote information which provided me with additional data about the company like market cap. Original I tried to use yfiance's .info method but was having issues when retrieving data for large list of stocks. 

After the data is retrieved I export the pandas data frames to a CSV on Google Cloud Storage. [Dagster](https://github.com/dagster-io/dagster) then triggers my [Airbyte](https://github.com/airbytehq/airbyte) connections to run to load the files into raw tables onto Big Query. Once the files are loaded my dbt models are then ran to transform the data into dim & fct tables. Lastly, my final data set is created and my dashboard is refreshed in Preset. 

### **Data Pipeline Overview**
  <img src="Assets\stock-market-data-pipeline.png" alt="postgresSQL logo"  width="600" height="400"/>

### **End Product**

<iframe
  width="600"
  height="400"
  seamless
  frameBorder="0"
  scrolling="no"
  src="https://0b99782f.us2a.app.preset.io/superset/explore/?form_data_key=njCjWkB6sVqNOEk3I16xBCb4MArMWkN8nsDxNaCv-IODw4FtvVbEJkDjEelmYw3l&slice_id=79&standalone=1&height=400"
>
</iframe>

  <img src="Assets\dashboard.jpg" alt="postgresSQL logo"  width="700" height="400"/>


