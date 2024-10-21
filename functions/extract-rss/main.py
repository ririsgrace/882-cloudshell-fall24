import os
from google.cloud import secretmanager
import duckdb
import pandas as pd
import yfinance as yf
import functions_framework
import json

project_id = 'ba882-rgk'
secret_id = 'secret2_duckdb'
version_id = 'latest'

db = 'stocks'
schema = 'stocks_schema'
db_schema = f"{db}.{schema}"

# GCP Cloud Functions HTTP trigger
# @functions_framework.http
# def extract_task(request):
#     try:
#         # Access Secret Manager to retrieve the MotherDuck token
#         sm = secretmanager.SecretManagerServiceClient()
#         name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
#         response = sm.access_secret_version(request={"name": name})
#         md_token = response.payload.data.decode("UTF-8")

#         # Connect to MotherDuck using the token
#         md = duckdb.connect(f'md:?motherduck_token={md_token}')
        
#         # Extract stock data for multiple tickers
#         tickers = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'FB', 'NFLX']
#         stock_data = {}
#         print("Fetching stock data for tickers...")

#         for ticker in tickers:
#             # Download the stock data
#             data = yf.download(ticker, start="2024-09-01", end="2024-10-04")
#             data = data.reset_index()  # Reset index to ensure 'Date' is a column

#             # Convert 'Date' column to string to handle pd.Timestamp serialization
#             data['Date'] = data['Date'].astype(str)

#             # Convert DataFrame to a list of dictionaries
#             stock_data[ticker] = data.to_dict(orient='records')

#         # Return the stock data as a JSON response
#         return json.dumps(stock_data), 200

#     except Exception as e:
#         error_message = f"An error occurred: {str(e)}"
#         print(error_message)
#         return error_message, 500
@functions_framework.http
def extract_task(request):
    try:
        # Access Secret Manager to retrieve the MotherDuck token
        sm = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = sm.access_secret_version(request={"name": name})
        md_token = response.payload.data.decode("UTF-8")

        # Connect to MotherDuck using the token
        md = duckdb.connect(f'md:?motherduck_token={md_token}')
        
        # Extract stock data for multiple tickers
        tickers = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']
        stock_data = {}
        print("Fetching stock data for tickers...")

        for ticker in tickers:
            # Get the last available date in the table for this ticker
            result = md.execute(f"SELECT MAX(Date) FROM {db_schema}.combined_stocks WHERE Ticker='{ticker}'").fetchone()
            last_date = result[0]
            print(f"Last available date for {ticker}: {last_date}")
            
            if last_date is None:
                last_date = "2024-09-01"  # Default start date if no data exists
            
            # Download the stock data starting from the last available date
            try:
                data = yf.download(ticker, start=last_date)
            except Exception as yf_error:
                print(f"Error fetching data for {ticker}: {yf_error}")
                continue  # Skip this ticker if there's an issue

            if data.empty:
                print(f"No data returned for {ticker} from {last_date}")
                continue

            data = data.reset_index()  # Reset index to ensure 'Date' is a column

            # Convert 'Date' column to string to handle pd.Timestamp serialization
            data['Date'] = data['Date'].astype(str)

            # Convert DataFrame to a list of dictionaries
            stock_data[ticker] = data.to_dict(orient='records')

        # Return the stock data as a JSON response
        return json.dumps(stock_data), 200

    except Exception as e:
        error_message = f"An error occurred: {str(e)}"
        print(error_message)
        return error_message, 500
