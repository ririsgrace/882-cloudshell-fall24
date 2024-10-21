# import os
# from google.cloud import secretmanager
# import duckdb
# import pandas as pd
# import yfinance as yf
# import functions_framework

# project_id = 'ba882-rgk'
# secret_id = 'secret2_duckdb'
# version_id = 'latest'

# db = 'stocks'
# schema = 'stocks_schema'
# db_schema = f"{db}.{schema}"

# # GCP Cloud Functions HTTP trigger
# @functions_framework.http
# def health_check(request):
#     return "OK", 200

# @functions_framework.http
# def extract_task(request):
#     print("Starting extract_task")
#     try:
#         # Your existing code or logic here

#         # End the task successfully
#         print("Completed successfully")
#         return "Task completed successfully", 200  # Return valid response
#     except Exception as e:
#         print(f"Error occurred: {str(e)}")
#         return f"Internal Server Error: {str(e)}", 500  # Return error response

# @functions_framework.http
# def extract_task(request):
#     sm = secretmanager.SecretManagerServiceClient()
    
#     # Build the resource name of the secret version
#     name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
    
#     # Access the secret version
#     response = sm.access_secret_version(request={"name": name})
#     md_token = response.payload.data.decode("UTF-8")
    
#     # Initiate the MotherDuck connection through an access token
#     md = duckdb.connect(f'md:?motherduck_token={md_token}')
    
#     # Extract stock data for multiple tickers
#     tickers = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'FB', 'NFLX']
#     stock_data = {}

#     print("Fetching stock data for tickers...")

#     for ticker in tickers:
#         data = yf.download(ticker, start="2024-09-01", end="2024-10-04")
#         stock_data[ticker] = data
#     return stock_data

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
        tickers = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'FB', 'NFLX']
        stock_data = {}
        print("Fetching stock data for tickers...")

        for ticker in tickers:
            # Download the stock data
            data = yf.download(ticker, start="2024-09-01", end="2024-10-04")
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