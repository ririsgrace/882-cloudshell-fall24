import os
from google.cloud import secretmanager
import duckdb
import pandas as pd
import json
import functions_framework

project_id = 'ba882-rgk'
secret_id = 'secret2_duckdb'
version_id = 'latest'

db = 'stocks'
schema = 'stocks_schema'
db_schema = f"{db}.{schema}"

# GCP Cloud Functions HTTP trigger
@functions_framework.http
def health_check(request):
    return "OK", 200

@functions_framework.http
def transform_task(request):
    try:
        # Log the content type for debugging
        print(f"Received Content-Type: {request.content_type}")

        # Ensure the request content type is application/json
        if request.content_type != 'application/json':
            return f"Unsupported Media Type: Content-Type must be 'application/json', but received '{request.content_type}'", 415

        # Parse the request JSON body
        stock_data = request.get_json()
        if stock_data is None:
            return "Bad Request: Request body must be valid JSON", 400

        print(f"Received stock data: {stock_data}")  # Log the received data for debugging

        # Access Secret Manager to retrieve the MotherDuck token
        sm = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = sm.access_secret_version(request={"name": name})
        md_token = response.payload.data.decode("UTF-8")

        # Connect to MotherDuck using the token
        md = duckdb.connect(f'md:?motherduck_token={md_token}')
        
        # Transform the data by adding a Ticker column and combining all data
        transformed_data = []
        for ticker, data in stock_data.items():
            if data:  # Ensure there is data to process
                df = pd.DataFrame(data)  # Convert each stock's data to a DataFrame
                df = df.rename(columns={"Adj Close": "Adj_Close"})  # Rename columns for consistency
                df['Ticker'] = ticker  # Add Ticker column
                transformed_data.append(df)
            else:
                print(f"No data for ticker {ticker}")

        # Combine all the transformed data if there is any
        if transformed_data:
            combined_data = pd.concat(transformed_data)
        else:
            return "No valid stock data received", 400

        # Convert the result to JSON for returning
        result = combined_data.to_json(orient="records")
        return result, 200

    except Exception as e:
        error_message = f"An error occurred while transforming stock data: {str(e)}"
        print(error_message)
        return error_message, 500
