import os
from google.cloud import secretmanager
import duckdb
import pandas as pd
import json
import functions_framework
import requests

project_id = 'ba882-rgk'
secret_id = 'secret2_duckdb'
version_id = 'latest'

db = 'stocks'
schema = 'stocks_schema'
db_schema = f"{db}.{schema}"

# Health check endpoint
@functions_framework.http
def health_check(request):
    return "OK", 200

# Function to invoke Docker-based service
def invoke_docker_function(url, payload):
    headers = {
        'Content-Type': 'application/json'  # Ensure correct Content-Type
    }

    try:
        # Log the payload for debugging
        print(f"Payload to Docker service: {payload}")

        response = requests.post(url, json=payload, headers=headers)

        # Raise an error for bad HTTP responses (4xx or 5xx)
        response.raise_for_status()

        return response
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
        print(f"Response content: {response.content}")  # Log the response content for debugging
        raise
    except Exception as err:
        print(f"Other error occurred: {err}")
        raise

@functions_framework.http
def transform_task(request):
    try:
        # Log the content type for debugging
        print(f"Received Content-Type: {request.content_type}")

        # Ensure the request content type is 'application/json'
        if request.content_type != 'application/json':
            print("Unsupported Media Type")
            return f"Unsupported Media Type: Content-Type must be 'application/json', but received '{request.content_type}'", 415

        # Parse the request JSON body
        stock_data = request.get_json(silent=True)
        if stock_data is None:
            print(f"Invalid or missing JSON body for content-type: {request.content_type}")
            return "Bad Request: Invalid or missing JSON body", 400

        print(f"Received stock data: {stock_data}")  # Log the received data for debugging

        # Initialize list to store transformed data
        transformed_data = []

        # Iterate over each ticker's data
        for ticker, data in stock_data.items():
            if data:  # Check if there is data to process
                try:
                    df = pd.DataFrame(data)  # Convert to DataFrame
                    df = df.rename(columns={"Adj Close": "Adj_Close"})  # Rename columns for consistency
                    df['Ticker'] = ticker  # Add Ticker column

                    # Check if the 'Date' is already in string format or in milliseconds
                    if isinstance(df['Date'].iloc[0], str):
                        # If it's already a string, convert it to datetime without specifying 'unit'
                        df['Date'] = pd.to_datetime(df['Date'], errors='coerce').dt.date
                    else:
                        # If it's in milliseconds, convert it with the 'ms' unit
                        df['Date'] = pd.to_datetime(df['Date'], unit='ms', errors='coerce').dt.date

                    # Filter out invalid or null dates
                    df = df[df['Date'].notnull() & (df['Date'] > pd.Timestamp('1970-01-01').date())]

                    if not df.empty:
                        transformed_data.append(df)
                    else:
                        print(f"No valid data found for ticker: {ticker}")
                except Exception as e:
                    print(f"Error processing data for ticker {ticker}: {str(e)}")
            else:
                print(f"No data for ticker: {ticker}")

        # Combine all transformed data into a single DataFrame
        if transformed_data:
            combined_data = pd.concat(transformed_data)
        else:
            print("No valid stock data after transformation")
            return "No valid stock data received", 400

        # Log the transformed data for debugging
        print(f"Transformed data (sample): {combined_data.head()}")

        # Convert the combined DataFrame to JSON
        result = combined_data.to_json(orient="records", date_format="iso")  # Ensure date is in ISO format

        # Return the transformed data as JSON with the appropriate content type
        return result, 200, {'Content-Type': 'application/json'}

    except Exception as e:
        error_message = f"An error occurred while transforming stock data: {str(e)}"
        print(error_message)
        return error_message, 500
