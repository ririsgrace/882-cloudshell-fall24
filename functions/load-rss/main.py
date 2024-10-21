# import os
# from google.cloud import secretmanager
# import duckdb
# import pandas as pd
# import functions_framework
# import requests  # Ensure requests is imported
# import logging
# import traceback

# # Setup logging
# logging.basicConfig(level=logging.INFO)

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
# def invoke_docker_function(url, payload):
#     headers = {
#         'Content-Type': 'application/json'
#     }
#     response = requests.post(url, json=payload, headers=headers)
#     response.raise_for_status()  # Ensure it raises HTTP errors if they occur
#     return response

# @functions_framework.http
# def load_task(request):
#     try:
#         # Parse the request JSON body
#         combined_data = request.get_json()
#         if combined_data is None or not isinstance(combined_data, list):
#             logging.error("Bad Request: Request body must be a non-empty JSON array")
#             return "Bad Request: Request body must be a non-empty JSON array", 400

#         # Convert JSON data into a Pandas DataFrame
#         try:
#             combined_df = pd.DataFrame(combined_data)
#         except ValueError as e:
#             logging.error(f"DataFrame Conversion Error: {str(e)}")
#             return f"DataFrame Conversion Error: {str(e)}", 400

#         # Log DataFrame info for debugging
#         logging.info(f"Received DataFrame: {combined_df.head()}")
        
#         # Access Secret Manager to retrieve the MotherDuck token
#         sm = secretmanager.SecretManagerServiceClient()
#         name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
#         response = sm.access_secret_version(request={"name": name})
#         md_token = response.payload.data.decode("UTF-8")
        
#         # Log successful token retrieval
#         logging.info(f"MotherDuck token retrieved: {md_token[:4]}...")

#         # Connect to MotherDuck using the token
#         md = duckdb.connect(f'md:?motherduck_token={md_token}')

#         # Ensure the schema exists
#         md.execute(f"CREATE SCHEMA IF NOT EXISTS {db_schema};")

#         # Define the schema and table name
#         table_name = "combined_stocks"

#         # Ensure the table exists or create it
#         md.execute(f"""
#         CREATE TABLE IF NOT EXISTS {db_schema}.{table_name} (
#             Date DATE,
#             Open FLOAT,
#             High FLOAT,
#             Low FLOAT,
#             Close FLOAT,
#             Adj_Close FLOAT,
#             Volume BIGINT,
#             Ticker VARCHAR
#         );
#         """)

#         # Validate the DataFrame schema
#         expected_columns = ["Date", "Open", "High", "Low", "Close", "Adj_Close", "Volume", "Ticker"]
#         if combined_df.columns.tolist() != expected_columns:
#             logging.error("Data does not match the expected schema")
#             return "Bad Request: Data does not match the expected schema", 400

#         # Register the DataFrame with DuckDB and insert the data
#         md.register('combined_df', combined_df)
#         md.execute(f"INSERT INTO {db_schema}.{table_name} SELECT * FROM combined_df;")

#         # Check if data was successfully inserted
#         result = md.execute(f"SELECT COUNT(*) FROM {db_schema}.{table_name};").fetchone()
#         logging.info(f"Rows inserted into {db_schema}.{table_name}: {result[0]}")

#         return f"Data Loaded Successfully. Rows inserted: {result[0]}", 200

#     except Exception as e:
#         # Log the full error message
#         logging.error(f"An error occurred: {traceback.format_exc()}")
#         error_message = f"An error occurred while loading data: {str(e)}"
#         return error_message, 500

import logging
import traceback
import os
from google.cloud import secretmanager
import duckdb
import pandas as pd
import functions_framework

project_id = 'ba882-rgk'
secret_id = 'secret2_duckdb'
version_id = 'latest'
db = 'stocks'
schema = 'stocks_schema'
db_schema = f"{db}.{schema}"

@functions_framework.http
def load_task(request):
    try:
        # Parse the request JSON body
        combined_data = request.get_json()
        if combined_data is None or not isinstance(combined_data, list) or not combined_data:
            logging.error("Bad Request: Request body must be a non-empty JSON array")
            return "Bad Request: Request body must be a non-empty JSON array", 400

        # Convert JSON data into a Pandas DataFrame
        try:
            combined_df = pd.DataFrame(combined_data)
        except ValueError as e:
            logging.error(f"DataFrame Conversion Error: {str(e)}")
            return f"DataFrame Conversion Error: {str(e)}", 400

        # Ensure that the 'Date' column is in the correct format (YYYY-MM-DD)
        if 'Date' in combined_df.columns:
            combined_df['Date'] = pd.to_datetime(combined_df['Date'], errors='coerce').dt.date
        
        # Log DataFrame info for debugging
        logging.info(f"Received DataFrame: {combined_df.head()}")

        # Access Secret Manager to retrieve the MotherDuck token
        sm = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = sm.access_secret_version(request={"name": name})
        md_token = response.payload.data.decode("UTF-8")

        # Connect to MotherDuck using the token
        md = duckdb.connect(f'md:?motherduck_token={md_token}')

        # Ensure the schema exists
        md.execute(f"CREATE SCHEMA IF NOT EXISTS {db_schema};")

        # Define the schema and table name
        table_name = "combined_stocks"

        # Ensure the table exists or create it with a primary key constraint on (Ticker, Date)
        md.execute(f"""
        CREATE TABLE IF NOT EXISTS {db_schema}.{table_name} (
            Date DATE,
            Open FLOAT,
            High FLOAT,
            Low FLOAT,
            Close FLOAT,
            Adj_Close FLOAT,
            Volume BIGINT,
            Ticker VARCHAR,
            PRIMARY KEY (Ticker, Date)  -- Create composite primary key
        );
        """)

        # Validate the DataFrame schema
        expected_columns = ["Date", "Open", "High", "Low", "Close", "Adj_Close", "Volume", "Ticker"]
        if not set(expected_columns).issubset(combined_df.columns):
            logging.error("Data does not match the expected schema")
            return "Bad Request: Data does not match the expected schema", 400

        # Register the DataFrame with DuckDB
        md.register('combined_df', combined_df)

        # Insert data using EXISTS to handle conflicts (avoid duplicate data)
        md.execute(f"""
            INSERT INTO {db_schema}.{table_name} (Date, Open, High, Low, Close, Adj_Close, Volume, Ticker)
            SELECT new_data.Date, new_data.Open, new_data.High, new_data.Low, new_data.Close, new_data.Adj_Close, new_data.Volume, new_data.Ticker
            FROM combined_df as new_data
            WHERE NOT EXISTS (
                SELECT 1 
                FROM {db_schema}.{table_name} existing
                WHERE new_data.Ticker = existing.Ticker
                AND new_data.Date = existing.Date
            );
        """)

        # Get the number of rows inserted
        result = md.execute(f"SELECT COUNT(*) FROM {db_schema}.{table_name};").fetchone()
        logging.info(f"Rows inserted into {db_schema}.{table_name}: {result[0]}")

        return f"Data Loaded Successfully. Total rows: {result[0]}", 200

    except Exception as e:
        # Log the full error message
        logging.error(f"An error occurred: {traceback.format_exc()}")
        error_message = f"An error occurred while loading data: {str(e)}"
        return error_message, 500
