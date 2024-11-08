# ---- AAPL only -----
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

# def task(request):
#     sm = secretmanager.SecretManagerServiceClient()

#     # Build the resource name of the secret version
#     name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

#     # Access the secret version
#     response = sm.access_secret_version(request={"name": name})
#     md_token = response.payload.data.decode("UTF-8")

#     # Initiate the MotherDuck connection through an access token
#     md = duckdb.connect(f'md:?motherduck_token={md_token}')

#     # Show databases
#     md.sql("SHOW DATABASES").show()

#     # Download stock data
#     ticker = "AAPL"
#     data = yf.download(ticker, start="2024-09-01", end="2024-10-04")
#     data = data.reset_index()
#     data = data.rename(columns={"Adj Close": "Adj_Close"})

#     # Database and schema setup
#     create_db_sql = f"CREATE DATABASE IF NOT EXISTS {db};"
#     md.sql(create_db_sql)
#     md.sql("SHOW DATABASES").show()

#     # Create schema and table
#     md.sql(f"CREATE SCHEMA IF NOT EXISTS {db_schema};")
    
#     print("creating schema")

#     tbl_name = "aapl"
#     table_create_sql = f"""
#     CREATE TABLE IF NOT EXISTS {db_schema}.{tbl_name}  (
#         Date DATE PRIMARY KEY,  -- Primary key
#         Open FLOAT(2),          -- Open Price
#         High FLOAT(2),          -- Highest Price
#         Low FLOAT(2),           -- Lowest Price
#         Close FLOAT(2),         -- Closing Price
#         Adj_Close FLOAT(2),     -- Adjusted Close Price
#         Volumn FLOAT(2)         -- Trading Volume
#     );
#     """
#     md.sql(table_create_sql)
#     md.sql(f"DESCRIBE {db_schema}.{tbl_name};").show()

#     # Insert the stock data into the table
#     md.execute(f"""INSERT INTO {db_schema}.{tbl_name} SELECT * FROM data WHERE Date NOT IN (SELECT Date FROM {db_schema}.{tbl_name})""")

#     return "Schema setup and data insertion successful!", 200

import os
from google.cloud import secretmanager
import duckdb
import pandas as pd
import yfinance as yf
import functions_framework

project_id = 'ba882-rgk'
secret_id = 'secret2_duckdb'
version_id = 'latest'

db = 'stocks'
schema = 'stocks_schema'
db_schema = f"{db}.{schema}"

tickers = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']

# GCP Cloud Functions HTTP trigger
@functions_framework.http
def health_check(request):
    return "OK", 200

@functions_framework.http
def schema_task(request):
    try:
        sm = secretmanager.SecretManagerServiceClient()
        
        # Build the resource name of the secret version
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        
        # Access the secret version
        response = sm.access_secret_version(request={"name": name})
        md_token = response.payload.data.decode("UTF-8")
        
        # Initiate the MotherDuck connection through an access token
        md = duckdb.connect(f'md:?motherduck_token={md_token}')
        
        # Database and schema setup
        create_db_sql = f"CREATE DATABASE IF NOT EXISTS {db};"
        md.sql(create_db_sql)
        md.sql("SHOW DATABASES").show()
        
        # Create schema
        md.sql(f"CREATE SCHEMA IF NOT EXISTS {db_schema};")
        
        # Create table with the correct structure
        tbl_name = "combined_stocks"
        
        table_create_sql = f"""
        CREATE TABLE IF NOT EXISTS {db_schema}.{tbl_name}  (
            Date DATE,              -- Date of the stock record
            Open FLOAT,             -- Open Price
            High FLOAT,             -- Highest Price
            Low FLOAT,              -- Lowest Price
            Close FLOAT,            -- Closing Price
            Adj_Close FLOAT,        -- Adjusted Close Price
            Volume BIGINT,          -- Trading Volume
            Ticker VARCHAR,         -- Stock Ticker Symbol
            PRIMARY KEY (Ticker, Date)  -- Composite primary key on Ticker and Date
        );
        """

        md.sql(table_create_sql)
        # md.sql(f"DESCRIBE {db_schema}.{tbl_name};").show()
        
        # Create individual tables for each ticker
        for ticker in tickers:
            ticker_table_name = f"{ticker.lower()}_stocks"  # Table name for each ticker
            ticker_table_create_sql = f"""
            CREATE TABLE IF NOT EXISTS {db_schema}.{ticker_table_name} (
                Date DATE,              -- Date of the stock record
                Open FLOAT,             -- Open Price
                High FLOAT,             -- Highest Price
                Low FLOAT,              -- Lowest Price
                Close FLOAT,            -- Closing Price
                Adj_Close FLOAT,        -- Adjusted Close Price
                Volume BIGINT,          -- Trading Volume
                Ticker VARCHAR,         -- Stock Ticker Symbol
                PRIMARY KEY (Ticker, Date)  -- Composite primary key on Ticker and Date
            );
            """
            md.sql(ticker_table_create_sql)
        
        # Verify table creation
        md.sql(f"DESCRIBE {db_schema}.{tbl_name};").show()
        
        for ticker in tickers:
            md.sql(f"DESCRIBE {db_schema}.{ticker.lower()}_stocks;").show
            
        return "Schema setup and table creation successful!", 200

    except Exception as e:
        return f"An error occurred: {str(e)}", 500
