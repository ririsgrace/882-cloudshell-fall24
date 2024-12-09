import os
from google.cloud import secretmanager
import duckdb
import pandas as pd
import yfinance as yf
import functions_framework

project_id = 'ba882-rgk'
secret_id = 'secret2_duckdb'
version_id = 'latest'
db = 'stocks_10'
schema = 'stocks_schema_10'
db_schema = f"{db}.{schema}"

tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'TSM', 'WMT', 'XOM', 'TSLA', 'AVGO']

# Most Active large cap stock
# AAPL (Apple) -
# MSFT (Microsoft) -
# AMZN (Amazon) -
# GOOGL (Google) -
# NVDA (NVIDIA) -
# TSM (Taiwan Semiconductor) -
# WMT (Walmart) -
# XOM (Exxon Mobil) -
# TSLA (Tesla) -
# AVGO (Broadcom Inc.) -

@functions_framework.http
def health_check(request):
    return "OK", 200

@functions_framework.http
def schema_task(request):
    try:
        print("Starting schema creation...")
        
        # Secret Manager connection
        sm = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = sm.access_secret_version(request={"name": name})
        md_token = response.payload.data.decode("UTF-8")
        
        # MotherDuck connection
        md = duckdb.connect(f'md:?motherduck_token={md_token}')
        print("Connected to MotherDuck")

        # Create database first
        md.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
        print(f"Database {db} created or verified")
        
        # Use the database
        md.sql(f"USE {db}")
        print(f"Using database {db}")

        # Create schema
        md.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        print(f"Schema {schema} created or verified")
        
        # Use schema
        md.sql(f"USE {schema}")
        print(f"Using schema {schema}")

        # Create table for news data
        print("Creating news table...")
        news_table_sql = """
        CREATE TABLE IF NOT EXISTS stock_news (
            ticker VARCHAR,
            source VARCHAR, -- News source name
            author VARCHAR, -- Article author
            title TEXT, -- Article title
            description TEXT, -- Article description
            content TEXT, -- Article content preview
            url VARCHAR, -- Article URL
            url_to_image VARCHAR, -- Image URL
            published_at TIMESTAMP, -- Publication date
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (ticker, url, source, author) -- Prevent duplicate articles
        );
        """
        md.sql(news_table_sql)
        print("News table created or verified")

        # Create table for GDP data
        print("Creating GDP table...")
        gdp_table_sql = """
        CREATE TABLE IF NOT EXISTS gdp_data (
            Date DATE, -- Date of GDP measurement
            GDP FLOAT, -- GDP value
            Real_GDP FLOAT, -- Real GDP Value
            GDP_Growth FLOAT, -- GDP growth rate
            PRIMARY KEY (Date) -- Primary key on Date
        );
        """
        md.sql(gdp_table_sql)
        print("GDP table created or verified")

        # Create combined stocks table
        print("Creating combined stocks table...")
        stocks_table_sql = """
        CREATE TABLE IF NOT EXISTS combined_stocks (
            Date DATE, -- Date of the stock record
            Open FLOAT, -- Open Price
            High FLOAT, -- Highest Price
            Low FLOAT, -- Lowest Price
            Close FLOAT, -- Closing Price
            Adj_Close FLOAT, -- Adjusted Close Price
            Volume BIGINT, -- Trading Volume
            Ticker VARCHAR, -- Stock Ticker Symbol
            PRIMARY KEY (Ticker, Date) -- Composite primary key on Ticker and Date
        );
        """
        md.sql(stocks_table_sql)
        print("Combined stocks table created or verified")

        # Create individual ticker tables
        print("\nCreating individual ticker tables...")
        for ticker in tickers:
            ticker_table_name = f"{ticker.lower()}_stocks"
            ticker_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {ticker_table_name} (
                Date DATE, -- Date of the stock record
                Open FLOAT, -- Open Price
                High FLOAT, -- Highest Price
                Low FLOAT, -- Lowest Price
                Close FLOAT, -- Closing Price
                Adj_Close FLOAT, -- Adjusted Close Price
                Volume BIGINT, -- Trading Volume
                Ticker VARCHAR, -- Stock Ticker Symbol
                PRIMARY KEY (Ticker, Date) -- Composite primary key on Ticker and Date
            );
            """
            md.sql(ticker_table_sql)
            print(f"Table for {ticker} created or verified")

        # Verify table creation
        print("\nVerifying table structures...")
        tables = md.sql("SHOW TABLES").fetchall()
        print("Created tables:")
        for table in tables:
            print(f"- {table[0]}")

        return "Schema setup and table creation successful!", 200

    except Exception as e:
        error_message = f"An error occurred: {str(e)}"
        print(error_message)
        return error_message, 500
        
# import os
# from google.cloud import secretmanager
# import duckdb
# import pandas as pd
# import yfinance as yf
# import functions_framework

# project_id = 'ba882-rgk'
# secret_id = 'secret2_duckdb'
# version_id = 'latest'

# db = 'stocks_10'
# schema = 'stocks_schema_10'
# db_schema = f"{db}.{schema}"

# tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'TSM', 'WMT', 'XOM', 'TSLA', 'AVGO']

# # Most Active large cap stock
# # AAPL (Apple) -
# # MSFT (Microsoft) - 
# # AMZN (Amazon) - 
# # GOOGL (Google) - 
# # NVDA (NVIDIA) - 
# # TSM (Taiwan Semiconductor) -
# # WMT (Walmart) -
# # XOM (Exxon Mobil) -
# # TSLA (Tesla) - 
# # AVGO (Broadcom Inc.) - 

# # GCP Cloud Functions HTTP trigger
# @functions_framework.http
# def health_check(request):
#     return "OK", 200

# @functions_framework.http
# def schema_task(request):
#     try:
#         sm = secretmanager.SecretManagerServiceClient()
        
#         # Build the resource name of the secret version
#         name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        
#         # Access the secret version
#         response = sm.access_secret_version(request={"name": name})
#         md_token = response.payload.data.decode("UTF-8")
        
#         # Initiate the MotherDuck connection through an access token
#         md = duckdb.connect(f'md:?motherduck_token={md_token}')
        
#         # Database and schema setup
#         create_db_sql = f"CREATE DATABASE IF NOT EXISTS {db};"
#         md.sql(create_db_sql)
#         md.sql("SHOW DATABASES").show()
        
#         # Create schema
#         md.sql(f"CREATE SCHEMA IF NOT EXISTS {db_schema};")
        
#         # Create table for news data:
#         news_table_sql = f"""
#         CREATE TABLE IF NOT EXISTS {db_schema}.stock_news (
#             id BIGINT AUTO_INCREMENT,
#             ticker VARCHAR,              
#             source VARCHAR,              -- News source name
#             author VARCHAR,              -- Article author
#             title TEXT,                  -- Article title
#             description TEXT,            -- Article description
#             content TEXT,                -- Article content preview
#             url VARCHAR,                 -- Article URL
#             url_to_image VARCHAR,        -- Image URL
#             published_at TIMESTAMP,      -- Publication date
#             created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
#             PRIMARY KEY (id),
#             UNIQUE (ticker, url)         -- Prevent duplicate articles
#         );
#         """
#         md.sql(news_table_sql)

#         # Create table for GDP data
#         table_create_sql = f"""
#         CREATE TABLE IF NOT EXISTS {db_schema}.gdp_data (
#             Date DATE,           -- Date of GDP measurement
#             GDP FLOAT,           -- GDP value
#             Real_GDP FLOAT,      -- Real GDP Value
#             GDP_Growth FLOAT,    -- GDP growth rate
#             PRIMARY KEY (Date)   -- Primary key on Date
#         );
#         """
#         md.sql(table_create_sql)

#         # Create table with the correct structure
#         tbl_name = "combined_stocks"
        
#         table_create_sql = f"""
#         CREATE TABLE IF NOT EXISTS {db_schema}.{tbl_name}  (
#             Date DATE,              -- Date of the stock record
#             Open FLOAT,             -- Open Price
#             High FLOAT,             -- Highest Price
#             Low FLOAT,              -- Lowest Price
#             Close FLOAT,            -- Closing Price
#             Adj_Close FLOAT,        -- Adjusted Close Price
#             Volume BIGINT,          -- Trading Volume
#             Ticker VARCHAR,         -- Stock Ticker Symbol
#             PRIMARY KEY (Ticker, Date)  -- Composite primary key on Ticker and Date
#         );
#         """

#         md.sql(table_create_sql)
        
#         # Create individual tables for each ticker
#         for ticker in tickers:
#             ticker_table_name = f"{ticker.lower()}_stocks"  # Table name for each ticker
#             ticker_table_create_sql = f"""
#             CREATE TABLE IF NOT EXISTS {db_schema}.{ticker_table_name} (
#                 Date DATE,              -- Date of the stock record
#                 Open FLOAT,             -- Open Price
#                 High FLOAT,             -- Highest Price
#                 Low FLOAT,              -- Lowest Price
#                 Close FLOAT,            -- Closing Price
#                 Adj_Close FLOAT,        -- Adjusted Close Price
#                 Volume BIGINT,          -- Trading Volume
#                 Ticker VARCHAR,         -- Stock Ticker Symbol
#                 PRIMARY KEY (Ticker, Date)  -- Composite primary key on Ticker and Date
#             );
#             """
#             md.sql(ticker_table_create_sql)
        
#         # Verify table creation
#         md.sql(f"DESCRIBE {db_schema}.{tbl_name};").show()
        
#         for ticker in tickers:
#             md.sql(f"DESCRIBE {db_schema}.{ticker.lower()}_stocks;").show
            
#         return "Schema setup and table creation successful!", 200

#     except Exception as e:
#         return f"An error occurred: {str(e)}", 500
