# import os
# import functions_framework
# from google.cloud import secretmanager
# import duckdb
# import pandas as pd
# import yfinance as yf

# @functions_framework.http
# def task(request):
#     sm = secretmanager.SecretManagerServiceClient()

#     project_id = 'ba882-rgk'
#     secret_id = 'secret2_duckdb'
#     version_id = 'latest'

#     # Build the resource name of the secre
#     # t version
#     name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

#     # Access the secret version
#     response = sm.access_secret_version(request={"name": name})
#     md_token = response.payload.data.decode("UTF-8")

#     # Initiate the MotherDuck connection through an access token through
#     md = duckdb.connect(f'md:?motherduck_token={md_token}') 

#     # Show databases
#     md.sql("SHOW DATABASES").show()

#     # Download stock data
#     ticker = "AAPL"
#     data = yf.download(ticker, start="2024-09-01", end="2024-10-04")
#     data = data.reset_index()
#     data = data.rename(columns={"Adj Close": "Adj_Close"})

#     # Database and schema setup
#     db = 'stocks'
#     schema = 'stocks_schema'
#     create_db_sql = f"CREATE DATABASE IF NOT EXISTS {db};"
#     md.sql(create_db_sql)
#     md.sql("SHOW DATABASES").show()

#     # Create schema and table
#     db_schema = f"{db}.{schema}"
#     md.sql(f"CREATE SCHEMA IF NOT EXISTS {db_schema};")

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
#     md.execute(f"INSERT INTO {db_schema}.{tbl_name} SELECT * FROM data")

#     # Return success message
#     return "Schema setup and data insertion successful!",200
    
# if __name__ == "__main__":
#     port = os.getenv("PORT", "8080")  # Always use port 8080 for Cloud Functions/Cloud Run
#     functions_framework.create_app('task').run(host="0.0.0.0", port=int(port))


# # # imports
# # from google.cloud import secretmanager
# # import duckdb
# # import feedparser
# # import pandas as pd
# # import yfinance as yf

# # # HTTP entry point for Cloud Functions
# # # @functions_framework.http

# # # instantiate the service
# # sm = secretmanager.SecretManagerServiceClient()

# # # settings
# # project_id = 'ba882-rgk'
# # secret_id = 'secret2_duckdb'   #<---------- this is the name of the secret you created
# # version_id = 'latest'

# # # Build the resource name of the secret version
# # name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

# # # Access the secret version
# # response = sm.access_secret_version(request={"name": name})
# # md_token = response.payload.data.decode("UTF-8")

# # # initiate the MotherDuck connection through an access token through
# # md = duckdb.connect(f'md:?motherduck_token={md_token}') 

# # # lets look at the high level bits
# # md.sql("SHOW DATABASES").show()

# # ticker = "AAPL"
# # data = yf.download(ticker, start="2024-09-01", end="2024-10-04")
# # data = data.reset_index()
# # data = data.rename(columns={"Adj Close": "Adj_Close"})
# # db = 'stocks'
# # schema = 'stocks_schema'
# # create_db_sql = f"CREATE DATABASE IF NOT EXISTS {db};"
# # md.sql(create_db_sql)
# # md.sql("SHOW DATABASES").show()

# # # create a fully qualified name that we can use from this point forward
# # db_schema = f"{db}.{schema}"
# # # use DDL to create the schema if it doesnt exist via an F string
# # md.sql(f"CREATE SCHEMA IF NOT EXISTS {db_schema};")
# # #table stuff

# # tbl_name = "aapl"

# # table_create_sql = f"""
# # CREATE TABLE IF NOT EXISTS {db_schema}.{tbl_name}  (
# #     Date DATE PRIMARY KEY,  -- Primary key
# #     Open FLOAT(2),             -- Open Price
# #     High FLOAT(2),                -- Highest Price
# #     Low FLOAT(2),          -- Lowest Price
# #     Close FLOAT(2),         -- Closing Price
# #     Adj_Close FLOAT(2),      -- Closing Price after applying splits and dividends
# #     Volumn FLOAT(2)         -- Trading Volume
# # );
# # """

# # md.sql(table_create_sql)
# # md.sql(f"DESCRIBE {db_schema}.{tbl_name};").show()

# # print(data)

# # # now insert the data directly from pands into the table
# # md.execute(f"INSERT INTO {db_schema}.{tbl_name} SELECT * FROM data;")