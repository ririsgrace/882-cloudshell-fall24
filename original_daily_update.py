# # imports
# from google.cloud import secretmanager
# import duckdb
# import yfinance as yf
# import pandas as pd
# import functions_framework
# import logging
# import os

# # Set up logging
# logging.basicConfig(level=logging.INFO)

# project_id = 'ba882-rgk'
# secret_id = 'secret2_duckdb'
# version_id = 'latest'

# db = 'stocks'
# schema = 'stocks_schema'
# db_schema = f"{db}.{schema}"

# # HTTP entry point for Cloud Functions
# @functions_framework.http
# def daily_update(request):
#     try:
#         # Step 1: Secret Manager to get the MotherDuck token
#         sm = secretmanager.SecretManagerServiceClient()
#         name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
#         response = sm.access_secret_version(request={"name": name})
#         md_token = response.payload.data.decode("UTF-8")

#         # Step 2: Connect to MotherDuck using the DuckDB token
#         md = duckdb.connect(f'md:?motherduck_token={md_token}')

#         # Step 3: Define your table variables
#         tbl_name = "aapl"

#         # Step 4: Fetch new stock data for today using yfinance
#         ticker = "AAPL"
#         new_data = yf.download(ticker, period="5d")

#         # Log the fetched data from yfinance for debugging
#         logging.info("Fetched new data from yfinance:")
#         logging.info(new_data)

#         if not new_data.empty:
#             new_data = new_data.reset_index()
#             new_data = new_data.rename(columns={"Adj Close": "Adj_Close"})

#             # Iterate over each row in the fetched data
#             # Check for each date in the fetched data and insert only new records
#             for index, row in new_data.iterrows():
#                 current_date = row['Date']
#                 existing_dates = md.sql(f"SELECT Date FROM {db_schema}.{tbl_name} WHERE Date = '{current_date}';").fetchall()
    
#                 if existing_dates:
#                     logging.info(f"Date {current_date} already exists. Skipping insertion.")
#                 else:
#                     # Construct the INSERT query dynamically using the row values
#                     insert_query = f"""
#                     INSERT INTO {db_schema}.{tbl_name} 
#                     VALUES ('{row['Date']}', {row['Open']}, {row['High']}, {row['Low']}, {row['Close']}, {row['Adj_Close']}, {row['Volume']})
#                     """
        
#                     # Execute the insert query
#                     logging.info(f"Inserting new data for {current_date}...")
#                     md.execute(insert_query)

#         else:
#             logging.info("No new data available to insert.")

#         return "Daily update successful!", 200

#     except Exception as e:
#         logging.error(f"Error during update: {str(e)}")
#         return f"Failed to update: {str(e)}", 500
