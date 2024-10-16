# # imports
# from google.cloud import secretmanager
# import duckdb
# import yfinance as yf
# import pandas as pd
# import functions_framework
# import os

# # HTTP entry point for Cloud Functions
# @functions_framework.http
# def daily_update(request):
#     # Step 1: Secret Manager to get the MotherDuck token
#     sm = secretmanager.SecretManagerServiceClient()

#     project_id = 'ba882-rgk'
#     secret_id = 'secret2_duckdb'
#     version_id = 'latest'

#     # Build the resource name of the secret version
#     name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

#     # Access the secret version
#     response = sm.access_secret_version(request={"name": name})
#     md_token = response.payload.data.decode("UTF-8")

#     # Step 2: Connect to MotherDuck using the DuckDB token
#     md = duckdb.connect(f'md:?motherduck_token={md_token}')

#     # Step 3: Define your schema and table variables
#     db = 'stocks'
#     schema = 'stocks_schema'
#     db_schema = f"{db}.{schema}"
#     tbl_name = "aapl"

#     # Step 4: Fetch new stock data for today using yfinance
#     ticker = "AAPL"
#     new_data = yf.download(ticker, period="1d")

#     # Reset index and rename columns to match your table
#     new_data = new_data.reset_index()
#     new_data = new_data.rename(columns={"Adj Close": "Adj_Close"})

#     # Check if the date already exists in the table
#     existing_dates = md.sql(f"SELECT Date FROM {db_schema}.{tbl_name} WHERE Date = '{new_data['Date'].iloc[0]}';").fetchall()

#     if existing_dates:
#         print(f"Date {new_data['Date'].iloc[0]} already exists. Skipping insertion.")
#     else:
#         # Insert new data if no duplicate date exists
#         print("Inserting new data...")
#         md.execute(f"INSERT INTO {db_schema}.{tbl_name} SELECT * FROM new_data")

#     # Return a success message
#     print("Daily update successful!")

# if __name__ == "__main__":
#     daily_update(request=None)  # Manually call the function without HTTP request