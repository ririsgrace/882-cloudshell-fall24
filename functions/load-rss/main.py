import logging
import traceback
import os
from google.cloud import secretmanager
import duckdb
import pandas as pd
import functions_framework
from google.cloud import storage
from io import StringIO

project_id = 'ba882-rgk'
secret_id = 'secret2_duckdb'
version_id = 'latest'
db = 'stocks'
schema = 'stocks_schema'
db_schema = f"{db}.{schema}"
bucket_name = 'rgk-ba882-fall24-finance'
path_name = 'stocks-data/'
 
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

        # Define the schema and table name for combined data
        table_name = "combined_stocks"

        # Ensure the combined_stocks table exists
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

        # Log the combined data before inserting into the combined_stocks table
        logging.info(f"Combined DataFrame before insert: {combined_df.head()}")

        # Check for missing Ticker values in the combined DataFrame
        missing_tickers = combined_df[combined_df['Ticker'].isnull()]
        if not missing_tickers.empty:
            logging.error(f"Rows with missing Ticker values: {missing_tickers}")

        # Register the DataFrame with DuckDB
        md.register('combined_df', combined_df)

        # Insert data into the combined_stocks table
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

        # Now, insert data into ticker-specific tables (e.g., aapl_stocks, msft_stocks, etc.)
        for ticker in combined_df['Ticker'].unique():
            logging.info(f"Inserting data for ticker: {ticker}")

            # Check if there are any missing values for the current ticker
            ticker_data = combined_df[combined_df['Ticker'] == ticker]
            if ticker_data['Ticker'].isnull().any():
                logging.error(f"Missing Ticker values for {ticker}!")

            # Clean up ticker name (strip spaces and convert to lowercase for table name)
            ticker_table_name = f"{ticker.lower()}_stocks"  # Assumes table names are like aapl_stocks

            # Ensure the ticker-specific table exists
            md.execute(f"""
                CREATE TABLE IF NOT EXISTS {db_schema}.{ticker_table_name} (
                    Date DATE,
                    Open FLOAT,
                    High FLOAT,
                    Low FLOAT,
                    Close FLOAT,
                    Adj_Close FLOAT,
                    Volume BIGINT,
                    Ticker VARCHAR,
                    PRIMARY KEY (Ticker, Date)
                );
            """)

            # Insert data into the ticker-specific table
            md.execute(f"""
                INSERT INTO {db_schema}.{ticker_table_name} (Date, Open, High, Low, Close, Adj_Close, Volume, Ticker)
                SELECT new_data.Date, new_data.Open, new_data.High, new_data.Low, new_data.Close, new_data.Adj_Close, new_data.Volume, new_data.Ticker
                FROM combined_df as new_data
                WHERE new_data.Ticker = '{ticker}'
                AND NOT EXISTS (
                    SELECT 1 
                    FROM {db_schema}.{ticker_table_name} existing
                    WHERE new_data.Ticker = existing.Ticker
                    AND new_data.Date = existing.Date
                );
            """)

        # Get the number of rows inserted into combined_stocks
        result = md.execute(f"SELECT COUNT(*) FROM {db_schema}.{table_name};").fetchone()
        logging.info(f"Rows inserted into {db_schema}.{table_name}: {result[0]}")

        # Convert the combined data and ticker tables to CSV files
        # Upload combined data CSV to bucket
        combined_csv = combined_df.to_csv(index=False)

        # Call the function to upload or append combined data CSV to bucket
        upload_to_bucket(combined_df, f"combined_stocks.csv")

        # For each ticker, upload or append to its corresponding ticker-specific CSV
        for ticker in combined_df['Ticker'].unique():
            ticker_data = combined_df[combined_df['Ticker'] == ticker]
            upload_to_bucket(ticker_data, f"{ticker.lower()}_stocks.csv")

        # upload_to_bucket(combined_csv, f"combined_stocks.csv")

        # # Upload each ticker's table to the bucket as CSV
        # for ticker in combined_df['Ticker'].unique():
        #     ticker_data = combined_df[combined_df['Ticker'] == ticker]
        #     ticker_csv = ticker_data.to_csv(index=False)
        #     upload_to_bucket(ticker_csv, f"{ticker.lower()}_stocks.csv")

        return f"Data Loaded Successfully. Total rows: {result[0]}", 200

    except Exception as e:
        # Log the full error message
        logging.error(f"An error occurred: {traceback.format_exc()}")
        error_message = f"An error occurred while loading data: {str(e)}"
        return error_message, 500

# def upload_to_bucket(csv_data, file_name):
#     """Uploads the CSV data to Google Cloud Storage"""
#     client = storage.Client(project=project_id)
#     bucket = client.bucket(bucket_name)
#     blob = bucket.blob(file_name)
    
#     blob.upload_from_string(csv_data, content_type='text/csv')
#     logging.info(f"Uploaded {file_name} to {bucket_name}")

def upload_to_bucket(new_data_df, file_name):
    """Uploads or appends CSV data to Google Cloud Storage."""
    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)
    blob_name = f'{path_name}{file_name}'
    blob = bucket.blob(blob_name)
    
    try:
        # Check if the file already exists in GCS
        if blob.exists():
            # Download the existing CSV data and read it into a DataFrame
            existing_csv_data = blob.download_as_text()
            existing_df = pd.read_csv(StringIO(existing_csv_data))
            # Merge and drop duplicates
            combined_df = pd.concat([existing_df, new_data_df]).drop_duplicates(subset=['Ticker', 'Date'], keep='last')
        else:
            combined_df = new_data_df
        
        # Convert to CSV and upload
        csv_data = combined_df.to_csv(index=False)
        blob.upload_from_string(csv_data, content_type='text/csv')
        logging.info(f"Uploaded {file_name} to {bucket_name}")

    except Exception as e:
        logging.error(f"Error uploading {file_name} to GCS: {str(e)}")
        raise e  # Optionally re-raise the exception
