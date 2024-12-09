
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
db = 'stocks_10'
schema = 'stocks_schema_10'
db_schema = f"{db}.{schema}"
# db_news_schema = f"{db}.news_schema"  # Assuming a separate schema for news
bucket_name = 'rgk-ba882-fall24-finance'
path_name = 'stocks-data/stocks-data-10'

# Initialize Google Cloud Storage client and bucket
client = storage.Client(project=project_id)
bucket = client.bucket(bucket_name)

@functions_framework.http
def load_task(request):

    try:
        # Parse the request JSON body
        data = request.get_json()
        if not data:
            logging.error("Bad Request: No data received")
            return "Bad Request: No data received", 400

        # Access Secret Manager to get the MotherDuck token
        sm = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = sm.access_secret_version(request={"name": name})
        md_token = response.payload.data.decode("UTF-8")

        # Connect to MotherDuck
        md = duckdb.connect(f'md:?motherduck_token={md_token}')

        # Load Stock Data
        if 'stock_data' in data:
            stock_data = data['stock_data']
            if stock_data and isinstance(stock_data, list):
                stock_df = pd.DataFrame(stock_data)
                stock_df['Date'] = pd.to_datetime(stock_df['Date']).dt.date

                # Insert into combined_stocks table
                md.execute(f"""
                    INSERT INTO {db_schema}.combined_stocks 
                    (Date, Open, High, Low, Close, Adj_Close, Volume, Ticker)
                    SELECT 
                        Date, Open, High, Low, Close, Adj_Close, Volume, Ticker
                    FROM stock_df
                    WHERE NOT EXISTS (
                        SELECT 1 
                        FROM {db_schema}.combined_stocks existing
                        WHERE stock_df.Ticker = existing.Ticker
                        AND stock_df.Date = existing.Date
                    );
                """)

                # Insert into individual ticker tables
                for ticker in stock_df['Ticker'].unique():
                    ticker_data = stock_df[stock_df['Ticker'] == ticker]
                    ticker_table = f"{ticker.lower()}_stocks"
                    
                    md.execute(f"""
                        INSERT INTO {db_schema}.{ticker_table}
                        (Date, Open, High, Low, Close, Adj_Close, Volume, Ticker)
                        SELECT 
                            Date, Open, High, Low, Close, Adj_Close, Volume, Ticker
                        FROM ticker_data
                        WHERE NOT EXISTS (
                            SELECT 1 
                            FROM {db_schema}.{ticker_table} existing
                            WHERE ticker_data.Date = existing.Date
                        );
                    """)

                    # Upload individual ticker data to GCS
                    ticker_blob = bucket.blob(f"{path_name}/{ticker.lower()}_stocks.csv")
                    ticker_blob.upload_from_string(ticker_data.to_csv(index=False), 
                                                   content_type='text/csv')

        # Load GDP Data
        if 'gdp_data' in data:
            gdp_data = data['gdp_data']
            if gdp_data and isinstance(gdp_data, list):
                gdp_df = pd.DataFrame(gdp_data)
                gdp_df['Date'] = pd.to_datetime(gdp_df['Date']).dt.date

                # Insert GDP data
                md.execute(f"""
                    INSERT INTO {db_schema}.gdp_data 
                    (Date, GDP, Real_GDP, GDP_Growth)
                    SELECT 
                        Date, GDP, Real_GDP, GDP_Growth
                    FROM gdp_df
                    WHERE NOT EXISTS (
                        SELECT 1 
                        FROM {db_schema}.gdp_data existing
                        WHERE gdp_df.Date = existing.Date
                    );
                """)

                # Upload GDP Data to GCS
                gdp_blob = bucket.blob(f"{path_name}/gdp_data.csv")
                gdp_blob.upload_from_string(gdp_df.to_csv(index=False), 
                                            content_type='text/csv')

        # Load News Data into MotherDuck and upload to GCS
        if 'news_data' in data:
            news_data = data['news_data']
            if news_data and isinstance(news_data, list):
                # Iterate over news data
                for news_item in news_data:
                    ticker = news_item.get('ticker')  # Assuming ticker is part of each news item
                    source = news_item.get('source')  # Source could be a string or a dict

                    # Check if `source` is a dictionary and extract the name, or use the string directly
                    if isinstance(source, dict):
                        source_name = source.get('name', 'Unknown')  # Fallback to 'Unknown'
                    else:
                        source_name = source or 'Unknown'  # Use the string directly or 'Unknown'

                    author = news_item.get('author', 'Unknown')  # Default to 'Unknown' if not present
                    title = news_item.get('title', 'No Title')  # Default to 'No Title' if not present
                    description = news_item.get('description', '')  # Default to empty string
                    content = news_item.get('content', '')  # Default to empty string
                    url = news_item.get('url', '')  # News article URL
                    url_to_image = news_item.get('urlToImage', '')  # URL to image
                    published_at = news_item.get('publishedAt')  # Published timestamp
                    created_at = pd.Timestamp.now()  # Current timestamp as `created_at`

                    # Insert article into the stock_news table in MotherDuck
                    md.execute(f"""
                        INSERT INTO {db_schema}.stock_news
                        (ticker, source, author, title, description, content, url, url_to_image, published_at, created_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        ticker, source_name, author, title, description, content, url, 
                        url_to_image, published_at, created_at
                    ))

                # Upload news data to GCS
                news_df = pd.DataFrame(news_data)
                news_blob = bucket.blob(f"{path_name}/news_data.csv")
                news_blob.upload_from_string(news_df.to_csv(index=False), 
                                             content_type='text/csv')

        return "Data loaded successfully!", 200

    except Exception as e:
        logging.error(f"Error in load_task: {traceback.format_exc()}")
        return f"Error loading data: {str(e)}", 500

def upload_to_bucket(df, file_name):
    """Uploads or appends data to Google Cloud Storage."""
    client = storage.Client(project=project_id)
    bucket = client.bucket(bucket_name)
    blob_name = f'{path_name}/{file_name}'
    blob = bucket.blob(blob_name)
    
    try:
        if blob.exists():
            # Merge with existing data
            existing_data = blob.download_as_text()
            existing_df = pd.read_csv(StringIO(existing_data))
            combined_df = pd.concat([existing_df, df]).drop_duplicates()
        else:
            combined_df = df
        
        # Upload combined data
        blob.upload_from_string(combined_df.to_csv(index=False), 
                              content_type='text/csv')
        logging.info(f"Uploaded {file_name} to {bucket_name}")

    except Exception as e:
        logging.error(f"Error uploading {file_name}: {str(e)}")
        raise e

# -- works with data gdp only --
# import logging
# import traceback
# from google.cloud import secretmanager, storage
# import duckdb
# import pandas as pd
# from flask import jsonify
# from io import StringIO
# import functions_framework

# # Constants
# project_id = 'ba882-rgk'
# secret_id = 'secret2_duckdb'
# version_id = 'latest'
# db = 'stocks_10'
# schema = 'stocks_schema_10'
# db_schema = f"{db}.{schema}"
# bucket_name = 'rgk-ba882-fall24-finance'
# path_name = 'stocks-data/stocks-data-10'

# @functions_framework.http
# def load_task(request):
#     try:
#         # Parse the request JSON body
#         data = request.get_json()
#         print(data)
#         if not data:
#             logging.error("Bad Request: No data received")
#             return jsonify({"error": "Bad Request: No data received"}), 400

#         # Access Secret Manager
#         sm = secretmanager.SecretManagerServiceClient()
#         name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
#         response = sm.access_secret_version(request={"name": name})
#         md_token = response.payload.data.decode("UTF-8")

#         # Connect to MotherDuck
#         md = duckdb.connect(f'md:?motherduck_token={md_token}')

#         # Load Stock Data
#         if 'stock_data' in data:
#             stock_data = data['stock_data']
#             if stock_data and isinstance(stock_data, dict):
#                 for ticker, ticker_data in stock_data.items():
#                     stock_df = pd.DataFrame(ticker_data)
#                     stock_df['Date'] = pd.to_datetime(stock_df['Date']).dt.date

#                     # Replace NaN values with None
#                     stock_df = stock_df.where(pd.notnull(stock_df), None)

#                     # Insert into combined_stocks table
#                     for _, row in stock_df.iterrows():
#                         md.execute(
#                             f"""
#                             INSERT INTO {db_schema}.combined_stocks 
#                             (Date, Open, High, Low, Close, Adj_Close, Volume, Ticker)
#                             VALUES (?, ?, ?, ?, ?, ?, ?, ?)
#                             """,
#                             (row['Date'], row['Open'], row['High'], row['Low'],
#                              row['Close'], row['Adj_Close'], row['Volume'], ticker)
#                         )

#                     # Upload ticker-specific stock data to Google Cloud Storage
#                     upload_to_bucket(stock_df, f"{ticker}_stocks.csv", bucket_name)

#         # Load News Data
#         if 'news_data' in data:
#             news_data = data['news_data']
#             if news_data and isinstance(news_data, dict):
#                 for ticker, news_articles in news_data.items():
#                     news_df = pd.DataFrame(news_articles)
#                     news_df['published_at'] = pd.to_datetime(news_df['published_at'])

#                     # Insert news data
#                     for _, row in news_df.iterrows():
#                         md.execute(
#                             f"""
#                             INSERT INTO {db_schema}.stock_news 
#                             (ticker, source, author, title, description, content, 
#                              url, url_to_image, published_at)
#                             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
#                             """,
#                             (row['ticker'], row['source'], row['author'], row['title'],
#                              row['description'], row['content'], row['url'], 
#                              row['url_to_image'], row['published_at'])
#                         )

#                     # Upload ticker-specific news data to Google Cloud Storage
#                     upload_to_bucket(news_df, f"{ticker}_news.csv", bucket_name)

#         # Load GDP Data
#         if 'gdp_data' in data:
#             gdp_data = data['gdp_data']
#             if gdp_data and isinstance(gdp_data, list):
#                 gdp_df = pd.DataFrame(gdp_data)
#                 gdp_df['Date'] = pd.to_datetime(gdp_df['Date']).dt.date
#                 gdp_df = gdp_df.replace({pd.NA: None, pd.NaT: None})
#                 gdp_df = gdp_df.where(pd.notnull(gdp_df), None)

#                 # Insert GDP data
#                 for _, row in gdp_df.iterrows():
#                     md.execute(
#                         f"""
#                         INSERT INTO {db_schema}.gdp_data 
#                         (Date, GDP, Real_GDP, GDP_Growth)
#                         VALUES (?, ?, ?, ?)
#                         """,
#                         (row['Date'], row['GDP'], row['Real_GDP'], row['GDP_Growth'])
#                     )

#                 # Upload GDP data to Google Cloud Storage
#                 upload_to_bucket(gdp_df, 'gdp_data.csv', bucket_name)

#         # Return a JSON response
#         return jsonify({"message": "Data loaded successfully!"}), 200

#     except Exception as e:
#         logging.error(f"Error in load_task: {traceback.format_exc()}")
#         return jsonify({"error": f"Error loading data: {str(e)}"}), 500

# def upload_to_bucket(df, file_name, bucket_name):
#     """Uploads or appends data to Google Cloud Storage."""
#     client = storage.Client(project=project_id)
#     bucket = client.bucket(bucket_name)
#     blob_name = f'{path_name}/{file_name}'
#     blob = bucket.blob(blob_name)
    
#     try:
#         if blob.exists():
#             # Merge with existing data
#             existing_data = blob.download_as_text()
#             existing_df = pd.read_csv(StringIO(existing_data))
#             combined_df = pd.concat([existing_df, df]).drop_duplicates()
#         else:
#             combined_df = df
        
#         # Upload combined data
#         blob.upload_from_string(combined_df.to_csv(index=False), 
#                                 content_type='text/csv')
#         logging.info(f"Uploaded {file_name} to {bucket_name}")

#     except Exception as e:
#         logging.error(f"Error uploading {file_name}: {str(e)}")
#         raise e
# -- end --


# @functions_framework.http
# def load_task(request):
#     try:
#         # Parse the request JSON body
#         data = request.get_json()
#         if not data:
#             logging.error("Bad Request: No data received")
#             return "Bad Request: No data received", 400

#         # Access Secret Manager
#         sm = secretmanager.SecretManagerServiceClient()
#         name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
#         response = sm.access_secret_version(request={"name": name})
#         md_token = response.payload.data.decode("UTF-8")

#         # Connect to MotherDuck
#         md = duckdb.connect(f'md:?motherduck_token={md_token}')

#         # Load Stock Data
#         if 'stock_data' in data:
#             stock_data = data['stock_data']
#             if stock_data and isinstance(stock_data, list):
#                 stock_df = pd.DataFrame(stock_data)
#                 stock_df['Date'] = pd.to_datetime(stock_df['Date']).dt.date

#                 # Replace NaN values with None
#                 stock_df = stock_df.where(pd.notnull(stock_df), None)

#                 # Insert into combined_stocks table
#                 for _, row in stock_df.iterrows():
#                     md.execute(
#                         f"""
#                         INSERT INTO {db_schema}.combined_stocks 
#                         (Date, Open, High, Low, Close, Adj_Close, Volume, Ticker)
#                         VALUES (?, ?, ?, ?, ?, ?, ?, ?)
#                         """,
#                         (row['Date'], row['Open'], row['High'], row['Low'],
#                          row['Close'], row['Adj_Close'], row['Volume'], row['Ticker'])
#                     )

#         # Load News Data
#         if 'news_data' in data:
#             news_data = data['news_data']
#             if news_data and isinstance(news_data, list):
#                 news_df = pd.DataFrame(news_data)
#                 news_df['published_at'] = pd.to_datetime(news_df['published_at'])

#                 # Insert news data
#                 for _, row in news_df.iterrows():
#                     md.execute(
#                         f"""
#                         INSERT INTO {db_schema}.stock_news 
#                         (ticker, source, author, title, description, content, 
#                          url, url_to_image, published_at)
#                         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
#                         """,
#                         (row['ticker'], row['source'], row['author'], row['title'],
#                          row['description'], row['content'], row['url'], 
#                          row['url_to_image'], row['published_at'])
#                     )

#         # Load GDP Data
#         if 'gdp_data' in data:
#             gdp_data = data['gdp_data']
#             if gdp_data and isinstance(gdp_data, list):
#                 gdp_df = pd.DataFrame(gdp_data)
#                 gdp_df['Date'] = pd.to_datetime(gdp_df['Date']).dt.date
#                 gdp_df = gdp_df.replace({pd.NA: None, pd.NaT: None})
#                 gdp_df = gdp_df.where(pd.notnull(gdp_df), None)

#                 # Insert GDP data
#                 for _, row in gdp_df.iterrows():
#                     md.execute(
#                         f"""
#                         INSERT INTO {db_schema}.gdp_data 
#                         (Date, GDP, Real_GDP, GDP_Growth)
#                         VALUES (?, ?, ?, ?)
#                         """,
#                         (row['Date'], row['GDP'], row['Real_GDP'], row['GDP_Growth'])
#                     )

#         # Upload data to Google Cloud Storage
#         upload_to_bucket(stock_df, 'combined_stocks.csv', bucket_name)
#         upload_to_bucket(news_df, 'news_data.csv', bucket_name)
#         upload_to_bucket(gdp_df, 'gdp_data.csv', bucket_name)

#         return "Data loaded successfully!", 200

#     except Exception as e:
#         logging.error(f"Error in load_task: {traceback.format_exc()}")
#         return f"Error loading data: {str(e)}", 500


# import logging
# import traceback
# import os
# from google.cloud import secretmanager
# import duckdb
# import pandas as pd
# import functions_framework
# from google.cloud import storage
# from io import StringIO

# project_id = 'ba882-rgk'
# secret_id = 'secret2_duckdb'
# version_id = 'latest'
# db = 'stocks_10'
# schema = 'stocks_schema_10'
# db_schema = f"{db}.{schema}"
# bucket_name = 'rgk-ba882-fall24-finance'
# path_name = 'stocks-data/stocks-data-10'

# @functions_framework.http
# def load_task(request):
#     try:
#         # Parse the request JSON body
#         data = request.get_json()
#         if not data:
#             logging.error("Bad Request: No data received")
#             return "Bad Request: No data received", 400

#         # Access Secret Manager
#         sm = secretmanager.SecretManagerServiceClient()
#         name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
#         response = sm.access_secret_version(request={"name": name})
#         md_token = response.payload.data.decode("UTF-8")

#         # Connect to MotherDuck
#         md = duckdb.connect(f'md:?motherduck_token={md_token}')

#         # Load Stock Data
#         if 'stock_data' in data:
#             stock_data = data['stock_data']
#             if stock_data and isinstance(stock_data, list):
#                 stock_df = pd.DataFrame(stock_data)
#                 stock_df['Date'] = pd.to_datetime(stock_df['Date']).dt.date

#                 # Insert into combined_stocks table
#                 md.execute(f"""
#                     INSERT INTO {db_schema}.combined_stocks 
#                     (Date, Open, High, Low, Close, Adj_Close, Volume, Ticker)
#                     SELECT 
#                         Date, Open, High, Low, Close, Adj_Close, Volume, Ticker
#                     FROM stock_df
#                     WHERE NOT EXISTS (
#                         SELECT 1 
#                         FROM {db_schema}.combined_stocks existing
#                         WHERE stock_df.Ticker = existing.Ticker
#                         AND stock_df.Date = existing.Date
#                     );
#                 """)

#                 # Insert into individual ticker tables
#                 for ticker in stock_df['Ticker'].unique():
#                     ticker_data = stock_df[stock_df['Ticker'] == ticker]
#                     ticker_table = f"{ticker.lower()}_stocks"
                    
#                     md.execute(f"""
#                         INSERT INTO {db_schema}.{ticker_table}
#                         (Date, Open, High, Low, Close, Adj_Close, Volume, Ticker)
#                         SELECT 
#                             Date, Open, High, Low, Close, Adj_Close, Volume, Ticker
#                         FROM ticker_data
#                         WHERE NOT EXISTS (
#                             SELECT 1 
#                             FROM {db_schema}.{ticker_table} existing
#                             WHERE ticker_data.Date = existing.Date
#                         );
#                     """)

#         # Load GDP Data
#         if 'gdp_data' in data:
#             gdp_data = data['gdp_data']
#             if gdp_data and isinstance(gdp_data, list):
#                 gdp_df = pd.DataFrame(gdp_data)
#                 gdp_df['Date'] = pd.to_datetime(gdp_df['Date']).dt.date

#                 # Insert GDP data
#                 md.execute(f"""
#                     INSERT INTO {db_schema}.gdp_data 
#                     (Date, GDP, Real_GDP, GDP_Growth)
#                     SELECT 
#                         Date, GDP, Real_GDP, GDP_Growth
#                     FROM gdp_df
#                     WHERE NOT EXISTS (
#                         SELECT 1 
#                         FROM {db_schema}.gdp_data existing
#                         WHERE gdp_df.Date = existing.Date
#                     );
#                 """)

#         # Upload to GCS
#         client = storage.Client(project=project_id)
#         bucket = client.bucket(bucket_name)

#         # Upload stock data
#         if 'stock_data' in data:
#             stock_blob = bucket.blob(f"{path_name}/combined_stocks.csv")
#             stock_blob.upload_from_string(stock_df.to_csv(index=False), 
#                                         content_type='text/csv')

#         # Upload GDP data
#         if 'gdp_data' in data:
#             gdp_blob = bucket.blob(f"{path_name}/gdp_data.csv")
#             gdp_blob.upload_from_string(gdp_df.to_csv(index=False), 
#                                       content_type='text/csv')

#         return "Data loaded successfully!", 200

#     except Exception as e:
#         logging.error(f"Error in load_task: {traceback.format_exc()}")
#         return f"Error loading data: {str(e)}", 500

# def upload_to_bucket(df, file_name):
#     """Uploads or appends data to Google Cloud Storage."""
#     client = storage.Client(project=project_id)
#     bucket = client.bucket(bucket_name)
#     blob_name = f'{path_name}/{file_name}'
#     blob = bucket.blob(blob_name)
    
#     try:
#         if blob.exists():
#             # Merge with existing data
#             existing_data = blob.download_as_text()
#             existing_df = pd.read_csv(StringIO(existing_data))
#             combined_df = pd.concat([existing_df, df]).drop_duplicates()
#         else:
#             combined_df = df
        
#         # Upload combined data
#         blob.upload_from_string(combined_df.to_csv(index=False), 
#                               content_type='text/csv')
#         logging.info(f"Uploaded {file_name} to {bucket_name}")

#     except Exception as e:
#         logging.error(f"Error uploading {file_name}: {str(e)}")
#         raise e

