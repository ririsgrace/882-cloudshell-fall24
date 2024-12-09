import os
from google.cloud import secretmanager
import duckdb
import pandas as pd
import yfinance as yf
import functions_framework
import json
from fredapi import Fred
import numpy as np
import requests
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import logging
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

project_id = 'ba882-rgk'
secret_id = 'secret2_duckdb'
version_id = 'latest'
db = 'stocks_10'
schema = 'stocks_schema_10'
db_schema = f"{db}.{schema}"

COMPANY_MAPPING = {
    'AAPL': 'Apple',
    'MSFT': 'Microsoft',
    'GOOGL': 'Google',
    'AMZN': 'Amazon',
    'NVDA': 'NVIDIA',
    'TSM': 'Taiwan Semiconductor',
    'WMT': 'Walmart',
    'XOM': 'Exxon Mobil',
    'TSLA': 'Tesla',
    'AVGO': 'Broadcom'
}

def round_stock_prices(df):
    """Round stock price columns to 2 decimal places"""
    price_columns = ['Open', 'High', 'Low', 'Close', 'Adj Close']
    for col in price_columns:
        if col in df.columns:
            df[col] = df[col].round(2)
    return df

def clean_gdp_data(df):
    """Clean and round GDP data, replacing NaN with None"""
    # Round GDP values
    if 'GDP' in df.columns:
        df['GDP'] = df['GDP'].round(3)
    if 'Real_GDP' in df.columns:
        df['Real_GDP'] = df['Real_GDP'].round(3)
    if 'GDP_Growth' in df.columns:
        df['GDP_Growth'] = df['GDP_Growth'].round(4)
    
    # Replace NaN with None
    return df.replace({np.nan: None})

def get_full_article_content(url):
    """Fetch the full article content from the URL"""
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')
    content = soup.get_text()
    return content

def get_company_news(ticker):
    """Get news for a company with error handling and rate limit handling"""
    API_KEY = '2e25611b157040a29cfe947f54f9a297'
    company_name = COMPANY_MAPPING.get(ticker, ticker)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)  # Fetch news from the last 24 hours
    query = f"({ticker} OR {company_name}) AND (stock OR shares OR trading OR earnings OR market)"
    url = f'https://newsapi.org/v2/everything'
    params = {
        'q': query,
        'from': start_date.strftime('%Y-%m-%d'),
        'to': end_date.strftime('%Y-%m-%d'),
        'language': 'en',
        'sortBy': 'publishedAt',
        'pageSize': 6,  # Limit to 6 articles per day
        'apiKey': API_KEY
    }

    try:
        logging.info(f"Fetching news for ticker: {ticker}")
        logging.info(f"API URL: {url}")
        logging.info(f"API Params: {params}")
        response = requests.get(url, params=params)
        response.raise_for_status()
        logging.info(f"Response status code: {response.status_code}")
        data = response.json()

        if data['status'] == 'ok':
            articles = data['articles']
            logging.info(f"Number of articles found: {len(articles)}")
            cleaned_articles = []
            for article in articles:
                try:
                    article_content = get_full_article_content(article['url'])
                except Exception as e:
                    article_content = article['content']
                    logging.error(f"Exception fetching full content for {ticker}: {str(e)}")

                cleaned_article = {
                    'source': article['source']['name'],
                    'author': article.get('author'),
                    'title': article['title'],
                    'description': article['description'],
                    'content': article_content,
                    'url': article['url'],
                    'url_to_image': article.get('urlToImage'),
                    'published_at': article['publishedAt'],
                    'ticker': ticker
                }
                cleaned_articles.append(cleaned_article)
            logging.info(f"Number of cleaned articles: {len(cleaned_articles)}")
            return cleaned_articles
        else:
            logging.error(f"NewsAPI response status is not 'ok': {data['status']}")
            return []
    except requests.exceptions.HTTPError as e:
        if response.status_code == 429:  # Rate limit exceeded
            logging.error(f"Rate limit exceeded for {ticker}. Skipping news data extraction for this ticker.")
            return []
        else:
            logging.error(f"Exception fetching news for {ticker}: {str(e)}")
            return []
    except Exception as e:
        logging.error(f"Exception fetching news for {ticker}: {str(e)}")
        return []

@functions_framework.http
def extract_task(request):
    try:
        logging.info("Starting extraction task...")
        
        # Secret Manager connection
        try:
            sm = secretmanager.SecretManagerServiceClient()
            logging.info("Connected to Secret Manager")
        except Exception as e:
            logging.error(f"Secret Manager connection error: {str(e)}")
            raise

        # MotherDuck connection
        try:
            name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
            response = sm.access_secret_version(request={"name": name})
            md_token = response.payload.data.decode("UTF-8")
            md = duckdb.connect(f'md:?motherduck_token={md_token}')
            logging.info("Connected to MotherDuck")
        except Exception as e:
            logging.error(f"MotherDuck connection error: {str(e)}")
            raise

        # Stock data extraction
        stock_data = {}
        for ticker in COMPANY_MAPPING.keys():
            try:
                logging.info(f"Processing ticker: {ticker}")
                result = md.execute(f"SELECT MAX(Date) FROM {db_schema}.combined_stocks WHERE Ticker='{ticker}'").fetchone()
                last_date = result[0]
                logging.info(f"Last date for {ticker}: {last_date}")
                
                if last_date is None:
                    last_date = "2024-01-01"
                    logging.info(f"No existing data for {ticker}, using default date: {last_date}")
                
                logging.info(f"Fetching data for {ticker} from {last_date}")
                data = yf.download(ticker, start=last_date)
                logging.info(f"Retrieved {len(data)} rows for {ticker}")
                
                if not data.empty:
                    # Round the stock prices before any other processing
                    data = round_stock_prices(data)
                    # Ensure Volume is integer
                    if 'Volume' in data.columns:
                        data['Volume'] = data['Volume'].astype(int)
                    
                    data = data.reset_index()
                    data['Date'] = data['Date'].astype(str)
                    stock_data[ticker] = data.to_dict(orient='records')
                    logging.info(f"Successfully processed {ticker} data")
                else:
                    logging.info(f"No new data for {ticker}")
            except Exception as e:
                logging.error(f"Error processing {ticker}: {str(e)}")
                continue

        # News data extraction
        news_data = {}
        for ticker in COMPANY_MAPPING.keys():
            try:
                news_articles = get_company_news(ticker)
                news_data[ticker] = news_articles
                logging.info(f"Completed news fetching task. Articles fetched: {len(news_articles)}")

                # Print the content of fetched news articles
                for idx, article in enumerate(news_articles, start=1):
                    logging.info(f"Article {idx}:")
                    logging.info(f"Title: {article['title']}")
                    logging.info(f"URL: {article['url']}")
                    logging.info("Content:")
                    logging.info(article['content'] if article['content'] else "Content not available.")
                    logging.info("-" * 80)
            except Exception as e:
                logging.error(f"Error fetching news for {ticker}: {str(e)}")
                continue

        # GDP data extraction
        try:
            logging.info("Starting GDP extraction")
            fred = Fred(api_key='100232daa7f29e84ec5e3823f0195095')
            
            gdp_result = md.execute(f"SELECT MAX(Date) FROM {db_schema}.gdp_data").fetchone()
            gdp_last_date = gdp_result[0]
            logging.info(f"Last GDP date: {gdp_last_date}")
            
            if gdp_last_date is None:
                gdp_last_date = "2024-01-01"
                logging.info("No existing GDP data, using default date")
            
            gdp_data = fred.get_series('GDP', observation_start=gdp_last_date)
            real_gdp_growth = fred.get_series('GDPC1', observation_start=gdp_last_date)
            
            logging.info(f"Retrieved {len(gdp_data)} GDP records")
            
            if len(gdp_data) > 0:
                gdp_df = pd.DataFrame({
                    'GDP': gdp_data,
                    'Real_GDP': real_gdp_growth
                }).reset_index()
                
                gdp_df['Date'] = gdp_df['index'].dt.strftime('%Y-%m-%d')
                gdp_df = gdp_df.drop('index', axis=1)
                
                # Calculate GDP Growth
                gdp_df['GDP_Growth'] = gdp_df['GDP'].pct_change() * 100
                
                # Clean and round GDP data
                gdp_df = clean_gdp_data(gdp_df)
                
                logging.info("GDP data processed successfully")
            else:
                gdp_df = pd.DataFrame()
                logging.info("No new GDP data")
            
        except Exception as e:
            logging.error(f"GDP extraction error: {str(e)}")
            gdp_df = pd.DataFrame()

        # Convert to records, ensuring no NaN values in the output
        gdp_records = []
        if not gdp_df.empty:
            for record in gdp_df.to_dict(orient='records'):
                clean_record = {k: None if pd.isna(v) else v for k, v in record.items()}
                gdp_records.append(clean_record)

        response_data = {
            'stock_data': stock_data,
            'news_data': news_data,
            'gdp_data': gdp_records
        }

        logging.info(f"Final response data summary:")
        logging.info(f"Stock tickers with data: {list(stock_data.keys())}")
        logging.info(f"News tickers with data: {list(news_data.keys())}")
        logging.info(f"GDP records: {len(gdp_records)}")

        return json.dumps(response_data, default=str), 200, {'Content-Type': 'application/json'}

    except Exception as e:
        error_message = f"An error occurred in extract_task: {str(e)}"
        logging.error(error_message)
        import traceback
        logging.error(traceback.format_exc())
        return error_message, 500
        
# import os
# from google.cloud import secretmanager
# import duckdb
# import pandas as pd
# import yfinance as yf
# import functions_framework
# import json
# from fredapi import Fred
# import numpy as np
# import requests
# from datetime import datetime, timedelta
# import requests
# from bs4 import BeautifulSoup

# project_id = 'ba882-rgk'
# secret_id = 'secret2_duckdb'
# version_id = 'latest'
# db = 'stocks_10'
# schema = 'stocks_schema_10'
# db_schema = f"{db}.{schema}"

# # Define tickers with company names for news search
# COMPANY_MAPPING = {
#     'AAPL': 'Apple',
#     'MSFT': 'Microsoft',
#     'GOOGL': 'Google',
#     'AMZN': 'Amazon',
#     'NVDA': 'NVIDIA',
#     'TSM': 'Taiwan Semiconductor',
#     'WMT': 'Walmart',
#     'XOM': 'Exxon Mobil',
#     'TSLA': 'Tesla',
#     'AVGO': 'Broadcom'
# }

# def round_stock_prices(df):
#     """Round stock price columns to 2 decimal places"""
#     price_columns = ['Open', 'High', 'Low', 'Close', 'Adj Close']
#     for col in price_columns:
#         if col in df.columns:
#             df[col] = df[col].round(2)
#     return df

# def clean_gdp_data(df):
#     """Clean and round GDP data, replacing NaN with None"""
#     # Round GDP values
#     if 'GDP' in df.columns:
#         df['GDP'] = df['GDP'].round(3)
#     if 'Real_GDP' in df.columns:
#         df['Real_GDP'] = df['Real_GDP'].round(3)
#     if 'GDP_Growth' in df.columns:
#         df['GDP_Growth'] = df['GDP_Growth'].round(4)
    
#     # Replace NaN with None
#     return df.replace({np.nan: None})

# # def get_company_news(ticker):
# #     """Get news for a company with error handling"""
# #     API_KEY = '18166159c5e1401286065af31ed61fa8'
# #     company_name = COMPANY_MAPPING.get(ticker, ticker)
    
# #     end_date = datetime.now()
# #     start_date = end_date - timedelta(days=7)
    
# #     query = f"({ticker} OR {company_name}) AND (stock OR shares OR trading OR earnings OR market)"
    
# #     url = f'https://newsapi.org/v2/everything'
# #     params = {
# #         'q': query,
# #         'from': start_date.strftime('%Y-%m-%d'),
# #         'to': end_date.strftime('%Y-%m-%d'),
# #         'language': 'en',
# #         'sortBy': 'publishedAt',
# #         'apiKey': API_KEY
# #     }
    
# #     try:
# #         response = requests.get(url, params=params)
# #         response.raise_for_status()
        
# #         data = response.json()
# #         if data['status'] == 'ok':
# #             articles = data['articles']
# #             cleaned_articles = []
# #             for article in articles:
# #                 cleaned_article = {
# #                     'source': article['source']['name'],
# #                     'author': article.get('author'),
# #                     'title': article['title'],
# #                     'description': article['description'],
# #                     'content': article['content'],
# #                     'url': article['url'],
# #                     'url_to_image': article.get('urlToImage'),
# #                     'published_at': article['publishedAt'],
# #                     'ticker': ticker
# #                 }
# #                 cleaned_articles.append(cleaned_article)
# #             return cleaned_articles
# #         return []
# #     except Exception as e:
# #         print(f"Exception fetching news for {ticker}: {str(e)}")
# #         return []

# def get_full_article_content(url):
#     """Fetch the full article content from the URL"""
#     response = requests.get(url)
#     soup = BeautifulSoup(response.content, 'html.parser')
#     content = soup.get_text()
#     return content

# def get_company_news(ticker):
#     """Get news for a company with error handling"""
#     API_KEY = '18166159c5e1401286065af31ed61fa8'
#     company_name = COMPANY_MAPPING.get(ticker, ticker)
#     end_date = datetime.now()
#     start_date = end_date - timedelta(days=7)
#     query = f"({ticker} OR {company_name}) AND (stock OR shares OR trading OR earnings OR market)"
#     url = f'https://newsapi.org/v2/everything'
#     params = {
#         'q': query,
#         'from': start_date.strftime('%Y-%m-%d'),
#         'to': end_date.strftime('%Y-%m-%d'),
#         'language': 'en',
#         'sortBy': 'publishedAt',
#         'apiKey': API_KEY
#     }

#     try:
#         response = requests.get(url, params=params)
#         response.raise_for_status()
#         data = response.json()
#         if data['status'] == 'ok':
#             articles = data['articles']
#             cleaned_articles = []
#             for article in articles:
#                 try:
#                     article_content = get_full_article_content(article['url'])
#                 except Exception as e:
#                     article_content = article['content']
#                     print(f"Exception fetching full content for {ticker}: {str(e)}")

#                 cleaned_article = {
#                     'source': article['source']['name'],
#                     'author': article.get('author'),
#                     'title': article['title'],
#                     'description': article['description'],
#                     'content': article_content,
#                     'url': article['url'],
#                     'url_to_image': article.get('urlToImage'),
#                     'published_at': article['publishedAt'],
#                     'ticker': ticker
#                 }
#                 cleaned_articles.append(cleaned_article)
#             return cleaned_articles
#         return []
#     except Exception as e:
#         print(f"Exception fetching news for {ticker}: {str(e)}")
#         return []

# @functions_framework.http
# def extract_task(request):
#     try:
#         print("Starting extraction task...")
        
#         # Secret Manager connection
#         try:
#             sm = secretmanager.SecretManagerServiceClient()
#             print("Connected to Secret Manager")
#         except Exception as e:
#             print(f"Secret Manager connection error: {str(e)}")
#             raise

#         # MotherDuck connection
#         try:
#             name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
#             response = sm.access_secret_version(request={"name": name})
#             md_token = response.payload.data.decode("UTF-8")
#             md = duckdb.connect(f'md:?motherduck_token={md_token}')
#             print("Connected to MotherDuck")
#         except Exception as e:
#             print(f"MotherDuck connection error: {str(e)}")
#             raise

#         # Stock data extraction
#         stock_data = {}
#         for ticker in COMPANY_MAPPING.keys():
#             try:
#                 print(f"Processing ticker: {ticker}")
#                 result = md.execute(f"SELECT MAX(Date) FROM {db_schema}.combined_stocks WHERE Ticker='{ticker}'").fetchone()
#                 last_date = result[0]
#                 print(f"Last date for {ticker}: {last_date}")
                
#                 if last_date is None:
#                     last_date = "2024-12-01" 
#                     print(f"No existing data for {ticker}, using default date: {last_date}")
                
#                 print(f"Fetching data for {ticker} from {last_date}")
#                 data = yf.download(ticker, start=last_date)
#                 print(f"Retrieved {len(data)} rows for {ticker}")
                
#                 if not data.empty:
#                     # Round the stock prices before any other processing
#                     data = round_stock_prices(data)
#                     # Ensure Volume is integer
#                     if 'Volume' in data.columns:
#                         data['Volume'] = data['Volume'].astype(int)
                    
#                     data = data.reset_index()
#                     data['Date'] = data['Date'].astype(str)
#                     stock_data[ticker] = data.to_dict(orient='records')
#                     print(f"Successfully processed {ticker} data")
#                 else:
#                     print(f"No new data for {ticker}")
#             except Exception as e:
#                 print(f"Error processing {ticker}: {str(e)}")
#                 continue

#         # News data extraction
#         news_data = {}
#         for ticker in COMPANY_MAPPING.keys():
#             try:
#                 print(f"Fetching news for {ticker}")
#                 news = get_company_news(ticker)
#                 if news:
#                     news_data[ticker] = news
#                     print(f"Retrieved {len(news)} news articles for {ticker}")
#             except Exception as e:
#                 print(f"Error fetching news for {ticker}: {str(e)}")
#                 continue

#         # GDP data extraction
#         try:
#             print("Starting GDP extraction")
#             fred = Fred(api_key='100232daa7f29e84ec5e3823f0195095')
            
#             gdp_result = md.execute(f"SELECT MAX(Date) FROM {db_schema}.gdp_data").fetchone()
#             gdp_last_date = gdp_result[0]
#             print(f"Last GDP date: {gdp_last_date}")
            
#             if gdp_last_date is None:
#                 gdp_last_date = "2024-01-01"
#                 print("No existing GDP data, using default date")
            
#             gdp_data = fred.get_series('GDP', observation_start=gdp_last_date)
#             real_gdp_growth = fred.get_series('GDPC1', observation_start=gdp_last_date)
            
#             print(f"Retrieved {len(gdp_data)} GDP records")
            
#             if len(gdp_data) > 0:
#                 gdp_df = pd.DataFrame({
#                     'GDP': gdp_data,
#                     'Real_GDP': real_gdp_growth
#                 }).reset_index()
                
#                 gdp_df['Date'] = gdp_df['index'].dt.strftime('%Y-%m-%d')
#                 gdp_df = gdp_df.drop('index', axis=1)
                
#                 # Calculate GDP Growth
#                 gdp_df['GDP_Growth'] = gdp_df['GDP'].pct_change() * 100
                
#                 # Clean and round GDP data
#                 gdp_df = clean_gdp_data(gdp_df)
                
#                 print("GDP data processed successfully")
#             else:
#                 gdp_df = pd.DataFrame()
#                 print("No new GDP data")
            
#         except Exception as e:
#             print(f"GDP extraction error: {str(e)}")
#             gdp_df = pd.DataFrame()

#         # Convert to records, ensuring no NaN values in the output
#         gdp_records = []
#         if not gdp_df.empty:
#             for record in gdp_df.to_dict(orient='records'):
#                 clean_record = {k: None if pd.isna(v) else v for k, v in record.items()}
#                 gdp_records.append(clean_record)

#         response_data = {
#             'stock_data': stock_data,
#             'news_data': news_data,
#             'gdp_data': gdp_records
#         }

#         print(f"Final response data summary:")
#         print(f"Stock tickers with data: {list(stock_data.keys())}")
#         print(f"News tickers with data: {list(news_data.keys())}")
#         print(f"GDP records: {len(gdp_records)}")

#         return json.dumps(response_data, default=str), 200, {'Content-Type': 'application/json'}

#     except Exception as e:
#         error_message = f"An error occurred: {str(e)}"
#         print(error_message)
#         return error_message, 500

# @functions_framework.http
# def health_check(request):
#     return "OK", 200
# -------
# import os
# from google.cloud import secretmanager
# import duckdb
# import pandas as pd
# import yfinance as yf
# import functions_framework
# import json
# from fredapi import Fred
# import numpy as np

# project_id = 'ba882-rgk'
# secret_id = 'secret2_duckdb'
# version_id = 'latest'
# db = 'stocks_10'
# schema = 'stocks_schema_10'
# db_schema = f"{db}.{schema}"

# # Define tickers list
# tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'TSM', 'WMT', 'XOM', 'TSLA', 'AVGO']


# def round_stock_prices(df):
#     """Round stock price columns to 2 decimal places"""
#     price_columns = ['Open', 'High', 'Low', 'Close', 'Adj Close']
#     for col in price_columns:
#         if col in df.columns:
#             df[col] = df[col].round(2)
#     return df


# def clean_gdp_data(df):
#     """Clean and round GDP data, replacing NaN with None"""
#     # Round GDP values
#     if 'GDP' in df.columns:
#         df['GDP'] = df['GDP'].round(3)
#     if 'Real_GDP' in df.columns:
#         df['Real_GDP'] = df['Real_GDP'].round(3)
#     if 'GDP_Growth' in df.columns:
#         df['GDP_Growth'] = df['GDP_Growth'].round(4)
    
#     # Replace NaN with None
#     return df.replace({np.nan: None})


# @functions_framework.http
# def extract_task(request):
#     try:
#         print("Starting extraction task...")
        
#         # Secret Manager connection
#         try:
#             sm = secretmanager.SecretManagerServiceClient()
#             print("Connected to Secret Manager")
#         except Exception as e:
#             print(f"Secret Manager connection error: {str(e)}")
#             raise

#         # MotherDuck connection
#         try:
#             name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
#             response = sm.access_secret_version(request={"name": name})
#             md_token = response.payload.data.decode("UTF-8")
#             md = duckdb.connect(f'md:?motherduck_token={md_token}')
#             print("Connected to MotherDuck")
#         except Exception as e:
#             print(f"MotherDuck connection error: {str(e)}")
#             raise

#         # Stock data extraction
#         stock_data = {}
#         for ticker in tickers:
#             try:
#                 print(f"Processing ticker: {ticker}")
#                 result = md.execute(f"SELECT MAX(Date) FROM {db_schema}.combined_stocks WHERE Ticker='{ticker}'").fetchone()
#                 last_date = result[0]
#                 print(f"Last date for {ticker}: {last_date}")
                
#                 if last_date is None:
#                     last_date = "2024-01-01"
#                     print(f"No existing data for {ticker}, using default date: {last_date}")
                
#                 print(f"Fetching data for {ticker} from {last_date}")
#                 data = yf.download(ticker, start=last_date)
#                 print(f"Retrieved {len(data)} rows for {ticker}")
                
#                 if not data.empty:
#                     # Round the stock prices before any other processing
#                     data = round_stock_prices(data)
#                     # Ensure Volume is integer
#                     if 'Volume' in data.columns:
#                         data['Volume'] = data['Volume'].astype(int)
                    
#                     data = data.reset_index()
#                     data['Date'] = data['Date'].astype(str)
#                     stock_data[ticker] = data.to_dict(orient='records')
#                     print(f"Successfully processed {ticker} data")
#                 else:
#                     print(f"No new data for {ticker}")
#             except Exception as e:
#                 print(f"Error processing {ticker}: {str(e)}")
#                 continue

#         # GDP data extraction
#         try:
#             print("Starting GDP extraction")
#             fred = Fred(api_key='100232daa7f29e84ec5e3823f0195095')
            
#             # Get last GDP date
#             gdp_result = md.execute(f"SELECT MAX(Date) FROM {db_schema}.gdp_data").fetchone()
#             gdp_last_date = gdp_result[0]
#             print(f"Last GDP date: {gdp_last_date}")
            
#             if gdp_last_date is None:
#                 gdp_last_date = "2024-01-01"
#                 print("No existing GDP data, using default date")
            
#             gdp_data = fred.get_series('GDP', observation_start=gdp_last_date)
#             real_gdp_growth = fred.get_series('GDPC1', observation_start=gdp_last_date)
            
#             print(f"Retrieved {len(gdp_data)} GDP records")
            
#             if len(gdp_data) > 0:
#                 gdp_df = pd.DataFrame({
#                     'GDP': gdp_data,
#                     'Real_GDP': real_gdp_growth
#                 }).reset_index()
                
#                 gdp_df['Date'] = gdp_df['index'].dt.strftime('%Y-%m-%d')
#                 gdp_df = gdp_df.drop('index', axis=1)
                
#                 # Calculate GDP Growth
#                 gdp_df['GDP_Growth'] = gdp_df['GDP'].pct_change() * 100
                
#                 # Clean and round GDP data
#                 gdp_df = clean_gdp_data(gdp_df)
                
#                 print("GDP data processed successfully")
#             else:
#                 gdp_df = pd.DataFrame()
#                 print("No new GDP data")
            
#         except Exception as e:
#             print(f"GDP extraction error: {str(e)}")
#             gdp_df = pd.DataFrame()

#         # Convert to records, ensuring no NaN values in the output
#         gdp_records = []
#         if not gdp_df.empty:
#             for record in gdp_df.to_dict(orient='records'):
#                 clean_record = {k: None if pd.isna(v) else v for k, v in record.items()}
#                 gdp_records.append(clean_record)

#         response_data = {
#             'stock_data': stock_data,
#             'gdp_data': gdp_records
#         }

#         print(f"Final response data summary:")
#         print(f"Stock tickers with data: {list(stock_data.keys())}")
#         print(f"GDP records: {len(gdp_records)}")

#         # Convert to JSON with NaN handling
#         return json.dumps(response_data, default=str), 200, {'Content-Type': 'application/json'}

#     except Exception as e:
#         error_message = f"An error occurred: {str(e)}"
#         print(error_message)
#         return error_message, 500