import os
from google.cloud import secretmanager
import duckdb
import pandas as pd
import json
import functions_framework
import requests
import traceback
import math
import numpy as np

project_id = 'ba882-rgk'
secret_id = 'secret2_duckdb'
version_id = 'latest'
db = 'stocks_10'
schema = 'stocks_schema_10'
db_schema = f"{db}.{schema}"

def clean_numeric_value(x):
    """Clean numeric values for JSON serialization"""
    if pd.isna(x) or x is None:
        return None
    if isinstance(x, (int, float)):
        if math.isnan(x) or math.isinf(x):
            return None
        if isinstance(x, float):
            return round(x, 4)
        return x
    return None

def clean_news_data(articles):
    """Clean news data and ensure valid values"""
    cleaned = []
    for article in articles:
        clean_article = {
            'ticker': article['ticker'],
            'source': article['source'],
            'author': article.get('author'),
            'title': article['title'],
            'description': article.get('description'),
            'content': article.get('content'),
            'url': article['url'],
            'url_to_image': article.get('url_to_image'),
            'published_at': article['published_at']
        }
        cleaned.append(clean_article)
    return cleaned

@functions_framework.http
def transform_task(request):
    try:
        # Verify content type
        if request.content_type != 'application/json':
            return f"Unsupported Media Type: Content-Type must be 'application/json'", 415

        # Parse request data
        data = request.get_json(silent=True)
        if data is None:
            return "Bad Request: Invalid or missing JSON body", 400
        
        transformed_data = {
            'stock_data': [],
            'news_data': [],
            'gdp_data': []
        }

        # Transform Stock Data
        if 'stock_data' in data:
            stock_data = data.get('stock_data', {})
            
            for ticker, ticker_data in stock_data.items():
                if not ticker_data:
                    continue
                    
                try:
                    df = pd.DataFrame(ticker_data)
                    
                    # Rename and add columns
                    df = df.rename(columns={"Adj Close": "Adj_Close"})
                    df['Ticker'] = ticker
                    
                    # Handle dates
                    df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
                    
                    # Clean price columns
                    price_columns = ['Open', 'High', 'Low', 'Close', 'Adj_Close']
                    for col in price_columns:
                        if col in df.columns:
                            df[col] = df[col].apply(clean_numeric_value)
                    
                    # Handle Volume
                    if 'Volume' in df.columns:
                        df['Volume'] = df['Volume'].apply(lambda x: int(x) if pd.notna(x) else None)

                    if not df.empty:
                        records = df.to_dict(orient='records')
                        transformed_data['stock_data'].extend(records)
                    
                except Exception as e:
                    print(f"Error processing {ticker}: {str(e)}")
                    continue

        # Transform News Data
        if 'news_data' in data:
            news_data = data.get('news_data', {})
            for ticker, articles in news_data.items():
                try:
                    cleaned_articles = clean_news_data(articles)
                    transformed_data['news_data'].extend(cleaned_articles)
                except Exception as e:
                    print(f"Error processing news for {ticker}: {str(e)}")

        # Transform GDP Data
        if 'gdp_data' in data:
            gdp_data = data.get('gdp_data', [])
            
            try:
                if gdp_data:
                    gdp_df = pd.DataFrame(gdp_data)
                    
                    # Handle dates
                    gdp_df['Date'] = pd.to_datetime(gdp_df['Date']).dt.strftime('%Y-%m-%d')
                    
                    # Clean numeric columns
                    numeric_cols = ['GDP', 'Real_GDP', 'GDP_Growth']
                    for col in numeric_cols:
                        if col in gdp_df.columns:
                            gdp_df[col] = gdp_df[col].apply(clean_numeric_value)
                    
                    # Convert to records and ensure no NaN values
                    records = []
                    for record in gdp_df.to_dict(orient='records'):
                        clean_record = {
                            k: None if pd.isna(v) else v 
                            for k, v in record.items()
                        }
                        records.append(clean_record)
                    
                    transformed_data['gdp_data'] = records
            
            except Exception as e:
                print(f"Error processing GDP data: {str(e)}")

        # Final verification step
        try:
            result = json.dumps(transformed_data)
            return result, 200, {'Content-Type': 'application/json'}
        except ValueError as e:
            print(f"JSON serialization error: {e}")
            return "Error: Could not serialize data", 500

    except Exception as e:
        error_detail = traceback.format_exc()
        error_message = f"Transform error: {str(e)}\nTraceback: {error_detail}"
        print(error_message)
        return {"error": str(e), "traceback": error_detail}, 500

# @functions_framework.http
# def health_check(request):
#     return "OK", 200
        
# import os
# from google.cloud import secretmanager
# import duckdb
# import pandas as pd
# import json
# import functions_framework
# import requests
# import traceback
# import math
# import numpy as np

# project_id = 'ba882-rgk'
# secret_id = 'secret2_duckdb'
# version_id = 'latest'
# db = 'stocks_10'
# schema = 'stocks_schema_10'
# db_schema = f"{db}.{schema}"


# def process_numeric(data):
#     """Process input data and round all numeric values"""
#     if isinstance(data, dict):
#         return {key: process_numeric(value) for key, value in data.items()}
#     elif isinstance(data, list):
#         return [process_numeric(item) for item in data]
#     elif isinstance(data, float):
#         if math.isnan(data) or math.isinf(data):
#             return None
#         return round(data, 2)  # Round all floats to 2 decimal places
#     elif isinstance(data, int):
#         return data
#     return data


# @functions_framework.http
# def transform_task(request):
#     try:
#         # Verify content type
#         if request.content_type != 'application/json':
#             return f"Unsupported Media Type: Content-Type must be 'application/json'", 415

#         # Parse request data
#         data = request.get_json(silent=True)
#         if data is None:
#             return "Bad Request: Invalid or missing JSON body", 400
        
#         # Round numbers in the input data first
#         data = process_numeric(data)
        
#         transformed_data = {
#             'stock_data': [],
#             'gdp_data': []
#         }

#         # Transform Stock Data
#         if 'stock_data' in data:
#             stock_data = data.get('stock_data', {})
            
#             for ticker, ticker_data in stock_data.items():
#                 if not ticker_data:
#                     continue
                    
#                 try:
#                     df = pd.DataFrame(ticker_data)
                    
#                     # Rename and add columns
#                     df = df.rename(columns={"Adj Close": "Adj_Close"})
#                     df['Ticker'] = ticker
                    
#                     # Handle dates
#                     df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
                    
#                     # Convert Volume to integer
#                     if 'Volume' in df.columns:
#                         df['Volume'] = df['Volume'].apply(lambda x: int(x) if pd.notna(x) else None)

#                     if not df.empty:
#                         records = df.to_dict(orient='records')
#                         transformed_data['stock_data'].extend(records)
                    
#                 except Exception as e:
#                     print(f"Error processing {ticker}: {str(e)}")
#                     continue

#         # Transform GDP Data
#         if 'gdp_data' in data:
#             gdp_data = data.get('gdp_data', [])
            
#             try:
#                 if gdp_data:
#                     gdp_df = pd.DataFrame(gdp_data)
                    
#                     # Handle dates
#                     gdp_df['Date'] = pd.to_datetime(gdp_df['Date']).dt.strftime('%Y-%m-%d')
                    
#                     # Convert NaN to None
#                     gdp_df = gdp_df.replace({np.nan: None})
                    
#                     transformed_data['gdp_data'] = gdp_df.to_dict(orient='records')
            
#             except Exception as e:
#                 print(f"Error processing GDP data: {str(e)}")

#         # Final verification step
#         try:
#             result = json.dumps(transformed_data)
#             return result, 200, {'Content-Type': 'application/json'}
#         except ValueError as e:
#             print(f"JSON serialization error: {e}")
#             return "Error: Could not serialize data", 500

#     except Exception as e:
#         error_message = f"Transform error: {str(e)}"
#         print(error_message)
#         return {"error": str(e)}, 500


# @functions_framework.http
# def health_check(request):
#     return "OK", 200


# @functions_framework.http
# def transform_task(request):
#     try:
#         # Verify content type
#         print(f"Received Content-Type: {request.content_type}")
#         if request.content_type != 'application/json':
#             print("Unsupported Media Type")
#             return f"Unsupported Media Type: Content-Type must be 'application/json'", 415

#         # Parse the request JSON body
#         input_data = request.get_json(silent=True)
#         if input_data is None:
#             print("Invalid or missing JSON body")
#             return "Bad Request: Invalid or missing JSON body", 400

#         # Initialize transformed data structure
#         transformed_data = {
#             'stock_data': [],
#             'gdp_data': []
#         }

#         # Transform Stock Data
#         if 'stock_data' in input_data:
#             stock_data = input_data['stock_data']
#             stock_transformed = []

#             for ticker, data in stock_data.items():
#                 if data:
#                     try:
#                         df = pd.DataFrame(data)
#                         df = df.rename(columns={"Adj Close": "Adj_Close"})
#                         df['Ticker'] = ticker

#                         # Handle date conversion
#                         if isinstance(df['Date'].iloc[0], str):
#                             df['Date'] = pd.to_datetime(df['Date'], errors='coerce').dt.date
#                         else:
#                             df['Date'] = pd.to_datetime(df['Date'], unit='ms', errors='coerce').dt.date

#                         # Filter invalid dates
#                         df = df[df['Date'].notnull() & (df['Date'] > pd.Timestamp('1970-01-01').date())]

#                         if not df.empty:
#                             stock_transformed.append(df)
#                     except Exception as e:
#                         print(f"Error processing stock data for ticker {ticker}: {str(e)}")

#             if stock_transformed:
#                 transformed_data['stock_data'] = pd.concat(stock_transformed).to_dict(orient='records')

#         # Transform GDP Data
#         if 'gdp_data' in input_data:
#             gdp_data = input_data['gdp_data']
#             try:
#                 gdp_df = pd.DataFrame(gdp_data)
                
#                 # Convert dates for GDP data
#                 gdp_df['Date'] = pd.to_datetime(gdp_df['Date'], errors='coerce').dt.date
                
#                 # Filter invalid dates
#                 gdp_df = gdp_df[gdp_df['Date'].notnull() & (gdp_df['Date'] > pd.Timestamp('1970-01-01').date())]
                
#                 if not gdp_df.empty:
#                     transformed_data['gdp_data'] = gdp_df.to_dict(orient='records')
#             except Exception as e:
#                 print(f"Error processing GDP data: {str(e)}")

#         # Verify we have some transformed data
#         if not transformed_data['stock_data'] and not transformed_data['gdp_data']:
#             print("No valid data after transformation")
#             return "No valid data received", 400

#         print(f"Transformed data sample - Stocks: {transformed_data['stock_data'][:2] if transformed_data['stock_data'] else 'None'}")
#         print(f"Transformed data sample - GDP: {transformed_data['gdp_data'][:2] if transformed_data['gdp_data'] else 'None'}")

#         # Return transformed data
#         return json.dumps(transformed_data), 200, {'Content-Type': 'application/json'}

#     except Exception as e:
#         error_message = f"An error occurred during transformation: {str(e)}"
#         print(error_message)
#         return error_message, 500
        
# import os
# from google.cloud import secretmanager
# import duckdb
# import pandas as pd
# import json
# import functions_framework
# import requests

# project_id = 'ba882-rgk'
# secret_id = 'secret2_duckdb'
# version_id = 'latest'

# db = 'stocks_10'
# schema = 'stocks_schema_10'
# db_schema = f"{db}.{schema}"

# # Health check endpoint
# @functions_framework.http
# def health_check(request):
#     return "OK", 200

# # Function to invoke Docker-based service
# def invoke_docker_function(url, payload):
#     headers = {
#         'Content-Type': 'application/json'  # Ensure correct Content-Type
#     }

#     try:
#         # Log the payload for debugging
#         print(f"Payload to Docker service: {payload}")

#         response = requests.post(url, json=payload, headers=headers)

#         # Raise an error for bad HTTP responses (4xx or 5xx)
#         response.raise_for_status()

#         return response
#     except requests.exceptions.HTTPError as http_err:
#         print(f"HTTP error occurred: {http_err}")
#         print(f"Response content: {response.content}")  # Log the response content for debugging
#         raise
#     except Exception as err:
#         print(f"Other error occurred: {err}")
#         raise

# @functions_framework.http
# def transform_task(request):
#     try:
#         # Log the content type for debugging
#         print(f"Received Content-Type: {request.content_type}")

#         # Ensure the request content type is 'application/json'
#         if request.content_type != 'application/json':
#             print("Unsupported Media Type")
#             return f"Unsupported Media Type: Content-Type must be 'application/json', but received '{request.content_type}'", 415

#         # Parse the request JSON body
#         stock_data = request.get_json(silent=True)
#         if stock_data is None:
#             print(f"Invalid or missing JSON body for content-type: {request.content_type}")
#             return "Bad Request: Invalid or missing JSON body", 400

#         print(f"Received stock data: {stock_data}")  # Log the received data for debugging

#         # Initialize list to store transformed data
#         transformed_data = []

#         # Iterate over each ticker's data
#         for ticker, data in stock_data.items():
#             if data:  # Check if there is data to process
#                 try:
#                     df = pd.DataFrame(data)  # Convert to DataFrame
#                     df = df.rename(columns={"Adj Close": "Adj_Close"})  # Rename columns for consistency
#                     df['Ticker'] = ticker  # Add Ticker column

#                     # Check if the 'Date' is already in string format or in milliseconds
#                     if isinstance(df['Date'].iloc[0], str):
#                         # If it's already a string, convert it to datetime without specifying 'unit'
#                         df['Date'] = pd.to_datetime(df['Date'], errors='coerce').dt.date
#                     else:
#                         # If it's in milliseconds, convert it with the 'ms' unit
#                         df['Date'] = pd.to_datetime(df['Date'], unit='ms', errors='coerce').dt.date

#                     # Filter out invalid or null dates
#                     df = df[df['Date'].notnull() & (df['Date'] > pd.Timestamp('1970-01-01').date())]

#                     if not df.empty:
#                         transformed_data.append(df)
#                     else:
#                         print(f"No valid data found for ticker: {ticker}")
#                 except Exception as e:
#                     print(f"Error processing data for ticker {ticker}: {str(e)}")
#             else:
#                 print(f"No data for ticker: {ticker}")

#         # Combine all transformed data into a single DataFrame
#         if transformed_data:
#             combined_data = pd.concat(transformed_data)
#         else:
#             print("No valid stock data after transformation")
#             return "No valid stock data received", 400

#         # Log the transformed data for debugging
#         print(f"Transformed data (sample): {combined_data.head()}")

#         # Convert the combined DataFrame to JSON
#         result = combined_data.to_json(orient="records", date_format="iso")  # Ensure date is in ISO format

#         # Return the transformed data as JSON with the appropriate content type
#         return result, 200, {'Content-Type': 'application/json'}

#     except Exception as e:
#         error_message = f"An error occurred while transforming stock data: {str(e)}"
#         print(error_message)
#         return error_message, 500
