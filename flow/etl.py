# The ETL job orchestrator for Docker-based Cloud Functions

# imports
import requests
import json
from prefect import flow, task

def invoke_docker_function(url: str, payload: dict):
    # Add headers for JSON content
    headers = {
        'Content-Type': 'application/json'
    }
    
    # Make request with headers
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    
    # Try to decode the response as JSON
    try:
        return response.json()
    except requests.exceptions.JSONDecodeError:
        print(f"Response was not JSON: {response.text}")
        return response.text

# @task(retries=2)
# def schema_task():
#     """Setup the stock schema"""
#     # Docker-based function endpoint URL for schema setup
#     url = "https://schema-service-807843960855.us-central1.run.app/"  
#     resp = invoke_docker_function(url, payload={})
#     return resp

# @task(retries=2)
# def extract_task():
#     """Extract stock data from Yahoo Finance"""
#     # Docker-based function endpoint URL for extract
#     url = "https://extract-service-807843960855.us-central1.run.app/" 
#     resp = invoke_docker_function(url, payload={})
#     return resp

# @task(retries=2)
# def transform_task(payload):
#     """Transform the stock data"""
#     try:
#         url = "https://transform-service-807843960855.us-central1.run.app/"
#         print(f"Sending payload to transform service: {payload}")
#         resp = invoke_docker_function(url, payload=payload)
#         print(f"Transform response: {resp}")
#         return resp
#     except Exception as e:
#         print(f"Error in transform task: {str(e)}")
#         raise
    
# @task(retries=2)
# def load_task(payload):
#     """Load the transformed stock data into the database"""
#     # Docker-based function endpoint URL for load
#     url = "https://load-service-807843960855.us-central1.run.app/"  
#     resp = invoke_docker_function(url, payload=payload)
#     return resp

# @task(retries=2)
# def model1_task():
#     """Create the model of stock data"""
#     # Docker-based function endpoint URL for model creation
#     url = "https://model1-service-807843960855.us-central1.run.app/" 
#     payload = {}  
#     resp = invoke_docker_function(url, payload) 
#     return resp

@task(retries=2)
def sentiment_task():
    """Create the sentiment of the news"""
    # Docker-based function endpoint URL for model creation
    url = "https://sentiment-service-807843960855.us-central1.run.app/"  
    payload = {}  
    resp = invoke_docker_function(url, payload)  
    return resp

@task(retries=2)
def tuning_task():
    """Perform hyperparameter tuning on the model of stock data"""
    # Docker-based function endpoint URL for model creation
    url = "https://model1-service-807843960855.us-central1.run.app/"  # Replace this if running elsewhere
    payload = {}  # Define the payload (empty dictionary for example, but you may need to add data here)
    resp = invoke_docker_function(url, payload)  # Pass the payload as argument
    return resp

@task(retries=2)
def lstm_task():
    """Create LSTM model of stock data"""
    # Docker-based function endpoint URL for model creation
    url = "https://model1-service-807843960855.us-central1.run.app/"  # Replace this if running elsewhere
    payload = {}  # Define the payload (empty dictionary for example, but you may need to add data here)
    resp = invoke_docker_function(url, payload)  # Pass the payload as argument
    return resp

# Prefect Flow
@flow(name="stock-etl-flow", log_prints=True)
def etl_flow():
    """The ETL flow which orchestrates Docker-hosted Cloud Functions for stock data"""

    # result = schema_task()
    # print("The schema setup completed")

    # extract_result = extract_task()
    # print("The stock data were extracted")
    # print(f"{extract_result}")

    # transform_result = transform_task(extract_result)
    # print("The transformation of the stock data completed")
    # print(f"{transform_result}")

    # result = load_task(transform_result)
    # print("The stock data were loaded into the database")

    # result = model1_task()
    # print("Model was created and stored in the bucket")

    result = sentiment_task()
    print("Sentiment model was created and stored in the database")

    result = tuning_task()
    print("Model hyperparameter was completed and stored in the bucket")

    result = lstm_task()
    print("LSTM Model was created and stored in the bucket")

# the job
if __name__ == "__main__":
    etl_flow()
