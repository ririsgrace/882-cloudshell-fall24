# The ETL job orchestrator for Docker-based Cloud Functions

# imports
import requests
import json
from prefect import flow, task

def invoke_docker_function(url: str, payload: dict):
    response = requests.post(url, json=payload)
    response.raise_for_status()
    
    # Try to decode the response as JSON
    try:
        return response.json()
    except requests.exceptions.JSONDecodeError:
        # If the response isn't JSON, return the plain text response
        return response.text

@task(retries=2)
def schema_task():
    """Setup the stock schema"""
    # Docker-based function endpoint URL for schema setup
    url = "https://schema-service-807843960855.us-central1.run.app/"  # Replace this if running elsewhere
    resp = invoke_docker_function(url, payload={})
    return resp

@task(retries=2)
def extract_task():
    """Extract stock data from Yahoo Finance"""
    # Docker-based function endpoint URL for extract
    url = "https://extract-service-807843960855.us-central1.run.app/"  # Replace this if running elsewhere
    resp = invoke_docker_function(url, payload={})
    return resp

@task(retries=2)
def transform_task(payload):
    """Transform the stock data"""
    # Docker-based function endpoint URL for transform
    url = "https://transform-service-807843960855.us-central1.run.app/"  # Replace this if running elsewhere
    resp = invoke_docker_function(url, payload=payload)
    return resp

@task(retries=2)
def load_task(payload):
    """Load the transformed stock data into the database"""
    # Docker-based function endpoint URL for load
    url = "https://load-service-807843960855.us-central1.run.app/"  # Replace this if running elsewhere
    resp = invoke_docker_function(url, payload=payload)
    return resp

@task(retries=2)
def model1_task():
    """Create the model of stock data"""
    # Docker-based function endpoint URL for model creation
    url = "https://model1-service-807843960855.us-central1.run.app/"  # Replace this if running elsewhere
    payload = {}  # Define the payload (empty dictionary for example, but you may need to add data here)
    resp = invoke_docker_function(url, payload)  # Pass the payload as argument
    return resp

@task(retries=2)
def tuning_task():
    """Perform hyperparameter tuning on the model of stock data"""
    # Docker-based function endpoint URL for model creation
    url = "https://model1-service-807843960855.us-central1.run.app/"  # Replace this if running elsewhere
    payload = {}  # Define the payload (empty dictionary for example, but you may need to add data here)
    resp = invoke_docker_function(url, payload)  # Pass the payload as argument
    return resp

# Prefect Flow
@flow(name="stock-etl-flow", log_prints=True)
def etl_flow():
    """The ETL flow which orchestrates Docker-hosted Cloud Functions for stock data"""

    result = schema_task()
    print("The schema setup completed")

    extract_result = extract_task()
    print("The stock data were extracted")
    print(f"{extract_result}")

    transform_result = transform_task(extract_result)
    print("The transformation of the stock data completed")
    print(f"{transform_result}")

    result = load_task(transform_result)
    print("The stock data were loaded into the database")

    result = model1_task()
    print("Model was created and stored in the bucket")

    tuning_result = tuning_task()
    print("Model was tuned and stored in the bucket")

# the job
if __name__ == "__main__":
    etl_flow()
