# ##################################################### imports

import functions_framework
import os
import pandas as pd 
import joblib
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error
from gcsfs import GCSFileSystem
from google.cloud import storage

import io 
import matplotlib.pyplot as plt

project_id = 'ba882-rgk'
project_region = 'us-central1'

@functions_framework.http
def model1_task(request):
    "Fit the model using a cloud function"

    GCS_PATH = "gs://rgk-ba882-fall24-finance/stocks-data/aapl_stocks.csv"
    
    # get the dataset
    df = pd.read_csv(GCS_PATH)
    print(df.head())
    
    # Debug: Check the shape of the dataframe
    print(f"Shape of the dataframe: {df.shape}")
    print(f"First few rows of the dataframe: {df.head()}")

    # Dropping rows with missing values
    apple = df.dropna()

    # Selecting features (Open, High, Low, Volume of all tickers) and target (Adj Close)
    features = apple.drop(['Adj_Close', 'Ticker', 'Date'], axis=1)  # Remove Adj Close from features
    target = apple[('Adj_Close')]  # Target is Adj_Close

    # Step 3: Train-Test Split
    features_train = features[:203]
    features_test = features[203:]

    target_train = target[:203]
    target_test = target[203:]

    print(f"Shape of features_train: {features_train.shape}")
    print(f"Shape of features_test: {features_test.shape}")
    print(f"Shape of target_train: {target_train.shape}")
    print(f"Shape of target_test: {target_test.shape}")

    print("Features test shape:", features_test.shape)
    if features_test.shape[0] == 0:
        raise ValueError("The input data is empty.")

    # Step 4: Create a Pipeline with StandardScaler and RandomForestRegressor
    pipeline = Pipeline([
        ('scaler', StandardScaler()),             # Scale features
        ('regressor', RandomForestRegressor())    # Random Forest Regressor
    ])

    # Step 5: Train the Model
    pipeline.fit(features_train, target_train)
    
    # Step 6: Evaluate the Model
    predictions = pipeline.predict(features_test)
    mse = mean_squared_error(target_test, predictions)
    print(f"Mean Squared Error: {mse}")

    # (Optional) Show predictions alongside actual values
    results = pd.DataFrame({'Actual': target_test, 'Predicted': predictions})
    print(results.head())

    # write this file to gcs
    GCS_BUCKET = "rgk-ba882-fall24-finance"
    GCS_PATH = "models/"
    FNAME = "model-1.joblib"
    GCS = f"gs://{GCS_BUCKET}/{GCS_PATH}{FNAME}"

    # Use GCSFileSystem to open a file in GCS
    with GCSFileSystem().open(GCS, 'wb') as f:
        joblib.dump(pipeline, f)
  
    plt.figure(figsize=(14,7))
    plt.plot(results['Actual'], label='Actual')
    plt.plot(results['Predicted'], label='Predicted')
    plt.title('Actual vs Predicted Close Prices')
    plt.xlabel('Date')
    plt.ylabel('Close Price')
    plt.legend()
    plt.show()

    # Save plot to a BytesIO object
    img = io.BytesIO()
    plt.savefig(img, format='png')
    img.seek(0)  # Go to the beginning of the file

    # Initialize GCS client and upload the image
    client = storage.Client()
    bucket = client.bucket('rgk-ba882-fall24-finance')  # Replace with your GCS bucket name
    blob = bucket.blob('plots/my_plot.png')  # Specify the path in GCS
    blob.upload_from_file(img, content_type='image/png')

    print("Plot uploaded to GCS!")

    # Return evaluation metrics
    return {
        'mse': mse,
        "model_path": GCS
    }

    # return return_data, 200

# # Imports
# import os
# import pandas as pd 
# import joblib
# from sklearn.model_selection import train_test_split
# from sklearn.pipeline import Pipeline
# from sklearn.ensemble import RandomForestRegressor
# from sklearn.preprocessing import StandardScaler
# from sklearn.metrics import mean_squared_error
# from gcsfs import GCSFileSystem
# from google.cloud import storage

# import io 
# import matplotlib.pyplot as plt

# # Set local file paths
# LOCAL_DATA_PATH = "gs://rgk-ba882-fall24-finance/aapl_stocks.csv"  # Ensure you have this file locally
# #MODEL_SAVE_PATH = "gs://rgk-ba882-fall24-finance/models/model-1.joblib"

# # Function to train the model
# def task():
#     "Fit the model using local data and save locally"

#     # Load the dataset locally
#     df = pd.read_csv(LOCAL_DATA_PATH)
#     print(df.head())
    
#     # Dropping rows with missing values
#     apple = df.dropna()

#     # Selecting features (Open, High, Low, Volume of all tickers) and target (Adj Close)
#     features = apple.drop(['Adj_Close', 'Ticker', 'Date'], axis=1)  # Remove Adj Close from features
#     target = apple['Adj_Close']  # Target is Adj_Close

#     # Train-Test Split (can adjust as needed for a different split)
#     features_train = features[:203]
#     features_test = features[203:]
#     target_train = target[:203]
#     target_test = target[203:]

#     # Create a Pipeline with StandardScaler and RandomForestRegressor
#     pipeline = Pipeline([
#         ('scaler', StandardScaler()),             # Scale features
#         ('regressor', RandomForestRegressor())    # Random Forest Regressor
#     ])

#     # Train the Model
#     pipeline.fit(features_train, target_train)

#     # Evaluate the Model
#     predictions = pipeline.predict(features_test)
#     mse = mean_squared_error(target_test, predictions)
#     print(f"Mean Squared Error: {mse}")

#     # Show predictions alongside actual values (optional)
#     results = pd.DataFrame({'Actual': target_test, 'Predicted': predictions})
#     print(results.head())

#     GCS_BUCKET = "rgk-ba882-fall24-finance"
#     GCS_PATH = "models/"
#     FNAME = "model-1.joblib"
#     GCS = f"gs://{GCS_BUCKET}/{GCS_PATH}{FNAME}"

#     # Use GCSFileSystem to open a file in GCS
#     gcs = GCSFileSystem()
#     with gcs.open(GCS, 'wb') as f:
#         joblib.dump(pipeline, f)

#     plt.figure(figsize=(14,7))
#     plt.plot(results['Actual'], label='Actual')
#     plt.plot(results['Predicted'], label='Predicted')
#     plt.title('Actual vs Predicted Close Prices')
#     plt.xlabel('Date')
#     plt.ylabel('Close Price')
#     plt.legend()
#     plt.show()

#     # Save plot to a BytesIO object
#     img = io.BytesIO()
#     plt.savefig(img, format='png')
#     img.seek(0)  # Go to the beginning of the file

#     # Initialize GCS client and upload the image
#     client = storage.Client()
#     bucket = client.bucket('rgk-ba882-fall24-finance')  # Replace with your GCS bucket name
#     blob = bucket.blob('plots/my_plot.png')  # Specify the path in GCS
#     blob.upload_from_file(img, content_type='image/png')

#     print("Plot uploaded to GCS!")

#     # Return evaluation metrics
#     return {
#         'mse': mse,
#         "model_path": GCS
#     }

# # Run the function
# if __name__ == "__main__":
#     results = task()
#     print("Training complete. Results:", results)
