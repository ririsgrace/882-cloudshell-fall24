# ---update Nov 12 ---
import functions_framework
import os
import pandas as pd 
import joblib
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error, r2_score
from gcsfs import GCSFileSystem
from google.cloud import storage, secretmanager
from datetime import datetime  
import io 
import matplotlib.pyplot as plt
import duckdb
import logging
import numpy as np

project_id = 'ba882-rgk'
project_region = 'us-central1'

@functions_framework.http
def model1_task(request):
    "Fit the model using a cloud function"
    try:
        GCS_PATH = "gs://rgk-ba882-fall24-finance/stocks-data/aapl_stocks.csv"
        
        # get the dataset
        df = pd.read_csv(GCS_PATH)
        df['Date'] = pd.to_datetime(df['Date'])
        
        # Dropping rows with missing values
        apple = df.dropna()

        # Selecting features and target
        features = apple.drop(['Adj_Close', 'Ticker', 'Date'], axis=1)
        target = apple[('Adj_Close')]

        # Use last 10 days for testing and next day prediction
        features_train = features[:-10]  # All data except last 10 days
        features_test = features[-10:]   # Last 10 days
        target_train = target[:-10]
        target_test = target[-10:]

        if features_test.shape[0] == 0:
            raise ValueError("The input data is empty.")

        # Create Pipeline
        pipeline = Pipeline([
            ('scaler', StandardScaler()),
            ('regressor', RandomForestRegressor(random_state=42))
        ])

        # Train the Model
        pipeline.fit(features_train, target_train)
        
        # Make predictions
        predictions = pipeline.predict(features_test)
        
        # Calculate metrics
        overall_mse = mean_squared_error(target_test, predictions)
        rmse = np.sqrt(overall_mse)
        roe = target_test - predictions
        mape = np.mean(np.abs((target_test - predictions) / target_test)) * 100

        # Create results DataFrame
        results = pd.DataFrame({
            'Date': apple['Date'].iloc[-10:].dt.date,
            'Actual': target_test,
            'Predicted': predictions,
            'roe': roe,
            'prediction_timestamp': datetime.now(),
            'model_version': '1.0.0',
            'overall_mse': overall_mse,
            'rmse': rmse,
            'mape': mape,
            'ticker': 'AAPL'
        })

        # Save model to GCS
        GCS_BUCKET = "rgk-ba882-fall24-finance"
        model_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        GCS_PATH = f"models/"
        FNAME = f"model-1_{model_timestamp}.joblib"
        GCS = f"gs://{GCS_BUCKET}/{GCS_PATH}{FNAME}"

        with GCSFileSystem().open(GCS, 'wb') as f:
            joblib.dump(pipeline, f)
      
        # Create enhanced plot
        plt.figure(figsize=(15,10))
        
        # Plot predictions
        plt.subplot(2,1,1)
        plt.plot(results['Date'], results['Actual'], 'b-', label='Actual', marker='o')
        plt.plot(results['Date'], results['Predicted'], 'r--', label='Predicted', marker='x')
        plt.title('Stock Price Prediction Analysis')
        plt.xlabel('Date')
        plt.ylabel('Price')
        plt.legend()
        plt.grid(True)
        
        # Plot errors
        plt.subplot(2,1,2)
        plt.bar(results['Date'], results['roe'], color='g', alpha=0.6, label='ROE')
        plt.axhline(y=0, color='r', linestyle='-', alpha=0.3)
        plt.title('Prediction Errors')
        plt.xlabel('Date')
        plt.ylabel('Error (Actual - Predicted)')
        plt.legend()
        plt.grid(True)
        
        plt.tight_layout()

        # Save plot with timestamp
        img = io.BytesIO()
        plt.savefig(img, format='png', dpi=300, bbox_inches='tight')
        img.seek(0)

        client = storage.Client()
        bucket = client.bucket('rgk-ba882-fall24-finance')
        blob = bucket.blob(f'plots/prediction_plot_{model_timestamp}.png')
        blob.upload_from_file(img, content_type='image/png')

        # Save predictions to GCS
        prediction_path = f'predictions/predicted_stock_{model_timestamp}.csv'
        with GCSFileSystem().open(f'gs://{GCS_BUCKET}/{prediction_path}', 'w') as f:
            results.to_csv(f, index=False)

        # Save to MotherDuck
        sm = secretmanager.SecretManagerServiceClient()
        secret_id = 'secret2_duckdb'
        version_id = 'latest'
        db = 'stocks'
        schema = 'stocks_schema'
        db_schema = f"{db}.{schema}"

        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = sm.access_secret_version(request={"name": name})
        md_token = response.payload.data.decode("UTF-8")

        # Connect to MotherDuck
        md = duckdb.connect(f'md:?motherduck_token={md_token}')
        
        # Create predictions table
        md.execute(f"""
            CREATE TABLE IF NOT EXISTS {db_schema}.predicted_stock (
                Date DATE,
                Actual FLOAT,
                Predicted FLOAT,
                roe FLOAT,
                prediction_timestamp TIMESTAMP,
                model_version VARCHAR,
                overall_mse FLOAT,
                rmse FLOAT, 
                mape FLOAT,
                ticker VARCHAR
            );
        """)

        # Register DataFrame with DuckDB
        md.register('results_df', results)

        # Insert new predictions
        md.execute(f"""
            INSERT INTO {db_schema}.predicted_stock 
            SELECT 
                Date, 
                Actual, 
                Predicted,
                roe, 
                prediction_timestamp,
                model_version,
                overall_mse,
                rmse,
                mape,
                ticker
            FROM results_df;
        """)

        md.close()
        print("Predictions saved to both GCS and MotherDuck!")

        return {
            'overall_mse': float(overall_mse),
            'rmse': float(rmse), 
            'mape': float(mape),
            "model_path": GCS,
            "prediction_path": f'gs://{GCS_BUCKET}/{prediction_path}',
            "status": "Success",
            "message": "Predictions saved successfully"
        }

    except Exception as e:
        logging.error(f"Error in model execution: {str(e)}")
        return {
            "status": "Error",
            "message": str(e)
        }, 500

#-- old version --
# import functions_framework
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
# from datetime import datetime  

# import io 
# import matplotlib.pyplot as plt
# import duckdb

# project_id = 'ba882-rgk'
# project_region = 'us-central1'

# @functions_framework.http
# def model1_task(request):
#     "Fit the model using a cloud function"

#     GCS_PATH = "gs://rgk-ba882-fall24-finance/stocks-data/aapl_stocks.csv"
    
#     # get the dataset
#     df = pd.read_csv(GCS_PATH)
#     df['Date'] = pd.to_datetime(df['Date'])
#     print(df.head())
    
#     # Debug: Check the shape of the dataframe
#     print(f"Shape of the dataframe: {df.shape}")
#     print(f"First few rows of the dataframe: {df.head()}")

#     # Dropping rows with missing values
#     apple = df.dropna()

#     # Selecting features (Open, High, Low, Volume of all tickers) and target (Adj Close)
#     features = apple.drop(['Adj_Close', 'Ticker', 'Date'], axis=1)  # Remove Adj Close from features
#     target = apple[('Adj_Close')]  # Target is Adj_Close

<<<<<<< HEAD
#     # Step 3: Train-Test Split
#     features_train = features[-10:]
#     features_test = features[-10:]

#     target_train = target[-10:]
#     target_test = target[-10:]
=======
    # Step 3: Train-Test Split
    features_train = features[-10:]
    features_test = features[-10:]

    target_train = target[-10:]
    target_test = target[-10:]
>>>>>>> 445dd31fa0442d832fccba899edead7a093d6445

#     print(f"Shape of features_train: {features_train.shape}")
#     print(f"Shape of features_test: {features_test.shape}")
#     print(f"Shape of target_train: {target_train.shape}")
#     print(f"Shape of target_test: {target_test.shape}")

#     print("Features test shape:", features_test.shape)
#     if features_test.shape[0] == 0:
#         raise ValueError("The input data is empty.")

#     # Step 4: Create a Pipeline with StandardScaler and RandomForestRegressor
#     pipeline = Pipeline([
#         ('scaler', StandardScaler()),             # Scale features
#         ('regressor', RandomForestRegressor())    # Random Forest Regressor
#     ])

#     # Step 5: Train the Model
#     pipeline.fit(features_train, target_train)
    
    # Step 6: Evaluate the Model
    predictions = pipeline.predict(features_test)
    mse = mean_squared_error(target_test, predictions)
    r2 = r2_score(target_test, predictions)
    print(f"Mean Squared Error: {mse}")
    print(f"R² Score: {r2}")

    # (Optional) Show predictions alongside actual values
    results = pd.DataFrame({'Date': apple['Date'].iloc[-10:].dt.date, 'Actual': target_test, 'Predicted': predictions})
    print(results.head())

#     # write this file to gcs
#     GCS_BUCKET = "rgk-ba882-fall24-finance"
#     GCS_PATH = "models/"
#     FNAME = "model-1.joblib"
#     GCS = f"gs://{GCS_BUCKET}/{GCS_PATH}{FNAME}"

#     # Use GCSFileSystem to open a file in GCS
#     with GCSFileSystem().open(GCS, 'wb') as f:
#         joblib.dump(pipeline, f)
  
    plt.figure(figsize=(14,7))
    plt.plot(results['Date'], results['Actual'], label='Actual')
    plt.plot(results['Date'], results['Predicted'], label='Predicted')
    plt.title('Actual vs Predicted Close Prices')
    plt.xticks(rotation = 15)
    plt.xlabel('Date')
    plt.ylabel('Close Price')
    plt.legend()
    plt.show()

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

#     # Save the result to the new table ont he bucket
#     # Add timestamp to results
#     results['prediction_timestamp'] = datetime.now()
#     # Save to GCS bucket
#     bucket_name = 'rgk-ba882-fall24-finance'
#     prediction_path = f'predictions/predicted_stock_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    
#     # Save to GCS
#     with GCSFileSystem().open(f'gs://{bucket_name}/{prediction_path}', 'w') as f:
#         results.to_csv(f, index=False)
    
#     # Save to motherduck
    
#     print("Predictions saved to both GCS and MotherDuck!")
    

#     # Return evaluation metrics
#     return {
#         'mse': mse,
#         "model_path": GCS
#     }

# --oldest version --
# import functions_framework
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

# project_id = 'ba882-rgk'
# project_region = 'us-central1'

# @functions_framework.http
# def model1_task(request):
#     "Fit the model using a cloud function"

#     GCS_PATH = "gs://rgk-ba882-fall24-finance/stocks-data/aapl_stocks.csv"
    
#     # get the dataset
#     df = pd.read_csv(GCS_PATH)
#     print(df.head())
    
#     # Debug: Check the shape of the dataframe
#     print(f"Shape of the dataframe: {df.shape}")
#     print(f"First few rows of the dataframe: {df.head()}")

#     # Dropping rows with missing values
#     apple = df.dropna()

#     # Selecting features (Open, High, Low, Volume of all tickers) and target (Adj Close)
#     features = apple.drop(['Adj_Close', 'Ticker', 'Date'], axis=1)  # Remove Adj Close from features
#     target = apple[('Adj_Close')]  # Target is Adj_Close

#     # Step 3: Train-Test Split
#     features_new = features[:250] #select 250 row of data only
#     features_train = features_new[:203]
#     features_test = features_new[203:]

#     target_train = target[:203]
#     target_test = target[203:]

#     print(f"Shape of features_train: {features_train.shape}")
#     print(f"Shape of features_test: {features_test.shape}")
#     print(f"Shape of target_train: {target_train.shape}")
#     print(f"Shape of target_test: {target_test.shape}")

#     print("Features test shape:", features_test.shape)
#     if features_test.shape[0] == 0:
#         raise ValueError("The input data is empty.")

#     # Step 4: Create a Pipeline with StandardScaler and RandomForestRegressor
#     pipeline = Pipeline([
#         ('scaler', StandardScaler()),             # Scale features
#         ('regressor', RandomForestRegressor())    # Random Forest Regressor
#     ])

#     # Step 5: Train the Model
#     pipeline.fit(features_train, target_train)
    
#     # Step 6: Evaluate the Model
#     predictions = pipeline.predict(features_test)
#     mse = mean_squared_error(target_test, predictions)
#     print(f"Mean Squared Error: {mse}")

#     # (Optional) Show predictions alongside actual values
#     results = pd.DataFrame({'Actual': target_test, 'Predicted': predictions})
#     print(results.head())

#     # write this file to gcs
#     GCS_BUCKET = "rgk-ba882-fall24-finance"
#     GCS_PATH = "models/"
#     FNAME = "model-1.joblib"
#     GCS = f"gs://{GCS_BUCKET}/{GCS_PATH}{FNAME}"

#     # Use GCSFileSystem to open a file in GCS
#     with GCSFileSystem().open(GCS, 'wb') as f:
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
    # Return evaluation metrics
    return {
        'mse': mse,
        'r2': r2,
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
