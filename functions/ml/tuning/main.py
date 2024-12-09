# tuning_task.py
import functions_framework
import os
import pandas as pd 
import joblib
from sklearn.model_selection import train_test_split, RandomizedSearchCV
from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error, r2_score
from scipy.stats import randint
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
def tuning_task(request):
    "Fit the model using a cloud function with hyperparameter tuning"
    try:
        GCS_PATH = "gs://rgk-ba882-fall24-finance/stocks-data/aapl_stocks.csv"
        
        # Get the dataset
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

        # Create Pipeline with hyperparameter tuning
        pipeline = Pipeline([
            ('scaler', StandardScaler()),
            ('regressor', RandomForestRegressor())
        ])

        # Define hyperparameter space
        param_distributions = {
            'regressor__n_estimators': randint(10, 200),
            'regressor__max_depth': randint(1, 30),
            'regressor__min_samples_split': randint(2, 30),
            'regressor__min_samples_leaf': randint(1, 30),
        }
        
        # Random search for hyperparameter tuning
        random_search = RandomizedSearchCV(
            pipeline, 
            param_distributions, 
            n_iter=50,
            scoring='neg_mean_squared_error',
            n_jobs=-1,
            cv=3,
            verbose=1, 
            random_state=42
        )
        
        print("Starting hyperparameter tuning...")
        random_search.fit(features_train, target_train)
        
        # Get best parameters and model
        best_params = random_search.best_params_
        print(f"Best Parameters: {best_params}")
        
        # Make predictions with best model
        best_pipeline = random_search.best_estimator_
        predictions = best_pipeline.predict(features_test)
        
        # Calculate metrics
        overall_mse = mean_squared_error(target_test, predictions)
        rmse = np.sqrt(overall_mse)
        r2 = r2_score(target_test, predictions)
        roe = target_test - predictions
        mape = np.mean(np.abs((target_test - predictions) / target_test)) * 100

        # Create results DataFrame
        results = pd.DataFrame({
            'Date': apple['Date'].iloc[-10:].dt.date,
            'Actual': target_test,
            'Predicted': predictions,
            'roe': roe,
            'prediction_timestamp': datetime.now(),
            'model_version': '1.0.0-tuned',
            'overall_mse': overall_mse,
            'rmse': rmse,
            'r2': r2,
            'mape': mape,
            'best_params': str(best_params),
            'ticker': 'AAPL'
        })

        # Save model to GCS
        GCS_BUCKET = "rgk-ba882-fall24-finance"
        model_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        GCS_PATH = f"models/"
        FNAME = f"model-1-tuned_{model_timestamp}.joblib"
        GCS = f"gs://{GCS_BUCKET}/{GCS_PATH}{FNAME}"

        with GCSFileSystem().open(GCS, 'wb') as f:
            joblib.dump(best_pipeline, f)
      
        # Create enhanced plot
        plt.figure(figsize=(15,10))
        
        # Plot predictions
        plt.subplot(2,1,1)
        plt.plot(results['Date'], results['Actual'], 'b-', label='Actual', marker='o')
        plt.plot(results['Date'], results['Predicted'], 'r--', label='Tuned Prediction', marker='x')
        plt.title('Stock Price Prediction Analysis (Tuned Model)')
        plt.xlabel('Date')
        plt.ylabel('Price')
        plt.legend()
        plt.grid(True)
        
        # Plot errors
        plt.subplot(2,1,2)
        plt.bar(results['Date'], results['roe'], color='g', alpha=0.6, label='ROE')
        plt.axhline(y=0, color='r', linestyle='-', alpha=0.3)
        plt.title('Prediction Errors (Tuned Model)')
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
        bucket = client.bucket(GCS_BUCKET)
        blob = bucket.blob(f'plots/tuned_prediction_plot_{model_timestamp}.png')
        blob.upload_from_file(img, content_type='image/png')

        # Save predictions to GCS
        prediction_path = f'predictions/tuned_stock_{model_timestamp}.csv'
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
            CREATE TABLE IF NOT EXISTS {db_schema}.tuned_predictions (
                Date DATE,
                Actual FLOAT,
                Predicted FLOAT,
                roe FLOAT,
                prediction_timestamp TIMESTAMP,
                model_version VARCHAR,
                overall_mse FLOAT,
                rmse FLOAT,
                r2 FLOAT,
                mape FLOAT,
                best_params TEXT,
                ticker VARCHAR
            );
        """)

        # Register DataFrame with DuckDB
        md.register('results_df', results)

        # Insert new predictions
        md.execute(f"""
            INSERT INTO {db_schema}.tuned_predictions 
            SELECT 
                Date, 
                Actual, 
                Predicted,
                roe, 
                prediction_timestamp,
                model_version,
                overall_mse,
                rmse,
                r2,
                mape,
                best_params,
                ticker
            FROM results_df;
        """)

        md.close()
        print("Tuned predictions saved to both GCS and MotherDuck!")

        return {
            'overall_mse': float(overall_mse),
            'rmse': float(rmse),
            'r2': float(r2),
            'mape': float(mape),
            'best_params': best_params,
            "model_path": GCS,
            "prediction_path": f'gs://{GCS_BUCKET}/{prediction_path}',
            "status": "Success",
            "message": "Tuned predictions saved successfully"
        }

    except Exception as e:
        logging.error(f"Error in model tuning execution: {str(e)}")
        return {
            "status": "Error",
            "message": str(e)
        }, 500