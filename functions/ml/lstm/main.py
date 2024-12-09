# ---update Nov 12 ---
import functions_framework
import os
import pandas as pd 
import joblib
from sklearn.preprocessing import MinMaxScaler, StandardScaler
from sklearn.model_selection import train_test_split
from gcsfs import GCSFileSystem
from google.cloud import storage, secretmanager
from datetime import datetime
import io 
import matplotlib.pyplot as plt
import duckdb
import logging
import numpy as np
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense

project_id = 'ba882-rgk'
project_region = 'us-central1'

@functions_framework.http
def lstm_task(request):
    "Fit the model using a cloud function"
    try:
        GCS_PATH = "gs://rgk-ba882-fall24-finance/stocks-data/combined_stocks.csv"
        
        # get the dataset
        df = pd.read_csv(GCS_PATH)
        df['Date'] = pd.to_datetime(df['Date'])
        
        # Dropping rows with missing values
        stocks = df.dropna()

        close_data = stocks.sort_values(by = ["Date", "Ticker"])[['Adj_Close']]

        # Normalize the data
        scaler = MinMaxScaler(feature_range=(0, 1))
        scaled_data = scaler.fit_transform(close_data)

        # Create sequences for LSTM
        def create_sequences(data, sequence_length):
            sequences = []
            labels = []
            for i in range(len(data) - sequence_length):
                sequences.append(data[i:i + sequence_length])
                labels.append(data[i + sequence_length])
            return np.array(sequences), np.array(labels)

        sequence_length = 7 # one week
        X, y = create_sequences(scaled_data, sequence_length)

        # Test, train split for last 30 days
        split = len(X) - 150
        X_train, X_test = X[:split], X[split:]
        y_train, y_test = y[:split], y[split:]

        # Split into validation set
        X_train, X_val, y_train, y_val = train_test_split(X_train, y_train, test_size=0.2, random_state=42)

        if X_test.shape[0] == 0:
            raise ValueError("The input data is empty.")

        # Create and fit LSTM model
        model = Sequential()
        model.add(LSTM(50, activation = "relu", input_shape=(sequence_length, 1)))
        model.add(Dense(1))
        model.compile(optimizer = 'adam', loss = 'mse')
        #model.summary()

        history = model.fit(X_train, y_train, epochs=20, validation_data=(X_val, y_val))

        loss = model.evaluate(X_test, y_test)
        print(f'Test Loss: {loss}')

        # Make predictions
        y_pred = model.predict(X_test)

        y_pred = scaler.inverse_transform(y_pred)
        y_test = scaler.inverse_transform(y_test)

        y_pred = y_pred.flatten()
        y_test = y_test.flatten()

        lstm_pred = stocks.sort_values(by = ["Date", "Ticker"])[['Date', 'Ticker']].iloc[-150:]

        lstm_pred['Predicted'] = y_pred
        lstm_pred['Actual'] = y_test
        lstm_pred['Pred_Error'] = lstm_pred['Actual'] - lstm_pred['Predicted']

        lstm_pred.sort_values(by = ["Ticker", "Date"], inplace = True)


        # Save model to GCS
        GCS_BUCKET = "rgk-ba882-fall24-finance"
        model_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        GCS_PATH = f"models/"
        FNAME = f"lstm_{model_timestamp}.joblib"
        GCS = f"gs://{GCS_BUCKET}/{GCS_PATH}{FNAME}"

        with GCSFileSystem().open(GCS, 'wb') as f:
            joblib.dump(model, f)

        # Create results DataFrame
        results = lstm_pred
        results['prediction_timestamp'] = datetime.now()
        results['model_version'] = '2.0.0'


        # Plot Predictions
        # Group the data by Ticker
        grouped = lstm_pred.groupby('Ticker')

        # Create subplots
        fig, axes = plt.subplots(nrows=5, ncols=1, figsize=(10, 15), sharex=True)
        fig.suptitle('Actual vs Predicted Prices for Stocks', fontsize=16)

        # Iterate through each group (each stock) and corresponding subplot
        for (ticker, group), ax in zip(grouped, axes):
            # Plot actual prices
            ax.plot(group['Date'], group['Actual'], label='Actual', color='blue')
            # Plot predicted prices
            ax.plot(group['Date'], group['Predicted'], label='Predicted', color='orange')
            
            # Customize the subplot
            ax.set_title(f'Stock: {ticker}', fontsize=12)
            ax.set_ylabel('Price')
            ax.legend(loc='upper left')

        # Adjust layout and show the plot
        plt.xlabel('Date')
        plt.tight_layout(rect=[0, 0, 1, 0.96])  # Leave space for the suptitle


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
            CREATE TABLE IF NOT EXISTS {db_schema}.lstm_stock (
                Date DATE,
                Ticker VARCHAR,
                Predicted FLOAT,
                Actual FLOAT,
                Pred_Error FLOAT,
                prediction_timestamp TIMESTAMP,
                model_version VARCHAR
            );
        """)

        # Register DataFrame with DuckDB
        md.register('results_df', results)

        # Insert new predictions
        md.execute(f"""
            INSERT INTO {db_schema}.lstm_stock 
            SELECT 
                Date,
                Ticker,
                Predicted,
                Actual,
                Pred_Error,
                prediction_timestamp,
                model_version
            FROM results_df;
        """)

        md.close()
        print("Predictions saved to both GCS and MotherDuck!")

        return {
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

