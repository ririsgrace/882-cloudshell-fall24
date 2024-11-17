import functions_framework
import os
import pandas as pd 
import joblib
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.model_selection import RandomizedSearchCV
from scipy.stats import randint
from gcsfs import GCSFileSystem
from google.cloud import storage

import io 
import matplotlib.pyplot as plt

project_id = 'ba882-rgk'
project_region = 'us-central1'

@functions_framework.http
def tuning_task(request):
    "Fit the model using a cloud function"

    GCS_PATH = "gs://rgk-ba882-fall24-finance/stocks-data/aapl_stocks.csv"
    
    # get the dataset
    df = pd.read_csv(GCS_PATH)
    df['Date'] = pd.to_datetime(df['Date'])
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
    features_train = features[-10:]
    features_test = features[-10:]

    target_train = target[-10:]
    target_test = target[-10:]

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

    # Step 5: Hyperparameter Tuning with RandomizedSearchCV
    param_distributions = {
        'regressor__n_estimators': randint(10, 200),    # Randomly select number of trees in range 50-200
        'regressor__max_depth': randint(1, 30),       # Different maximum depths
        'regressor__min_samples_split': randint(2, 30),           # Minimum samples required to split an internal node
        'regressor__min_samples_leaf': randint(1, 30),             # Minimum samples required to be at a leaf node
    }
    
    random_search = RandomizedSearchCV(
        pipeline, 
        param_distributions, 
        n_iter=50,                        # Number of random parameter combinations to try
        scoring='neg_mean_squared_error', # Use negative MSE as the scoring metric
        n_jobs=-1,                        # Use all available cores
        cv=3,                             # 3-fold cross-validation
        verbose=1, 
        random_state=42                   # Ensure reproducibility
    )
    random_search.fit(features_train, target_train)
    
    # Best parameters from RandomizedSearchCV
    best_params = random_search.best_params_
    print(f"Best Parameters: {best_params}")

    # Step 6: Train the best model
    best_pipeline = random_search.best_estimator_
    best_predictions = best_pipeline.predict(features_test)
    best_mse = mean_squared_error(target_test, best_predictions)
    best_r2 = r2_score(target_test, best_predictions)
    print(f"Tuned Model Mean Squared Error: {best_mse}")
    print(f"Tuned Model R² Score: {best_r2}")
    
    # Show predictions alongside actual values for the tuned model
    best_results = pd.DataFrame({
        'Date': apple['Date'].iloc[-10:].dt.date,
        'Actual': target_test,
        'Predicted': best_predictions
    })
    print(best_results.head())

    # write this file to gcs
    GCS_BUCKET = "rgk-ba882-fall24-finance"
    GCS_PATH = "models/"
    FNAME = "model-1-tuned.joblib"
    GCS = f"gs://{GCS_BUCKET}/{GCS_PATH}{FNAME}"

    # Use GCSFileSystem to open a file in GCS
    with GCSFileSystem().open(GCS, 'wb') as f:
        joblib.dump(pipeline, f)
  
    plt.figure(figsize=(14,7))
    plt.plot(best_results['Date'], best_results['Actual'], label='Actual')
    plt.plot(best_results['Date'], best_results['Predicted'], label='Predicted')
    plt.title('Actual vs Tuned Predicted Close Prices')
    plt.xticks(rotation = 15)
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
    blob = bucket.blob('plots/my_tuned_plot.png')  # Specify the path in GCS
    blob.upload_from_file(img, content_type='image/png')

    print("Plot uploaded to GCS!")

    # Return evaluation metrics
    return {
        'mse': best_mse,
        'r2': best_r2,
        "model_path": GCS
    }
