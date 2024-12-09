import functions_framework
import pandas as pd
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt
from transformers import BertTokenizer, BertForSequenceClassification
from transformers import pipeline
from google.cloud import storage, secretmanager  # Added secretmanager
from gcsfs import GCSFileSystem
import io
import duckdb  # Added duckdb

project_id = 'ba882-rgk'
project_region = 'us-central1'

@functions_framework.http
def sentiment_task(request):
    try:
        # GCS path to your data
        GCS_PATH = "gs://rgk-ba882-fall24-finance/stocks-data/stocks-data-10/news_data.csv"
        
        # Read data from GCS
        df = pd.read_csv(GCS_PATH)
        df['Date'] = pd.to_datetime(df['Date'])
        
        # Clean data
        news_df = df[
            df['content'].notna() &
            (df['content'].str.strip() != "") &
            (df['content'].str.len() > 100)
        ]
        news_df.reset_index(drop=True, inplace=True)
        
        # Initialize FinBERT
        tokenizer = BertTokenizer.from_pretrained('yiyanghkust/finbert-tone')
        finbert = BertForSequenceClassification.from_pretrained('yiyanghkust/finbert-tone', num_labels=3)
        sentiment_pipeline = pipeline('sentiment-analysis', model=finbert, tokenizer=tokenizer, truncation=True, max_length=512)
        
        def finbert_sentiment(text):
            result = sentiment_pipeline(text)
            return result[0]['label']
        
        # Add Sentiment column
        news_df['Sentiment'] = news_df['content'].apply(finbert_sentiment)
        
        # Save results to GCS
        GCS_BUCKET = "rgk-ba882-fall24-finance"
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save results CSV
        output_path = f'sentiment_analysis/results_{timestamp}.csv'
        with GCSFileSystem().open(f'gs://{GCS_BUCKET}/{output_path}', 'w') as f:
            news_df.to_csv(f, index=False)
        
        # # Create and save plot
        # plt.figure(figsize=(12, 6))
        # news_df['Sentiment'].value_counts().plot(kind='bar')
        # plt.title('Sentiment Distribution')
        
        # # Save plot to GCS
        # img = io.BytesIO()
        # plt.savefig(img, format='png')
        # img.seek(0)
        
        # client = storage.Client()
        # bucket = client.bucket(GCS_BUCKET)
        # blob = bucket.blob(f'sentiment_analysis/plot_{timestamp}.png')
        # blob.upload_from_file(img, content_type='image/png')

        # Add MotherDuck integration
        # Get MotherDuck token from Secret Manager
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
        
        # Create sentiment table if it doesn't exist
        md.execute(f"""
            CREATE TABLE IF NOT EXISTS {db_schema}.news_sentiment (
                Date DATE,
                ticker VARCHAR,
                content TEXT,
                Sentiment VARCHAR,
                analysis_timestamp TIMESTAMP
            );
        """)

        # Prepare data for MotherDuck
        news_df['analysis_timestamp'] = datetime.now()
        
        # Register DataFrame with DuckDB
        md.register('news_df', news_df)

        # Insert new sentiment results
        md.execute(f"""
            INSERT INTO {db_schema}.news_sentiment 
            SELECT 
                Date,
                ticker,
                content,
                Sentiment,
                analysis_timestamp
            FROM news_df;
        """)

        md.close()
        print("Results saved to both GCS and MotherDuck!")

        return {
            "status": "Success",
            "results_path": f'gs://{GCS_BUCKET}/{output_path}',
            "plot_path": f'gs://{GCS_BUCKET}/sentiment_analysis/plot_{timestamp}.png',
            "message": "Results saved to both GCS and MotherDuck successfully"
        }
        
    except Exception as e:
        return {
            "status": "Error",
            "message": str(e)
        }, 500