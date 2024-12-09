# gdp_collector.py
import pandas as pd
import requests
from datetime import datetime

def get_gdp_data(api_key, start_date=None, end_date=None):
    """Fetch GDP data from FRED"""
    base_url = "https://api.stlouisfed.org/fred/series/observations"
    
    params = {
        'series_id': 'GDP',  # Gross Domestic Product
        'api_key': api_key,
        'file_type': 'json',
        'frequency': 'q',  # Quarterly
        'observation_start': start_date,
        'observation_end': end_date
    }
    
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        
        data = response.json()
        df = pd.DataFrame(data['observations'])
        
        df['date'] = pd.to_datetime(df['date'])
        df['value'] = pd.to_numeric(df['value'])
        
        # Convert quarterly data to daily
        daily_dates = pd.date_range(start=df['date'].min(), end=df['date'].max(), freq='D')
        daily_df = df.set_index('date').reindex(daily_dates).ffill()
        
        return daily_df.reset_index().rename(columns={
            'index': 'date',
            'value': 'gdp'
        })
        
    except Exception as e:
        print(f"Error fetching GDP data: {e}")
        return None
