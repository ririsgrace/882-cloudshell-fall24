# fred_collector.py
import pandas as pd
import requests
from datetime import datetime

def get_ffr_data(api_key, start_date=None, end_date=None):
    """Fetch Federal Funds Rate data from FRED"""
    base_url = "https://api.stlouisfed.org/fred/series/observations"
    
    params = {
        'series_id': 'DFF',  # Daily Federal Funds Rate
        'api_key': api_key,
        'file_type': 'json',
        'frequency': 'd',
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
        
        return df[['date', 'value']].rename(columns={'value': 'ffr'})
        
    except Exception as e:
        print(f"Error fetching FFR data: {e}")
        return None
