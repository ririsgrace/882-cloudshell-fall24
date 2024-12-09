# functions/macro-analysis/test_local.py

from fred_collector import get_ffr_data
from gdp_collector import get_gdp_data
from datetime import datetime, timedelta
import pandas as pd

def test_macro_analysis():
    """Test macro analysis locally"""
    try:
        print("Starting macro analysis test...")
        
        # Your FRED API key
        api_key = '100232daa7f29e84ec5e3823f0195095'
        
        # Set date range (last 6 months)
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=180)).strftime('%Y-%m-%d')
        
        # Test FFR data collection
        print("\nFetching FFR data...")
        ffr_data = get_ffr_data(api_key, start_date, end_date)
        if ffr_data is not None:
            print("FFR data sample:")
            print(ffr_data.head())
        
        # Test GDP data collection
        print("\nFetching GDP data...")
        gdp_data = get_gdp_data(api_key, start_date, end_date)
        if gdp_data is not None:
            print("GDP data sample:")
            print(gdp_data.head())
        
        # Create combined features
        print("\nCreating combined features...")
        features = pd.DataFrame(index=pd.date_range(start=start_date, end=end_date, freq='D'))
        
        # Add FFR features
        if ffr_data is not None:
            ffr_series = ffr_data.set_index('date')['ffr']
            features['ffr_rate'] = ffr_series
            features['ffr_momentum'] = (ffr_series / ffr_series.shift(5) - 1) * 100
        
        # Add GDP features
        if gdp_data is not None:
            gdp_series = gdp_data.set_index('date')['gdp']
            features['gdp_level'] = gdp_series
            features['gdp_growth'] = gdp_series.pct_change() * 100
        
        # Fill missing values
        features = features.fillna(method='ffill').fillna(method='bfill')
        
        print("\nFinal features:")
        print(features.head())
        
        # Save to CSV for inspection
        output_file = 'macro_analysis_test.csv'
        features.to_csv(output_file)
        print(f"\nResults saved to {output_file}")
        
        return features
        
    except Exception as e:
        print(f"Error in test: {str(e)}")
        return None

if __name__ == "__main__":
    test_macro_analysis()