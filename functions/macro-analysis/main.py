# main.py
import functions_framework
import pandas as pd
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt
from fred_collector import get_ffr_data
from gdp_collector import get_gdp_data

# main.py
# main.py

def analyze_macro_data():
    """Analyze historical FFR data"""
    try:
        print("Starting analysis...")
        api_key = '100232daa7f29e84ec5e3823f0195095'
        
        # Use historical dates
        start_date = '2023-01-01'  # Start from beginning of 2023
        end_date = '2023-12-01'    # Up to December 2023 (known historical data)
        
        print(f"Analyzing historical data from {start_date} to {end_date}")
        
        # Get FFR data
        print("Fetching FFR data...")
        ffr_data = get_ffr_data(api_key, start_date, end_date)
        if ffr_data is None:
            raise Exception("Failed to fetch FFR data")
        
        print(f"Received {len(ffr_data)} data points")
        
        # Create features DataFrame
        dates = pd.date_range(start=start_date, end=end_date, freq='D')
        features = pd.DataFrame(index=dates)
        
        # Process FFR data
        ffr_data.set_index('date', inplace=True)
        features['ffr_rate'] = ffr_data['ffr']
        features['ffr_rate'] = features['ffr_rate'].ffill()
        
        # Calculate changes
        features['ffr_1d_change'] = features['ffr_rate'].diff()
        features['ffr_5d_change'] = features['ffr_rate'].diff(5)
        features['ffr_20d_change'] = features['ffr_rate'].diff(20)
        
        # Moving averages
        features['ma5'] = features['ffr_rate'].rolling(window=5).mean()
        features['ma20'] = features['ffr_rate'].rolling(window=20).mean()
        
        # Regime detection
        features['rate_regime'] = np.where(
            features['ffr_5d_change'] > 0.01, 'Tightening',
            np.where(features['ffr_5d_change'] < -0.01, 'Easing', 'Neutral')
        )
        
        # Print Analysis
        print("\nRate Analysis Summary:")
        print("-" * 50)
        
        print("\nFFR Statistics:")
        print(f"Most Recent FFR Rate: {features['ffr_rate'].iloc[-1]:.2f}%")
        print(f"Average FFR Rate: {features['ffr_rate'].mean():.2f}%")
        print(f"FFR Rate Range: {features['ffr_rate'].min():.2f}% to {features['ffr_rate'].max():.2f}%")
        
        print("\nRecent Changes:")
        print(f"Latest 1-day change: {features['ffr_1d_change'].iloc[-1]:.3f}%")
        print(f"Latest 5-day change: {features['ffr_5d_change'].iloc[-1]:.3f}%")
        print(f"Latest 20-day change: {features['ffr_20d_change'].iloc[-1]:.3f}%")
        
        print("\nRate Regime Distribution:")
        print(features['rate_regime'].value_counts())
        
        print("\nLast 10 days of analysis:")
        last_10 = features.tail(10)
        print("\nDate\t\tFFR Rate\t1d Change\t5d Change\t20d Change\tRegime")
        print("-" * 90)
        for idx, row in last_10.iterrows():
            print(f"{idx.date()}\t{row['ffr_rate']:.2f}%\t"
                  f"{row['ffr_1d_change']:.3f}%\t"
                  f"{row['ffr_5d_change']:.3f}%\t"
                  f"{row['ffr_20d_change']:.3f}%\t"
                  f"{row['rate_regime']}")
        
        # Create visualization
        plt.figure(figsize=(15, 10))
        
        # Plot FFR Rate and Moving Averages
        plt.subplot(2, 1, 1)
        plt.plot(features.index, features['ffr_rate'], 'b-', label='FFR Rate', linewidth=2)
        plt.plot(features.index, features['ma5'], 'r--', label='5-day MA')
        plt.plot(features.index, features['ma20'], 'g--', label='20-day MA')
        plt.title('Federal Funds Rate Trends')
        plt.legend()
        plt.grid(True)
        
        # Plot Rate Changes
        plt.subplot(2, 1, 2)
        plt.plot(features.index, features['ffr_5d_change'], 'b-', label='5-day Change')
        plt.plot(features.index, features['ffr_20d_change'], 'r-', label='20-day Change')
        plt.axhline(y=0, color='k', linestyle='--', alpha=0.3)
        plt.title('Rate Changes Over Time')
        plt.legend()
        plt.grid(True)
        
        plt.tight_layout()
        plt.savefig('ffr_analysis.png')
        print("\nPlot saved as 'ffr_analysis.png'")
        
        # Save data
        features.to_csv('ffr_analysis.csv')
        print("Results saved to 'ffr_analysis.csv'")
        
        return features
        
    except Exception as e:
        print(f"Error in analysis: {str(e)}")
        return None

if __name__ == "__main__":
    analyze_macro_data()