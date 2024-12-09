# main.py
import functions_framework
import pandas as pd
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt
from fred_collector import get_ffr_data
from gdp_collector import get_gdp_data

def analyze_macro_data():
    """Analyze macroeconomic data from Dec 2023 to present"""
    try:
        print("Starting analysis...")
        api_key = '100232daa7f29e84ec5e3823f0195095'
        
        # Set date range from December 2023 to present
        start_date = '2023-12-01'
        end_date = '2024-12-08'  # Today
        
        print(f"Analyzing data from {start_date} to {end_date}")
        
        # Get data
        print("Fetching FFR data...")
        ffr_data = get_ffr_data(api_key, start_date, end_date)
        if ffr_data is None:
            raise Exception("Failed to fetch FFR data")
        
        print("Fetching GDP data...")
        gdp_data = get_gdp_data(api_key, start_date, end_date)
        if gdp_data is None:
            raise Exception("Failed to fetch GDP data")
        
        # Create features DataFrame
        print("Processing features...")
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
        
        # Process GDP data
        gdp_data.set_index('date', inplace=True)
        features['gdp_level'] = gdp_data['gdp']
        features['gdp_level'] = features['gdp_level'].ffill()
        features['gdp_qoq_growth'] = features['gdp_level'].pct_change(90) * 100
        
        # Determine rate regime
        features['rate_regime'] = np.where(
            (features['ffr_rate'] > features['ma20']) & (features['ffr_5d_change'] > 0),
            'Tightening',
            np.where(
                (features['ffr_rate'] < features['ma20']) & (features['ffr_5d_change'] < 0),
                'Easing',
                'Neutral'
            )
        )
        
        # Print Analysis
        print("\nAnalysis Results:")
        print("-" * 50)
        print(f"Total days analyzed: {len(features)}")
        
        print("\nFFR Statistics:")
        print(f"Current FFR Rate: {features['ffr_rate'].iloc[-1]:.2f}%")
        print(f"Average FFR Rate: {features['ffr_rate'].mean():.2f}%")
        print(f"FFR Rate Range: {features['ffr_rate'].min():.2f}% to {features['ffr_rate'].max():.2f}%")
        
        print("\nRate Changes:")
        print(f"Recent 1-day change: {features['ffr_1d_change'].iloc[-1]:.3f}%")
        print(f"Recent 5-day change: {features['ffr_5d_change'].iloc[-1]:.3f}%")
        print(f"Recent 20-day change: {features['ffr_20d_change'].iloc[-1]:.3f}%")
        
        print("\nRate Regime Distribution:")
        print(features['rate_regime'].value_counts())
        
        # Show the last 10 days of data
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
        
        # Plot FFR and Moving Averages
        plt.subplot(2, 1, 1)
        plt.plot(features.index, features['ffr_rate'], 'b-', label='FFR Rate')
        plt.plot(features.index, features['ma5'], 'r--', label='5-day MA')
        plt.plot(features.index, features['ma20'], 'g--', label='20-day MA')
        plt.title('Federal Funds Rate with Moving Averages (Dec 2023 - Present)')
        plt.legend()
        plt.grid(True)
        
        # Plot Rate Changes
        plt.subplot(2, 1, 2)
        plt.plot(features.index, features['ffr_5d_change'], 'b-', label='5-day Change')
        plt.plot(features.index, features['ffr_20d_change'], 'r-', label='20-day Change')
        plt.title('Rate Changes')
        plt.legend()
        plt.grid(True)
        
        plt.tight_layout()
        plt.savefig('current_rate_analysis.png')
        print("\nPlot saved as 'current_rate_analysis.png'")
        
        # Save to CSV
        features.to_csv('current_rate_analysis.csv')
        print("Results saved to 'current_rate_analysis.csv'")
        
        return features
        
    except Exception as e:
        print(f"Error in analysis: {str(e)}")
        return None

if __name__ == "__main__":
    analyze_macro_data()