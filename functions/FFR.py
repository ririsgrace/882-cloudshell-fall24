import pandas as pd
import numpy as np
from scipy.stats import zscore

def process_macro_indicators(ffr_data, gdp_data, stock_dates):
    """
    Process FFR and GDP data to create features for stock prediction
    
    Parameters:
    ffr_data (pd.DataFrame): Federal Funds Rate data with 'date' and 'ffr' columns
    gdp_data (pd.DataFrame): GDP data with 'date' and 'gdp' columns
    stock_dates (pd.DatetimeIndex): Dates from stock data to align with
    
    Returns:
    pd.DataFrame: Processed features aligned with stock dates
    """
    features = pd.DataFrame(index=stock_dates)
    
    # Process FFR Features
    ffr_data = ffr_data.set_index('date')
    
    # 1. FFR Level Features
    features['ffr_rate'] = ffr_data['ffr'].reindex(stock_dates).fillna(method='ffill')
    
    # 2. FFR Change Features
    features['ffr_1d_change'] = features['ffr_rate'].diff()
    features['ffr_5d_change'] = features['ffr_rate'].diff(5)
    features['ffr_20d_change'] = features['ffr_rate'].diff(20)
    
    # 3. FFR Momentum (Rate of Change)
    features['ffr_momentum'] = (features['ffr_rate'] / features['ffr_rate'].shift(5) - 1) * 100
    
    # 4. FFR Volatility
    features['ffr_volatility'] = features['ffr_rate'].rolling(20).std()
    
    # Process GDP Features
    gdp_data = gdp_data.set_index('date')
    
    # 5. GDP Level Features
    features['gdp_level'] = gdp_data['gdp'].reindex(stock_dates).fillna(method='ffill')
    
    # 6. GDP Growth Rate
    features['gdp_growth'] = features['gdp_level'].pct_change() * 100
    
    # 7. GDP Momentum
    features['gdp_momentum'] = (features['gdp_level'] / features['gdp_level'].shift(20) - 1) * 100
    
    # 8. Combined Features
    features['gdp_ffr_ratio'] = features['gdp_level'] / features['ffr_rate']
    
    # 9. Z-scores for detecting extreme conditions
    features['ffr_zscore'] = zscore(features['ffr_rate'].fillna(method='ffill'))
    features['gdp_zscore'] = zscore(features['gdp_level'].fillna(method='ffill'))
    
    # 10. Market Regime Indicators
    features['high_rate_regime'] = (features['ffr_zscore'] > 1).astype(int)
    features['low_growth_regime'] = (features['gdp_zscore'] < -1).astype(int)
    
    # Fill any remaining NaN values
    features = features.fillna(method='ffill').fillna(method='bfill')
    
    return features

def create_market_signal(macro_features, sentiment_scores, technical_signals, weights=None):
    """
    Combine macro features with sentiment and technical signals
    
    Parameters:
    macro_features (pd.DataFrame): Processed macro features
    sentiment_scores (pd.Series): Daily sentiment scores
    technical_signals (pd.Series): Technical analysis signals
    weights (dict): Optional weights for different components
    
    Returns:
    pd.Series: Combined market signal
    """
    if weights is None:
        weights = {
            'macro': 0.3,
            'sentiment': 0.3,
            'technical': 0.4
        }
    
    # Normalize macro features
    macro_signal = (
        weights['macro'] * (
            0.4 * np.sign(macro_features['ffr_momentum']) +
            0.4 * np.sign(macro_features['gdp_momentum']) +
            0.2 * (-1 * macro_features['high_rate_regime'])  # Negative impact of high rates
        )
    )
    
    # Combine signals
    combined_signal = (
        macro_signal +
        weights['sentiment'] * sentiment_scores +
        weights['technical'] * technical_signals
    )
    
    return pd.Series(
        np.where(combined_signal > 0, 'BUY',
                np.where(combined_signal < 0, 'SELL', 'HOLD')),
        index=macro_features.index
    )

def generate_trading_recommendations(signals, confidence_threshold=0.6):
    """
    Generate trading recommendations based on signals
    
    Parameters:
    signals (pd.DataFrame): Combined signals and features
    confidence_threshold (float): Threshold for signal strength
    
    Returns:
    pd.DataFrame: Trading recommendations with confidence levels
    """
    recommendations = pd.DataFrame(index=signals.index)
    
    # Calculate signal strength based on feature combinations
    signal_strength = np.abs(
        signals['ffr_zscore'] * 0.3 +
        signals['gdp_zscore'] * 0.3 +
        signals['ffr_momentum'] * 0.2 +
        signals['gdp_momentum'] * 0.2
    )
    
    # Generate recommendations
    recommendations['action'] = signals['combined_signal']
    recommendations['confidence'] = signal_strength
    recommendations['rationale'] = recommendations.apply(
        lambda x: generate_rationale(x, signals.loc[x.name]), axis=1
    )
    
    return recommendations

def generate_rationale(rec, features):
    """Generate text rationale for recommendation"""
    rationale = []
    
    if abs(features['ffr_zscore']) > 1:
        rationale.append(f"Interest rates are {'high' if features['ffr_zscore'] > 0 else 'low'}")
    
    if abs(features['gdp_zscore']) > 1:
        rationale.append(f"GDP growth is {'strong' if features['gdp_zscore'] > 0 else 'weak'}")
    
    if abs(features['ffr_momentum']) > 0.1:
        rationale.append(f"Interest rates are {'rising' if features['ffr_momentum'] > 0 else 'falling'}")
    
    return "; ".join(rationale) if rationale else "Normal market conditions"

