import yfinance as yf

# Fetch the data for the past 5 days
ticker = "AAPL"
new_data = yf.download(ticker, period="5d")

# Display the data to check the actual dates
print(new_data)