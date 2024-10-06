
#imports
import yfinance as yf

# Using some list of the tickers, using the batch size to avoid overloading API.
# List of tickers (could be a large list)
tickers = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'FB', 'NFLX'] 

# Download data in batches of 3 tickers at a time
batch_size = 3
for i in range(0, len(tickers), batch_size):
    batch_tickers = tickers[i:i+batch_size]
    stock_data = yf.download(batch_tickers, period='1mo', interval='1d')
    print(stock_data)

# retrieve dividen, income statement, balance sheet, cash flow from 1 ticker for example

# List of Available information from yahoo finance
# Define the stock ticker
ticker = 'AAPL'

# Create a Ticker object
ticker_info = yf.Ticker(ticker)

# Access various attributes and methods
print("Company Info:", ticker_info.info)
print("Historical Data:", ticker_info.history(period="1mo"))
print("Financials:", ticker_info.financials)
print("Balance Sheet:", ticker_info.balance_sheet)
print("Cash Flow:", ticker_info.cashflow)
print("Earnings:", ticker_info.earnings)
print("Dividends:", ticker_info.dividends)
print("Splits:", ticker_info.splits)
print("Recommendations:", ticker_info.recommendations)
print("Institutional Holders:", ticker_info.institutional_holders)