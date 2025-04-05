import yfinance as yf
from config import logger_config


logger = logger_config.get_logger(__name__)


def calculate_rsi(prices, window=14):
    """Calculate Relative Strength Index"""
    delta = prices.diff()
    gain = (delta.where(delta > 0, 0)).fillna(0)
    loss = (-delta.where(delta < 0, 0)).fillna(0)
    
    avg_gain = gain.rolling(window=window).mean()
    avg_loss = loss.rolling(window=window).mean()
    
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def calculate_macd(prices, fast=12, slow=26, signal=9):
    """Calculate MACD, MACD Signal and MACD Histogram"""
    exp1 = prices.ewm(span=fast, adjust=False).mean()
    exp2 = prices.ewm(span=slow, adjust=False).mean()
    macd = exp1 - exp2
    signal_line = macd.ewm(span=signal, adjust=False).mean()
    histogram = macd - signal_line
    return macd, signal_line, histogram


def download_stock_pricing_data(ticker,period,interval):
    try:
        logger.info(f"Starting to download pricing data for {ticker}")
        price_data = yf.download(tickers=ticker,period=period,interval=interval) # dataframe
        logger.info(f"Completed download of pricing data for {ticker}")

        price_data.reset_index(inplace=True)
        price_data.columns = ["Date", "Close", "High", "Low", "Open", "Volume"]

        # add ticker information
        price_data["Ticker"] = ticker

        # create ticker object to access additional data
        stock = yf.Ticker(ticker)
        
        # Add basic 
        logger.info(f"Starting to download additional pricing data for {ticker}")
        price_data['MarketCap'] = stock.info.get('marketCap', None)
        price_data['Beta'] = stock.info.get('beta', None)
        price_data['PE_Ratio'] = stock.info.get('trailingPE', None)
        price_data['Forward_PE'] = stock.info.get('forwardPE', None)
        price_data['Dividend_Rate'] = stock.info.get('dividendRate', None)
        price_data['Dividend_Yield'] = stock.info.get('dividendYield', None)
            
        # Add moving averages
        price_data['MA20'] = price_data['Close'].rolling(window=20).mean()
        price_data['MA50'] = price_data['Close'].rolling(window=50).mean()
        price_data['MA200'] = price_data['Close'].rolling(window=200).mean()
            
        # Add technical indicators
        price_data['RSI'] = calculate_rsi(price_data['Close'], 14)
        price_data['MACD'], price_data['MACD_Signal'], price_data['MACD_Hist'] = calculate_macd(price_data['Close'])
            
        # Add volatility measures
        price_data['Daily_Return'] = price_data['Close'].pct_change()
        price_data['Volatility_20d'] = price_data['Daily_Return'].rolling(window=20).std() * (252 ** 0.5)  # Annualized
            
        # Add trading volume indicators
        price_data['Volume_MA20'] = price_data['Volume'].rolling(window=20).mean()
        price_data['Volume_Ratio'] = price_data['Volume'] / price_data['Volume_MA20']

        logger.info(f"Completed download additional pricing data for {ticker}")
        return price_data

    except Exception as e:
        logger.error(f"Error getting pricing data for {ticker} : {str(e)}",exc_info=True)
        raise

def download_stock_fundamental_data(ticker):
    """ Get fundamental data for a specific ticker """
    stock = yf.Ticker(ticker)
    fundamentals = {}
    try:
        # Basic company info
        fundamentals['Company_Name'] = stock.info.get('longName', None)
        fundamentals['Sector'] = stock.info.get('sector', None)
        fundamentals['Industry'] = stock.info.get('industry', None)
        fundamentals['Country'] = stock.info.get('country', None)
        fundamentals['Exchange'] = stock.info.get('exchange', None)
        
        # Financial metrics
        fundamentals['Market_Cap'] = stock.info.get('marketCap', None)
        fundamentals['Enterprise_Value'] = stock.info.get('enterpriseValue', None)
        fundamentals['Trailing_PE'] = stock.info.get('trailingPE', None)
        fundamentals['Forward_PE'] = stock.info.get('forwardPE', None)
        fundamentals['PEG_Ratio'] = stock.info.get('pegRatio', None)
        fundamentals['Price_to_Book'] = stock.info.get('priceToBook', None)
        fundamentals['EV_to_Revenue'] = stock.info.get('enterpriseToRevenue', None)
        fundamentals['EV_to_EBITDA'] = stock.info.get('enterpriseToEbitda', None)
        
        # Profitability
        fundamentals['Profit_Margin'] = stock.info.get('profitMargins', None)
        fundamentals['Operating_Margin'] = stock.info.get('operatingMargins', None)
        fundamentals['ROA'] = stock.info.get('returnOnAssets', None)
        fundamentals['ROE'] = stock.info.get('returnOnEquity', None)
        
        # Growth metrics
        fundamentals['Revenue_Growth'] = stock.info.get('revenueGrowth', None)
        fundamentals['Earnings_Growth'] = stock.info.get('earningsGrowth', None)
        
        # Dividend information
        fundamentals['Dividend_Rate'] = stock.info.get('dividendRate', None)
        fundamentals['Dividend_Yield'] = stock.info.get('dividendYield', None)
        fundamentals['Payout_Ratio'] = stock.info.get('payoutRatio', None)

        # Financial statements (last reported)

        # Balance sheet items 
        balance_sheet = stock.balance_sheet.T
        # print(balance_sheet.columns)
        # print(balance_sheet.columns)
        if not balance_sheet.empty:
            
            fundamentals['Total_Assets'] = balance_sheet.iloc[0].get('Total Assets', None)
            fundamentals['Total_Debt'] = balance_sheet.iloc[0].get('Total Debt', None)
            # fundamentals['Total_Debt'] = (
            #     balance_sheet.iloc[-1].get('LongTermDebt', 0) + 
            #     balance_sheet.iloc[-1].get('ShortTermDebt', 0)
            # )
            fundamentals['Total_Equity'] = balance_sheet.iloc[0].get('Stockholders Equity', None)
        # Income statement items

        income_stmt = stock.income_stmt.T
        # print(income_stmt.columns)
        if not income_stmt.empty:
            fundamentals['Revenue'] = income_stmt.iloc[0].get('Total Revenue', None)
            fundamentals['Net_Income'] = income_stmt.iloc[0].get('Net Income', None)
            fundamentals['EBITDA'] = income_stmt.iloc[0].get('EBITDA', None)
        
        # Cash flow items

        cash_flow = stock.cashflow.T
        # print(cash_flow.columns)
        if not cash_flow.empty:
            fundamentals['Operating_Cash_Flow'] = cash_flow.iloc[0].get('Operating Cash Flow', None)
            fundamentals['Free_Cash_Flow'] = cash_flow.iloc[0].get('Free Cash Flow', None)

    except Exception as e:
        logger.error(f"Error getting financial statements for {ticker} : {str(e)}",exc_info=True)
        raise
        
    return fundamentals


def download_stock_data(tickers, period = "1y",interval = "1d"):
    """
    Download stock data for specified tickers
    tickers: list or string of ticker symbols
    period: valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
    interval: valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
    
    """
    # If one single stock is provided, convert it into a list
    if isinstance(tickers,str):
        tickers=[tickers]

    all_data = {}
    for ticker in tickers:
        try:
            price_data = download_stock_pricing_data(ticker,period,interval)
            fundamental_data = download_stock_fundamental_data(ticker=ticker) # returns a data frame
            for key,val in fundamental_data.items():
                price_data[key] = val

            all_data[ticker] = price_data

        except Exception as e:
            logger.error(f"Error getting price data for {ticker}: {str(e)}", exc_info=True)
            raise
    return all_data

