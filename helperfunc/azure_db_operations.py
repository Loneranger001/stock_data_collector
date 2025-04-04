import pyodbc
from sqlalchemy import create_engine, Table, Column, Float, Date, Integer,BigInteger,Numeric,MetaData, String , exc
from azure.identity import DefaultAzureCredential
import pandas as pd
from config.logger_config import get_logger

# fetch credentials 
# username = os.environ.get("AZURE_SQL_USER")
# password = os.environ.get("AZURE_SQL_PASSWORD")
# server   = os.environ.get("AZURE_SQL_SERVER")
# database = os.environ.get("AZURE_SQL_DB")

# connection_string = f"mssql+pymssql://{username}:{password}@{server}/{database}"

# engine = create_engine(connection_string,
#         connect_args={"login_timeout":90, # connection timeout
#         "timeout":300})                   # query timeout

logger = get_logger(__name__)

def save_to_azure_db(df,engine,chunk_size=1000):
    try:
        metadata = MetaData()
        stock_daily_data = Table(
            "stock_daily_data", 
            metadata,
            Column("Ticker", String(50), primary_key=True, nullable=False),
            Column("Date", Date, primary_key=True, nullable=False),
            Column("Close", Numeric(20, 2), nullable=False),
            Column("High", Numeric(20, 2), nullable=False),
            Column("Low", Numeric(20, 2), nullable=False),
            Column("Open", Numeric(20, 2), nullable=False),
            Column("Volume", BigInteger, nullable=False),
            Column("MarketCap", Numeric(20, 2), nullable=True),
            Column("Beta", Numeric(20, 2), nullable=True),
            Column("PE_Ratio", Numeric(20, 2), nullable=True),
            Column("Forward_PE", Numeric(20, 2), nullable=True),
            Column("Dividend_Rate", Numeric(20, 2), nullable=True),
            Column("Dividend_Yield", Numeric(20, 2), nullable=True),
            Column("MA20", Numeric(20, 2), nullable=True),
            Column("MA50", Numeric(20, 2), nullable=True),
            Column("MA200", Numeric(20, 2), nullable=True),
            Column("RSI", Numeric(20, 2), nullable=True),
            Column("MACD", Numeric(20, 2), nullable=True),
            Column("MACD_Signal", Numeric(20, 2), nullable=True),
            Column("MACD_Hist", Numeric(20, 2), nullable=True),
            Column("Daily_Return", Numeric(20, 2), nullable=True),
            Column("Volatility_20d", Numeric(20, 2), nullable=True),
            Column("Volume_MA20", Numeric(20, 2), nullable=True),
            Column("Volume_Ratio", Numeric(20, 2), nullable=True),
            Column("Company_Name", String(50), nullable=True),
            Column("Sector", String(50), nullable=True),
            Column("Industry", String(50), nullable=True),
            Column("Country", String(50), nullable=True),
            Column("Exchange", String(50), nullable=True),
            Column("Market_Cap", Numeric(20, 2), nullable=True),
            Column("Enterprise_Value", Numeric(20, 2), nullable=True),
            Column("Trailing_PE", Numeric(20, 2), nullable=True),
            Column("PEG_Ratio", Numeric(20, 2), nullable=True),
            Column("Price_to_Book", Numeric(20, 2), nullable=True),
            Column("EV_to_EBITDA", Numeric(20, 2), nullable=True),
            Column("EV_to_Revenue", Numeric(20, 2), nullable=True),
            Column("Profit_Margin", Numeric(20, 2), nullable=True),
            Column("Operating_Margin", Numeric(20, 2), nullable=True),
            Column("ROA", Numeric(20, 2), nullable=True),
            Column("ROE", Numeric(20, 2), nullable=True),
            Column("Revenue_Growth", Numeric(20, 2), nullable=True),
            Column("Earnings_Growth", Numeric(20, 2), nullable=True),
            Column("Payout_Ratio", Numeric(20, 2), nullable=True),
            Column("Total_Assets", Numeric(20, 2), nullable=True),
            Column("Total_Debt", Numeric(20, 2), nullable=True),
            Column("Total_Equity", Numeric(20, 2), nullable=True),
            Column("Revenue", Numeric(20, 2), nullable=True),
            Column("Net_Income", Numeric(20, 2), nullable=True),
            Column("EBITDA", Numeric(20, 2), nullable=True),
            Column("Operating_Cash_Flow", Numeric(20, 2), nullable=True),
            Column("Free_Cash_Flow", Numeric(20, 2), nullable=True)

        )

        # create table if it doesn't exist
        metadata.create_all(engine, checkfirst=True)
        # handle nulls
        df = clean_df(df)
        logger.info(f"Number of null value in dataframe: {df.isna().sum().sum()}")
        # convert dataframe to dictionary
        records = df.to_dict("records")
        total_records = len(records)
        logger.info(f"Total number of records to be inserted is: {total_records}")

        # process in chunks 
        with engine.begin() as connection:
            error_flag = False
            for i in range(0,total_records,chunk_size):
                chunk = records[i:i + chunk_size]
                try:
                    result=connection.execute(stock_daily_data.insert().values(chunk))
                    logger.info(f"Number of rows inserted : {result.rowcount}")
                except exc.IntegrityError as e:
                    logger.warning(f"Integrity error in chunk {1//chunk_size + 1}: {e}")
                    error_flag=True

            if not error_flag:
                logger.info(f"Successfully inserted data into Azure SQL Database")
            else:
                logger.error(f"One or more chunk failed.")    

    except exc.SQLAlchemyError as e:
        logger.error(f"Database error: {str(e)}",exc_info=True)
        raise
    except Exception as e:
        logger.error(f"An unexpected error occured: {e}",exc_info=True)
        raise
def clean_df(df):
    # First convert any string 'nan' to actual NaN
    # df = df.replace('nan', pd.NA)
    # Then replace all NaN values with None for SQL compatibility
    df = df.where(pd.notnull(df), 0)
    return df