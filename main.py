import os
from config import logger_config
from helperfunc import scrap_stock_data
from helperfunc import azure_db_operations
from sqlalchemy import create_engine, exc

# get logger for the main script

logger = logger_config.get_logger(__name__)

def main():
    try:
        # Fetch connection details 
        username = os.environ.get("AZURE_SQL_USER")
        password = os.environ.get("AZURE_SQL_PASSWORD")
        server   = os.environ.get("AZURE_SQL_SERVER")
        database = os.environ.get("AZURE_SQL_DB")

        connection_string = f"mssql+pymssql://{username}:{password}@{server}/{database}?login_timeout=120&timeout=300"

        engine = create_engine(connection_string)
        
        # get list of stocks


        # fetch stock data
        logger.info(f"Starting to download stock data")
        stock_data = scrap_stock_data.download_stock_data(["META","MSFT"],period="1d",interval="1d")
        logger.info(f"Stock data download complete")
        

        # save to database
        for ticker,df in stock_data.items():
            logger.info(f"Number of rows in {ticker} is {df.shape[0]}")
            azure_db_operations.save_to_azure_db(df,engine)
    except Exception as e:
        logger.error(f"An unexpected error occured in main(): {str(e)}",exc_info=True)
        raise


if __name__ == "__main__":
    main()

    