import os
# import pyodbc
from config.logger_config import get_logger
from helperfunc.scrap_stock_data import download_stock_data
from helperfunc.azure_db_operations import save_to_azure_db
from sqlalchemy import create_engine

# logging.basicConfig(filename="scrap_stock_data.log",
#                     filemode="a",
#                     format="%(asctime)s - %(levelname)s - %(message)s",
#                     level=logging.INFO,
#                     datefmt="%Y-%m-%d %H:%M:%S")

# logging.basicConfig(
#                     format="%(asctime)s - %(levelname)s - %(message)s",
#                     level=logging.INFO,
#                     datefmt="%Y-%m-%d %H:%M:%S")

# get logger for the main script

logger = get_logger(__name__)

def main():
    try:
        # Fetch connection details 
        username = os.environ.get("AZURE_SQL_USER")
        password = os.environ.get("AZURE_SQL_PASSWORD")
        server   = os.environ.get("AZURE_SQL_SERVER")
        database = os.environ.get("AZURE_SQL_DB")

        connection_string = f"mssql+pymssql://{username}:{password}@{server}/{database}?login_timeout=120&timeout=300"

        engine = create_engine(connection_string)
        
        # engine = create_engine(connection_string,
        #         connect_args={"login_timeout":120, # connection timeout
        #         "timeout":300})                   # query timeout
        
        # get list of stocks
    

        # fetch stock data
        logger.info(f"Starting to download stock data")
        stock_data = download_stock_data(["META","MSFT"],period="1y",interval="1d")
        logger.info(f"Stock data download complete")
        

        # save to database
        for ticker,df in stock_data.items():
            logger.info(f"Number of rows in {ticker} is {df.shape[0]}")
            save_to_azure_db(df,engine)
    except Exception as e:
        logger.error(f"An unexpected error occured in main(): {str(e)}",exc_info=True)
        raise


if __name__ == "__main__":
    main()

    