import pycurl
from io import BytesIO
from config import logger_config
import sys, os
# FTP server details
ftp_host = "ftp.nasdaqtrader.com/"
ftp_path = "Symboldirectory/"
filenames = ["nasdaqlisted.txt", "otherlisted.txt"]


logger = logger_config.get_logger(__name__)

if len(sys.argv) < 3:
    logger.error(f"Usage: ftp_operations.py <ftp_server> <ftp_path> <file_name>")
    exit(1)
else:
    ftp_host = sys.argv[1]
    ftp_path = sys.argv[2]
    filenames = [sys.argv[3]] if isinstance(sys.argv[3], str) else filenames


logger.info(f"ftp_host: {ftp_host};ftp_path = {ftp_path};filename={filenames}")

# Function to download a file using pycurl
def download_file(filename):
    try:
        buffer = BytesIO()
        
        # Initialize curl object
        c = pycurl.Curl()
        
        # Set URL with full path to file
        c.setopt(c.URL, f"ftp://{ftp_host}{ftp_path}{filename}")
        
        # Write data to buffer
        c.setopt(c.WRITEDATA, buffer)
        
        # Perform the request
        c.perform()
        
        # Get status code
        status_code = c.getinfo(c.RESPONSE_CODE)
        
        # Close curl object
        c.close()
        
        # Write buffer contents to file
        if status_code == 0:  # FTP success
            with open(filename, "wb") as file:
                file.write(buffer.getvalue())
            # print(f"Successfully downloaded {filename}")
            logger.info(f"Successfully downloaded {filename}")
            return file
        else:
            # print(f"Failed to download {filename}, status code: {status_code}")
            raise Exception(f"Failed to download {filename}, status code: {status_code}")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise

# List directory contents
def list_directory():
    try:
        buffer = BytesIO()
        
        # Initialize curl object
        c = pycurl.Curl()
        
        # Set URL to directory
        c.setopt(c.URL, f"ftp://{ftp_host}{ftp_path}")
        
        # Set to list only
        c.setopt(c.DIRLISTONLY, 1)
        
        # Write data to buffer
        c.setopt(c.WRITEDATA, buffer)
        
        # Perform the request
        c.perform()
        
        # Close curl object
        c.close()
        # Print directory contents
        print("Directory contents:")
        print(buffer.getvalue().decode('utf-8'))

    except Exception as e:
        logger.error(f"Unexpected error :{e}")


# # Execute the code
# list_directory()

# Download each file
for filename in filenames:
    file=download_file(filename)
    print(file.name)