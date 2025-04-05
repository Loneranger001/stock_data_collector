import logging
import os


def get_logger(name):
    """
    Returns a logger with given configuration
    """
    logger = logging.getLogger(name)

    if not logger.handlers:
        logger.setLevel(logging.INFO)
        
        # create console and file handlers
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)


        file_handler = logging.FileHandler(os.path.join(os.getcwd(),"application.log"),"w")
        file_handler.setLevel(logging.INFO)

        # create formatter
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

        # set the formatter to handlers
        console_handler.setFormatter(formatter)
        file_handler.setFormatter(formatter)

        # set the handlers to logger
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

    return logger

