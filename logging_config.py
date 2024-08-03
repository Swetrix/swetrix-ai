import os
import logging
import logging.handlers
from datetime import datetime

def setup_logger(name: str, log_dir: str = 'logs'):
    """Function to setup a logger with the given name and save logs to the specified directory."""
    os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = os.path.join(log_dir, f'{name}_{timestamp}.log')
    file_handler = logging.handlers.RotatingFileHandler(log_file, maxBytes=5*1024*1024, backupCount=3)
    file_handler.setLevel(logging.DEBUG)

    console_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
    file_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    console_handler.setFormatter(console_format)
    file_handler.setFormatter(file_format)

    if not logger.hasHandlers():
        logger.addHandler(console_handler)
        logger.addHandler(file_handler)

    return logger

# Example usage in your modules
if __name__ == "__main__":
    logger = setup_logger('example_logger')
    logger.info('This is an info message')
    logger.debug('This is a debug message')
