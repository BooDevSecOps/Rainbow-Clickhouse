import os
import logging
from logging.handlers import TimedRotatingFileHandler

def setup_logging():
    log_folder = 'logs'
    os.makedirs(log_folder, exist_ok=True)

    log_path = os.path.join(log_folder, 'api_logs.log')

    log_handler = TimedRotatingFileHandler(log_path, when='midnight', interval=1, backupCount=7, encoding='utf-8', delay=False)
    log_handler.setLevel(logging.INFO)
    log_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

    logger = logging.getLogger(__name__)
    logger.addHandler(log_handler)

    return logger
