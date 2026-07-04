from datetime import datetime
import logging
from logging.handlers import RotatingFileHandler
import os

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
# Keeps logs cleanly nested inside the logger_file/logs directory structure
LOG_DIR = os.path.join(CURRENT_DIR, "logs")

def get_logger(module_name="ingestion"):
    """
    Creates an isolated logger instance for Apache Airflow tasks.
    Streams logs dynamically to standard output (Airflow UI) while
    simultaneously writing to rotating local module-specific log files.
    """
    os.makedirs(LOG_DIR, exist_ok=True)
    
    # Generate dynamic log files named after the specific module/file invoking it
    log_file = os.path.join(LOG_DIR, f"{module_name}_log.log")
    
    logger = logging.getLogger(module_name)

    if logger.handlers:
        return logger
    
    logger.setLevel(logging.INFO)

    # 1. File Handler: Writes to local disk with auto-rotation safeguards
    file_handler = RotatingFileHandler(
        filename=log_file,
        mode='a',
        maxBytes=5*1024*1024, # 5MB limit per file
        backupCount=10
    )
    
    # 2. Stream Handler: CRITICAL FOR AIRFLOW. Captures output in Airflow Web UI
    stream_handler = logging.StreamHandler()

    # Uniform log message structure formatting matching your exact keywords
    log_formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    
    file_handler.setFormatter(log_formatter)
    stream_handler.setFormatter(log_formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    
    return logger