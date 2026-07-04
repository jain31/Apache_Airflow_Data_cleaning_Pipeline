"""
db_connection.py
Manages the SQLAlchemy engine pool connection configuration for MySQL.
"""

import os
from sqlalchemy import create_engine, text
from logger_file.logger import get_logger

log = get_logger("db_connection")

# 1. Define the Global Connection String Variable
DEFAULT_DB_URI = "mysql+pymysql://root:123456789@127.0.0.1:3306/medallion_db"

def get_database_engine():
    """Initializes and verifies the SQLAlchemy connection pool."""
    log.info("Initializing database connection engine pool...")
    engine = create_engine(DEFAULT_DB_URI)
    
    try:
        with engine.connect() as connection:
            # Verified safe execution syntax under SQLAlchemy 2.0
            connection.execute(text("SELECT 1")) 
        log.info("Database connection engine pool verified successfully.")
        return engine
    except Exception as e:
        log.critical(f"Failed to initialize database engine pool: {str(e)}")
        raise e