import os
import logging
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from logger_file.logger import get_logger

log = get_logger("db_connection")

# Fallback URI connection string (Updates dynamically via production environment variables)
# Format: postgresql+psycopg2://user:password@host:port/dbname
DEFAULT_DB_URI = os.getenv("AIRFLOW_VAR_DATABASE_URL", "postgresql+psycopg2://postgres:postgres@localhost:5432/medallion_db")

def get_database_engine(connection_uri: str = DEFAULT_DB_URI):
    """
    Creates and returns a SQLAlchemy engine instance equipped with connection pooling.
    Optimized for high-concurrency pipeline writes.
    """
    try:
        log.info("Initializing database connection engine pool...")
        engine = create_engine(
            connection_uri,
            pool_size=10,          # Keeps up to 10 persistent connections open
            max_overflow=20,       # Allows spikes up to 20 additional transient connections
            pool_recycle=1800,     # Recycles connection blocks every 30 minutes to prevent stale locks
            pool_pre_ping=True     # Validates liveness before issuing a query execution request
        )
        # Test connectivity immediately
        with engine.connect() as conn:
            conn.execute("SELECT 1")
        log.info("Database connection pool established successfully.")
        return engine
    except SQLAlchemyError as e:
        log.critical(f"Failed to initialize database engine pool: {str(e)}")
        raise e