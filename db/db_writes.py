import pandas as pd
from sqlalchemy import Engine
from sqlalchemy.exc import SQLAlchemyError
from logger_file.logger import get_logger

log = get_logger("db_writer")

def load_dataframe_to_table(df: pd.DataFrame, table_name: str, engine: Engine, schema: str = "public", if_exists: str = "append") -> bool:
    """
    Streams a Pandas DataFrame straight into a target relational database table.
    
    Parameters:
        df (pd.DataFrame): Cleaned records to write.
        table_name (str): Destination table name (e.g., 'silver_orders').
        engine (Engine): Active SQLAlchemy connection engine instance.
        schema (str): Target database schema layer. Defaults to 'public'.
        if_exists (str): Action pattern if table exists ('fail', 'replace', or 'append').
        
    Returns:
        bool: True if writing completes successfully, False otherwise.
    """
    if df.empty:
        log.warning(f"Skipping insertion routine. DataFrame provided for table '{table_name}' is completely empty.")
        return True

    total_rows = len(df)
    log.info(f"Initiating bulk write of {total_rows} records into target table '{schema}.{table_name}'...")

    try:
        # Use context manager connection block to guarantee resource teardown
        with engine.begin() as connection:
            df.to_sql(
                name=table_name,
                con=connection,
                schema=schema,
                if_exists=if_exists,
                index=False,
                chunksize=10000,        # Batches insertions into sets of 10k rows to avoid memory bloat
                method="multi"         # Converts insert statements into high-speed multi-row structures
            )
        log.info(f"Successfully loaded data. status=success | table={schema}.{table_name} | rows_inserted={total_rows}")
        return True
        
    except SQLAlchemyError as e:
        log.error(f"Database write execution failed on table '{table_name}': {str(e)}")
        return False
    except Exception as e:
        log.error(f"Unexpected operational anomaly encountered during ingestion write execution: {str(e)}")
        return False