import pandas as pd
from sqlalchemy import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.dialects.mysql import insert
from logger_file.logger import get_logger

log = get_logger("db_writer")

def _insert_ignore_method(table, conn, keys, data_iter):
    """
    Custom insertion callable for pandas.to_sql to execute MySQL 'INSERT IGNORE'
    structures for high-speed bulk ingestion while bypassing primary key collisions.
    """
    # Create the base insert mapping structure using SQLAlchemy's Core layout
    data = [dict(zip(keys, row)) for row in data_iter]
    
    # Construct a MySQL-specific INSERT statement prefixed with IGNORE
    stmt = insert(table.table).values(data).prefix_with("IGNORE")
    
    # Execute statement on the current active connection context
    conn.execute(stmt)

def load_dataframe_to_table(df: pd.DataFrame, table_name: str, engine: Engine, schema: str = "public", if_exists: str = "append") -> bool:
    """
    Streams a Pandas DataFrame straight into a target relational database table.
    Bypasses duplicate primary keys gracefully using INSERT IGNORE.
    
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
    log.info(f"Initiating bulk write of {total_rows} records into target table '{schema}.{table_name}' using INSERT IGNORE...")

    try:
        # Use context manager connection block to guarantee resource teardown
        with engine.begin() as connection:
            df.to_sql(
                name=table_name,
                con=connection,
                schema=schema,
                if_exists=if_exists,
                index=False,
                chunksize=10000,              # Batches insertions into sets of 10k rows to avoid memory bloat
                method=_insert_ignore_method   # Uses custom compiler logic to handle duplicates without throwing exceptions
            )
        log.info(f"Successfully executed load routine. status=completed | table={schema}.{table_name} | processing_attempted={total_rows}")
        return True
        
    except SQLAlchemyError as e:
        log.error(f"Database write execution failed on table '{table_name}': {str(e)}")
        return False
    except Exception as e:
        log.error(f"Unexpected operational anomaly encountered during ingestion write execution: {str(e)}")
        return False