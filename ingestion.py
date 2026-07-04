"""
ingestion.py
Handles dynamic CSV document discovery and structural ingestion.
Loads raw files safely into the 'raw_bronze' database layer.
"""

import os
import hashlib
from pathlib import Path
import pandas as pd
from sqlalchemy import text
from logger_file.logger import get_logger

log = get_logger("ingestion_engine")

def _discover_files():
    """
    Scans the local directory structures for target raw CSV source files.
    Prioritizes the 'src/' directory, falls back to root.
    """
    # Check parent/src and local src folder paths
    paths_to_check = [
        Path(__file__).parent / "src",
        Path("src"),
        Path(".")
    ]
    
    found_files = []
    for path in paths_to_check:
        if path.exists():
            csv_files = list(path.glob("*.csv"))
            if csv_files:
                found_files.extend(csv_files)
                break # Stop at the first directory that yields files

    log.info(f"action=discover_files | files_found={len(found_files)}")
    return found_files


def generate_file_hash(file_path):
    """Generates a unique MD5 hash signature for a file to prevent duplicate processing."""
    hasher = hashlib.md5()
    with open(file_path, "rb") as f:
        buf = f.read(1024 * 64)
        while len(buf) > 0:
            hasher.update(buf)
            buf = f.read(1024 * 64)
    return hasher.hexdigest()


def process_file(file_path, column_alias, engine):
    """
    Reads incoming datasets, maps flexible names, isolates 
    expected columns, and records raw streams into raw_bronze.
    """
    log.info(f"--- Launching Ingestion Lifecycle Loop for file: {file_path.name} ---")
    
    try:
        # 1. Read raw CSV file
        df = pd.read_csv(file_path)
        if df.empty:
            log.error(f"Inbound file validation rejected: {file_path.name} is empty.")
            return None
            
        # 2. Re-map header strings using Column Aliases mapping dictionary
        mapped_columns = {}
        for canonical_name, aliases in column_alias.items():
            for df_col in df.columns:
                clean_df_col = str(df_col).lower().strip()
                if clean_df_col == canonical_name or clean_df_col in aliases:
                    mapped_columns[df_col] = canonical_name
                    
        df = df.rename(columns=mapped_columns)

        # 3. Handle Extra Columns (Graceful Truncation Rule)
        expected_fields = list(column_alias.keys())
        found_fields = [col for col in df.columns if col in expected_fields]
        
        # Determine if there are extra columns we don't need
        if len(df.columns) > len(expected_fields) or len(found_fields) != len(df.columns):
            extra_cols = [c for c in df.columns if c not in expected_fields]
            log.warning(f"Extra columns detected: {extra_cols}. Truncating dataset down to expected core layout schema.")
            
        # Slice dataframe down to keep ONLY the matching core pipeline columns
        df = df[found_fields]

        # 4. Final verification: Check if we are missing any critical columns
        minimum_required_columns = 12
        if len(df.columns) < minimum_required_columns:
            log.error(f"validation=failed | status=quarantine | reason=missing_core_columns | expected_at_least={minimum_required_columns} | found={len(df.columns)}")
            return None

        # 5. Track file ingestion using an MD5 hash signature
        file_hash = generate_file_hash(file_path)
        
        # 6. Push a direct clone of this raw file into Bronze Database Layer
        bronze_df = df.copy()
        bronze_df["file_source_hash"] = file_hash
        
        # Ensure string format consistency for landing table
        for col in bronze_df.columns:
            if col not in ["row_id", "ingested_at", "file_source_hash"]:
                bronze_df[col] = bronze_df[col].astype(str)

        # Load to MySQL raw_bronze.orders_landing
        bronze_df.to_sql(
            name="orders_landing",
            con=engine,
            schema="raw_bronze",
            if_exists="append",
            index=False
        )
        log.info(f"Successfully archived raw dump records to raw_bronze.orders_landing for file: {file_path.name}")
        
        return df

    except Exception as e:
        log.error(f"Fatal exception inside ingestion layer processing engine: {str(e)}")
        return None