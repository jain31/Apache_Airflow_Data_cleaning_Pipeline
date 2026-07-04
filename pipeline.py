"""
pipeline.py
The core orchestration engine that structures, parses, and cleanses the raw dataset
columns prior to pushing records downstream into the Medallion Database tables.
"""

import os
import pandas as pd
from logger_file.logger import get_logger

# Import your custom standalone parsers
from parsers.parse_order_date import clean_date
from parsers.parse_phone import clean_phone
from parsers.parse_pincode import clean_pincode
from parsers.parse_product_name import clean_product_name
from parsers.parse_quantity import clean_quantity
from parsers.parse_state import clean_state
from parsers.parse_total_amount import clean_total_amount
from parsers.parse_unit_price import clean_unit_price

# Import remaining parsers (Placeholder structures to align your CSV headers)
# If you haven't split these out yet, these safe functions handle their structural cleanup inline
from parsers.parse_rating import clean_rating

# Import Null Handling Component
from null_values.null import handle_nulls

# Import Database Interaction Engine
from db.db_connection import get_database_engine
from db.db_writes import load_dataframe_to_table

log = get_logger("master_pipeline")

def run_ingestion_pipeline(file_path: str):
    """
    Executes the end-to-end processing loop over the dirty CSV file dataset.
    """
    if not os.path.exists(file_path):
        log.error(f"Execution terminated. Target CSV data source file not found: {file_path}")
        return False
        
    log.info(f"Master Ingestion Pipeline initiated for data target: {file_path}")
    
    # Read CSV data directly as raw string objects to protect formatting anomalies from stripping early
    raw_df = pd.read_csv(file_path, dtype=str)
    original_df = raw_df.copy()
    
    # -------------------------------------------------------------------------
    # STAGE 1: RAW BRONZE PERSISTENCE
    # -------------------------------------------------------------------------
    engine = get_database_engine()
    load_dataframe_to_table(raw_df, "orders_landing", engine, schema="raw_bronze", if_exists="append")
    
    # -------------------------------------------------------------------------
    # STAGE 2: ATOMIC COLUMN CLEANING LAYER
    # -------------------------------------------------------------------------
    log.info("Beginning localized column parsing routine passes...")
    work_df = raw_df.copy()
    
    # Clean structural strings, structural digits, dates, and amounts
    try:
        work_df, _ = clean_date(work_df, original_df)
        work_df, _ = clean_phone(work_df, original_df)
        work_df, _ = clean_pincode(work_df, original_df)
        work_df, _ = clean_product_name(work_df, original_df)
        work_df, _ = clean_quantity(work_df, original_df)
        work_df, _ = clean_state(work_df, original_df)
        work_df, _ = clean_total_amount(work_df, original_df)
        work_df, _ = clean_unit_price(work_df, original_df)
        work_df, _ = clean_rating(work_df, original_df)
        
        # Safe baseline cleaning functions for categorical dimensions that don't need complex parsing rules
        work_df["order_id"] = work_df["order_id"].fillna(pd.NA).str.strip().str.upper()
        work_df["customer_id"] = work_df["customer_id"].fillna(pd.NA).str.strip().str.upper()
        work_df["city"] = work_df["city"].fillna(pd.NA).str.strip().str.upper()
        work_df["category"] = work_df["category"].fillna(pd.NA).str.strip().str.title()
        work_df["payment_method"] = work_df["payment_method"].fillna(pd.NA).str.strip().str.upper()
        
        # Enforce numeric transformations to drop strings labeled as 'N/A' or 'null' down into real pandas NaN cells
        work_df["discount_pct"] = pd.to_numeric(work_df["discount_pct"].str.replace("%", "", regex=False), errors="coerce") / 100.0
        work_df["discount"] = work_df["discount_pct"].fillna(0.0)
        work_df = work_df.drop(columns=["discount_pct"], errors="ignore")
        
    except Exception as e:
        log.critical(f"Parser execution phase collapsed due to an unhandled data processing exception: {str(e)}")
        raise e

    # -------------------------------------------------------------------------
    # STAGE 3: NULL VALUE IMPUTATION AND STATISTICAL VALIDATION
    # -------------------------------------------------------------------------
    log.info("Routing cleansed dataset columns to statistical null-imputation matrices...")
    
    date_cols = ["order_date"]
    continuous_cols = ["quantity", "unit_price", "total_amount", "rating", "discount"]
    categorical_cols = ["order_id", "customer_id", "product_name", "category", "payment_method", "city", "state", "pincode"]
    
    clean_df, quarantine_null_df, report = handle_nulls(
        df=work_df,
        original_filename=os.path.basename(file_path),
        date_cols=date_cols,
        continuous_cols=continuous_cols,
        categorical_cols=categorical_cols
    )
    
    # -------------------------------------------------------------------------
    # STAGE 4: SILVER LAYER LOAD
    # -------------------------------------------------------------------------
    log.info("Streaming perfectly clean, imputed dataframe arrays down into database Silver storage layer...")
    
    # Type cast normalization to match the clean_silver.orders schema exactly
    clean_df["quantity"] = clean_df["quantity"].astype(int)
    clean_df["unit_price"] = clean_df["unit_price"].astype(float)
    clean_df["discount"] = clean_df["discount"].astype(float)
    clean_df["total_amount"] = clean_df["total_amount"].astype(float)
    clean_df["rating"] = clean_df["rating"].astype(float)
    
    # Write to target database table
    success = load_dataframe_to_table(clean_df, "orders", engine, schema="clean_silver", if_exists="append")
    
    if success:
        log.info("Medallion Core Pipeline run completed successfully for clean_silver schema.")
    else:
        log.error("Pipeline finished execution, but database writers failed to commit transactions.")
        
    return success

if __name__ == "__main__":
    # Test script against your dirty dataset locally
    csv_target = os.path.join(os.path.dirname(__file__), "dirty_dataset.csv")
    run_ingestion_pipeline(csv_target)