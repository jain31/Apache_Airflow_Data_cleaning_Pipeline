import os
import logging
import pandas as pd

# 1. Isolated Quarantine Directory Configuration
QUARANTINE_DIR = os.path.join("quarantine", "quarantine_quantity")

# 2. Isolated Logger Setup Configuration for this specific file
def _setup_isolated_logger():
    log_dir = "logger_file"
    os.makedirs(log_dir, exist_ok=True)
    log_filepath = os.path.join(log_dir, "parse_quantity.log")
    
    file_logger = logging.getLogger("parse_quantity")
    file_logger.setLevel(logging.INFO)
    
    if not file_logger.handlers:
        handler = logging.FileHandler(log_filepath)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        file_logger.addHandler(handler)
    return file_logger

logger = _setup_isolated_logger()
logger.info("Quantity parser execution process initiated.")

def parse_quantity(val):
    if pd.isnull(val) or str(val).strip() == "":
        return pd.NA
    
    val_str = str(val).strip()
    if val_str.lower() in ["nan", "null", "none"]:
        return pd.NA
        
    try:
        # Convert to float first to safely catch decimal format numbers (e.g., '2.0')
        # Apply abs() to fix negative entries, then cast to an integer
        n_val = float(val_str)
        return int(abs(n_val))
    except ValueError:
        # Text entries go straight to quarantine
        return pd.NA

def clean_quantity(df, original_df):
    df = df.copy()
    df['quantity_clean'] = df['quantity'].apply(parse_quantity)
    
    # Identify items that evaluated to pd.NA (missing or unparseable text values)
    unparsed_quantity = df['quantity_clean'].isna()
    failed_indexes = df[unparsed_quantity].index.tolist()

    total_rows = len(df)
    quarantine_count = len(failed_indexes)
    cleaned_count = total_rows - quarantine_count
    quarantine_path = None

    if failed_indexes:
        os.makedirs(QUARANTINE_DIR, exist_ok=True)
        quarantine_rows = original_df.loc[failed_indexes].copy()

        quarantine_path = os.path.join(
            QUARANTINE_DIR,
            f"quarantine_quantity_{pd.Timestamp.now().strftime('%d-%m-%y')}.csv"
        )
        quarantine_rows.to_csv(quarantine_path, index=True)
        
        clean_df = df.drop(index=failed_indexes).copy()
    else:
        clean_df = df.copy()

    clean_df = clean_df.drop(columns=["quantity"])
    clean_df = clean_df.rename(columns={"quantity_clean": "quantity"})
    clean_df["quantity"] = clean_df["quantity"].astype(int)

    if failed_indexes:
        logger.warning(
            f"status = success | total_rows = {total_rows} | "
            f"cleaned_rows = {cleaned_count} | quarantine_rows = {quarantine_count} | "
            f"quarantine_path = {quarantine_path}"
        )
    else:
        logger.info(
            f"status = success | total_rows = {total_rows} | "
            f"cleaned_rows = {cleaned_count} | quarantine_rows = {quarantine_count}"
        )

    return clean_df, failed_indexes