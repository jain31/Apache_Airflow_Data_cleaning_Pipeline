import os
import logging
import pandas as pd

# 1. Isolated Quarantine Directory Configuration
QUARANTINE_DIR = os.path.join("quarantine", "quarantine_unit_price")

# 2. Isolated Logger Setup Configuration for this specific file
def _setup_isolated_logger():
    log_dir = "logger_file"
    os.makedirs(log_dir, exist_ok=True)
    log_filepath = os.path.join(log_dir, "parse_unit_price.log")
    
    file_logger = logging.getLogger("parse_unit_price")
    file_logger.setLevel(logging.INFO)
    
    if not file_logger.handlers:
        handler = logging.FileHandler(log_filepath)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        file_logger.addHandler(handler)
    return file_logger

logger = _setup_isolated_logger()
logger.info("Unit Price parser execution process initiated.")

def _parse_unit_price(val):
    if pd.isnull(val) or str(val).strip() == "":
        return pd.NA
    
    val = str(val).strip()
    if val.lower() == 'inf':
        return pd.NA
        
    # Strip currency noise, commas, and formatting trailing symbols like '#'
    val = val.replace("Rs.", "").replace("$", "").strip()
    val = val.replace(",", "")
    val = val.rstrip("#").rstrip(".").strip()

    try:
        return float(val)
    except ValueError:
        # Mark unparseable data as pd.NA to route it to quarantine
        return pd.NA

def clean_unit_price(df, original_df):
    df = df.copy()
    df['unit_price_clean'] = df['unit_price'].apply(_parse_unit_price)
    
    # Identify items that evaluated to pd.NA (failed validation rules)
    unparsed_unit_price = df['unit_price_clean'].isna()
    failed_indexes = df[unparsed_unit_price].index.tolist()

    total_rows = len(df)
    quarantine_count = len(failed_indexes)
    cleaned_count = total_rows - quarantine_count
    quarantine_path = None

    if failed_indexes:
        os.makedirs(QUARANTINE_DIR, exist_ok=True)
        quarantine_rows = original_df.loc[failed_indexes].copy()

        quarantine_path = os.path.join(
            QUARANTINE_DIR,
            f"quarantine_unit_price_{pd.Timestamp.now().strftime('%d-%m-%y')}.csv"
        )
        quarantine_rows.to_csv(quarantine_path, index=True)
        
        clean_df = df.drop(index=failed_indexes).copy()
    else:
        clean_df = df.copy()

    clean_df = clean_df.drop(columns=["unit_price"])
    clean_df = clean_df.rename(columns={"unit_price_clean": "unit_price"})
    clean_df["unit_price"] = clean_df["unit_price"].astype(float)

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