import os
import logging
import pandas as pd

# 1. Isolated Quarantine Directory Configuration
QUARANTINE_DIR = os.path.join("quarantine", "quarantine_discount")

# 2. Isolated Logger Setup Configuration for this specific file
def _setup_isolated_logger():
    log_dir = "logger_file"
    os.makedirs(log_dir, exist_ok=True)
    log_filepath = os.path.join(log_dir, "parse_discount.log")
    
    file_logger = logging.getLogger("parse_discount")
    file_logger.setLevel(logging.INFO)
    
    if not file_logger.handlers:
        handler = logging.FileHandler(log_filepath)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        file_logger.addHandler(handler)
    return file_logger

logger = _setup_isolated_logger()
logger.info("Discount parser execution process initiated.")

def _parse_discount(val):
    if pd.isnull(val) or str(val).strip() == "":
        return 0.0  # Safe logical fallback assumption if discount blank

    val_str = str(val).strip().lower()
    
    try:
        # Check if formatted explicitly as a percentage string (e.g. '15%')
        is_pct = '%' in val_str
        val_str = val_str.replace('%', '').strip()
        
        num = float(val_str)
        
        # Standardize representation to a fractional float format (e.g., 0.15 instead of 15.0)
        if is_pct or num > 1.0:
            num = num / 100.0
            
        # Business validation boundary sanity check
        if num < 0.0 or num > 1.0:
            return pd.NA
            
        return num
    except ValueError:
        return pd.NA

def clean_discount(df, original_df):
    df = df.copy()
    df["discount_clean"] = df["discount"].apply(_parse_discount)

    unparsed_mask = df["discount_clean"].isna()
    failed_indexes = df[unparsed_mask].index.tolist()

    total_rows = len(df)
    quarantine_count = len(failed_indexes)
    cleaned_count = total_rows - quarantine_count
    quarantine_path = None

    if failed_indexes:
        os.makedirs(QUARANTINE_DIR, exist_ok=True)
        quarantine_df = original_df.loc[failed_indexes].copy()
        quarantine_path = os.path.join(
            QUARANTINE_DIR,
            f"quarantine_discount_{pd.Timestamp.now().strftime('%d-%m-%y')}.csv",
        )
        quarantine_df.to_csv(quarantine_path, index=True)
        clean_df = df.drop(index=failed_indexes).copy()
    else:
        clean_df = df.copy()

    clean_df = clean_df.drop(columns=["discount"])
    clean_df = clean_df.rename(columns={"discount_clean": "discount"})
    clean_df["discount"] = clean_df["discount"].astype(float)

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