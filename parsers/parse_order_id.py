import os
import logging
import re
import pandas as pd

# 1. Isolated Quarantine Directory Configuration
QUARANTINE_DIR = os.path.join("quarantine", "quarantine_order_id")

# 2. Isolated Logger Setup Configuration for this specific file
def _setup_isolated_logger():
    log_dir = "logger_file"
    os.makedirs(log_dir, exist_ok=True)
    log_filepath = os.path.join(log_dir, "parse_order_id.log")
    
    file_logger = logging.getLogger("parse_order_id")
    file_logger.setLevel(logging.INFO)
    
    if not file_logger.handlers:
        handler = logging.FileHandler(log_filepath)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        file_logger.addHandler(handler)
    return file_logger

logger = _setup_isolated_logger()
logger.info("Order ID parser execution process initiated.")

def _parse_order_id(val):
    """
    Validates and formats Order IDs.
    Returns pd.NA if the ID is missing or completely invalid, 
    otherwise returns a cleaned alphanumeric uppercase string.
    """
    if pd.isnull(val) or str(val).strip() == "":
        return pd.NA
        
    val_str = str(val).strip().upper()
    
    # Remove common accidental punctuation anomalies but keep alphanumeric strings
    clean_val = re.sub(r'[^\w\-]', '', val_str)
    
    if clean_val == "":
        return pd.NA
        
    return clean_val


def clean_order_id(df, original_df):
    df = df.copy()
    df["order_id_parsed"] = df["order_id"].apply(_parse_order_id)

    # Identify items that evaluated to pd.NA or failed validation checks
    unparsed_mask = df["order_id_parsed"].isna()
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
            f"quarantine_order_id_{pd.Timestamp.now().strftime('%d-%m-%y')}.csv",
        )
        quarantine_df.to_csv(quarantine_path, index=True)
        clean_df = df.drop(index=failed_indexes).copy()
    else:
        clean_df = df.copy()

    clean_df = clean_df.drop(columns=["order_id"])
    clean_df = clean_df.rename(columns={"order_id_parsed": "order_id"})
    clean_df["order_id"] = clean_df["order_id"].astype(str)

    if failed_indexes:
        logger.warning(
            f"status = success | "
            f"total_rows = {total_rows} | "
            f"cleaned_rows = {cleaned_count} | "
            f"quarantine_rows = {quarantine_count} | "
            f"quarantine_path = {quarantine_path}"
        )
    else:
        logger.info(
            f"status = success | "
            f"total_rows = {total_rows} | "
            f"cleaned_rows = {cleaned_count} | "
            f"quarantine_rows = {quarantine_count}"
        )

    return clean_df, failed_indexes