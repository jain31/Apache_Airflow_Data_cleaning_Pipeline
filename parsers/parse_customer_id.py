import os
import re
import logging
import pandas as pd

# 1. Isolated Quarantine Directory Configuration
QUARANTINE_DIR = os.path.join("quarantine", "quarantine_customer_id")

# 2. Isolated Logger Setup Configuration for this specific file
def _setup_isolated_logger():
    log_dir = "logger_file"
    os.makedirs(log_dir, exist_ok=True)
    log_filepath = os.path.join(log_dir, "parse_customer_id.log")
    
    file_logger = logging.getLogger("parse_customer_id")
    file_logger.setLevel(logging.INFO)
    
    if not file_logger.handlers:
        handler = logging.FileHandler(log_filepath)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        file_logger.addHandler(handler)
    return file_logger

logger = _setup_isolated_logger()
logger.info("Customer ID parser execution process initiated.")

# Compilation of target identification regex expressions
valid_pattern = re.compile(r'^cust_\d{4}$')
numeric_only_pattern = re.compile(r'^\d+$')        
underscore_digit_pattern = re.compile(r'^_\d+$')
cust_no_digits_pattern = re.compile(r'^cust_$')

def _parse_customer_id(val):
    if (
        pd.isnull(val)
        or str(val).strip() == ""
        or str(val).lower() in ["nan", "null", "n/a", "<na>"]
    ):
        return pd.NA
        
    val = str(val).strip().lower()
    
    if valid_pattern.match(val):
        return val.upper()  
        
    if numeric_only_pattern.match(val):
        padded_digits = val.zfill(4)
        return f"CUST_{padded_digits}"
        
    if underscore_digit_pattern.match(val):
        digits = val.split('_')[1]
        padded_digits = digits.zfill(4)
        return f"CUST_{padded_digits}"
        
    if cust_no_digits_pattern.match(val):
        return pd.NA    
        
    if val.startswith("cust_"):
        digits = val.replace("cust_", "")
        if digits.isdigit():
            return f"CUST_{digits.zfill(4)}"
            
    return pd.NA

def clean_customer_id(df, original_df):
    df = df.copy()
    df['customer_id_clean'] = df['customer_id'].apply(_parse_customer_id)
    unparsed_total = df['customer_id_clean'].apply(lambda x: not isinstance(x, str))

    failed_indexes = df[unparsed_total].index.tolist()
    
    total_rows = len(df)
    quarantine_count = len(failed_indexes)
    cleaned_count = total_rows - quarantine_count
    quarantine_path = None

    if failed_indexes:
        os.makedirs(QUARANTINE_DIR, exist_ok=True)
        quarantine_rows = original_df.loc[failed_indexes].copy()

        quarantine_path = os.path.join(
            QUARANTINE_DIR,
            f"quarantine_customer_id_{pd.Timestamp.now().strftime('%d-%m-%y')}.csv"
        )
        quarantine_rows.to_csv(quarantine_path, index=True)
        clean_df = df.drop(index=failed_indexes).copy()
    else:
        clean_df = df.copy()

    clean_df = clean_df.drop(columns=["customer_id"])
    clean_df = clean_df.rename(columns={"customer_id_clean": "customer_id"})
    clean_df["customer_id"] = clean_df["customer_id"].astype(str)

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