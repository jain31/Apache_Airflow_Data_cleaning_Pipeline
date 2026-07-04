import os
import re
import logging
import pandas as pd

# 1. Isolated Quarantine Directory Configuration
QUARANTINE_DIR = os.path.join("quarantine", "quarantine_phone")

# 2. Isolated Logger Setup Configuration for this specific file
def _setup_isolated_logger():
    log_dir = "logger_file"
    os.makedirs(log_dir, exist_ok=True)
    log_filepath = os.path.join(log_dir, "parse_phone.log")
    
    file_logger = logging.getLogger("parse_phone")
    file_logger.setLevel(logging.INFO)
    
    if not file_logger.handlers:
        handler = logging.FileHandler(log_filepath)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        file_logger.addHandler(handler)
    return file_logger

logger = _setup_isolated_logger()
logger.info("Phone parser execution process initiated.")

def parse_phone(val):
    if pd.isnull(val) or str(val).strip() == "":
        return pd.NA
        
    val = str(val).strip()
    digits = re.sub(r'\D', '', val)
    
    # Strip country code prefixes to standardize down to local 10 digits
    if len(digits) == 12 and digits.startswith("91"):
        digits = digits[2:]
    elif len(digits) == 11 and digits.startswith("0"):
        digits = digits[1:]
        
    if len(digits) == 10:
        return digits
    else:
        # If it doesn't resolve to exactly 10 numeric units, return pd.NA to route to quarantine
        return pd.NA
    
def clean_phone(df, original_df):
    df = df.copy()
    df['phone_clean'] = df['phone'].apply(parse_phone)
    
    # Identify items that evaluated to pd.NA or failed validation checks
    unparsed_phone = df['phone_clean'].isna()
    failed_indexes = df[unparsed_phone].index.tolist()

    total_rows = len(df)
    quarantine_count = len(failed_indexes)
    cleaned_count = total_rows - quarantine_count
    quarantine_path = None

    if failed_indexes:
        os.makedirs(QUARANTINE_DIR, exist_ok=True)
        quarantine_rows = original_df.loc[failed_indexes].copy()

        quarantine_path = os.path.join(
            QUARANTINE_DIR,
            f"quarantine_phone_{pd.Timestamp.now().strftime('%d-%m-%y')}.csv"
        )
        quarantine_rows.to_csv(quarantine_path, index=True)
        
        clean_df = df.drop(index=failed_indexes).copy()
    else:
        clean_df = df.copy()

    clean_df = clean_df.drop(columns=["phone"])
    clean_df = clean_df.rename(columns={"phone_clean": "phone"})
    clean_df["phone"] = clean_df["phone"].astype(str)

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