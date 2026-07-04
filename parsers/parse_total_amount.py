import os
import logging
import pandas as pd

# 1. Isolated Quarantine Directory Configuration
QUARANTINE_DIR = os.path.join("quarantine", "quarantine_total_amount")

# 2. Isolated Logger Setup Configuration for this specific file
def _setup_isolated_logger():
    log_dir = "logger_file"
    os.makedirs(log_dir, exist_ok=True)
    log_filepath = os.path.join(log_dir, "parse_total_amount.log")
    
    file_logger = logging.getLogger("parse_total_amount")
    file_logger.setLevel(logging.INFO)
    
    if not file_logger.handlers:
        handler = logging.FileHandler(log_filepath)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        file_logger.addHandler(handler)
    return file_logger

logger = _setup_isolated_logger()
logger.info("Total Amount parser execution process initiated.")

def parse_total_amount(val):
    if pd.isnull(val) or str(val).strip() == "":
        return pd.NA
        
    val = str(val).strip()
    # Strip out currency markers, punctuation noise, and accidental negative signs
    val = val.replace("Rs", "").replace("$", "").replace("-", "")
    val = val.replace(",", "").strip()
    
    try:
        return float(val)
    except ValueError:
        # Returns pd.NA for non-numeric messy strings to trigger quarantine route
        return pd.NA
    
def clean_total_amount(df, original_df):
    df = df.copy()
    df['total_amount_clean'] = df['total_amount'].apply(parse_total_amount)
    
    # Identify items that evaluated to pd.NA (failed cleaning/parsing rules)
    unparsed_total = df['total_amount_clean'].isna()
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
            f"quarantine_total_amount_{pd.Timestamp.now().strftime('%d-%m-%y')}.csv"
        )
        quarantine_rows.to_csv(quarantine_path, index=True)
        
        clean_df = df.drop(index=failed_indexes).copy()
    else:
        clean_df = df.copy()

    clean_df = clean_df.drop(columns=["total_amount"])
    clean_df = clean_df.rename(columns={"total_amount_clean": "total_amount"})
    clean_df["total_amount"] = clean_df["total_amount"].astype(float)

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