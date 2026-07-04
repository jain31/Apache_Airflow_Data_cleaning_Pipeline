import os
import logging
import pandas as pd

# 1. Isolated Quarantine Directory Configuration
QUARANTINE_DIR = os.path.join("quarantine", "quarantine_payment_method")

# 2. Isolated Logger Setup Configuration for this specific file
def _setup_isolated_logger():
    log_dir = "logger_file"
    os.makedirs(log_dir, exist_ok=True)
    log_filepath = os.path.join(log_dir, "parse_payment_method.log")
    
    file_logger = logging.getLogger("parse_payment_method")
    file_logger.setLevel(logging.INFO)
    
    if not file_logger.handlers:
        handler = logging.FileHandler(log_filepath)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        file_logger.addHandler(handler)
    return file_logger

logger = _setup_isolated_logger()
logger.info("Payment Method parser execution process initiated.")

def parse_payment_method(val):
    if pd.isnull(val) or str(val).strip().lower() in ["nan", "null"]:
        return pd.NA
    
    val = str(val).strip()
    val_lower = val.lower()
    
    if val_lower in ["duplicate"]:
        return pd.NA
        
    # Standardizing common valid methods (fixed typo " net anking" to "net banking")
    if val_lower in ["upi", "cod", "wallet", "debit card", "credit card", "net banking"]:
        if val_lower in ["upi", "cod"]:
            return val.upper()
        else:
            return val.title()
            
    # If it's not an recognized valid strategy pattern, mark for quarantine
    return pd.NA

def clean_payment_method(df, original_df):
    df = df.copy()
    df["payment_method_clean"] = df['payment_method'].apply(parse_payment_method)
    
    # Identify items that evaluated to pd.NA or failed validation checks
    unparsed_payment_method = df["payment_method_clean"].isna()
    failed_indexes = df[unparsed_payment_method].index.tolist()

    total_rows = len(df)
    quarantine_count = len(failed_indexes)
    cleaned_count = total_rows - quarantine_count
    quarantine_path = None

    if failed_indexes:
        os.makedirs(QUARANTINE_DIR, exist_ok=True)
        quarantine_rows = original_df.loc[failed_indexes].copy()

        quarantine_path = os.path.join(
            QUARANTINE_DIR,
            f"quarantine_payment_method_{pd.Timestamp.now().strftime('%d-%m-%y')}.csv"
        )
        quarantine_rows.to_csv(quarantine_path, index=True)
        
        clean_df = df.drop(index=failed_indexes).copy()
    else:
        clean_df = df.copy()

    clean_df = clean_df.drop(columns=["payment_method"])
    clean_df = clean_df.rename(columns={"payment_method_clean": "payment_method"})
    clean_df["payment_method"] = clean_df["payment_method"].astype(str)

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