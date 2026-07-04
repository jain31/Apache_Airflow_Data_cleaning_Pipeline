import os
import re
import logging
import pandas as pd

# 1. Isolated Quarantine Directory Configuration
QUARANTINE_DIR = os.path.join("quarantine", "quarantine_product_name")

# 2. Isolated Logger Setup Configuration for this specific file
def _setup_isolated_logger():
    log_dir = "logger_file"
    os.makedirs(log_dir, exist_ok=True)
    log_filepath = os.path.join(log_dir, "parse_product_name.log")
    
    file_logger = logging.getLogger("parse_product_name")
    file_logger.setLevel(logging.INFO)
    
    if not file_logger.handlers:
        handler = logging.FileHandler(log_filepath)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        file_logger.addHandler(handler)
    return file_logger

logger = _setup_isolated_logger()
logger.info("Product Name parser execution process initiated.")

def parse_product_name(val):   
    if (
        pd.isnull(val)
        or str(val).strip() == ""
        or str(val).lower() in ["nan", "null", "n/a", "<na>"]
    ):
        return pd.NA

    val = str(val).strip()

    # Removes extra text following hyphens, commas, or parentheses, and normalizes spacing
    val = re.sub(r"\s*([-(,].*)$", "", val)
    val = re.sub(r"\s+", " ", val)

    cleaned = val.strip()
    return cleaned if cleaned != "" else pd.NA

def clean_product_name(df, original_df):
    df = df.copy()
    df["product_name_clean"] = df['product_name'].apply(parse_product_name)
    
    # Identify items that evaluated to pd.NA (missing or completely stripped out)
    unparsed_product_name = df["product_name_clean"].isna()
    failed_indexes = df[unparsed_product_name].index.tolist()

    total_rows = len(df)
    quarantine_count = len(failed_indexes)
    cleaned_count = total_rows - quarantine_count
    quarantine_path = None

    if failed_indexes:
        os.makedirs(QUARANTINE_DIR, exist_ok=True)
        quarantine_rows = original_df.loc[failed_indexes].copy()

        quarantine_path = os.path.join(
            QUARANTINE_DIR,
            f"quarantine_product_name_{pd.Timestamp.now().strftime('%d-%m-%y')}.csv"
        )
        quarantine_rows.to_csv(quarantine_path, index=True)
        
        clean_df = df.drop(index=failed_indexes).copy()
    else:
        clean_df = df.copy()

    clean_df = clean_df.drop(columns=["product_name"])
    clean_df = clean_df.rename(columns={"product_name_clean": "product_name"})
    clean_df["product_name"] = clean_df["product_name"].astype(str)

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