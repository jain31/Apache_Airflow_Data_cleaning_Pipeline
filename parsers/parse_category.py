import os
import logging
import pandas as pd

# 1. Isolated Quarantine Directory Configuration
QUARANTINE_DIR = os.path.join("quarantine", "quarantine_category")

# 2. Isolated Logger Setup Configuration for this specific file
def _setup_isolated_logger():
    log_dir = "logger_file"
    os.makedirs(log_dir, exist_ok=True)
    log_filepath = os.path.join(log_dir, "parse_category.log")
    
    file_logger = logging.getLogger("parse_category")
    file_logger.setLevel(logging.INFO)
    
    if not file_logger.handlers:
        handler = logging.FileHandler(log_filepath)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        file_logger.addHandler(handler)
    return file_logger

logger = _setup_isolated_logger()
logger.info("Category parser execution process initiated.")

def _parse_category(val):
    if pd.isnull(val) or str(val).strip() == "" or str(val).lower() in ["nan", "null", "none"]:
        return pd.NA
    
    # Clean whitespace and standardize to Title Case
    cleaned_val = str(val).strip().title()
    return cleaned_val

def clean_category(df, original_df):
    df = df.copy()
    df["category_clean"] = df["category"].apply(_parse_category)

    unparsed_mask = df["category_clean"].isna()
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
            f"quarantine_category_{pd.Timestamp.now().strftime('%d-%m-%y')}.csv",
        )
        quarantine_df.to_csv(quarantine_path, index=True)
        clean_df = df.drop(index=failed_indexes).copy()
    else:
        clean_df = df.copy()

    clean_df = clean_df.drop(columns=["category"])
    clean_df = clean_df.rename(columns={"category_clean": "category"})
    clean_df["category"] = clean_df["category"].astype(str)

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