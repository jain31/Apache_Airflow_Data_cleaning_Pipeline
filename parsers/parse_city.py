import os
import logging
import pandas as pd

# 1. Isolated Quarantine Directory Configuration
QUARANTINE_DIR = os.path.join("quarantine", "quarantine_city")

# 2. Isolated Logger Setup Configuration for this specific file
def _setup_isolated_logger():
    log_dir = "logger_file"
    os.makedirs(log_dir, exist_ok=True)
    log_filepath = os.path.join(log_dir, "parse_city.log")
    
    file_logger = logging.getLogger("parse_city")
    file_logger.setLevel(logging.INFO)
    
    if not file_logger.handlers:
        handler = logging.FileHandler(log_filepath)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        file_logger.addHandler(handler)
    return file_logger

logger = _setup_isolated_logger()
logger.info("City parser execution process initiated.")

def _parse_city(val):
    if pd.isnull(val) or str(val).strip() == "" or str(val).lower() in ["nan", "null", "none"]:
        return pd.NA
    
    # Strip whitespace, strip trailing punctuation accents, convert to Title Case
    cleaned_val = str(val).strip().title()
    
    # Discard pure numeric strings accidentally entered under city name
    if cleaned_val.isdigit():
        return pd.NA
        
    return cleaned_val

def clean_city(df, original_df):
    df = df.copy()
    df["city_clean"] = df["city"].apply(_parse_city)

    unparsed_mask = df["city_clean"].isna()
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
            f"quarantine_city_{pd.Timestamp.now().strftime('%d-%m-%y')}.csv",
        )
        quarantine_df.to_csv(quarantine_path, index=True)
        clean_df = df.drop(index=failed_indexes).copy()
    else:
        clean_df = df.copy()

    clean_df = clean_df.drop(columns=["city"])
    clean_df = clean_df.rename(columns={"city_clean": "city"})
    clean_df["city"] = clean_df["city"].astype(str)

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