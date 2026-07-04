import os
import logging
import pandas as pd

# 1. Isolated Quarantine Directory Configuration
QUARANTINE_DIR = os.path.join("quarantine", "quarantine_rating")

# 2. Isolated Logger Setup Configuration for this specific file
def _setup_isolated_logger():
    log_dir = "logger_file"
    os.makedirs(log_dir, exist_ok=True)
    log_filepath = os.path.join(log_dir, "parse_rating.log")
    
    file_logger = logging.getLogger("parse_rating")
    file_logger.setLevel(logging.INFO)
    
    if not file_logger.handlers:
        handler = logging.FileHandler(log_filepath)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        file_logger.addHandler(handler)
    return file_logger

logger = _setup_isolated_logger()
logger.info("Rating parser execution process initiated.")

def parse_rating(val):
    if pd.isnull(val) or str(val).strip() == "":
        return 0.0  # Safe default if rating is simply blank
        
    val_str = str(val).strip()
    if val_str.lower() in ["nan", "null", "none"]:
        return 0.0

    try:
        n_val = float(val_str)
        # Handle accidental negative values safely
        n_val = abs(n_val)
        
        # Validation constraint: Ratings must fall within standard 1 to 5 bounds
        if 1.0 <= n_val <= 5.0:
            return round(n_val, 1)
        elif n_val == 0.0:
            return 0.0
        else:
            # If rating values are out of bounds (e.g. 54.0), route to quarantine
            return pd.NA
    except ValueError:
        # String entries go straight to quarantine
        return pd.NA

def clean_rating(df, original_df):
    df = df.copy()
    df['rating_clean'] = df['rating'].apply(parse_rating)
    
    # Identify items that evaluated to pd.NA (failed processing constraints)
    unparsed_rating = df['rating_clean'].isna()
    failed_indexes = df[unparsed_rating].index.tolist()

    total_rows = len(df)
    quarantine_count = len(failed_indexes)
    cleaned_count = total_rows - quarantine_count
    quarantine_path = None

    if failed_indexes:
        os.makedirs(QUARANTINE_DIR, exist_ok=True)
        quarantine_rows = original_df.loc[failed_indexes].copy()

        quarantine_path = os.path.join(
            QUARANTINE_DIR,
            f"quarantine_rating_{pd.Timestamp.now().strftime('%d-%m-%y')}.csv"
        )
        quarantine_rows.to_csv(quarantine_path, index=True)
        
        clean_df = df.drop(index=failed_indexes).copy()
    else:
        clean_df = df.copy()

    clean_df = clean_df.drop(columns=["rating"])
    clean_df = clean_df.rename(columns={"rating_clean": "rating"})
    clean_df["rating"] = clean_df["rating"].astype(float)

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