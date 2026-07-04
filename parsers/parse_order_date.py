import os
from datetime import datetime
import logging
import pandas as pd

# 1. Isolated Quarantine Directory Configuration
QUARANTINE_DIR = os.path.join("quarantine", "quarantine_date")
ist_offset = pd.Timedelta(hours=5, minutes=30)

# 2. Isolated Logger Setup Configuration for this specific file
def _setup_isolated_logger():
    log_dir = "logger_file"
    os.makedirs(log_dir, exist_ok=True)
    log_filepath = os.path.join(log_dir, "parse_order_date.log")
    
    file_logger = logging.getLogger("parse_order_date")
    file_logger.setLevel(logging.INFO)
    
    if not file_logger.handlers:
        handler = logging.FileHandler(log_filepath)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        file_logger.addHandler(handler)
    return file_logger

logger = _setup_isolated_logger()
logger.info("Order Date parser execution process initiated.")

def _parse_order_date(val):
    date_formats = [
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%d-%m-%Y",
        "%b %d, %Y",
        "%d-%b-%Y",
    ]

    if pd.isnull(val) or str(val).strip() == "":
        return pd.NaT

    if isinstance(val, (pd.Timestamp, datetime)):
        return val

    val = str(val).strip()

    if val.isdigit():
        return pd.Timestamp(int(val), unit="s") + ist_offset

    for fmt in date_formats:
        try:
            return datetime.strptime(val, fmt)
        except ValueError:
            continue

    return val


def clean_date(df, original_df):
    df = df.copy()
    df["order_date_parsed"] = df["order_date"].apply(_parse_order_date)

    unparsed_mask = df["order_date_parsed"].apply(lambda x: isinstance(x, str))
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
            f"quarantine_date_{pd.Timestamp.now().strftime('%d-%m-%y')}.csv",
        )
        quarantine_df.to_csv(quarantine_path, index=True)
        clean_df = df.drop(index=failed_indexes).copy()
    else:
        clean_df = df.copy()

    clean_df = clean_df.drop(columns=["order_date"])
    clean_df = clean_df.rename(columns={"order_date_parsed": "order_date"})
    clean_df["order_date"] = pd.to_datetime(
        clean_df["order_date"], errors="coerce"
    )

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