"""
pipeline.py
The master non-Airflow orchestrator. Run this file directly via terminal:
$ python pipeline.py
"""

import os
from pathlib import Path
import pandas as pd
from sqlalchemy import text
from logger_file.logger import get_logger

# Import structural ingestion steps
from ingestion import process_file, _discover_files

# Import your custom standalone parsers
from parsers.parse_order_date import clean_date
from parsers.parse_phone import clean_phone
from parsers.parse_pincode import clean_pincode
from parsers.parse_product_name import clean_product_name
from parsers.parse_quantity import clean_quantity
from parsers.parse_state import clean_state
from parsers.parse_total_amount import clean_total_amount
from parsers.parse_unit_price import clean_unit_price
from parsers.parse_rating import clean_rating

# Import Null Handling Component
from null_values.null import handle_nulls

# Import Database Interaction Engine
from db.db_connection import get_database_engine
from db.db_writes import load_dataframe_to_table

log = get_logger("standalone_pipeline")

# Define target columns and headers expected by the parser
COLUMN_ALIAS = {
    "order_id": ["order id", "ord_id"],
    "customer_id": ["customer id", "cust_id"],
    "order_date": ["date", "order_date"],
    "product_name": ["product", "item_name"],
    "category": ["category", "type"],
    "quantity": ["qty", "count"],
    "unit_price": ["price", "rate"],
    "total_amount": ["total", "amount"],
    "payment_method": ["payment", "method"],
    "city": ["city", "location"],
    "state": ["state", "region"],
    "phone": ["phone_number", "telephone"],
    "pincode": ["zip", "zipcode"],
    "rating": ["stars", "score"],
    "discount": ["discount", "discount_pct", "disc", "discount_percentage"]
}

def run_gold_aggregations(engine):
    """Executes Gold Layer business aggregations directly inside MySQL."""
    log.info("Computing analytical Gold Layer views...")
    
    fact_sales_sql = """
        INSERT INTO analytical_gold.fact_sales_performance 
            (order_id, order_date, customer_id, category, net_revenue, units_sold, customer_rating)
        SELECT 
            order_id, DATE(order_date), customer_id, category,
            (quantity * unit_price) - (quantity * unit_price * discount) AS net_revenue,
            quantity AS units_sold, rating AS customer_rating
        FROM clean_silver.orders
        ON DUPLICATE KEY UPDATE
            net_revenue = VALUES(net_revenue),
            units_sold = VALUES(units_sold),
            customer_rating = VALUES(customer_rating);
    """
    
    dim_city_sql = """
        INSERT INTO analytical_gold.dim_city_daily_summary 
            (sales_date, city, state, total_orders, gross_revenue, avg_rating)
        SELECT 
            DATE(order_date) AS sales_date, city, state,
            COUNT(DISTINCT order_id) AS total_orders,
            SUM(total_amount) AS gross_revenue, AVG(rating) AS avg_rating
        FROM clean_silver.orders
        GROUP BY DATE(order_date), city, state
        ON DUPLICATE KEY UPDATE
            total_orders = VALUES(total_orders),
            gross_revenue = VALUES(gross_revenue),
            avg_rating = VALUES(avg_rating),
            updated_at = CURRENT_TIMESTAMP;
    """
    
    try:
        with engine.begin() as conn:
            conn.execute(text(fact_sales_sql))
            conn.execute(text(dim_city_sql))
        log.info("Gold analytical transformation summaries compiled successfully.")
    except Exception as e:
        log.error(f"Failed to generate Gold summary matrices: {str(e)}")


def execute_pipeline():
    """Finds new files in your directory, cleans columns, handles nulls, and saves to MySQL."""
    engine = get_database_engine()
    
    # 1. Look for waiting CSV documents inside your project directory folder
    files_to_process = _discover_files()
    if not files_to_process:
        log.info("No fresh CSV source documents found to clean.")
        return

    for file_path in files_to_process:
        log.info(f"Processing raw document stream: {file_path.name}")
        
        # 2. Run ingestion deduplication and file checking
        incremental_df = process_file(file_path, COLUMN_ALIAS, engine)
        if incremental_df is None or incremental_df.empty:
            log.info(f"Skipping {file_path.name}: Either duplicate or empty.")
            continue
            
        original_df = incremental_df.copy()
        work_df = incremental_df.copy()
        
        # 3. Clean Individual Columns
        try:
            work_df, _ = clean_date(work_df, original_df)
            work_df, _ = clean_phone(work_df, original_df)
            work_df, _ = clean_pincode(work_df, original_df)
            work_df, _ = clean_product_name(work_df, original_df)
            work_df, _ = clean_quantity(work_df, original_df)
            work_df, _ = clean_state(work_df, original_df)
            work_df, _ = clean_total_amount(work_df, original_df)
            work_df, _ = clean_unit_price(work_df, original_df)
            work_df, _ = clean_rating(work_df, original_df)
            
            # Simple categorical strings
            work_df["order_id"] = work_df["order_id"].fillna(pd.NA).str.strip().str.upper()
            work_df["customer_id"] = work_df["customer_id"].fillna(pd.NA).str.strip().str.upper()
            work_df["city"] = work_df["city"].fillna(pd.NA).str.strip().str.upper()
            work_df["category"] = work_df["category"].fillna(pd.NA).str.strip().str.title()
            work_df["payment_method"] = work_df["payment_method"].fillna(pd.NA).str.strip().str.upper()
            work_df["discount"] = pd.to_numeric(work_df["discount"], errors="coerce").fillna(0.0)
        except Exception as e:
            log.error(f"Parser execution phase crashed on file {file_path.name}: {str(e)}")
            continue

        # 4. Impute Null Values
        date_cols = ["order_date"]
        continuous_cols = ["quantity", "unit_price", "total_amount", "rating", "discount"]
        categorical_cols = ["order_id", "customer_id", "product_name", "category", "payment_method", "city", "state", "pincode"]
        
        clean_df, _, _ = handle_nulls(work_df, file_path.name, date_cols, continuous_cols, categorical_cols)

        # Ensure correct datatypes before writing to DB
        clean_df["quantity"] = clean_df["quantity"].astype(int)
        clean_df["unit_price"] = clean_df["unit_price"].astype(float)
        clean_df["discount"] = clean_df["discount"].astype(float)
        clean_df["total_amount"] = clean_df["total_amount"].astype(float)
        clean_df["rating"] = clean_df["rating"].astype(float)

        # 5. Write Pristine Records into Silver MySQL Database Layer
        success = load_dataframe_to_table(clean_df, "orders", engine, schema="clean_silver", if_exists="append")
        
        if success:
            # 6. Run Analytical Gold Summaries
            run_gold_aggregations(engine)
            log.info(f"Pipeline finished completely for: {file_path.name}")

if __name__ == "__main__":
    execute_pipeline()