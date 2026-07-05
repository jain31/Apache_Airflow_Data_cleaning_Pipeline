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
# FIX: Aligned perfectly to match ingestion mappings exactly
COLUMN_ALIAS = {
    "order_id": ["order id", "ord_id", "order_no"],
    "customer_id": ["customer id", "cust_id"],
    "order_date": ["date", "order_date", "order date"],
    "product_name": ["product", "item_name", "product name"],
    "category": ["category", "type"],
    "quantity": ["qty", "count", "quantity"],
    "unit_price": ["price", "rate", "unit_price", "unit price"],
    "total_amount": ["total", "amount", "total_amount", "total amount"],
    "payment_method": ["payment", "method", "payment method"],
    "city": ["city", "location"],
    "state": ["state", "region"],
    "phone": ["phone_number", "telephone", "phone"],
    "pincode": ["zip", "zipcode", "pincode"],
    "rating": ["stars", "score", "rating"],
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
            
            # FIX: Added defensive existence check for 'discount' column before transforming string fields
            if "discount" not in work_df.columns:
                # If a parser dropped or isolated it, check original copy or initialize gracefully
                if "discount" in original_df.columns:
                    work_df["discount"] = original_df["discount"]
                else:
                    work_df["discount"] = 0.0

            # Simple categorical strings transformation loops
            work_df["order_id"] = work_df["order_id"].fillna(pd.NA).astype(str).str.strip().str.upper()
            work_df["customer_id"] = work_df["customer_id"].fillna(pd.NA).astype(str).str.strip().str.upper()
            work_df["city"] = work_df["city"].fillna(pd.NA).astype(str).str.strip().str.upper()
            work_df["category"] = work_df["category"].fillna(pd.NA).astype(str).str.strip().str.title()
            work_df["payment_method"] = work_df["payment_method"].fillna(pd.NA).astype(str).str.strip().str.upper()
            work_df["discount"] = pd.to_numeric(work_df["discount"], errors="coerce").fillna(0.0)
            
        except Exception as e:
            log.error(f"Parser execution phase crashed on file {file_path.name}: {str(e)}")
            continue

        # 4. Impute Null Values
        date_cols = ["order_date"]
        continuous_cols = ["quantity", "unit_price", "total_amount", "rating", "discount"]
        categorical_cols = ["order_id", "customer_id", "product_name", "category", "payment_method", "city", "state", "pincode"]
        
        clean_df, _, _ = handle_nulls(work_df, file_path.name, date_cols, continuous_cols, categorical_cols)

        # Ensure correct datatypes safely before database writing pipelines
        clean_df["quantity"] = pd.to_numeric(clean_df["quantity"], errors="coerce").fillna(1).astype(int)
        clean_df["unit_price"] = pd.to_numeric(clean_df["unit_price"], errors="coerce").fillna(0.0).astype(float)
        clean_df["total_amount"] = pd.to_numeric(clean_df["total_amount"], errors="coerce").fillna(0.0).astype(float)
        clean_df["rating"] = pd.to_numeric(clean_df["rating"], errors="coerce").fillna(0.0).astype(float)

        # FIX: Normalize whole percentage numbers (e.g., 15.0 -> 0.15) to match MySQL DECIMAL range constraints
        clean_df["discount"] = pd.to_numeric(clean_df["discount"], errors="coerce").fillna(0.0).astype(float)
        clean_df.loc[clean_df["discount"] > 1.0, "discount"] = clean_df["discount"] / 100.0

        # 5. Write Pristine Records into Silver MySQL Database Layer
        success = load_dataframe_to_table(clean_df, "orders", engine, schema="clean_silver", if_exists="append")
        
        if success:
            # 6. Run Analytical Gold Summaries
            run_gold_aggregations(engine)
            log.info(f"Pipeline finished completely for: {file_path.name}")

if __name__ == "__main__":
    execute_pipeline()