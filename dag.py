"""
dag.py
Apache Airflow DAG workflow orchestrator for the Medallion Data Pipeline using MySQL.
Triggers ingestion, cleans raw columns, performs null imputations, and updates 
the Silver and Gold database layers.
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Fallback path adjustment to ensure Airflow worker clusters can import pipeline.py
DAG_ROOT = str(Path(__file__).resolve().parent)
if DAG_ROOT not in sys.path:
    sys.path.insert(0, DAG_ROOT)

# Using MySqlOperator for native MySQL query execution blocks
from airflow.providers.mysql.operators.mysql import MySqlOperator
from pipeline import run_ingestion_pipeline
from logger_file.logger import get_logger

log = get_logger("airflow_dag")

# --- Configuration & Paths ---
DIRTY_DATASET_PATH = os.getenv("INGESTION_SOURCE_FILE", "/opt/airflow/dags/dirty_dataset.csv")
DB_CONN_ID = "mysql_medallion"  # Match this unique identifier string with your Airflow Connection UI settings

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def _execute_cleaning_pipeline_task(**kwargs):
    """
    Task wrapper block executing the data clean, transform, and silver load loop.
    """
    log.info(f"Airflow Task Context: Launching end-to-end cleaning runtime loop on {DIRTY_DATASET_PATH}")
    success = run_ingestion_pipeline(DIRTY_DATASET_PATH)
    
    if not success:
        raise ValueError("The localized file processing operations encountered errors. Terminating DAG execution.")
    log.info("Airflow Task Context: Pipeline layer parsing successfully accomplished.")

# --- DAG Definition ---
with DAG(
    dag_id="medallion_ingestion_pipeline_mysql",
    default_args=default_args,
    description="Cleans, standardizes, and processes input sales orders into Silver/Gold analytical MySQL layers.",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["medallion", "ingestion", "mysql"],
) as dag:

    # Task 1: Execute Python Column Parsers & Null Imputations -> Load Clean Silver Table
    clean_and_load_silver = PythonOperator(
        task_id="clean_and_load_silver",
        python_callable=_execute_cleaning_pipeline_task,
    )

    # Task 2: Build Fact Sales Data for Gold Analytical Layer from pristine Silver Records via UPSERT
    refresh_gold_fact_sales = MySqlOperator(
        task_id="refresh_gold_fact_sales",
        mysql_conn_id=DB_CONN_ID,
        sql="""
            INSERT INTO analytical_gold.fact_sales_performance 
                (order_id, order_date, customer_id, category, net_revenue, units_sold, customer_rating)
            SELECT 
                order_id,
                DATE(order_date) AS order_date,
                customer_id,
                category,
                (quantity * unit_price) - (quantity * unit_price * discount) AS net_revenue,
                quantity AS units_sold,
                rating AS customer_rating
            FROM clean_silver.orders
            ON DUPLICATE KEY UPDATE
                net_revenue = VALUES(net_revenue),
                units_sold = VALUES(units_sold),
                customer_rating = VALUES(customer_rating);
        """,
    )

    # Task 3: Build Aggregated Dimensional Summary Tables for BI Dashboard Consumption via UPSERT
    refresh_gold_dim_city_summary = MySqlOperator(
        task_id="refresh_gold_dim_city_summary",
        mysql_conn_id=DB_CONN_ID,
        sql="""
            INSERT INTO analytical_gold.dim_city_daily_summary 
                (sales_date, city, state, total_orders, gross_revenue, avg_rating)
            SELECT 
                DATE(order_date) AS sales_date,
                city,
                state,
                COUNT(DISTINCT order_id) AS total_orders,
                SUM(total_amount) AS gross_revenue,
                AVG(rating) AS avg_rating
            FROM clean_silver.orders
            GROUP BY DATE(order_date), city, state
            ON DUPLICATE KEY UPDATE
                total_orders = VALUES(total_orders),
                gross_revenue = VALUES(gross_revenue),
                avg_rating = VALUES(avg_rating),
                updated_at = CURRENT_TIMESTAMP;
        """,
    )

    # --- Task Dependency Architecture Structure ---
    clean_and_load_silver >> [refresh_gold_fact_sales, refresh_gold_dim_city_summary]