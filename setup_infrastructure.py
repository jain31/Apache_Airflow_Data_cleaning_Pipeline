"""
setup_infrastructure.py
Automated infrastructure setup. Creates the target database if missing
and provisions all schemas, tables, and indexes without syntax errors.
"""

import os
from sqlalchemy import text, create_engine
from db.db_connection import DEFAULT_DB_URI
from logger_file.logger import get_logger

log = get_logger("infrastructure_setup")

def build_medallion_infrastructure():
    # 1. Determine the correct SQL script file path dynamically
    primary_path = os.path.join(os.path.dirname(__file__), "db", "database_design.sql")
    fallback_path = os.path.join(os.path.dirname(__file__), "database_design.sql")
    
    if os.path.exists(primary_path):
        target_sql_path = primary_path
    elif os.path.exists(fallback_path):
        target_sql_path = fallback_path
    else:
        log.error(f"SQL architecture file missing. Checked paths:\n1. {primary_path}\n2. {fallback_path}")
        return False

    try:
        # 2. Connect to default 'sys' database to bootstrap 'medallion_db' if it doesn't exist
        sys_uri = DEFAULT_DB_URI.replace("/medallion_db", "/sys")
        bootstrap_engine = create_engine(sys_uri)
        
        log.info("Checking if database container 'medallion_db' exists on server...")
        with bootstrap_engine.begin() as conn:
            conn.execute(text("CREATE DATABASE IF NOT EXISTS medallion_db;"))
        bootstrap_engine.dispose()

        # 3. Connect directly to our fresh medallion_db database
        engine = create_engine(DEFAULT_DB_URI)
        
        log.info(f"Targeting infrastructure DDL file deployment source: {target_sql_path}")
        with open(target_sql_path, "r") as f:
            sql_script = f.read()

        log.info("Executing DDL scripts to build table architectures...")
        with engine.begin() as connection:
            # Safely split and execute individual SQL queries sequentially
            statements = sql_script.split(";")
            for statement in statements:
                clean_statement = statement.strip()
                if clean_statement:
                    connection.execute(text(clean_statement))
                    
        log.info("Infrastructure status = SUCCESS. Database schemas, tables, and constraints fully initialized.")
        return True
        
    except Exception as e:
        log.critical(f"DDL Infrastructure deployment crashed: {str(e)}")
        return False

if __name__ == "__main__":
    print("--- Starting Medallion Database Schema Setup Process ---")
    build_medallion_infrastructure()