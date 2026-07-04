-- database_design.sql
-- Medallion Schema Architecture Setup Script for Ingestion Pipelines

CREATE SCHEMA IF NOT EXISTS raw_bronze;
CREATE SCHEMA IF NOT EXISTS clean_silver;
CREATE SCHEMA IF NOT EXISTS analytical_gold;

-------------------------------------------------------------------------------
-- 1. BRONZE LAYER (raw_bronze)
-- Purpose: Schema-less/Loose-typing landing area. Captures raw CSV state exactly 
-- as received, prior to executing parsing scripts.
-------------------------------------------------------------------------------

DROP TABLE IF EXISTS raw_bronze.orders_landing CASCADE;
CREATE TABLE raw_bronze.orders_landing (
    ingestion_id     BIGSERIAL PRIMARY KEY,
    order_id         VARCHAR(255),
    customer_id      VARCHAR(255),
    order_date       VARCHAR(255),
    product_name     VARCHAR(255),
    category         VARCHAR(255),
    quantity         VARCHAR(255),
    unit_price       VARCHAR(255),
    discount         VARCHAR(255),
    total_amount     VARCHAR(255),
    payment_method   VARCHAR(255),
    city             VARCHAR(255),
    state            VARCHAR(255),
    rating           VARCHAR(255),
    inserted_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-------------------------------------------------------------------------------
-- 2. SILVER LAYER (clean_silver)
-- Purpose: Cleaned, strictly typed, structured, and parsed records.
-- Fully cleared of nulls and formatting issues by your Python parsers.
-------------------------------------------------------------------------------

DROP TABLE IF EXISTS clean_silver.orders CASCADE;
CREATE TABLE clean_silver.orders (
    order_id         VARCHAR(100) PRIMARY KEY, -- Cleaned Alphanumeric String
    customer_id      VARCHAR(50) NOT NULL,    -- Structured Format CUST_XXXX
    order_date       TIMESTAMP NOT NULL,      -- Evaluated and Standardized Timestamp
    product_name     VARCHAR(255) NOT NULL,   -- Stripped and Normalized Strings
    category         VARCHAR(100) NOT NULL,   -- Title-Cased Category Label
    quantity         INT NOT NULL CHECK (quantity > 0), 
    unit_price       NUMERIC(12, 2) NOT NULL CHECK (unit_price >= 0),
    discount         NUMERIC(5, 4) DEFAULT 0.0000 CHECK (discount BETWEEN 0.0 AND 1.0),
    total_amount     NUMERIC(14, 2) NOT NULL CHECK (total_amount >= 0),
    payment_method   VARCHAR(50) NOT NULL,    -- Standardized [UPI, COD, Card, etc.]
    city             VARCHAR(100) NOT NULL,
    state            VARCHAR(100) NOT NULL,
    rating           NUMERIC(2, 1) DEFAULT 0.0 CHECK (rating BETWEEN 0.0 AND 5.0),
    processed_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Performance Optimization Indexes for Silver Processing & Merges
CREATE INDEX idx_silver_orders_date ON clean_silver.orders(order_date);
CREATE INDEX idx_silver_orders_customer ON clean_silver.orders(customer_id);
CREATE INDEX idx_silver_orders_category ON clean_silver.orders(category);

-------------------------------------------------------------------------------
-- 3. GOLD LAYER (analytical_gold)
-- Purpose: Aggregated business-ready dimensional modeling tables.
-------------------------------------------------------------------------------

-- Fact Table: Highly optimized for fast business metrics compilation
DROP TABLE IF EXISTS analytical_gold.fact_sales_performance CASCADE;
CREATE TABLE analytical_gold.fact_sales_performance (
    order_id         VARCHAR(100) PRIMARY KEY,
    order_date       DATE NOT NULL,
    customer_id      VARCHAR(50) NOT NULL,
    category         VARCHAR(100) NOT NULL,
    net_revenue      NUMERIC(14, 2) NOT NULL, -- (quantity * unit_price) - discount factors
    units_sold       INT NOT NULL,
    customer_rating  NUMERIC(2, 1)
);

-- Aggregated Dimensional Table: Daily City Performance Metrics
DROP TABLE IF EXISTS analytical_gold.dim_city_daily_summary CASCADE;
CREATE TABLE analytical_gold.dim_city_daily_summary (
    summary_id       BIGSERIAL PRIMARY KEY,
    sales_date       DATE NOT NULL,
    city             VARCHAR(100) NOT NULL,
    state            VARCHAR(100) NOT NULL,
    total_orders     INT NOT NULL,
    gross_revenue    NUMERIC(16, 2) NOT NULL,
    avg_rating       NUMERIC(3, 2),
    updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_city_date UNIQUE (sales_date, city, state)
);

-- Fast Indexing Paths for Analytical Querying
CREATE INDEX idx_gold_fact_date ON analytical_gold.fact_sales_performance(order_date);
CREATE INDEX idx_gold_summary_lookup ON analytical_gold.dim_city_daily_summary(sales_date, city);