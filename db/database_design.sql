-- Create Target Schema Layers if they don't exist
CREATE SCHEMA IF NOT EXISTS raw_bronze;
CREATE SCHEMA IF NOT EXISTS clean_silver;
CREATE SCHEMA IF NOT EXISTS analytical_gold;

-- -------------------------------------------------------------------------------
-- 1. BRONZE LAYER
-- -------------------------------------------------------------------------------
DROP TABLE IF EXISTS raw_bronze.orders_landing;

CREATE TABLE raw_bronze.orders_landing (
    row_id BIGINT NOT NULL AUTO_INCREMENT,
    file_source_hash VARCHAR(64) NOT NULL,
    ingested_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    order_id TEXT,
    customer_id TEXT,
    order_date TEXT,
    product_name TEXT,
    category TEXT,
    quantity TEXT,
    unit_price TEXT,
    total_amount TEXT,
    payment_method TEXT,
    city TEXT,
    state TEXT,
    phone TEXT,
    pincode TEXT,
    rating TEXT,
    discount TEXT,
    PRIMARY KEY (row_id)
) ENGINE=InnoDB;

-- -------------------------------------------------------------------------------
-- 2. SILVER LAYER
-- -------------------------------------------------------------------------------
DROP TABLE IF EXISTS clean_silver.orders;

CREATE TABLE clean_silver.orders (
    order_id VARCHAR(50) NOT NULL,
    customer_id VARCHAR(50) NOT NULL,
    order_date DATETIME NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    total_amount DECIMAL(12, 2) NOT NULL,
    payment_method VARCHAR(50) NOT NULL,
    city VARCHAR(100) NOT NULL,
    state VARCHAR(100) NOT NULL,
    phone VARCHAR(20) NOT NULL,
    pincode VARCHAR(10) NOT NULL,
    rating DECIMAL(3, 2) NOT NULL,
    discount DECIMAL(5, 4) DEFAULT 0.0000,
    processed_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (order_id)
) ENGINE=InnoDB;

-- -------------------------------------------------------------------------------
-- 3. GOLD LAYER - Fact Sales Table
-- -------------------------------------------------------------------------------
DROP TABLE IF EXISTS analytical_gold.fact_sales_performance;

CREATE TABLE analytical_gold.fact_sales_performance (
    order_id VARCHAR(50) NOT NULL,
    order_date DATE NOT NULL,
    customer_id VARCHAR(50) NOT NULL,
    category VARCHAR(100) NOT NULL,
    net_revenue DECIMAL(14, 2) NOT NULL,
    units_sold INT NOT NULL,
    customer_rating DECIMAL(3,2) NOT NULL,
    PRIMARY KEY (order_id)
) ENGINE=InnoDB;

-- -------------------------------------------------------------------------------
-- 4. GOLD LAYER - Dimensional Summary Table
-- -------------------------------------------------------------------------------
DROP TABLE IF EXISTS analytical_gold.dim_city_daily_summary;

CREATE TABLE analytical_gold.dim_city_daily_summary (
    sales_date DATE NOT NULL,
    city VARCHAR(100) NOT NULL,
    state VARCHAR(100) NOT NULL,
    total_orders INT NOT NULL,
    gross_revenue DECIMAL(16, 2) NOT NULL,
    avg_rating DECIMAL(3, 2) NOT NULL,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (sales_date, city, state)
) ENGINE=InnoDB;