-- ================================================================
-- DataPulse — Database Schema
-- Database : PostgreSQL (use SQLite for local dev)
-- Author   : Shubham Naralkar
-- ================================================================

-- Drop if re-running
DROP TABLE IF EXISTS sales;
DROP TABLE IF EXISTS etl_audit_log;

-- ────────────────────────────────────────────────────────────────
-- MAIN SALES TABLE
-- ────────────────────────────────────────────────────────────────
CREATE TABLE sales (
    order_id            VARCHAR(12)   PRIMARY KEY,
    customer_id         VARCHAR(10)   NOT NULL,
    product             VARCHAR(100)  NOT NULL,
    category            VARCHAR(50)   NOT NULL,
    quantity            INTEGER       NOT NULL CHECK (quantity > 0),
    unit_price          NUMERIC(12,2) NOT NULL CHECK (unit_price > 0),
    sales               NUMERIC(12,2) NOT NULL CHECK (sales > 0),
    profit              NUMERIC(12,2) NOT NULL,
    date                DATE          NOT NULL,
    region              VARCHAR(20)   NOT NULL,
    discount_pct        NUMERIC(5,2)  DEFAULT 0,
    year                INTEGER,
    month               INTEGER,
    quarter             INTEGER,
    month_name          VARCHAR(10),
    day_of_week         VARCHAR(15),
    profit_margin_pct   NUMERIC(6,2),
    revenue_per_unit    NUMERIC(12,2),
    profit_tier         VARCHAR(10),
    customer_segment    VARCHAR(15),
    is_outlier          SMALLINT      DEFAULT 0,
    year_month          VARCHAR(8),
    loaded_at           TIMESTAMP     DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for query performance
CREATE INDEX idx_sales_date       ON sales(date);
CREATE INDEX idx_sales_category   ON sales(category);
CREATE INDEX idx_sales_region     ON sales(region);
CREATE INDEX idx_sales_customer   ON sales(customer_id);
CREATE INDEX idx_sales_year_month ON sales(year_month);

-- ────────────────────────────────────────────────────────────────
-- ETL AUDIT LOG
-- ────────────────────────────────────────────────────────────────
CREATE TABLE etl_audit_log (
    run_id          SERIAL PRIMARY KEY,
    run_timestamp   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    rows_extracted  INTEGER,
    rows_dropped    INTEGER,
    rows_loaded     INTEGER,
    status          VARCHAR(10),
    notes           TEXT
);


-- ================================================================
-- DataPulse — SQL Analytics Queries
-- Demonstrates: Aggregations, Window Functions, CTEs, Subqueries
-- ================================================================


-- ────────────────────────────────────────────────────────────────
-- Q1 | EXECUTIVE KPIs — Overall Summary
-- ────────────────────────────────────────────────────────────────
SELECT
    COUNT(DISTINCT order_id)                        AS total_orders,
    COUNT(DISTINCT customer_id)                     AS unique_customers,
    ROUND(SUM(sales), 0)                            AS total_revenue,
    ROUND(SUM(profit), 0)                           AS total_profit,
    ROUND(AVG(profit_margin_pct), 2)                AS avg_margin_pct,
    ROUND(SUM(sales) / COUNT(DISTINCT customer_id), 0) AS revenue_per_customer,
    SUM(CASE WHEN profit < 0 THEN 1 ELSE 0 END)    AS loss_making_orders
FROM sales;


-- ────────────────────────────────────────────────────────────────
-- Q2 | MONTHLY SALES TREND with MoM Growth (Window Function)
-- ────────────────────────────────────────────────────────────────
WITH monthly AS (
    SELECT
        year_month,
        year,
        month,
        ROUND(SUM(sales), 0)  AS monthly_sales,
        ROUND(SUM(profit), 0) AS monthly_profit,
        COUNT(order_id)        AS order_count
    FROM sales
    GROUP BY year_month, year, month
    ORDER BY year_month
)
SELECT
    year_month,
    monthly_sales,
    monthly_profit,
    order_count,
    LAG(monthly_sales) OVER (ORDER BY year_month)  AS prev_month_sales,
    ROUND(
        (monthly_sales - LAG(monthly_sales) OVER (ORDER BY year_month))
        / NULLIF(LAG(monthly_sales) OVER (ORDER BY year_month), 0) * 100,
        2
    )                                               AS mom_growth_pct,
    SUM(monthly_sales) OVER (
        ORDER BY year_month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                               AS cumulative_sales
FROM monthly;


-- ────────────────────────────────────────────────────────────────
-- Q3 | TOP 10 PRODUCTS by Revenue
-- ────────────────────────────────────────────────────────────────
SELECT
    product,
    category,
    COUNT(order_id)                AS total_orders,
    SUM(quantity)                  AS units_sold,
    ROUND(SUM(sales), 0)           AS total_revenue,
    ROUND(SUM(profit), 0)          AS total_profit,
    ROUND(AVG(profit_margin_pct), 2) AS avg_margin_pct,
    RANK() OVER (ORDER BY SUM(sales) DESC) AS revenue_rank
FROM sales
GROUP BY product, category
ORDER BY total_revenue DESC
LIMIT 10;


-- ────────────────────────────────────────────────────────────────
-- Q4 | CATEGORY PERFORMANCE BREAKDOWN
-- ────────────────────────────────────────────────────────────────
SELECT
    category,
    COUNT(order_id)                           AS orders,
    ROUND(SUM(sales), 0)                      AS revenue,
    ROUND(SUM(profit), 0)                     AS profit,
    ROUND(AVG(profit_margin_pct), 2)          AS avg_margin_pct,
    ROUND(SUM(sales) / SUM(SUM(sales)) OVER () * 100, 2) AS revenue_share_pct,
    ROUND(SUM(profit) / SUM(SUM(profit)) OVER () * 100, 2) AS profit_share_pct
FROM sales
GROUP BY category
ORDER BY revenue DESC;


-- ────────────────────────────────────────────────────────────────
-- Q5 | REGION PERFORMANCE with Rankings
-- ────────────────────────────────────────────────────────────────
SELECT
    region,
    COUNT(DISTINCT customer_id)               AS unique_customers,
    COUNT(order_id)                           AS total_orders,
    ROUND(SUM(sales), 0)                      AS revenue,
    ROUND(SUM(profit), 0)                     AS profit,
    ROUND(AVG(profit_margin_pct), 2)          AS avg_margin_pct,
    DENSE_RANK() OVER (ORDER BY SUM(sales) DESC)  AS sales_rank,
    DENSE_RANK() OVER (ORDER BY SUM(profit) DESC) AS profit_rank
FROM sales
GROUP BY region
ORDER BY revenue DESC;


-- ────────────────────────────────────────────────────────────────
-- Q6 | CUSTOMER SEGMENTATION — RFM-style
-- ────────────────────────────────────────────────────────────────
WITH customer_stats AS (
    SELECT
        customer_id,
        COUNT(order_id)                   AS frequency,
        ROUND(SUM(sales), 0)              AS monetary,
        MAX(date)                         AS last_order_date,
        MIN(date)                         AS first_order_date,
        COUNT(DISTINCT category)          AS categories_bought
    FROM sales
    GROUP BY customer_id
)
SELECT
    customer_id,
    frequency,
    monetary,
    last_order_date,
    categories_bought,
    NTILE(4) OVER (ORDER BY monetary DESC)   AS monetary_quartile,
    NTILE(4) OVER (ORDER BY frequency DESC)  AS frequency_quartile,
    CASE
        WHEN frequency >= 7 AND monetary >= 50000 THEN 'Champion'
        WHEN frequency >= 5                        THEN 'Loyal'
        WHEN frequency >= 3                        THEN 'Potential'
        ELSE                                            'At Risk'
    END AS rfm_segment
FROM customer_stats
ORDER BY monetary DESC
LIMIT 50;


-- ────────────────────────────────────────────────────────────────
-- Q7 | QUARTERLY SALES vs PREVIOUS QUARTER (YoY Comparison)
-- ────────────────────────────────────────────────────────────────
SELECT
    year,
    quarter,
    ROUND(SUM(sales), 0)   AS q_sales,
    ROUND(SUM(profit), 0)  AS q_profit,
    LAG(ROUND(SUM(sales), 0)) OVER (
        PARTITION BY quarter ORDER BY year
    )                       AS same_q_prev_year_sales,
    ROUND(
        (SUM(sales) - LAG(SUM(sales)) OVER (PARTITION BY quarter ORDER BY year))
        / NULLIF(LAG(SUM(sales)) OVER (PARTITION BY quarter ORDER BY year), 0) * 100,
        2
    )                       AS yoy_growth_pct
FROM sales
GROUP BY year, quarter
ORDER BY year, quarter;


-- ────────────────────────────────────────────────────────────────
-- Q8 | DISCOUNT IMPACT ANALYSIS
-- ────────────────────────────────────────────────────────────────
SELECT
    CASE
        WHEN discount_pct = 0   THEN 'No Discount'
        WHEN discount_pct <= 10 THEN '1–10%'
        WHEN discount_pct <= 20 THEN '11–20%'
        ELSE '20%+'
    END                                    AS discount_band,
    COUNT(order_id)                        AS orders,
    ROUND(AVG(sales), 0)                   AS avg_order_value,
    ROUND(AVG(profit_margin_pct), 2)       AS avg_margin_pct,
    ROUND(SUM(profit), 0)                  AS total_profit
FROM sales
GROUP BY discount_band
ORDER BY avg_margin_pct DESC;


-- ────────────────────────────────────────────────────────────────
-- Q9 | PRODUCT RANKING within Each Category (Window Function)
-- ────────────────────────────────────────────────────────────────
SELECT
    category,
    product,
    ROUND(SUM(sales), 0)   AS revenue,
    ROUND(SUM(profit), 0)  AS profit,
    RANK() OVER (
        PARTITION BY category
        ORDER BY SUM(sales) DESC
    ) AS rank_in_category
FROM sales
GROUP BY category, product
HAVING RANK() OVER (PARTITION BY category ORDER BY SUM(sales) DESC) <= 3
ORDER BY category, rank_in_category;


-- ────────────────────────────────────────────────────────────────
-- Q10 | 3-MONTH ROLLING AVERAGE SALES (Moving Average)
-- ────────────────────────────────────────────────────────────────
WITH monthly AS (
    SELECT
        year_month,
        ROUND(SUM(sales), 0) AS monthly_sales
    FROM sales
    GROUP BY year_month
)
SELECT
    year_month,
    monthly_sales,
    ROUND(AVG(monthly_sales) OVER (
        ORDER BY year_month
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ), 0) AS rolling_3m_avg
FROM monthly
ORDER BY year_month;
