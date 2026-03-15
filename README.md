# 🔷 DataPulse — Intelligent Sales Analytics & ETL Platform

> **Portfolio-grade end-to-end data engineering + analytics project**  
> Stack: Python · Pandas · SQLite/PostgreSQL · SQL Window Functions · Dash · Plotly

---

## 🏗 Architecture

```
Raw CSV Data
    ↓
etl/etl_pipeline.py     ← Extract → Validate → Transform → Load
    ↓
database/datapulse.db   ← SQLite (swap to PostgreSQL in prod)
    ↓
analytics/sql_queries.sql  ← 10 business analytics queries
    ↓
dashboard/app.py           ← Interactive Dash dashboard
    ↓
Business Insights
```

---

## 📁 Project Structure

```
DataPulse/
├── data/
│   └── sales_data.csv          # 2000 rows synthetic e-commerce data
├── etl/
│   └── etl_pipeline.py         # Full ETL with validation + audit
├── database/
│   ├── schema.sql               # Table schema + indexes
│   └── datapulse.db             # Generated after ETL run
├── analytics/
│   └── sql_queries.sql          # 10 analytics queries
├── dashboard/
│   └── app.py                   # Dash + Plotly dashboard
├── notebooks/
│   └── analysis.ipynb           # (optional) EDA notebook
└── README.md
```

---

## ⚡ Quick Start

### 1. Install dependencies
```bash
pip install dash plotly pandas numpy
```

### 2. Run ETL pipeline
```bash
cd DataPulse
python etl/etl_pipeline.py
```

This will:
- Read `data/sales_data.csv`
- Run 6 data validation checks
- Derive 8 new business columns
- Load into `database/datapulse.db`
- Print audit summary

### 3. Launch dashboard
```bash
python dashboard/app.py
# Open http://127.0.0.1:8050
```

---

## 📊 Dataset Schema

| Column | Type | Description |
|---|---|---|
| order_id | VARCHAR | Unique order identifier |
| customer_id | VARCHAR | Customer reference |
| product | VARCHAR | Product name |
| category | VARCHAR | Electronics / Clothing / Home & Kitchen / Books / Sports |
| quantity | INT | Units ordered |
| unit_price | DECIMAL | Price per unit (INR) |
| sales | DECIMAL | Total order value |
| profit | DECIMAL | Gross profit |
| date | DATE | Order date |
| region | VARCHAR | North / South / East / West / Central |
| discount_pct | DECIMAL | Discount applied (%) |

**Derived columns added by ETL:**
`year`, `month`, `quarter`, `profit_margin_pct`, `profit_tier`, `customer_segment`, `is_outlier`, `year_month`

---

## 🧠 ETL Validation Checks

1. Required column presence
2. Duplicate order_id detection
3. Null value check on critical fields
4. Numeric range validation (sales > 0, qty > 0)
5. Category & Region whitelist validation
6. Date format parsing

---

## 📈 SQL Analytics Queries (10 queries)

| # | Query | Technique |
|---|---|---|
| 1 | Executive KPI Summary | Aggregations |
| 2 | Monthly Sales Trend + MoM Growth | LAG() window function |
| 3 | Top 10 Products | RANK() window function |
| 4 | Category Breakdown | SUM() OVER() — revenue share |
| 5 | Region Performance | DENSE_RANK() |
| 6 | Customer Segmentation | NTILE(), CTEs, CASE |
| 7 | QoQ / YoY Comparison | PARTITION BY quarter |
| 8 | Discount Impact Analysis | CASE bucketing |
| 9 | Product Rank within Category | RANK() PARTITION BY |
| 10 | 3-Month Rolling Average | ROWS BETWEEN sliding window |

---

## 🖥 Dashboard Features

- **KPI Cards** — Revenue, Profit, Margin, Orders, Customers
- **Filters** — Year / Category / Region (all charts update reactively)
- **Monthly Trend** — Bar + Line combo (Sales + Profit)
- **Category Donut** — Revenue share by category
- **Region Performance** — Grouped horizontal bar
- **Top Products** — Color-scaled horizontal bar
- **Sales vs Profit Scatter** — Product-level bubble chart
- **Discount Impact** — Margin by discount band
- **Data Table** — Paginated recent transactions

---

## 🚀 Production Upgrade Path

| Feature | Dev | Prod |
|---|---|---|
| Database | SQLite | PostgreSQL / Snowflake |
| Scheduler | Manual | Apache Airflow / Prefect |
| Deployment | Local | Docker + AWS ECS |
| BI Layer | Dash | Power BI / Tableau |
| Monitoring | Log file | Datadog / CloudWatch |

---

## 👨‍💻 Author

**Shubham Naralkar**  
Data Associate — ICE Mortgage Technology  
naralkarshubham04@gmail.com
