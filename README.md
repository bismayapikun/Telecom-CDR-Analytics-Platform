# 📡 Telecom CDR Analytics Platform

End-to-end Data Engineering project simulating a real-world telecom Call Detail Records (CDR) analytics system built for Wipro Assessment — February 2026.

---

## 🎯 Project Overview

This project demonstrates a complete data engineering pipeline for a telecom company processing **50,000+ CDR records** with advanced analytics, real-time streaming, and enterprise-grade security.

---

## 🛠 Tech Stack

| Technology | Purpose |
|------------|----------|
| Python 3.12 | Data Generation, ETL Pipeline |
| Apache PySpark 4.0 | Batch + Streaming Processing |
| Snowflake | Cloud Data Warehouse |
| Tableau Public | Interactive Dashboard |
| Google Colab | Development Environment |
| Draw.io | Architecture Diagram |

---

## 📁 Project Structure

```
telecom-cdr-analytics-platform/

├── notebooks/
│   ├── 1_CDR_Analytics_Wipro.ipynb
│   ├── 2_CDR_Advanced_SQL.ipynb
│   ├── 3_CDR_ETL_Pipeline.ipynb
│   ├── 4_CDR_Spark_Pipeline.ipynb
│   ├── 5_CDR_Snowflake_Optimization.ipynb
│   ├── 6_CDR_Security_RBAC.ipynb
│   ├── 7_CDR_Performance_Tuning.ipynb
│   └── README.md
│
├── sql/
│   ├── snowflake_queries.sql
│   └── README.md
│
├── dashboard/
│   ├── Visualization Images
│   ├── CDR_Analytics_Dashboard.png
│   ├── Telecom_CDR_Analytics_Dashboard.twbx
│   ├── cdr_data.csv
│   └── README.md
│
├── architecture/
│   ├── CDR_Architecture_Diagram.drawio
│   ├── CDR_Architecture_Diagram.png
│   └── README.md
│
├── Presentation/
│   └── CDR_Presentation.pptx
│
├── LICENSE
└── README.md
```

---

## 🏗 Architecture Flow

```
Python Generator
      ↓
STG_CDR
      ↓
ETL Pipeline
      ↓
FACT_CDR
      ↓
Analytics Views
      ↓
Tableau Dashboard

        ↘
      Apache Spark
   (Batch + Streaming)
```

---

## 📓 Notebooks

| # | Notebook | Description |
|---|----------|------------|
| 1 | 1_CDR_Analytics_Wipro.ipynb | Generates 50,000 realistic telecom CDR records and loads to Snowflake |
| 2 | 2_CDR_Advanced_SQL.ipynb | 5 advanced SQL queries (CTE, Window Functions, Fraud Detection) |
| 3 | 3_CDR_ETL_Pipeline.ipynb | ETL from STG_CDR to FACT_CDR |
| 4 | 4_CDR_Spark_Pipeline.ipynb | Apache Spark batch + streaming |
| 5 | 5_CDR_Snowflake_Optimization.ipynb | Clustering, Time Travel, JSON |
| 6 | 6_CDR_Security_RBAC.ipynb | RBAC, Masking, Governance |
| 7 | 7_CDR_Performance_Tuning.ipynb | Secure Views & optimization |

---

## 📊 Dashboard

Built with **Tableau Public** using exported Snowflake data.

Includes:
- Monthly Revenue Trend  
- Call Type Revenue  
- Network Performance  
- Fraud Analysis  
- Cell Tower Performance  
- Network Distribution  
- Monthly Calls  
- Combined Dashboard View  

---

## 🧠 Features Implemented

### Data Warehouse Design
- Star Schema (FACT_CDR + 5 Dimensions)
- STAGING, DWH, ANALYTICS, STREAMING schemas
- 3 Snowflake Warehouses (INGEST_WH, TRANSFORM_WH, REPORTING_WH)

### Data Generation
- 50,000 telecom records
- 500 unique subscribers
- 5 cell towers
- 4 call types
- Full year 2024 dataset

### Advanced SQL
- Revenue Trend using CTE + LAG()
- Customer Ranking using RANK() & NTILE()
- Rolling Fraud Detection (7-day average)
- Network Performance Ranking

### ETL Pipeline
- Batch processing (5,000 records per batch)
- Dimension key mapping
- 100% data integrity
- Runtime: 52 seconds

### Apache Spark
- Batch aggregations
- 500ms micro-batch streaming simulation
- PySpark DataFrame API

### Snowflake Optimization
- Clustering Keys
- Time Travel (7 days retention)
- Semi-Structured JSON using VARIANT + PARSE_JSON()

### Security & Governance
- RBAC roles (CDR_ADMIN / CDR_ANALYST / CDR_VIEWER)
- Data Masking (Phone numbers & charges)
- Row Access Policies
- Data Classification Tags
- Audit Logging

### Performance Tuning
- Secure Views:
  - VW_MONTHLY_REVENUE
  - VW_CUSTOMER_SUMMARY
  - VW_NETWORK_PERFORMANCE

---

## 📈 Key Statistics

| Metric | Value |
|--------|--------|
| Total CDR Records | 50,000 |
| Unique Subscribers | 500 |
| Date Range | Jan 1 – Dec 31, 2024 |
| Total Revenue | ₹59,405.55 |
| Fraud Rate | 0.5% (250 records) |
| Roaming Rate | 2% |
| Average Drop Rate | 14–15% |
| ETL Data Loss | 0% |
| ETL Runtime | 52 seconds |

---

## 🚀 How to Run

1. Open any notebook in Google Colab  
2. Run Cell 1 to install dependencies  
3. Configure Snowflake credentials (if required)  
4. Run cells sequentially  

### Prerequisites
- Google Account (Colab)
- Snowflake Account
- Python 3.12+

---

## 👤 Author

**Chandan Sahoo**  
Data Engineering Assessment — Wipro  
February 2026
