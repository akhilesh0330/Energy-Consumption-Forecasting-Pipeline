⚡ Energy Consumption Forecasting Pipeline

Production-ready Azure Data Engineering project for energy consumption analytics using Azure Data Factory, ADLS Gen2, Azure Databricks, PySpark, Delta Lake, dbt Cloud, Databricks Workflows, Apache Airflow, and Power BI. The project follows the Medallion Architecture (Bronze, Silver, Gold) to build a scalable Lakehouse solution.

📖 Project Overview

The Energy Consumption Forecasting Pipeline is an end-to-end Azure Data Engineering project that ingests energy consumption datasets, transforms them through the Bronze, Silver, and Gold layers, and delivers business-ready analytics dashboards.

The pipeline uses Azure Data Factory for ingestion, Azure Databricks for ETL processing, dbt Cloud for dimensional modeling and transformations, Databricks Workflows for orchestration, and Power BI for business reporting.

🛠️ Technology Stack
Azure Data Lake Storage Gen2 (ADLS)
Azure Data Factory
Azure Databricks
PySpark
Delta Lake
Unity Catalog
dbt Cloud
Databricks SQL Warehouse
Databricks Workflows
Apache Airflow
Power BI
Git & GitHub
Pytest
📂 Project Workflow
Kaggle Energy Dataset
        │
        ▼
Azure Data Lake Storage Gen2
(Raw CSV Files)
        │
        ▼
Azure Data Factory
(CSV → Parquet)
        │
        ▼
Azure Data Lake Storage Gen2
(Transformed Parquet Files)
        │
        ▼
Azure Databricks
        │
        ▼
Bronze Layer
(Raw Delta Tables)
        │
        ▼
Silver Layer
(Clean & Validated Delta Tables)
        │
        ▼
dbt Cloud
(Staging → Dimensions → Fact → Marts → Dashboard)
        │
        ▼
Gold Layer
        │
        ▼
Databricks SQL Warehouse
        │
        ▼
Power BI / Databricks Dashboard
✅ Implementation Flow
Step 1 – Data Ingestion

Objective

Upload raw CSV datasets into ADLS Gen2.

Activities

Created Storage Account
Created ADLS Container
Uploaded CSV files
Verified data upload
Step 2 – Data Conversion

Objective

Convert CSV files into Parquet using Azure Data Factory.

Activities

Created ADF Pipeline
Configured Source and Sink datasets
Converted CSV to Parquet
Stored Parquet files in ADLS
Step 3 – Bronze Layer

Objective

Load Parquet files into Delta tables while preserving raw data.

Tables

Bronze Energy Usage
Bronze Weather Stream
Bronze Device Metrics
Bronze Grid Load
Bronze Tariff Metrics

Features

Metadata Columns
Audit Logging
Watermarking
Schema Evolution
Error Handling
Step 4 – Silver Layer

Objective

Clean and standardize the Bronze data.

Tables

Energy Usage
Weather Stream
Device Metrics
Grid Load
Tariff Metrics

Features

Null Handling
Duplicate Removal
Data Type Conversion
Data Validation
Surrogate Keys
SCD Type 2
OPTIMIZE
ZORDER
VACUUM
Step 5 – Gold Layer (dbt Cloud)
Staging Models
stg_energy
stg_weather
stg_device
stg_grid
stg_tariff
Dimension Models
dim_date
dim_household
dim_weather
dim_device
dim_grid
dim_tariff
Fact Model
fact_energy
Mart Models
daily_summary
monthly_summary
yearly_summary
Dashboard Model
executive_dashboard
🔄 Pipeline Orchestration
Databricks Workflows
Bronze Notebook
Silver Notebook
dbt Gold Models
Apache Airflow (Project Orchestration)
📊 Monitoring
Audit Log
Watermark Table
Databricks Workflow Monitoring
Pipeline Status Tracking
✅ Testing
dbt Tests
Pytest
SQL Validation
📈 Dashboards
Energy Consumption Dashboard
Total Energy Usage
Average Energy Usage
Energy by Region
Energy by City
Daily Consumption Trend
Weather Impact Dashboard
Temperature vs Energy Usage
Humidity Analysis
Rainfall Impact
Weather KPIs
Device Monitoring Dashboard
Runtime Analysis
Device Efficiency
Device Temperature
Meter Type Analysis
Billing Analytics Dashboard
Total Revenue
Monthly Billing
Revenue by Region
Revenue by Customer Category
🎯 Business Outcomes
Automated end-to-end ETL pipeline
Improved data quality through Medallion Architecture
Scalable Delta Lake implementation
Centralized business reporting
Automated workflow orchestration
Business-ready dashboards for energy analytics
Foundation for future energy consumption forecasting
