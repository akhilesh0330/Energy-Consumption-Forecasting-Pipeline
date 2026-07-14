# ⚡ Energy Consumption Forecasting Pipeline

Production-ready Azure Data Engineering project for **Energy Consumption Analytics** using **Azure Data Lake Storage Gen2 (ADLS), Azure Data Factory, Azure Databricks (PySpark), Delta Lake, dbt Cloud, Databricks Workflows, Apache Airflow, and Power BI**.

The project implements an end-to-end ETL pipeline following the **Medallion Architecture (Bronze → Silver → Gold)** to transform raw energy datasets into business-ready analytics.

---

# 📖 Project Overview

The **Energy Consumption Forecasting Pipeline** is an end-to-end Azure Data Engineering project designed to build a scalable and reliable Lakehouse architecture.

The project automates data ingestion, transformation, validation, dimensional modeling, workflow orchestration, and dashboard reporting using Azure cloud services.

The pipeline follows the **Medallion Architecture** to improve data quality from raw ingestion to analytics-ready datasets.

---

# 🏗 High Level Architecture

<p align="center">
<img src="Architecture/HLD.png" width="1000">
</p>

---

# 🏛 Low Level Architecture

<p align="center">
<img src="Architecture/LLD.png" width="1000">
</p>

---

# ⚙ Technology Stack

| Category | Technology |
|-----------|------------|
| Cloud | Microsoft Azure |
| Storage | Azure Data Lake Storage Gen2 |
| ETL | Azure Data Factory |
| Processing | Azure Databricks |
| Language | PySpark |
| Storage Format | Delta Lake |
| Modeling | dbt Cloud |
| SQL Engine | Databricks SQL Warehouse |
| Orchestration | Databricks Workflows, Apache Airflow |
| Reporting | Power BI / Databricks Dashboards |
| Testing | Pytest |
| Version Control | Git & GitHub |

---

# 📂 End-to-End Project Flow

```text
CSV Files
        │
        ▼
Azure Data Lake Storage Gen2
        │
        ▼
Azure Data Factory
(CSV → Parquet)
        │
        ▼
Azure Data Lake Storage
(Parquet Files)
        │
        ▼
Azure Databricks
        │
        ▼
Bronze Layer
        │
        ▼
Silver Layer
        │
        ▼
dbt Cloud
(Staging → Dimensions → Fact → Marts)
        │
        ▼
Gold Layer
        │
        ▼
Databricks SQL Warehouse
        │
        ▼
Dashboards
```

---

# 🥉 Bronze Layer

- Read Parquet files from ADLS
- Raw Delta Tables
- Metadata Columns
- Audit Columns
- Watermarking
- Schema Evolution
- Error Logging

---

# 🥈 Silver Layer

- Data Validation
- Remove Duplicates
- Handle Null Values
- Standardize Data
- Data Type Conversion
- Surrogate Keys
- SCD Type 2
- Delta Optimization

---

# 🥇 Gold Layer (dbt)

### Staging Models

- stg_energy
- stg_weather
- stg_device
- stg_grid
- stg_tariff

### Dimension Models

- dim_date
- dim_household
- dim_device
- dim_weather
- dim_grid
- dim_tariff

### Fact Model

- fact_energy

### Mart Models

- daily_summary
- monthly_summary
- yearly_summary

### Dashboard Model

- executive_dashboard

---

# 🔄 Pipeline Orchestration

- Azure Data Factory
- Azure Databricks
- Databricks Workflows
- Apache Airflow
- dbt Cloud

---

# 🧪 Testing

- Pytest
- dbt Tests
- SQL Validation
- Data Quality Checks

---

# 📊 Dashboard Gallery

## Executive Dashboard

<p align="center">
<img src="Dashboard/executive_dashboard.jpg" width="900">
</p>

---

## Weather Impact Dashboard

<p align="center">
<img src="Dashboard/weather_impact_dashboard.jpg" width="900">
</p>

---

## Device Monitoring Dashboard

<p align="center">
<img src="Dashboard/device_monitoring_dashboard.jpg" width="900">
</p>

---

## Billing Analytics Dashboard

<p align="center">
<img src="Dashboard/billing_analytics_dashboard.jpg" width="900">
</p>

---

# 📁 Repository Structure

```text
Energy-Consumption-Forecasting-Pipeline

│── Architecture
│   ├── HLD.png
│   └── LLD.png
│
│── Bronze
│── Silver
│── Gold
│── Dashboard
│── Testing
│── docs
│── README.md
```

---

# 🎯 Project Highlights

- End-to-End Azure Data Engineering Pipeline
- Medallion Architecture
- Azure Data Factory ETL
- Azure Databricks (PySpark)
- Delta Lake
- dbt Cloud Data Modeling
- Databricks SQL Warehouse
- Databricks Dashboards
- Apache Airflow Orchestration
- Data Validation & Quality Checks
- Pytest Automation
- GitHub Version Control

---

# 📌 Business Outcomes

- Automated ETL Pipeline
- Improved Data Quality
- Lakehouse Architecture
- Scalable Analytics Platform
- Business-ready Gold Layer
- Interactive Dashboards
- Optimized Query Performance

---

# 👨‍💻 Author

**Akhilesh Chanda**

Azure Data Engineering Project
