# End-to-End Data Engineering Pipeline using Databricks

## Overview

This project demonstrates an **end-to-end data engineering pipeline** built on **Databricks Serverless Compute** using **Apache Spark, PySpark, and SQL**.
The pipeline follows the **Medallion Architecture (Bronze → Silver → Gold)** to ensure scalable, structured, and reliable data processing.

The system ingests raw data, performs transformations, and loads the final structured data into **dimension and fact tables** for analytical workloads.

---

## Architecture

![Data Pipeline Architecture](architecture/data_pipeline_diagram.png)

The pipeline is organized into three main layers:

### 1. Extraction Layer (Bronze)

* Raw data is initially uploaded to the **Default Schema**.
* Data is ingested into the **Bronze Layer** using **PySpark and SQL**.
* The Bronze layer stores raw and minimally processed data.
* Staging tables are used before loading into the main Bronze table.

### 2. Transformation Layer (Silver)

* Data from the Bronze layer is cleaned and transformed.
* Data quality checks and schema standardization are applied.
* Processed datasets are stored in the **Silver Schema**.
* Staging tables are used to prepare structured datasets.

### 3. Load Layer (Gold)

* Data is modeled for analytics using **dimensional modeling**.
* **Dimension tables** are created to store descriptive attributes.
* **Fact tables** store transactional or measurable data.
* These tables support **business intelligence and reporting**.

---

## Data Pipeline Flow

Default Schema → Bronze Layer → Silver Layer → Gold Layer

1. **Data Ingestion**

   * Upload raw dataset to Default Schema

2. **Bronze Processing**

   * Store raw data
   * Maintain staging and main tables

3. **Silver Processing**

   * Perform transformation and cleansing
   * Prepare structured datasets

4. **Gold Modeling**

   * Build dimensional models
   * Create fact and dimension tables

5. **Pipeline Automation**

   * Scheduled execution using **Databricks Jobs**

---

## Technologies Used

* Databricks (Serverless Compute)
* Apache Spark
* PySpark
* SQL
* Delta Lake
* Medallion Architecture
* Databricks Jobs (Workflow Scheduling)

---

## Key Features

* End-to-end **ETL pipeline implementation**
* Scalable processing using **Apache Spark**
* Structured **Bronze, Silver, and Gold layers**
* **Dimensional data modeling**
* Automated pipeline scheduling
* Designed for **analytics-ready datasets**

---

## Repository Structure

```
databricks-medallion-data-pipeline
│
├── data
│   └── sample_dataset.csv
│
├── notebooks
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_transformation.py
│   └── 03_gold_modeling.py
│
├── architecture
│   └── data_pipeline_diagram.png
│
└── README.md
```

---

## Future Improvements

* Implement **data quality validation checks**
* Add **incremental data loading**
* Integrate with **streaming pipelines**
* Add **CI/CD integration for Databricks workflows**

---

## Author

Monjur Alahi
Aspiring Data Engineer | Data Engineering & Analytics Enthusiast
