# HR Data Pipeline Project

## Overview
This project demonstrates a full **end-to-end HR data pipeline** using the **Bronze → Silver → Gold** architecture on **Snowflake**, orchestrated by **Airflow**. The pipeline ingests raw HR CSV files from **Amazon S3**, cleans and deduplicates employee data, and creates analytics tables for reporting. All transformations are implemented as **stored procedures** inside Snowflake.

---

## Project Structure

### 1. Bronze Layer
- **Purpose:** Store raw HR data exactly as received.  
- **Details:** 
  - Raw CSV files are uploaded to an **S3 bucket**.
  - Snowflake reads files using a **stage** connected to S3.  
  - Each row includes a `LOAD_DATE` to track when it was ingested.  
- **Incremental:** Appends new rows only; existing rows remain untouched.  
- **Procedure:** `bronze.sp_load_hr_employee()`

### 2. Silver Layer
- **Purpose:** Clean and deduplicate HR data.  
- **Transformations:**  
  - Trim employee names.  
  - Normalize `SEX` column (`M/F → MALE/FEMALE`).  
  - Convert termination flag (`TERMED`) to boolean `IS_TERMINATED`.  
  - Keep only **latest record per employee** based on `LOAD_DATE`.  
- **Incremental:** Processes only new records from Bronze that are not already in Silver.  
- **Procedure:** `silver.sp_build_hr_employee_clean()`

### 3. Gold Layer
- **Purpose:** Create analytics tables for reporting.  
- **Tables:**  
  1. **Dimension Table:** `gold.dim_employee` — employee master data.  
  2. **Fact Table:** `gold.fact_headcount` — daily snapshot of active employees by department.  
  3. **Fact Table:** `gold.fact_attrition` — daily snapshot of terminated employees by department.  
- **Incremental:** Inserts only new dimension records and daily snapshot facts; historical data is preserved.  
- **Procedures:**  
  - `gold.sp_build_dim_employee()`  
  - `gold.sp_build_fact_headcount()`  
  - `gold.sp_build_fact_attrition()`

---

## S3 Integration
- HR CSV files are stored in an **Amazon S3 bucket**.  
- Snowflake reads these files using a **stage**, which connects via a **storage integration**.  
- This allows the pipeline to automatically ingest new files into the Bronze table without manual uploads.  
- Raw data is tracked with `LOAD_DATE` to support incremental processing.

---

## Incremental Data Handling
1. **Bronze Layer:** Appends new CSV rows; `LOAD_DATE` tracks ingestion.  
2. **Silver Layer:** Processes only new records from Bronze based on `LOAD_DATE`; deduplicates employees and normalizes columns.  
3. **Gold Layer:** Inserts new dimension records and daily snapshot facts for headcount and attrition. Historical data is preserved; only new data is added each day.

---

## Airflow DAG
- **DAG Name:** `hr_bronze_silver_gold_pipeline`  
- **Purpose:** Orchestrates the execution of all stored procedures.  
- **Flow:**

