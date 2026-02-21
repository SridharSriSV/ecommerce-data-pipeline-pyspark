# ecommerce-data-pipeline-pyspark
An end-to-end data pipeline built with PySpark to process, clean, and partition e-commerce log data
# Scalable E-Commerce Data Pipeline

## Project Overview
This project is an end-to-end Data Engineering pipeline built using **PySpark**. It processes raw e-commerce transaction logs, cleanses the data (handling nulls and date formats), and aggregates business metrics for downstream analytics.

##  Tech Stack
* **Language:** Python
* **Big Data Engine:** Apache Spark (PySpark)
* **Storage Format:** Parquet
* **Concepts:** Data Quality, Partitioning, Aggregations

##  Pipeline Architecture
1. **Bronze Layer (Ingestion):** Reads raw, semi-structured data.
2. **Silver Layer (Transformation):** * Handled `NULL` values in financial columns to prevent data loss.
   * Standardized date string formats (`yyyy-MM-dd`) to proper DateTypes.
   * Converted duration metrics into readable formats (hours).
3. **Gold Layer (Aggregation):** Grouped data by movie/product to calculate total watched hours/revenue.
4. **Storage (Optimization):** Exported final DataFrames into **Parquet** format, partitioned by `movie` (Low Cardinality) to prevent the small-file problem and optimize query performance.

## Key Learnings & Optimizations
* Chose `.fillna(0)` over `.dropna()` to preserve vital user records while maintaining accurate revenue aggregations.
* Prevented memory bottlenecks by utilizing proper PySpark aggregation functions instead of Python native functions.
