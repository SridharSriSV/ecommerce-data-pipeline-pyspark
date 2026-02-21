#  Scalable E-Commerce Data Pipeline

##  Project Overview
This project is an end-to-end Data Engineering pipeline built using **PySpark**. It is designed to process multiple streams of e-commerce data, including raw transaction logs and complex, semi-structured API data (Nested JSON). The pipeline cleanses the data, flattens nested structures, handles missing values, and optimizes storage for downstream analytics.

##  Repository Structure
* **`ShopSmart_Pipeline.py`**: The core ETL script. Handles data ingestion, `NULL` value imputation (to preserve user metrics), and outputs to partitioned Parquet files.
* **`Nested_JSON_Parser.py`**: A specialized script for processing semi-structured API payloads. It utilizes PySpark's `explode()` function and dot notation to flatten nested arrays and structs into a relational format.

##  Tech Stack
* **Language:** Python
* **Big Data Engine:** Apache Spark (PySpark)
* **Storage Format:** Parquet (Columnar)
* **Concepts:** Data Quality, Partitioning, JSON Flattening, Aggregations

##  Pipeline Architecture
1. **Bronze Layer (Ingestion):** Reads raw CSV logs and Nested JSON arrays from simulated web servers.
2. **Silver Layer (Transformation):** * **Missing Data:** Handled `NULL` values in financial columns (`.fillna(0)`) to prevent data loss for Daily Active User (DAU) metrics.
   * **JSON Parsing:** Extracted data from nested dictionaries and exploded arrays (`explode()`) to create granular row-level data.
3. **Gold Layer (Aggregation):** Prepared clean, flattened DataFrames ready for BI tools.
4. **Storage (Optimization):** Exported final DataFrames into **Parquet** format, partitioned by `Country` (Low Cardinality) to prevent the small-file problem and optimize query performance.

##  Key Learnings & Optimizations
* Successfully parsed deeply nested JSON arrays avoiding complex Python loops by utilizing native PySpark functions.
* Chose to impute missing revenue data rather than dropping records (`.dropna()`), ensuring user behavioral tracking remained accurate.
* Implemented partition pruning strategies to enhance analytic query speeds on the final data lake layer.
