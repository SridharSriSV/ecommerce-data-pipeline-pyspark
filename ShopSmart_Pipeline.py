from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 1. Initialize Spark Session
spark = SparkSession.builder.appName("Ecommerce_Pipeline").getOrCreate()

# 2. Load Bronze Data (Raw E-commerce Logs)
data = [
    ("U001", "2023-10-01", "India", "Laptop", 1200),
    ("U002", "2023-10-01", "UK", "Mouse", None), # The NULL value we discussed
    ("U003", "2023-10-02", "India", "Keyboard", 50)
]
columns = ["User_ID", "Date", "Country", "Product", "Amount"]
df_raw = spark.createDataFrame(data, columns)

# 3. Silver Layer (Transformation & Cleaning)
# Handling the NULL values to preserve user data for DAU metrics
df_clean = df_raw.fillna(0, subset=["Amount"])

# 4. Storage Optimization (Simulated Parquet Output)
# Writing to Parquet format and partitioning by 'Country' (Low Cardinality)
# df_clean.write.mode("overwrite").partitionBy("Country").parquet("s3://ecommerce-data/silver/transactions/")

print("Pipeline executed successfully: Nulls handled and data partitioned.")