from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

# 1. Initialize Spark Session
spark = SparkSession.builder.appName("NestedJSON_Processing").getOrCreate()

# 2. Simulate Raw Nested JSON Data (e.g., from an API or Web Server)
# Notice how 'purchases' is an array [] containing multiple items
json_data = [
    """{"log_id": "L001", "user": {"id": "U123", "country": "India"}, "purchases": [{"item": "Laptop", "amount": 1200}, {"item": "Mouse", "amount": 25}]}""",
    """{"log_id": "L002", "user": {"id": "U124", "country": "UK"}, "purchases": [{"item": "Keyboard", "amount": 50}]}"""
]

# Read the JSON strings into a PySpark DataFrame
rdd = spark.sparkContext.parallelize(json_data)
df_raw = spark.read.json(rdd)

# 3. Transformation Phase 1: Flatten the Structs (Dictionaries)
# We use dot notation to pull 'id' and 'country' out of the 'user' struct
df_flattened = df_raw.select(
    col("log_id"),
    col("user.id").alias("user_id"),
    col("user.country").alias("country"),
    col("purchases") # This is still an array
)

# 4. Transformation Phase 2: Explode the Arrays
# 'explode' takes the list of purchases and creates a new row for EVERY item
df_exploded = df_flattened.withColumn("purchase_item", explode(col("purchases")))

# 5. Final Select: Clean Column Names for the Gold Layer
df_final = df_exploded.select(
    col("log_id"),
    col("user_id"),
    col("country"),
    col("purchase_item.item").alias("product_name"),
    col("purchase_item.amount").alias("revenue")
)

# 6. Output to Parquet (Simulated)
print("Successfully parsed nested JSON arrays and flattened schema.")
df_final.show()
# df_final.write.mode("overwrite").parquet("s3://ecommerce-data/silver/flattened_json/")