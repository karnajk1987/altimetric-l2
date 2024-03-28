# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StructType, StructField

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("DataProcessing") \
    .getOrCreate()

# Define schema for the dataset
schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("transaction_amount", IntegerType(), True)
])

# Load data from source (CSV file)
source_df = spark.read.csv("s3://your-bucket/data.csv", header=True, schema=schema)

# Perform data transformations (example: aggregations)
result_df = source_df.groupBy("customer_id").agg({"transaction_amount": "sum"})

# Write results to Snowflake table
snowflake_options = {
    "sfURL": "your-snowflake-url",
    "sfAccount": "your-account",
    "sfUser": "your-username",
    "sfPassword": "your-password",
    "sfDatabase": "your-database",
    "sfSchema": "your-schema",
    "sfWarehouse": "your-warehouse",
    "dbtable": "snowflake_table"
}

result_df.write.format("snowflake") \
    .options(**snowflake_options) \
    .mode("overwrite") \
    .save()

# Stop SparkSession
spark.stop()
