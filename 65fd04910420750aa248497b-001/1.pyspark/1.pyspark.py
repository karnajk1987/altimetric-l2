# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import mean, median, stddev
import os

# Get the current working directory
current_dir = os.getcwd()


# Create a SparkSession
spark = SparkSession.builder \
    .appName("TransactionSummaryStats") \
    .getOrCreate()
    
    
# # Construct the absolute path to the dataset file using the current working directory
# dataset_path = os.path.join(current_dir, "dataset.csv")

dataset_path = "/home/ec2-user/environment/65fd04910420750aa248497b-001/1.pyspark/dataset.csv"
transactions_df = spark.read.csv(dataset_path, header=True, inferSchema=True)
# Load the dataset into a DataFrame
# # Assuming the dataset is in CSV format and has columns like 'customer_id', 'transaction_amount', etc.
# transactions_df = spark.read.csv(dataset_path, header=True, inferSchema=True)

# Display the schema of the DataFrame
transactions_df.printSchema()

# Calculate summary statistics
summary_stats = transactions_df.select(
    mean('transaction_amount').alias('mean_transaction_amount'),
    median('transaction_amount').alias('median_transaction_amount'),
    stddev('transaction_amount').alias('stddev_transaction_amount')
).collect()[0]

# Display the summary statistics
print("Summary Statistics:")
print("Mean Transaction Amount:", summary_stats['mean_transaction_amount'])
print("Median Transaction Amount:", summary_stats['median_transaction_amount'])
print("Standard Deviation of Transaction Amount:", summary_stats['stddev_transaction_amount'])

# Stop the SparkSession
spark.stop()