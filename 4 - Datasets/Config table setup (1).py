# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import dbutils

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# Configure the necessary input and output paths
config_table_path = "adl://https://adlsinsightshub.dfs.core.windows.net/DataQuality/Data_Quality_Config_Table.csv"
bronze_folder_path = "adl://https://adlsinsightshub.dfs.core.windows.net/DataQuality/Bronze"
silver_folder_path = "adl://https://adlsinsightshub.dfs.core.windows.net/DataQuality/Silver"
quarantine_folder_path = "adl://https://adlsinsightshub.dfs.core.windows.net/DataQuality/Quarantine"

# Load the configuration table
config_table = spark.read.option("header", "true").csv(config_table_path)

# Get the list of incoming datasets (e.g., CSV files)
incoming_datasets = dbutils.fs.ls("dbfs:/https://adlsinsightshub.blob.core.windows.net/insightshub/NOX_Reading_Station/source/latest/YYYY%3D2021/station_20211.csv")

# Iterate over each dataset
for dataset in incoming_datasets:
    # Extract the dataset name from the file path
    dataset_name = dataset.name

    # Load the dataset as a DataFrame
    dataset_df = spark.read.option("header", "true").csv(dataset.path)

    # Perform data quality checks using the configuration table
    # Iterate over each row in the configuration table
    for row in config_table.collect():
        table_name = row["table_name"]
        # Perform data quality checks for the current table_name
        # Example data quality check: checking for null values in all columns
        null_check_columns = row["null_check_columns"].split(",")
        null_check_failed_rows = dataset_df.filter(
            ~dataset_df[column].isNotNull() for column in null_check_columns
        )

        # Create a results DataFrame with the data quality check results
        results_df = spark.createDataFrame(
            [
                (
                    dataset_name,
                    table_name,
                    null_check_failed_rows.count() == 0,  # True if check passed, False otherwise
                )
            ],
            ["dataset_name", "table_name", "check_passed"]
        )

        # Save the results as a CSV file in the Bronze folder
        results_df.write.option("header", "true").csv(f"{bronze_folder_path}/{dataset_name}_results.csv")

        # Filter out the failed rows and save the cleansed data to the Silver folder
        silver_df = dataset_df.subtract(null_check_failed_rows)
        silver_df.write.option("header", "true").csv(f"{silver_folder_path}/{dataset_name}_silver.csv")

        # Save the failed rows to the Quarantine folder
        null_check_failed_rows.write.option("header", "true").csv(f"{quarantine_folder_path}/{dataset_name}_quarantine.csv")


# COMMAND ----------


