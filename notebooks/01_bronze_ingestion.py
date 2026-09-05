# Databricks notebook source
from pyspark.sql import SparkSession

# 1. Ensure you are using your project catalog and schema
spark.sql("USE CATALOG telecom_project;")
spark.sql("USE SCHEMA bronze;")

# Path to your Unity Catalog volume where the CSVs were uploaded
# (Update the volume name if you named yours differently)
volume_path = "/Volumes/telecom_project/bronze/raw_csv_files/"

# List of all your table names
tables = [
    "demographics", "addresses", "customers", "mobile_plans", "devices", 
    "sim_cards", "billing_accounts", "payment_methods", "discounts", 
    "account_discounts", "subscriptions", "vas", "sub_vas", "cdr", 
    "udr", "sms_records", "invoices", "payments", "stores", "employees", 
    "network_towers", "roaming_partners", "support_tickets", 
    "ticket_resolutions", "call_center_logs"
]

# 2. Loop through each file, read as DataFrame, and write as a Delta Table
for table_name in tables:
    file_path = f"{volume_path}{table_name}.csv"
    
    # Read CSV with header and inferred schema
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(file_path)
    
    # Write to Delta table in the bronze schema
    df.write.format("delta") \
        .mode("overwrite") \
        .saveAsTable(f"telecom_project.bronze.{table_name}")
        
    print(f"Successfully ingested and created Bronze table: telecom_project.bronze.{table_name}")

# COMMAND ----------

display(spark.sql("SELECT * FROM telecom_project.bronze.customers LIMIT 5"))