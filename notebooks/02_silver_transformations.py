# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# 1. Set catalog and schema
spark.sql("USE CATALOG telecom_project;")
spark.sql("USE SCHEMA silver;")

# 2. Process Billing Accounts (Fixing the boolean/string type issue cleanly)
df_billing_raw = spark.read.table("telecom_project.bronze.billing_accounts")

df_billing_clean = df_billing_raw \
    .withColumn("paperless", when(col("paperless") == "True", 1).otherwise(0)) \
    .dropDuplicates(["account_id"])

# Write to Silver Delta table
df_billing_clean.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("telecom_project.silver.billing_accounts")

print("Silver table created: telecom_project.silver.billing_accounts")

# 3. Process Customers (Standardizing types and trimming whitespace)
df_cust_raw = spark.read.table("telecom_project.bronze.customers")

df_cust_clean = df_cust_raw \
    .withColumn("customer_id", col("customer_id").cast("string")) \
    .dropDuplicates(["customer_id"])

df_cust_clean.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("telecom_project.silver.customers")

print("Silver table created: telecom_project.silver.customers")

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, lit

# 1. Process Subscriptions (Already succeeded, but kept here for completeness)
df_sub_raw = spark.read.table("telecom_project.bronze.subscriptions")

df_sub_clean = df_sub_raw \
    .withColumn("phone_number", col("phone_number").cast("string")) \
    .dropDuplicates(["sub_id"]) \
    .dropna(subset=["sub_id", "customer_id"])

df_sub_clean.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("telecom_project.silver.subscriptions")

print("Silver table created: telecom_project.silver.subscriptions")

# 2. Process Devices (Fixed to use 'brand' instead of 'device_type')
df_dev_raw = spark.read.table("telecom_project.bronze.devices")

df_dev_clean = df_dev_raw \
    .withColumn("brand", coalesce(col("brand"), lit("Unknown"))) \
    .dropDuplicates(["device_id"])

df_dev_clean.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("telecom_project.silver.devices")

print("Silver table created: telecom_project.silver.devices")

# 3. Process Invoices (Cast financial amounts to proper float types)
df_inv_raw = spark.read.table("telecom_project.bronze.invoices")

df_inv_clean = df_inv_raw \
    .withColumn("amount_due", col("amount_due").cast("float")) \
    .dropDuplicates(["invoice_id"])

df_inv_clean.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("telecom_project.silver.invoices")

print("Silver table created: telecom_project.silver.invoices")

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 1. Process Payments (Using 'amount_paid')
df_pay_raw = spark.read.table("telecom_project.bronze.payments")

df_pay_clean = df_pay_raw \
    .withColumn("amount_paid", col("amount_paid").cast("float")) \
    .dropDuplicates(["payment_id"])

df_pay_clean.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("telecom_project.silver.payments")

print("Silver table created: telecom_project.silver.payments")

# 2. Process Stores
df_store_raw = spark.read.table("telecom_project.bronze.stores")

df_store_clean = df_store_raw \
    .dropDuplicates(["store_id"])

df_store_clean.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("telecom_project.silver.stores")

print("Silver table created: telecom_project.silver.stores")

# 3. Process Support Tickets
df_ticket_raw = spark.read.table("telecom_project.bronze.support_tickets")

df_ticket_clean = df_ticket_raw \
    .dropDuplicates(["ticket_id"])

df_ticket_clean.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("telecom_project.silver.support_tickets")

print("Silver table created: telecom_project.silver.support_tickets")

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 1. Process Demographics (Using 'demo_id')
df_demo_raw = spark.read.table("telecom_project.bronze.demographics")

df_demo_clean = df_demo_raw \
    .dropDuplicates(["demo_id"])

df_demo_clean.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("telecom_project.silver.demographics")

print("Silver table created: telecom_project.silver.demographics")

# 2. Process Addresses
df_addr_raw = spark.read.table("telecom_project.bronze.addresses")

df_addr_clean = df_addr_raw \
    .dropDuplicates(["address_id"])

df_addr_clean.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("telecom_project.silver.addresses")

print("Silver table created: telecom_project.silver.addresses")

# 3. Process Mobile Plans
df_plan_raw = spark.read.table("telecom_project.bronze.mobile_plans")

df_plan_clean = df_plan_raw \
    .dropDuplicates(["plan_id"])

df_plan_clean.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("telecom_project.silver.mobile_plans")

print("Silver table created: telecom_project.silver.mobile_plans")

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 1. Process SIM Cards
df_sim_raw = spark.read.table("telecom_project.bronze.sim_cards")

df_sim_clean = df_sim_raw \
    .dropDuplicates(["sim_id"])

df_sim_clean.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("telecom_project.silver.sim_cards")

print("Silver table created: telecom_project.silver.sim_cards")

# 2. Process Payment Methods
df_pm_raw = spark.read.table("telecom_project.bronze.payment_methods")

df_pm_clean = df_pm_raw \
    .dropDuplicates(["method_id"])

df_pm_clean.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("telecom_project.silver.payment_methods")

print("Silver table created: telecom_project.silver.payment_methods")

# 3. Process Discounts
df_disc_raw = spark.read.table("telecom_project.bronze.discounts")

df_disc_clean = df_disc_raw \
    .dropDuplicates(["discount_id"])

df_disc_clean.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("telecom_project.silver.discounts")

print("Silver table created: telecom_project.silver.discounts")

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 1. Process Account Discounts
df_ad_raw = spark.read.table("telecom_project.bronze.account_discounts")

df_ad_clean = df_ad_raw \
    .dropDuplicates()

df_ad_clean.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("telecom_project.silver.account_discounts")

print("Silver table created: telecom_project.silver.account_discounts")

# 2. Process VAS (Value Added Services)
df_vas_raw = spark.read.table("telecom_project.bronze.vas")

df_vas_clean = df_vas_raw \
    .dropDuplicates(["vas_id"])

df_vas_clean.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("telecom_project.silver.vas")

print("Silver table created: telecom_project.silver.vas")

# 3. Process Sub VAS (Subscription to VAS mapping)
df_subvas_raw = spark.read.table("telecom_project.bronze.sub_vas")

df_subvas_clean = df_subvas_raw \
    .dropDuplicates()

df_subvas_clean.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("telecom_project.silver.sub_vas")

print("Silver table created: telecom_project.silver.sub_vas")

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 1. Process CDR (Call Detail Records)
df_cdr = spark.read.table("telecom_project.bronze.cdr").dropDuplicates()
df_cdr.write.format("delta").mode("overwrite").saveAsTable("telecom_project.silver.cdr")
print("Silver table created: telecom_project.silver.cdr")

# 2. Process UDR (Usage Data Records)
df_udr = spark.read.table("telecom_project.bronze.udr").dropDuplicates()
df_udr.write.format("delta").mode("overwrite").saveAsTable("telecom_project.silver.udr")
print("Silver table created: telecom_project.silver.udr")

# 3. Process SMS Records
df_sms = spark.read.table("telecom_project.bronze.sms_records").dropDuplicates()
df_sms.write.format("delta").mode("overwrite").saveAsTable("telecom_project.silver.sms_records")
print("Silver table created: telecom_project.silver.sms_records")

# 4. Process Employees
df_emp = spark.read.table("telecom_project.bronze.employees").dropDuplicates()
df_emp.write.format("delta").mode("overwrite").saveAsTable("telecom_project.silver.employees")
print("Silver table created: telecom_project.silver.employees")

# 5. Process Network Towers
df_towers = spark.read.table("telecom_project.bronze.network_towers").dropDuplicates()
df_towers.write.format("delta").mode("overwrite").saveAsTable("telecom_project.silver.network_towers")
print("Silver table created: telecom_project.silver.network_towers")

# 6. Process Roaming Partners
df_roaming = spark.read.table("telecom_project.bronze.roaming_partners").dropDuplicates()
df_roaming.write.format("delta").mode("overwrite").saveAsTable("telecom_project.silver.roaming_partners")
print("Silver table created: telecom_project.silver.roaming_partners")

# 7. Process Ticket Resolutions
df_resolutions = spark.read.table("telecom_project.bronze.ticket_resolutions").dropDuplicates()
df_resolutions.write.format("delta").mode("overwrite").saveAsTable("telecom_project.silver.ticket_resolutions")
print("Silver table created: telecom_project.silver.ticket_resolutions")

# 8. Process Call Center Logs
df_logs = spark.read.table("telecom_project.bronze.call_center_logs").dropDuplicates()
df_logs.write.format("delta").mode("overwrite").saveAsTable("telecom_project.silver.call_center_logs")
print("Silver table created: telecom_project.silver.call_center_logs")