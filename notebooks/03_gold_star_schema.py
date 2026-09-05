# Databricks notebook source
from pyspark.sql import SparkSession

# Read clean silver tables
customers = spark.read.table("telecom_project.silver.customers")
demographics = spark.read.table("telecom_project.silver.demographics")
addresses = spark.read.table("telecom_project.silver.addresses")

# Join customers with demographics on demo_id, and addresses on address_id
dim_customer = customers \
    .join(demographics, "demo_id", "left") \
    .join(addresses, "address_id", "left")

# Save to Gold schema
dim_customer.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("telecom_project.gold.dim_customer")

print("Gold Dimension table created: telecom_project.gold.dim_customer")

# 2. Build Gold Fact: fact_revenue
# Combines invoices and payments to track financial transactions by account
invoices = spark.read.table("telecom_project.silver.invoices")
payments = spark.read.table("telecom_project.silver.payments")

fact_revenue = invoices \
    .join(payments, "invoice_id", "left")

fact_revenue.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("telecom_project.gold.fact_revenue")

print("Gold table created: telecom_project.gold.fact_revenue")

# COMMAND ----------

from pyspark.sql import SparkSession

# 1. Build Gold Dimension: dim_device
devices = spark.read.table("telecom_project.silver.devices")
dim_device = devices.dropDuplicates(["device_id"])

dim_device.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("telecom_project.gold.dim_device")

print("Gold Dimension table created: telecom_project.gold.dim_device")

# 2. Build Gold Dimension: dim_plan
plans = spark.read.table("telecom_project.silver.mobile_plans")
dim_plan = plans.dropDuplicates(["plan_id"])

dim_plan.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("telecom_project.gold.dim_plan")

print("Gold Dimension table created: telecom_project.gold.dim_plan")

# 3. Build Gold Fact: fact_call_usage (CDR analytics)
cdr = spark.read.table("telecom_project.silver.cdr")
fact_call_usage = cdr.dropDuplicates()

fact_call_usage.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("telecom_project.gold.fact_call_usage")

print("Gold Fact table created: telecom_project.gold.fact_call_usage")

# COMMAND ----------

from pyspark.sql import SparkSession

# 1. Build Gold Fact: fact_data_usage (UDR - Usage Data Records)
udr = spark.read.table("telecom_project.silver.udr")
fact_data_usage = udr.dropDuplicates()

fact_data_usage.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("telecom_project.gold.fact_data_usage")

print("Gold Fact table created: telecom_project.gold.fact_data_usage")

# 2. Build Gold Fact: fact_support_tickets (Support & Resolution Analytics)
tickets = spark.read.table("telecom_project.silver.support_tickets")
resolutions = spark.read.table("telecom_project.silver.ticket_resolutions")

# Join support tickets with their resolutions if applicable, otherwise save clean tickets
fact_support_tickets = tickets \
    .join(resolutions, "ticket_id", "left") \
    .dropDuplicates(["ticket_id"])

fact_support_tickets.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("telecom_project.gold.fact_support_tickets")

print("Gold Fact table created: telecom_project.gold.fact_support_tickets")

# 3. Build Gold Fact/Bridge: fact_subscriptions (Core Subscription Mapping)
subscriptions = spark.read.table("telecom_project.silver.subscriptions")
fact_subscriptions = subscriptions.dropDuplicates(["sub_id"])

fact_subscriptions.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("telecom_project.gold.fact_subscriptions")

print("Gold Table created: telecom_project.gold.fact_subscriptions")

print("\n Congratulations! Your entire Gold Star Schema pipeline is now fully complete!")

# COMMAND ----------

from pyspark.sql import SparkSession

# 1. Build Gold Dimension: dim_store
stores = spark.read.table("telecom_project.silver.stores")
dim_store = stores.dropDuplicates(["store_id"])

dim_store.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("telecom_project.gold.dim_store")

print("Gold Dimension table created: telecom_project.gold.dim_store")

# 2. Build Gold Dimension: dim_employee
employees = spark.read.table("telecom_project.silver.employees")
dim_employee = employees.dropDuplicates(["emp_id"])

dim_employee.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("telecom_project.gold.dim_employee")

print("Gold Dimension table created: telecom_project.gold.dim_employee")

print("Gold Dimension table created: telecom_project.gold.dim_employee")

# 3. Build Gold Dimension: dim_network_tower
towers = spark.read.table("telecom_project.silver.network_towers")
dim_network_tower = towers.dropDuplicates()

dim_network_tower.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("telecom_project.gold.dim_network_tower")

print("Gold Dimension table created: telecom_project.gold.dim_network_tower")

# 4. Build Gold Dimension: dim_roaming_partner
roaming = spark.read.table("telecom_project.silver.roaming_partners")
dim_roaming_partner = roaming.dropDuplicates()

dim_roaming_partner.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("telecom_project.gold.dim_roaming_partner")

print("Gold Dimension table created: telecom_project.gold.dim_roaming_partner")

# 5. Build Gold Fact: fact_sms_usage (SMS Tracking)
sms = spark.read.table("telecom_project.silver.sms_records")
fact_sms_usage = sms.dropDuplicates()

fact_sms_usage.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("telecom_project.gold.fact_sms_usage")

print("Gold Fact table created: telecom_project.gold.fact_sms_usage")

# 6. Build Gold Fact: fact_call_center_logs (Customer Support Interaction logs)
logs = spark.read.table("telecom_project.silver.call_center_logs")
fact_call_center_logs = logs.dropDuplicates()

fact_call_center_logs.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("telecom_project.gold.fact_call_center_logs")

print("Gold Fact table created: telecom_project.gold.fact_call_center_logs")

print("\n Absolute perfection! Every single table from your pipeline is now fully modeled into your Gold layer star/constellation schema!")