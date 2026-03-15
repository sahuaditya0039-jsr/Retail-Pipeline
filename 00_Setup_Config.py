# Databricks notebook source
# Notebook: 00_Setup_Config

# 1. Define the Unity Catalog Namespaces
# Replace these with your actual names if you created them in the UI
CATALOG_NAME = "ecom_any_catalog"
SCHEMA_NAME  = "ecom_schema"

# 2. Define the Full Table Paths (3-Tier Namespace)
# This is what you will call in your spark.table() or saveAsTable() commands
bronze_table = f"{CATALOG_NAME}.{SCHEMA_NAME}.bronze_sales"
silver_table = f"{CATALOG_NAME}.{SCHEMA_NAME}.silver_sales"
gold_table   = f"{CATALOG_NAME}.{SCHEMA_NAME}.gold_city_performance"

# 3. Define the Raw Data Path (Pointed to your Volume)
# Volumes are the UC replacement for dbfs mounts
raw_data_path ="/Volumes/workspace/ecom_project/ecom_raw_data/data.json"

# 4. Create the Hierarchy (If it doesn't exist)
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}")

# Set the session context so you don't have to type the catalog name constantly
spark.sql(f"USE CATALOG {CATALOG_NAME}")
spark.sql(f"USE SCHEMA {SCHEMA_NAME}")

print(f" Configured: {CATALOG_NAME}.{SCHEMA_NAME}")
