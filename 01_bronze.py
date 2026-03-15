# Databricks notebook source
# MAGIC %md
# MAGIC ## running the config notebook

# COMMAND ----------

# MAGIC %run "/Users/sahuaditya0039@gmail.com/E-Commerce Analytics/00_Setup_Config"

# COMMAND ----------

# MAGIC %md
# MAGIC ## reading the data from the volume 

# COMMAND ----------


df = spark.read.format("json").option("multiline", "true").load(raw_data_path )
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## loading the data inside the bronze delta table 

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable(bronze_table)

print(f"Successfully updated {bronze_table}")
