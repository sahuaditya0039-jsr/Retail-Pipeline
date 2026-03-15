# Databricks notebook source
# MAGIC %md
# MAGIC ## running the config file

# COMMAND ----------

# MAGIC %run "/Workspace/Users/sahuaditya0039@gmail.com/E-Commerce Analytics/00_Setup_Config"

# COMMAND ----------

# MAGIC %md
# MAGIC ## reading the bronze table 

# COMMAND ----------

bronze_df = spark.table(bronze_table)
bronze_df.display()

# COMMAND ----------

import pyspark.sql.functions as f
silver_df = bronze_df.withColumn("items",f.explode(f.col("items")))

silver_df =silver_df.select(
    "order_id",
    "customer_id",
    "order_date",
    "city",
    f.col("items.prod").alias("product_name"),
    f.col("items.qty").cast("int").alias("quantity"),
    f.col("items.price").cast("decimal(10,2)").alias("unit_price")
    )


# COMMAND ----------

silver_df = silver_df.withColumn("line_item_total", f.col("quantity") * f.col("unit_price"))
silver_df = silver_df.withColumn("processed_datetime", f.current_timestamp())

# COMMAND ----------

silver_df.display()

# COMMAND ----------

silver_df.write.format("delta").mode("overwrite").saveAsTable(silver_table)

print(f"Silver layer complete! Data saved to {silver_table}")
silver_df.display()
