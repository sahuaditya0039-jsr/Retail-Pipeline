# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "4"
# ///
# MAGIC %run "/Workspace/Users/sahuaditya0039@gmail.com/E-Commerce Analytics/00_Setup_Config"

# COMMAND ----------

# MAGIC %md
# MAGIC ## reading the silver table

# COMMAND ----------

silver_df = spark.table(silver_table)
display(silver_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## aggregation

# COMMAND ----------

from pyspark.sql import functions as F
gold_city_performance = silver_df.groupBy("city") \
    .agg(
        F.sum("line_item_total").alias("total_revenue"),
        F.countDistinct("order_id").alias("number_of_orders"),
        F.avg("line_item_total").alias("avg_order_value")
    ) \
    .orderBy(F.col("total_revenue").desc())

# COMMAND ----------

gold_city_performance = gold_city_performance.withColumn("total_revenue", F.round("total_revenue", 2)) \
                                             .withColumn("avg_order_value", F.round("avg_order_value", 2))

gold_city_performance.write.format("delta").mode("overwrite").saveAsTable(gold_table)

print(f"Gold analytics complete! Table saved to {gold_table}")
display(gold_city_performance)
