# End-to-End E-Commerce Data Pipeline (Medallion Architecture)

## Project Overview
An automated ETL pipeline built in Databricks using PySpark to transform raw nested JSON retail data into actionable business insights.

## Architecture
The project follows the Medallion Architecture to ensure data quality:
Bronze: Raw ingestion of JSON files from Unity Catalog Volumes.
Silver: Data cleaning, schema enforcement, and array flattening using explode.
Gold: Business-level aggregations (City-wise revenue and order trends).

## Tech Stack
Language: PySpark (Python)
Platform: Databricks 
Storage: Delta Lake
Orchestration: Databricks Notebook Workflows

## Key Insights
Developed a 'City Performance' dashboard identifying top-revenue regions and average order values.
