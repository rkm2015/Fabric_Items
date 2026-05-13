# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "3aed7b50-2b26-42a1-82ed-c40a68684fba",
# META       "default_lakehouse_name": "Dev_Lakehouse",
# META       "default_lakehouse_workspace_id": "35f8c3f3-0a5f-4298-a25b-87c98e34404e",
# META       "known_lakehouses": [
# META         {
# META           "id": "3aed7b50-2b26-42a1-82ed-c40a68684fba"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE GOLD_LAYER.dim_date AS
# MAGIC WITH DateRange AS (
# MAGIC   SELECT explode(sequence(to_date('2020-01-01'), to_date('2030-12-31'), interval 1 day)) AS calendar_date
# MAGIC )
# MAGIC SELECT
# MAGIC   year(calendar_date) * 10000 + month(calendar_date) * 100 + day(calendar_date) AS date_sk,
# MAGIC   calendar_date,
# MAGIC   year(calendar_date) AS year,
# MAGIC   month(calendar_date) AS month,
# MAGIC   date_format(calendar_date, 'MMMM') AS month_name,
# MAGIC   quarter(calendar_date) AS quarter,
# MAGIC   date_format(calendar_date, 'EEEE') AS day_name,
# MAGIC   CASE WHEN date_format(calendar_date, 'E') IN ('Sat', 'Sun') THEN 1 ELSE 0 END AS is_weekend
# MAGIC FROM DateRange;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from GOLD_LAYER.dim_date

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
