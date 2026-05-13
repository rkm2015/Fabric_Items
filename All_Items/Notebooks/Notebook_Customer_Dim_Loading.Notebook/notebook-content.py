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

# MARKDOWN ********************

# - This note book will have incremental load logic
# - This notebook has silve logic to stage incremental data and get truely changed/new data by comparing with dimension
# - This Tracks SCD1 and SCD2 Types


# CELL ********************

# MAGIC %%sql
# MAGIC --delete from BRONZE_LAYER.bronze_customers
# MAGIC delete from GOLD_LAYER.dim_customer

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Optimized version of Incremental Load** 
# 
# In Previous cell
# 
# - Multiple Action Triggers: You are calling .count() multiple times. In Spark, each .count() is a separate "Action" that re-scans the data.
# 
# - Redundant SQL Query: You are running a spark.sql query to find the Max date, then performing a separate Spark read for the CSV.
# 
# - Schema Inference: Using .option("inferSchema", "true") forces Spark to read the CSV twice (once to guess types, once to load data).

# CELL ********************

from pyspark.sql.functions import max, col, to_date, lit
from pyspark.sql.types import StructType, StructField, StringType

# 1. Define the Schema (Reading everything as String initially to avoid corruption)
custom_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("customer_code", StringType(), True),
    StructField("customer_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("country", StringType(), True),
    StructField("customer_type", StringType(), True),
    StructField("record_date", StringType(), True) 
])

# 2. Get the Watermark from the existing table
try:
    # We cast to DATE so we are comparing actual dates, not strings
    max_date_val = spark.sql("SELECT MAX(record_date) FROM BRONZE_LAYER.bronze_customers").collect()[0][0]
    # Ensure last_watermark is a string in YYYY-MM-DD format for the filter
    last_watermark = str(max_date_val) if max_date_val else '1900-01-01'
except Exception:
    # If table doesn't exist, start from the beginning
    last_watermark = '1900-01-01'

print(f"Active Watermark: {last_watermark}")

# 3. Read the Source CSV
source_path = "Files/Source_Data/Customer.csv"
source_df = spark.read.format("csv") \
    .option("header", "true") \
    .schema(custom_schema) \
    .load(source_path)

# 4. Transform and Filter
# We convert the CSV's 'dd-MM-yyyy' into a real Spark DateType
processed_df = source_df.withColumn("record_date", to_date(col("record_date"), 'dd-MM-yyyy'))

# Now filter using the proper Date objects
delta_records = processed_df.filter(col("record_date") >= lit(last_watermark))

# 5. Write to Bronze Layer
if delta_records.count() > 0:
    delta_records.write.format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable("BRONZE_LAYER.bronze_customers")
    
    print(f"✅ Success: Processed {delta_records.count()} records into Bronze.")
else:
    print("ℹ️ No new records found. All records in CSV are older than or equal to the current watermark.")
    # Show what we found in the file vs the watermark for debugging
    print("Sample from file (after conversion):")
    processed_df.select("record_date").show(1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from BRONZE_LAYER.bronze_customers order by customer_id,record_date

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS GOLD_LAYER.dim_customer (
# MAGIC   customer_sk   BIGINT,
# MAGIC   customer_id   INT,
# MAGIC   customer_code STRING,
# MAGIC   customer_name STRING,
# MAGIC   city          STRING,
# MAGIC   state         STRING,
# MAGIC   country       STRING,
# MAGIC   customer_type STRING,
# MAGIC   valid_from    DATE,
# MAGIC   valid_to      DATE,
# MAGIC   is_current    BOOLEAN
# MAGIC ) USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- We only stage records that are NEWER than our last run 
# MAGIC -- OR differ from the current active record in Gold.
# MAGIC -- Means either a new insert or an update/expire
# MAGIC --To treat customer_type as SCD Type 1 (overwrite) while keeping other fields as SCD Type 2 (history), you need to change your logic 
# MAGIC --so that a change in customer_type updates all existing records for that customer (including historical ones) rather than triggering a new version.
# MAGIC --Remove customer_type from the comparison check. This ensures that if only the customer_type changes, we don't treat it as a new SCD2 row.
# MAGIC CREATE OR REPLACE TABLE SILVER_LAYER.silver_customers_staged AS
# MAGIC WITH RankedBronze AS (
# MAGIC     SELECT *,
# MAGIC            ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY record_date DESC) as internal_rank
# MAGIC     FROM BRONZE_LAYER.bronze_customers
# MAGIC ),
# MAGIC LatestUniqueBronze AS (
# MAGIC     SELECT * FROM RankedBronze WHERE internal_rank = 1
# MAGIC ),
# MAGIC ChangesOnly AS (
# MAGIC     SELECT 
# MAGIC         CAST(b.customer_id AS INT) AS customer_id,
# MAGIC         b.customer_code,
# MAGIC         b.customer_name,
# MAGIC         b.city,
# MAGIC         b.state,
# MAGIC         b.country,
# MAGIC         b.customer_type,
# MAGIC         TO_DATE(b.record_date) AS record_date,
# MAGIC         -- Add a flag to detect if ONLY customer_type changed
# MAGIC         CASE WHEN (b.city <> g.city OR b.state <> g.state OR b.country <> g.country) THEN 'SCD2'
# MAGIC              WHEN (b.customer_type <> g.customer_type) THEN 'SCD1'
# MAGIC              ELSE 'NEW' END AS change_type
# MAGIC     FROM LatestUniqueBronze b
# MAGIC     LEFT JOIN GOLD_LAYER.dim_customer g ON b.customer_id = g.customer_id AND g.is_current = true
# MAGIC     WHERE g.customer_id IS NULL 
# MAGIC        OR b.city <> g.city OR b.state <> g.state OR b.country <> g.country 
# MAGIC        OR b.customer_type <> g.customer_type
# MAGIC )
# MAGIC SELECT * FROM ChangesOnly;
# MAGIC 
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from SILVER_LAYER.silver_customers_staged

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- SCD Type 1: Overwrite customer_type everywhere for these customers
# MAGIC MERGE INTO GOLD_LAYER.dim_customer AS target
# MAGIC USING SILVER_LAYER.silver_customers_staged AS source
# MAGIC ON target.customer_id = source.customer_id
# MAGIC WHEN MATCHED AND target.customer_type <> source.customer_type THEN
# MAGIC     UPDATE SET target.customer_type = source.customer_type;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Expire ONLY if SCD2 attributes changed
# MAGIC MERGE INTO GOLD_LAYER.dim_customer AS target
# MAGIC USING SILVER_LAYER.silver_customers_staged AS source
# MAGIC ON target.customer_id = source.customer_id AND target.is_current = true
# MAGIC WHEN MATCHED AND (
# MAGIC     target.city <> source.city OR 
# MAGIC     target.state <> source.state OR 
# MAGIC     target.country <> source.country
# MAGIC ) THEN 
# MAGIC     UPDATE SET 
# MAGIC         target.is_current = false, 
# MAGIC         target.valid_to = source.record_date;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from GOLD_LAYER.dim_customer order by customer_id

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Insert new versions
# MAGIC INSERT INTO GOLD_LAYER.dim_customer
# MAGIC SELECT 
# MAGIC     (SELECT COALESCE(MAX(customer_sk), 0) FROM dim_customer) + 
# MAGIC     ROW_NUMBER() OVER (ORDER BY s.customer_id) AS customer_sk,
# MAGIC     s.customer_id,
# MAGIC     s.customer_code,
# MAGIC     s.customer_name,
# MAGIC     s.city,
# MAGIC     s.state,
# MAGIC     s.country,
# MAGIC     s.customer_type,
# MAGIC     s.record_date AS valid_from,
# MAGIC     NULL AS valid_to,
# MAGIC     true AS is_current
# MAGIC FROM SILVER_LAYER.silver_customers_staged s
# MAGIC where s.change_type<>'SCD1';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC 
# MAGIC select * from GOLD_LAYER.dim_customer


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
