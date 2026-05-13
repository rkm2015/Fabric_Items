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

df = spark.read.format("csv").option("header","true").load("Files/Source_Data/Product.csv")
# df now is a Spark DataFrame containing CSV data from "Files/Source_Data/Product.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/Source_Data/Product_Changes.csv")
# df now is a Spark DataFrame containing CSV data from "Files/Source_Data/Product_Changes.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/Source_Data/Product_Changes_SCD_1_2.csv")
# df now is a Spark DataFrame containing CSV data from "Files/Source_Data/Product_Changes_SCD_1_2.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC delete from BRONZE_LAYER.bronze_product

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Bronze Layer: Ingest with Timestamp
# This code reads the source and appends it to the BRONZE_LAYER.bronze_product table, adding a current_timestamp() to track when the data arrived.

# CELL ********************

from pyspark.sql.functions import current_timestamp, from_utc_timestamp, date_format

# Load source
df_source = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("Files/Source_Data/Product.csv")

# Add ingestion_date as a sortable string (IST)
df_bronze = df_source.withColumn(
    "ingestion_date", 
    date_format(from_utc_timestamp(current_timestamp(), "Asia/Kolkata"), "yyyy-MM-dd HH:mm:ss")
)

# Count for logging
new_records_count = df_bronze.count()

# Write with mergeSchema to handle the change from DATE to STRING
df_bronze.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("BRONZE_LAYER.bronze_product")

print(f" Successfully added {new_records_count} records to BRONZE_LAYER.bronze_product.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT *  from BRONZE_LAYER.bronze_product ORDER by product_id 

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Silver Layer: Identify "True" Changes
# To build an SCD, you shouldn't process every record in Bronze every time. You only want the latest version of each product that is different from what is currently in your Gold dimension.

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS GOLD_LAYER.dim_product 
# MAGIC (
# MAGIC     product_sk BIGINT,        -- Surrogate Key (Primary Key)
# MAGIC     product_id INT,           -- Natural Key from Source
# MAGIC     product_code STRING,
# MAGIC     product_name STRING,
# MAGIC     category STRING,
# MAGIC     sub_category STRING,
# MAGIC     list_price DECIMAL(10,2),
# MAGIC     valid_from DATE,          -- Start of record version
# MAGIC     valid_to DATE,            -- End of record version (NULL if current)
# MAGIC     is_current BOOLEAN        -- Flag for the latest version
# MAGIC )
# MAGIC USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC delete from GOLD_LAYER.dim_product

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Create a staging table of only NEW or CHANGED records
# MAGIC CREATE OR REPLACE TABLE SILVER_LAYER.silver_products_staged AS
# MAGIC WITH LatestBronze AS (
# MAGIC     SELECT * FROM (
# MAGIC         SELECT *,
# MAGIC                -- This ranks by our new string format (Highest timestamp = 1)
# MAGIC                ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY ingestion_date DESC) as rank
# MAGIC         FROM BRONZE_LAYER.bronze_product
# MAGIC     ) WHERE rank = 1
# MAGIC )
# MAGIC SELECT 
# MAGIC     b.product_id, b.product_code, b.product_name, 
# MAGIC     b.category, b.sub_category, b.list_price
# MAGIC FROM LatestBronze b
# MAGIC LEFT JOIN GOLD_LAYER.dim_product g 
# MAGIC     ON b.product_id = g.product_id 
# MAGIC     AND g.is_current = true
# MAGIC WHERE g.product_id IS NULL             -- New Product
# MAGIC    OR b.product_name <> g.product_name -- Type 2 Change
# MAGIC    OR b.list_price <> g.list_price     -- Type 2 Change
# MAGIC    OR b.category <> g.category         -- Type 2 Change
# MAGIC    OR b.sub_category <> g.sub_category;-- Type 1 Change
# MAGIC 
# MAGIC    select * from SILVER_LAYER.silver_products_staged ;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Gold Layer**: SCD Type 1 & Type 2 Logic
# In this scenario:
# 
# **Type 1 (Overwrite)**: sub_category (updates all historical records for that product).
# 
# **Type 2 (History)**: product_name and list_price (expires the old row and inserts a new one).

# MARKDOWN ********************

# **Part A: Type 1 Update (Overwrite)**
# Use this for attributes where you don't care about history (e.g., correcting a typo in sub_category).

# CELL ********************

# MAGIC %%sql
# MAGIC -- Overwrite sub_category for all existing records of that product
# MAGIC MERGE INTO GOLD_LAYER.dim_product AS target
# MAGIC USING SILVER_LAYER.silver_products_staged AS source
# MAGIC ON target.product_id = source.product_id
# MAGIC WHEN MATCHED AND target.sub_category <> source.sub_category THEN
# MAGIC     UPDATE SET target.sub_category = source.sub_category;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# **Part B: Type 2 Update (Expire Old + Insert New)**
# 
# Use this for attributes where history is vital (e.g., list_price or product_name).


# CELL ********************

# MAGIC %%sql
# MAGIC select * from GOLD_LAYER.dim_product order by product_id

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- STEP 2: SCD Type 2 (Expire Old Versions)
# MAGIC MERGE INTO GOLD_LAYER.dim_product AS target
# MAGIC USING SILVER_LAYER.silver_products_staged AS source
# MAGIC ON target.product_id = source.product_id AND target.is_current = true
# MAGIC WHEN MATCHED AND (
# MAGIC     target.product_name <> source.product_name OR 
# MAGIC     target.list_price <> source.list_price OR
# MAGIC     target.category <> source.category
# MAGIC ) THEN 
# MAGIC     UPDATE SET 
# MAGIC         target.is_current = false, 
# MAGIC         target.valid_to = CURRENT_DATE();
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- STEP 3: Insert New/Updated Versions
# MAGIC INSERT INTO GOLD_LAYER.dim_product
# MAGIC SELECT 
# MAGIC     (SELECT COALESCE(MAX(product_sk), 0) FROM GOLD_LAYER.dim_product) + 
# MAGIC     ROW_NUMBER() OVER (ORDER BY s.product_id) AS product_sk,
# MAGIC     s.product_id, s.product_code, s.product_name, s.category, s.sub_category, s.list_price,
# MAGIC     CURRENT_DATE() AS valid_from, NULL AS valid_to, true AS is_current
# MAGIC FROM SILVER_LAYER.silver_products_staged s
# MAGIC LEFT JOIN GOLD_LAYER.dim_product d ON s.product_id = d.product_id AND d.is_current = true
# MAGIC WHERE d.product_id IS NULL;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
