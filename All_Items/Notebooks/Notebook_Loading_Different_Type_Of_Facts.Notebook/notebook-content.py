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
# MAGIC -- Backdate customers so they match the sales dates
# MAGIC UPDATE GOLD_LAYER.dim_customer 
# MAGIC SET valid_from = '2020-01-01';
# MAGIC 
# MAGIC -- Backdate products so they match the sales dates
# MAGIC UPDATE GOLD_LAYER.dim_product 
# MAGIC SET valid_from = '2020-01-01';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql.functions import current_timestamp, from_utc_timestamp, date_format, lit

# 1. Read the new sales data
source_path = "Files/Source_Data/Sales_Operations.csv"
df_sales_raw = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(source_path)

# 2. Add Audit column in IST (Clean format)
df_bronze_sales = df_sales_raw.withColumn(
    "load_timestamp", 
    date_format(from_utc_timestamp(current_timestamp(), "Asia/Kolkata"), "yyyy-MM-dd HH:mm:ss")
)

# 3. Write to Bronze
df_bronze_sales.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("BRONZE_LAYER.bronze_sales_source")

print(f"✅ Bronze Sales Source updated with {df_bronze_sales.count()} records.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from BRONZE_LAYER.bronze_sales_source

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
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

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS GOLD_LAYER.fact_sales_standard (
# MAGIC     order_id STRING,
# MAGIC     customer_sk BIGINT,        -- Surrogate Key from GOLD_LAYER.dim_customer
# MAGIC     product_sk BIGINT,         -- Surrogate Key from GOLD_LAYER.dim_product
# MAGIC     date_sk INT,               -- Surrogate Key from GOLD_LAYER.dim_date (YYYYMMDD)
# MAGIC     amount DECIMAL(18,2)
# MAGIC )
# MAGIC USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS GOLD_LAYER.fact_sales_current_snapshot (
# MAGIC     order_id STRING,
# MAGIC     customer_sk BIGINT,
# MAGIC     product_sk BIGINT,
# MAGIC     date_sk INT,
# MAGIC     amount DECIMAL(18,2)
# MAGIC )
# MAGIC USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE GOLD_LAYER.fact_order_fulfillment (
# MAGIC     order_id STRING,
# MAGIC     customer_sk BIGINT,
# MAGIC     product_sk BIGINT,  -- Added for  Product Dimension
# MAGIC     date_sk INT,         -- Added for  Date Dimension
# MAGIC     order_date DATE,
# MAGIC     ship_date DATE,
# MAGIC     delivery_date DATE,
# MAGIC     days_to_ship INT,
# MAGIC     days_to_deliver INT,
# MAGIC     is_completed BOOLEAN
# MAGIC ) USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select min(order_date),max(order_date) from BRONZE_LAYER.bronze_sales_source

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select min(valid_from),max(valid_from) from GOLD_LAYER.dim_customer

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select min(valid_from),max(valid_from) from GOLD_LAYER.dim_product

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from BRONZE_LAYER.bronze_sales_source

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT INTO GOLD_LAYER.fact_sales_standard
# MAGIC SELECT 
# MAGIC     s.order_id,
# MAGIC     c.customer_sk,
# MAGIC     p.product_sk,
# MAGIC     d.date_sk,
# MAGIC     CAST(s.amount AS DECIMAL(18,2))
# MAGIC FROM BRONZE_LAYER.bronze_sales_source s
# MAGIC -- Fix: Convert dd-MM-yyyy string to a real Date for the join
# MAGIC JOIN GOLD_LAYER.dim_customer c 
# MAGIC     ON s.customer_id = c.customer_id 
# MAGIC     AND to_date(s.order_date, 'dd-MM-yyyy') BETWEEN c.valid_from AND COALESCE(c.valid_to, '9999-12-31')
# MAGIC JOIN GOLD_LAYER.dim_product p 
# MAGIC     ON s.product_id = p.product_id 
# MAGIC     AND to_date(s.order_date, 'dd-MM-yyyy') BETWEEN p.valid_from AND COALESCE(p.valid_to, '9999-12-31')
# MAGIC JOIN GOLD_LAYER.dim_date d 
# MAGIC     ON to_date(s.order_date, 'dd-MM-yyyy') = d.calendar_date;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from GOLD_LAYER.fact_sales_standard

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC TRUNCATE TABLE GOLD_LAYER.fact_sales_current_snapshot;
# MAGIC INSERT INTO GOLD_LAYER.fact_sales_current_snapshot
# MAGIC SELECT 
# MAGIC     s.order_id,
# MAGIC     c.customer_sk,
# MAGIC     p.product_sk,
# MAGIC     d.date_sk,
# MAGIC     CAST(s.amount AS DECIMAL(18,2)),
# MAGIC     current_time() as snapshot_date
# MAGIC FROM BRONZE_LAYER.bronze_sales_source s
# MAGIC JOIN GOLD_LAYER.dim_customer c ON s.customer_id = c.customer_id AND c.is_current = true
# MAGIC JOIN GOLD_LAYER.dim_product p ON s.product_id = p.product_id AND p.is_current = true
# MAGIC JOIN GOLD_LAYER.dim_date d ON CAST(s.order_date AS DATE) = d.calendar_date;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from GOLD_LAYER.fact_sales_current_snapshot

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC delete from GOLD_LAYER.fact_order_fulfillment

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC MERGE INTO GOLD_LAYER.fact_order_fulfillment AS target
# MAGIC USING (
# MAGIC     SELECT 
# MAGIC         s.order_id,
# MAGIC         c.customer_sk,
# MAGIC         p.product_sk,         -- New: Product SK
# MAGIC         d.date_sk,            -- New: Date SK (linked to Order Date)
# MAGIC         to_date(s.order_date, 'dd-MM-yyyy') as clean_order_date,
# MAGIC         to_date(s.ship_date, 'dd-MM-yyyy') as clean_ship_date,
# MAGIC         to_date(s.delivery_date, 'dd-MM-yyyy') as clean_delivery_date
# MAGIC     FROM BRONZE_LAYER.bronze_sales_source s
# MAGIC     JOIN GOLD_LAYER.dim_customer c 
# MAGIC         ON s.customer_id = c.customer_id 
# MAGIC         AND c.is_current = true
# MAGIC     JOIN GOLD_LAYER.dim_product p 
# MAGIC         ON s.product_id = p.product_id 
# MAGIC         AND p.is_current = true
# MAGIC     JOIN GOLD_LAYER.dim_date d 
# MAGIC         ON to_date(s.order_date, 'dd-MM-yyyy') = d.calendar_date
# MAGIC ) AS source
# MAGIC ON target.order_id = source.order_id
# MAGIC 
# MAGIC WHEN MATCHED THEN
# MAGIC     UPDATE SET 
# MAGIC         target.customer_sk = source.customer_sk,
# MAGIC         target.product_sk = source.product_sk,
# MAGIC         target.date_sk = source.date_sk,
# MAGIC         target.ship_date = source.clean_ship_date,
# MAGIC         target.delivery_date = source.clean_delivery_date,
# MAGIC         target.days_to_ship = DATEDIFF(source.clean_ship_date, source.clean_order_date),
# MAGIC         target.days_to_deliver = DATEDIFF(source.clean_delivery_date, source.clean_order_date),
# MAGIC         target.is_completed = CASE WHEN source.clean_delivery_date IS NOT NULL THEN true ELSE false END
# MAGIC 
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (
# MAGIC         order_id, 
# MAGIC         customer_sk, 
# MAGIC         product_sk, 
# MAGIC         date_sk, 
# MAGIC         order_date, 
# MAGIC         ship_date, 
# MAGIC         delivery_date, 
# MAGIC         days_to_ship, 
# MAGIC         days_to_deliver, 
# MAGIC         is_completed
# MAGIC     )
# MAGIC     VALUES (
# MAGIC         source.order_id, 
# MAGIC         source.customer_sk, 
# MAGIC         source.product_sk, 
# MAGIC         source.date_sk, 
# MAGIC         source.clean_order_date, 
# MAGIC         source.clean_ship_date, 
# MAGIC         source.clean_delivery_date,
# MAGIC         DATEDIFF(source.clean_ship_date, source.clean_order_date),
# MAGIC         DATEDIFF(source.clean_delivery_date, source.clean_order_date),
# MAGIC         CASE WHEN source.clean_delivery_date IS NOT NULL THEN true ELSE false END
# MAGIC     );

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from GOLD_LAYER.fact_order_fulfillment

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
