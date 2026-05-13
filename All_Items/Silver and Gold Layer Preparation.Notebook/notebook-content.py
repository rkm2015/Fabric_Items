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

# In the Silver layer, we focus on data quality: removing nulls, fixing data types, and ensuring the data is ready for modeling.


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, current_timestamp, trim

# List of tables to process from Bronze to Silver
tables = [
    "Address_LH", "Customer_LH", "CustomerAddress_LH", 
    "Product_LH", "ProductCategory_LH", "SalesOrderDetail_LH", "SalesOrderHeader_LH"
]

for table in tables:
    # Read from Bronze
    df = spark.read.table(f"dev_lakehouse.bronze.{table}")
    
    # Standardize: Trim strings and add a processing timestamp
    for col_name in df.columns:
        if dict(df.dtypes)[col_name] == "string":
            df = df.withColumn(col_name, trim(col(col_name)))
            
    df = df.withColumn("_silver_load_date", current_timestamp())
    
    # Write to Silver (Overwrite for staging, or Append if you want history)
    df.write.format("delta").mode("overwrite").saveAsTable(f"dev_lakehouse.silver.{table}")

print("Silver Layer populated successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import *
from pyspark.sql.functions import col, lit, current_timestamp, sha2, concat_ws, monotonically_increasing_id

# 1. Source Data Join
source_df = spark.sql("""
    SELECT 
        p.ProductID as SourceID,
        p.Name as ProductName,
        c.Name as CategoryName,
        p.StandardCost,
        p.ListPrice
    FROM dev_lakehouse.silver.Product_LH p
    LEFT JOIN dev_lakehouse.silver.ProductCategory_LH c ON p.ProductCategoryID = c.ProductCategoryID
""")
source_df = source_df.withColumn("row_hash", sha2(concat_ws("|", *source_df.columns), 256))

# 2. Check if table exists; if not, create it manually with Spark to avoid the IDENTITY error
if not spark.catalog.tableExists("dev_lakehouse.gold.DimProduct"):
    source_df.limit(0).withColumn("ProductSK", lit(0).cast("long")) \
             .withColumn("IsCurrent", lit(True)) \
             .withColumn("ValidFrom", current_timestamp()) \
             .withColumn("ValidTo", current_timestamp()) \
             .write.format("delta").mode("overwrite").saveAsTable("dev_lakehouse.gold.DimProduct")

# 3. Merge Logic: Expire changed records
target_table = DeltaTable.forName(spark, "dev_lakehouse.gold.DimProduct")
target_table.alias("t").merge(
    source_df.alias("s"),
    "t.SourceID = s.SourceID AND t.IsCurrent = true AND t.row_hash <> s.row_hash"
).whenMatchedUpdate(set = {
    "IsCurrent": "false",
    "ValidTo": "current_timestamp()"
}).execute()

# 4. Insert New Records with a generated ID
new_records = source_df.alias("s").join(
    spark.table("dev_lakehouse.gold.DimProduct").filter("IsCurrent = true").alias("t"),
    (col("s.SourceID") == col("t.SourceID")) & (col("s.row_hash") == col("t.row_hash")),
    "left_anti"
).select(
    monotonically_increasing_id().alias("ProductSK"), # Generates the Surrogate Key
    "s.*", 
    lit(True).alias("IsCurrent"), 
    current_timestamp().alias("ValidFrom"), 
    lit(None).cast("timestamp").alias("ValidTo")
)

new_records.write.format("delta").mode("append").saveAsTable("dev_lakehouse.gold.DimProduct")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from delta.tables import *
from pyspark.sql.functions import col, lit, current_timestamp, sha2, concat_ws, monotonically_increasing_id

# 1. Join Customer and Address
source_df = spark.sql("""
    SELECT 
        c.CustomerID as SourceID,
        c.FirstName, c.LastName, c.EmailAddress, c.Phone,
        a.AddressLine1, a.City, a.StateProvince, a.CountryRegion
    FROM dev_lakehouse.silver.Customer_LH c
    LEFT JOIN dev_lakehouse.silver.CustomerAddress_LH ca ON c.CustomerID = ca.CustomerID
    LEFT JOIN dev_lakehouse.silver.Address_LH a ON ca.AddressID = a.AddressID
""")
source_df = source_df.withColumn("row_hash", sha2(concat_ws("|", *source_df.columns), 256))

# 2. Schema Initializer
if not spark.catalog.tableExists("dev_lakehouse.gold.DimCustomer"):
    source_df.limit(0).withColumn("CustomerSK", lit(0).cast("long")) \
             .withColumn("IsCurrent", lit(True)) \
             .withColumn("ValidFrom", current_timestamp()) \
             .withColumn("ValidTo", current_timestamp()) \
             .write.format("delta").mode("overwrite").saveAsTable("dev_lakehouse.gold.DimCustomer")

# 3. Merge Logic
target_table = DeltaTable.forName(spark, "dev_lakehouse.gold.DimCustomer")
target_table.alias("t").merge(
    source_df.alias("s"),
    "t.SourceID = s.SourceID AND t.IsCurrent = true AND t.row_hash <> s.row_hash"
).whenMatchedUpdate(set = {"IsCurrent": "false", "ValidTo": "current_timestamp()"}).execute()

# 4. Filter and Insert with Surrogate Key
new_records = source_df.alias("s").join(
    spark.table("dev_lakehouse.gold.DimCustomer").filter("IsCurrent = true").alias("t"),
    (col("s.SourceID") == col("t.SourceID")) & (col("s.row_hash") == col("t.row_hash")),
    "left_anti"
).select(
    monotonically_increasing_id().alias("CustomerSK"),
    "s.*", 
    lit(True).alias("IsCurrent"), 
    current_timestamp().alias("ValidFrom"), 
    lit(None).cast("timestamp").alias("ValidTo")
)

new_records.write.format("delta").mode("append").saveAsTable("dev_lakehouse.gold.DimCustomer")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

# 1. Load Dimensions (Filter for current records to get the correct SK)
dim_product = spark.read.table("dev_lakehouse.gold.DimProduct").filter("IsCurrent = true")
dim_customer = spark.read.table("dev_lakehouse.gold.DimCustomer").filter("IsCurrent = true")

# 2. Load Silver Sales Data
header = spark.read.table("dev_lakehouse.silver.SalesOrderHeader_LH")
detail = spark.read.table("dev_lakehouse.silver.SalesOrderDetail_LH")

# 3. Build Fact Table
# We join Detail and Header, then lookup the Surrogate Keys from Gold
fact_sales = detail.join(header, "SalesOrderID") \
    .join(dim_product, detail.ProductID == dim_product.SourceID, "left") \
    .join(dim_customer, header.CustomerID == dim_customer.SourceID, "left") \
    .select(
        col("SalesOrderID"),
        col("CustomerSK"),   # Surrogate Key from DimCustomer
        col("ProductSK"),    # Surrogate Key from DimProduct
        col("OrderDate"),
        col("OrderQty"),
        col("UnitPrice"),
        # Calculation fix: Multiply Qty by Price to avoid the missing LineTotal error
        (col("OrderQty") * col("UnitPrice")).alias("Revenue")
    )

# 4. Write to Gold
fact_sales.write.format("delta").mode("overwrite").saveAsTable("dev_lakehouse.gold.FactSales")

print("FactSales built successfully with Surrogate Keys.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
