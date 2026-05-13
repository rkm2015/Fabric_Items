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
# META     },
# META     "warehouse": {}
# META   }
# META }

# CELL ********************

df_product=spark.read.table('Product_LH')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/data/Customer.csv")
# df now is a Spark DataFrame containing CSV data from "Files/data/Customer.csv".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

DF_Stored_Filtered = df.filter("StoreID > 900")
display(DF_Stored_Filtered)

DF_Stored_Filtered.write \
    .format("parquet") \
    .mode("overwrite") \
    .save("Files/data/Customer_900.parquet")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.createOrReplaceTempView('VW_Customer')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df.write.mode('overwrite').saveAsTable('Customer_table')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

DF_Stored_Filtered = df.filter("StoreID > 900")
display(DF_Stored_Filtered)

DF_Stored_Filtered.write \
    .format("parquet") \
    .mode("overwrite") \
    .save("Files/data/Customer_900.parquet")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select count(*) from Customer_table

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from VW_Customer

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Dev_Lakehouse.dbo.Address_LH LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select * from Dev_Lakehouse.dbo.Address_LH where City='London'

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_product.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_alias = df_product.select(
    df_product.ProductID.alias("ID"),
    df_product.Name.alias("Product_Name"),
    df_product.ListPrice.alias("Price")
)
display(df_alias)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

df_alias = df_product.select(
    col("ProductID").alias("P_id"), 
    col("Name").alias("Product_Name")
)

display(df_alias)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_product.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format("csv").option("header","true").load("Files/data/Customer.csv")
# df now is a Spark DataFrame containing CSV data from "Files/data/Customer.csv".



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df.limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_filtered=df.filter('CustomerID=1')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_filtered)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

display(df_product)
df_withcolumn = df_product.withColumn(
    "DiscountPrice",
    col("ListPrice") * 0.9
)
display(df_withcolumn)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_order_asc = df_product.orderBy("ListPrice")
display(df_order_asc)

df_order_desc = df_product.orderBy(col("ListPrice").desc())
display(df_order_desc)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_group = df_product.groupBy("Color").count()
display(df_group)


df_group2 = df_product.groupBy("ProductCategoryID").avg("ListPrice")
display(df_group2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dropdup = df_product.select("Color").dropna()
display(df_dropdup)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_dropdup2 = df_product.dropDuplicates(["Color", "Size"])
display(df_dropdup2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col
df=spark.read.table('Product_LH')
filtered_df=df.filter(col("Color").isNull())
display(filtered_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col
df_product=spark.read.table('Product_LH')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


df_final = (
    df_product
        .filter(col("ListPrice") > 100)
        .withColumn("DiscountPrice", col("ListPrice") * 0.85)
        .select(
            col("ProductID").alias("ID"),
            col("Name"),
            col("Color"),
            col("ListPrice"),
            col("DiscountPrice")
        )
        .orderBy(col("DiscountPrice").desc())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

# Ensure df_product is loaded
df_product = spark.read.table('Product_LH')

# Chain the transformations correctly
df_final = (
    df_product
    .filter(col("ListPrice") > 100)
    .withColumn("DiscountPrice", col("ListPrice") * 0.85)
    .select(
        col("ProductID").alias("ID"), 
        col("Name"), 
        col("Color"), 
        col("ListPrice"), 
        col("DiscountPrice")
    )
    .orderBy(col("DiscountPrice").desc())
)

# Display the result
display(df_final)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT 
# MAGIC     ProductID AS ID, 
# MAGIC     Name, 
# MAGIC     Color, 
# MAGIC     ListPrice, 
# MAGIC     (ListPrice * 0.85) AS DiscountPrice
# MAGIC FROM Product_LH
# MAGIC WHERE ListPrice > 100
# MAGIC ORDER BY DiscountPrice DESC

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_parti.rdd.getNumPartitions()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_parti=df_product.repartition(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_coal=df_parti.coalesce(2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_coal.rdd.getNumPartitions()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
