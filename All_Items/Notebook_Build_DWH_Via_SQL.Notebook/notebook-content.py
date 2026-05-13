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
# MAGIC -- 1. Create the table without the IDENTITY property
# MAGIC CREATE TABLE IF NOT EXISTS dev_lakehouse.gold.DimProduct (
# MAGIC     ProductSK BIGINT,
# MAGIC     SourceID INT,
# MAGIC     ProductName STRING,
# MAGIC     CategoryName STRING,
# MAGIC     StandardCost DOUBLE,
# MAGIC     ListPrice DOUBLE,
# MAGIC     row_hash STRING,
# MAGIC     IsCurrent BOOLEAN,
# MAGIC     ValidFrom TIMESTAMP,
# MAGIC     ValidTo TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC -- 2. Expire old records (This part works fine)
# MAGIC MERGE INTO dev_lakehouse.gold.DimProduct AS target
# MAGIC USING (
# MAGIC     SELECT 
# MAGIC         p.ProductID AS SourceID,
# MAGIC         SHA2(CONCAT_WS('|', p.ProductID, p.Name, p.StandardCost), 256) AS current_hash
# MAGIC     FROM dev_lakehouse.silver.Product_LH p
# MAGIC ) AS source
# MAGIC ON target.SourceID = source.SourceID AND target.IsCurrent = true
# MAGIC WHEN MATCHED AND target.row_hash <> source.current_hash THEN
# MAGIC   UPDATE SET target.IsCurrent = false, target.ValidTo = CURRENT_TIMESTAMP();
# MAGIC 
# MAGIC -- 3. Insert new records with generated Surrogate Keys
# MAGIC -- We find the current max SK to continue the sequence
# MAGIC INSERT INTO dev_lakehouse.gold.DimProduct
# MAGIC SELECT 
# MAGIC     COALESCE((SELECT MAX(ProductSK) FROM dev_lakehouse.gold.DimProduct), 0) + 
# MAGIC     ROW_NUMBER() OVER (ORDER BY p.ProductID) AS ProductSK,
# MAGIC     p.ProductID, p.Name, c.Name, p.StandardCost, p.ListPrice,
# MAGIC     SHA2(CONCAT_WS('|', p.ProductID, p.Name, p.StandardCost), 256),
# MAGIC     true, CURRENT_TIMESTAMP(), NULL
# MAGIC FROM dev_lakehouse.silver.Product_LH p
# MAGIC LEFT JOIN dev_lakehouse.silver.ProductCategory_LH c ON p.ProductCategoryID = c.ProductCategoryID
# MAGIC WHERE NOT EXISTS (
# MAGIC     SELECT 1 FROM dev_lakehouse.gold.DimProduct t 
# MAGIC     WHERE t.SourceID = p.ProductID AND t.IsCurrent = true
# MAGIC );

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- 1. Drop the table to fix the schema mismatch
# MAGIC DROP TABLE IF EXISTS dev_lakehouse.gold.DimCustomer;
# MAGIC 
# MAGIC -- 2. Recreate with BIGINT for both Keys
# MAGIC CREATE TABLE dev_lakehouse.gold.DimCustomer (
# MAGIC     CustomerSK BIGINT,
# MAGIC     SourceID BIGINT, -- Changed from INT to BIGINT
# MAGIC     FirstName STRING,
# MAGIC     LastName STRING,
# MAGIC     EmailAddress STRING,
# MAGIC     Phone STRING,
# MAGIC     AddressLine1 STRING,
# MAGIC     City STRING,
# MAGIC     StateProvince STRING,
# MAGIC     CountryRegion STRING,
# MAGIC     row_hash STRING,
# MAGIC     IsCurrent BOOLEAN,
# MAGIC     ValidFrom TIMESTAMP,
# MAGIC     ValidTo TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC -- 3. Expire logic
# MAGIC MERGE INTO dev_lakehouse.gold.DimCustomer AS target
# MAGIC USING (
# MAGIC     SELECT 
# MAGIC         c.CustomerID AS SourceID, 
# MAGIC         SHA2(CONCAT_WS('|', c.FirstName, c.LastName, c.EmailAddress), 256) AS h
# MAGIC     FROM dev_lakehouse.silver.Customer_LH c
# MAGIC ) AS source
# MAGIC ON target.SourceID = source.SourceID AND target.IsCurrent = true
# MAGIC WHEN MATCHED AND target.row_hash <> source.h THEN
# MAGIC   UPDATE SET target.IsCurrent = false, target.ValidTo = CURRENT_TIMESTAMP();
# MAGIC 
# MAGIC -- 4. Insert logic with Manual SK Generation
# MAGIC INSERT INTO dev_lakehouse.gold.DimCustomer
# MAGIC SELECT 
# MAGIC     COALESCE((SELECT MAX(CustomerSK) FROM dev_lakehouse.gold.DimCustomer), 0) + 
# MAGIC     ROW_NUMBER() OVER (ORDER BY c.CustomerID) AS CustomerSK,
# MAGIC     CAST(c.CustomerID AS BIGINT) AS SourceID,
# MAGIC     c.FirstName, 
# MAGIC     c.LastName, 
# MAGIC     c.EmailAddress, 
# MAGIC     c.Phone,
# MAGIC     a.AddressLine1, 
# MAGIC     a.City, 
# MAGIC     a.StateProvince, 
# MAGIC     a.CountryRegion,
# MAGIC     SHA2(CONCAT_WS('|', c.FirstName, c.LastName, c.EmailAddress), 256) AS row_hash,
# MAGIC     true AS IsCurrent, 
# MAGIC     CURRENT_TIMESTAMP() AS ValidFrom, 
# MAGIC     NULL AS ValidTo
# MAGIC FROM dev_lakehouse.silver.Customer_LH c
# MAGIC LEFT JOIN dev_lakehouse.silver.CustomerAddress_LH ca ON c.CustomerID = ca.CustomerID
# MAGIC LEFT JOIN dev_lakehouse.silver.Address_LH a ON ca.AddressID = a.AddressID
# MAGIC WHERE NOT EXISTS (
# MAGIC     SELECT 1 FROM dev_lakehouse.gold.DimCustomer t 
# MAGIC     WHERE t.SourceID = c.CustomerID AND t.IsCurrent = true
# MAGIC );

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# The "Snapshot" Approach (Current Logic)
# The current logic uses INSERT OVERWRITE.
# 
# What it does: Every time the pipeline runs, it re-joins the entire history of sales with the current version of dimensions (IsCurrent = true).
# 
# Best for: When you want your reports to always show the most recent attributes of a customer (e.g., if a customer moved from New York to London, all their historical sales now show "London").
# 
# Pros: Very simple to maintain; no complex merge logic.
# 
# Cons: You lose the "Point-in-Time" accuracy that SCD Type 2 is designed to provide.

# CELL ********************

# MAGIC %%sql
# MAGIC -- Step 1: Create the table
# MAGIC CREATE TABLE IF NOT EXISTS dev_lakehouse.gold.FactSales_CurrentSnapshot (
# MAGIC     SalesOrderID INT,
# MAGIC     CustomerSK BIGINT,
# MAGIC     ProductSK BIGINT,
# MAGIC     OrderDate TIMESTAMP,
# MAGIC     OrderQty INT,
# MAGIC     UnitPrice DOUBLE,
# MAGIC     Revenue DOUBLE
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC -- Step 2: Load the data using Current version logic
# MAGIC INSERT OVERWRITE dev_lakehouse.gold.FactSales_CurrentSnapshot
# MAGIC SELECT 
# MAGIC     d.SalesOrderID,
# MAGIC     c.CustomerSK,
# MAGIC     p.ProductSK,
# MAGIC     h.OrderDate,
# MAGIC     d.OrderQty,
# MAGIC     d.UnitPrice,
# MAGIC     (d.OrderQty * d.UnitPrice) AS Revenue
# MAGIC FROM dev_lakehouse.silver.SalesOrderDetail_LH d
# MAGIC JOIN dev_lakehouse.silver.SalesOrderHeader_LH h 
# MAGIC     ON d.SalesOrderID = h.SalesOrderID
# MAGIC LEFT JOIN dev_lakehouse.gold.DimProduct p 
# MAGIC     ON d.ProductID = p.SourceID 
# MAGIC     AND p.IsCurrent = true
# MAGIC LEFT JOIN dev_lakehouse.gold.DimCustomer c 
# MAGIC     ON h.CustomerID = c.SourceID 
# MAGIC     AND c.IsCurrent = true;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# To implement Strategy 2 (Point-in-Time) logic, we change how the Fact table joins to the Dimension tables. Instead of looking for IsCurrent = true, we join based on the OrderDate falling within the ValidFrom and ValidTo window of the dimension record.
# 
# This ensures that if a customer moved or a product category changed, the sale is associated with the attributes that were active on the actual day of the transaction.

# CELL ********************

# MAGIC %%sql
# MAGIC -- Step 1: Create the table first to avoid TABLE_OR_VIEW_NOT_FOUND
# MAGIC CREATE TABLE IF NOT EXISTS dev_lakehouse.gold.FactSales_PointInTime (
# MAGIC     SalesOrderID INT,
# MAGIC     CustomerSK BIGINT,
# MAGIC     ProductSK BIGINT,
# MAGIC     OrderDate TIMESTAMP,
# MAGIC     OrderQty INT,
# MAGIC     UnitPrice DOUBLE,
# MAGIC     Revenue DOUBLE
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC -- Step 2: Load the data using Point-in-Time logic
# MAGIC INSERT OVERWRITE dev_lakehouse.gold.FactSales_PointInTime
# MAGIC SELECT 
# MAGIC     d.SalesOrderID,
# MAGIC     c.CustomerSK,
# MAGIC     p.ProductSK,
# MAGIC     h.OrderDate,
# MAGIC     d.OrderQty,
# MAGIC     d.UnitPrice,
# MAGIC     (d.OrderQty * d.UnitPrice) AS Revenue
# MAGIC FROM dev_lakehouse.silver.SalesOrderDetail_LH d
# MAGIC JOIN dev_lakehouse.silver.SalesOrderHeader_LH h 
# MAGIC     ON d.SalesOrderID = h.SalesOrderID
# MAGIC LEFT JOIN dev_lakehouse.gold.DimProduct p 
# MAGIC     ON d.ProductID = p.SourceID 
# MAGIC     AND h.OrderDate >= p.ValidFrom 
# MAGIC     AND (h.OrderDate < p.ValidTo OR p.ValidTo IS NULL)
# MAGIC LEFT JOIN dev_lakehouse.gold.DimCustomer c 
# MAGIC     ON h.CustomerID = c.SourceID 
# MAGIC     AND h.OrderDate >= c.ValidFrom 
# MAGIC     AND (h.OrderDate < c.ValidTo OR c.ValidTo IS NULL);

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# see how to build an Accumulating Snapshot fact next to track the time difference between OrderDate and ShipDate .Building an Accumulating Snapshot Fact is a great way to monitor business process efficiency (like "Order-to-Ship" latency). Unlike the previous two fact tables which capture a single point in time, this table is designed to be updated as the order moves through different milestones.

# MARKDOWN ********************

# The Logic: Accumulating Snapshot Fact
# An accumulating snapshot usually has:
# 
# Multiple Date Keys: One for each milestone (Order Date, Due Date, Ship Date).
# 
# Duration Metrics: Calculated columns showing the days between milestones.
# 
# Status Indicators: To show if the order is still "In Progress" or "Complete

# MARKDOWN ********************

# Key Observations for your Star Schema
# Process Performance: You can now build a Power BI visual showing the "Average Days to Ship" over time.
# 
# Milestone Tracking: If ShipDate is NULL in your source system, DaysToShip will also be NULL. This tells the business that the order is still sitting in the warehouse.
# 
# Relationship to SCD: In this snapshot, we usually join with IsCurrent = true because we are often interested in the current state of the customer responsible for the pending shipment.

# MARKDOWN ********************

# The "Update" Condition: Notice the line WHEN MATCHED AND (target.ShipDate IS NULL AND source.ShipDate IS NOT NULL). This is a performance optimization that tells Spark to only perform the update if a previously empty ShipDate has now been filled.

# CELL ********************

# MAGIC %%sql
# MAGIC -- Step 1: Ensure the table exists (remains the same)
# MAGIC CREATE TABLE IF NOT EXISTS dev_lakehouse.gold.FactSales_AccumulatingSnapshot (
# MAGIC     SalesOrderID INT,
# MAGIC     CustomerSK BIGINT,
# MAGIC     ProductSK BIGINT,
# MAGIC     OrderDate TIMESTAMP,
# MAGIC     DueDate TIMESTAMP,
# MAGIC     ShipDate TIMESTAMP,
# MAGIC     DaysToShip INT,
# MAGIC     IsShipped BOOLEAN,
# MAGIC     Revenue DOUBLE
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC -- Step 2: Use MERGE to update milestones or insert new orders
# MAGIC MERGE INTO dev_lakehouse.gold.FactSales_AccumulatingSnapshot AS target
# MAGIC USING (
# MAGIC     SELECT 
# MAGIC         d.SalesOrderID,
# MAGIC         c.CustomerSK,
# MAGIC         p.ProductSK,
# MAGIC         h.OrderDate,
# MAGIC         h.DueDate,
# MAGIC         h.ShipDate,
# MAGIC         DATEDIFF(h.ShipDate, h.OrderDate) AS DaysToShip,
# MAGIC         CASE WHEN h.ShipDate IS NOT NULL THEN true ELSE false END AS IsShipped,
# MAGIC         (d.OrderQty * d.UnitPrice) AS Revenue
# MAGIC     FROM dev_lakehouse.silver.SalesOrderDetail_LH d
# MAGIC     JOIN dev_lakehouse.silver.SalesOrderHeader_LH h ON d.SalesOrderID = h.SalesOrderID
# MAGIC     LEFT JOIN dev_lakehouse.gold.DimProduct p ON d.ProductID = p.SourceID AND p.IsCurrent = true
# MAGIC     LEFT JOIN dev_lakehouse.gold.DimCustomer c ON h.CustomerID = c.SourceID AND c.IsCurrent = true
# MAGIC ) AS source
# MAGIC ON target.SalesOrderID = source.SalesOrderID
# MAGIC WHEN MATCHED AND (target.ShipDate IS NULL AND source.ShipDate IS NOT NULL) THEN
# MAGIC   UPDATE SET 
# MAGIC     target.ShipDate = source.ShipDate,
# MAGIC     target.DaysToShip = source.DaysToShip,
# MAGIC     target.IsShipped = source.IsShipped
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (SalesOrderID, CustomerSK, ProductSK, OrderDate, DueDate, ShipDate, DaysToShip, IsShipped, Revenue)
# MAGIC   VALUES (source.SalesOrderID, source.CustomerSK, source.ProductSK, source.OrderDate, source.DueDate, source.ShipDate, source.DaysToShip, source.IsShipped, source.Revenue);

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
