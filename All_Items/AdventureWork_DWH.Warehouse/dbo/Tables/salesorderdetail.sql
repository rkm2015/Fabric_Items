CREATE TABLE [dbo].[salesorderdetail] (

	[SalesOrderID] int NULL, 
	[SalesOrderDetailID] int NULL, 
	[OrderQty] smallint NULL, 
	[ProductID] int NULL, 
	[UnitPrice] decimal(19,4) NULL, 
	[UnitPriceDiscount] decimal(19,4) NULL, 
	[rowguid] varchar(8000) NULL, 
	[ModifiedDate] datetime2(6) NULL
);