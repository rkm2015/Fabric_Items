CREATE TABLE [dbo].[ControlTable] (

	[ControlID] int NOT NULL, 
	[SourceTableName] varchar(200) NOT NULL, 
	[TargetTableName] varchar(200) NOT NULL, 
	[WatermarkColumn] varchar(200) NOT NULL, 
	[WatermarkValue] datetime2(6) NULL, 
	[FilterClause] varchar(500) NULL, 
	[StreamNumber] int NOT NULL, 
	[IsActive] bit NOT NULL, 
	[Partition_column] varchar(100) NULL
);