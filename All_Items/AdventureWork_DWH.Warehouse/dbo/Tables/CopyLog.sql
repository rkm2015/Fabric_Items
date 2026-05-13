CREATE TABLE [dbo].[CopyLog] (

	[LogID] bigint IDENTITY NOT NULL, 
	[TableName] varchar(255) NULL, 
	[StartTime] datetime2(6) NULL, 
	[EndTime] datetime2(6) NULL, 
	[RowsInserted] int NULL, 
	[Status] varchar(50) NULL, 
	[PipelineRunId] varchar(100) NULL
);