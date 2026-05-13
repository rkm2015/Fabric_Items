CREATE   PROCEDURE usp_log_copy_details
    @TableName VARCHAR(255),
    @StartTime DATETIME,
    @EndTime DATETIME,
    @RowsInserted INT,
    @Status VARCHAR(50),
    @PipelineRunId VARCHAR(100)
AS
BEGIN
    INSERT INTO [dbo].[CopyLog] (TableName, StartTime, EndTime, RowsInserted, Status, PipelineRunId)
    VALUES (@TableName, @StartTime, @EndTime, @RowsInserted, @Status, @PipelineRunId);
END