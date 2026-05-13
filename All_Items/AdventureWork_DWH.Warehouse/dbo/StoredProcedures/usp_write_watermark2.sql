CREATE   PROCEDURE usp_write_watermark2
@LastModifiedtime datetime,
@TableName varchar(50)
AS
BEGIN
UPDATE [dbo].[ControlTable]
SET WatermarkValue = @LastModifiedtime
WHERE SourceTableName = @TableName
END