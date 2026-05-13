CREATE TABLE [dbo].[customer] (

	[CustomerSK] bigint NULL, 
	[SourceID] bigint NULL, 
	[FirstName] varchar(8000) NULL, 
	[LastName] varchar(8000) NULL, 
	[EmailAddress] varchar(8000) NULL, 
	[Phone] varchar(8000) NULL, 
	[AddressLine1] varchar(8000) NULL, 
	[City] varchar(8000) NULL, 
	[StateProvince] varchar(8000) NULL, 
	[CountryRegion] varchar(8000) NULL, 
	[row_hash] varchar(8000) NULL, 
	[IsCurrent] bit NULL, 
	[ValidFrom] datetime2(6) NULL, 
	[ValidTo] datetime2(6) NULL
);