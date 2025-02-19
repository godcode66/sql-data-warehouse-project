-- Create database 'DataWarehouse'

USE master;
GO


IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'RectoDataWarehouse')
BEGIN
  ALTER RectoDataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
  DROP DATABASE RectoDataWarehouse;
END;
GO
  
CREATE DATABASE RectoDataWarehouse;
GO

USE RectoDataWarehouse;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO
  
CREATE SCHEMA silver;
GO
  
CREATE SCHEMA gold;
GO
