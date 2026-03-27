/*
======= Create Database and Schemas ============
Purpose: This script checks and creates a new database 'Sql_DWH' if it does not exists.
         If the database already exists, it will drop it and create it again.
         Also 3 schemas, 'bronze', 'silver' and 'gold' are also created.

Warning: Proceed with caution. This script should only be used for cleanup activity. 
         Ensure to have proper backups before proceeding as it will delete all the objects and data premanently
*/
use master;
go
if exists (select 1 from sys.database where name = 'Sql_DWH')
  begin
  drop database Sql_DWH;
  end;
go  
create database Sql_DWH;
go
use Sql_DWH;
go
create schema bronze;
go
create schema silver;
go
create schema gold;
go
