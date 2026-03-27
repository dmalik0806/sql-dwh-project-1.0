/*
===============================================================================
SQL Script: Create Profiling for Bronze Tables
===============================================================================
Script Purpose:
    This script will capture the min and max values for each column in all the
	bronze tables. This can be useful while doing unit testing between
	bronze and the source files
===============================================================================
*/

DECLARE @Tables TABLE
(SchemaName varchar(20),
TableName  varchar(20)
);

INSERT INTO @Tables (SchemaName, TableName)
VALUES 
('bronze', 'crm_cust_info'),
('bronze', 'crm_prd_info'),
('bronze', 'crm_sales_detail'),
('bronze', 'erp_cust_az12'),
('bronze', 'erp_loc_a101'),
('bronze', 'erp_px_cat_g1v2');

if OBJECT_ID('tempdb..#Result','U') is not null drop table #Result;
CREATE TABLE #Result
(
    SchemaName SYSNAME,
    TableName  SYSNAME,
    ColumnName SYSNAME,
    MinValue   SQL_VARIANT,
    MaxValue   SQL_VARIANT
);

declare @tblname varchar(30), @schemaname varchar(30),@fulltablename varchar(60),@objectid int,@SQL NVARCHAR(MAX);

declare tbl_cursor cursor for
select schemaname, tablename from @Tables

open tbl_cursor;
	fetch next from tbl_cursor into @schemaname, @tblname;
		while @@fetch_status = 0
			begin
				set @fulltablename = quotename(@schemaname)+'.'+quotename(@tblname);
				set @objectid = object_id(@fulltablename);
				if @objectid is not null
				begin
					set @SQL=''
					select @SQL=@SQL+
					'SELECT 
						''' + @schemaname + ''' AS SchemaName,
						''' + @tblname + ''' AS TableName,
						''' + c.name + ''' AS ColumnName,
						CONVERT(VARCHAR(100),MIN(' + QUOTENAME(c.name) + ')) AS MinValue,
						CONVERT(VARCHAR(100),MAX(' + QUOTENAME(c.name) + ')) AS MaxValue
					FROM ' + @FullTableName + '
					UNION ALL '
					FROM sys.columns c
					WHERE c.object_id = @ObjectID
					
					IF LEN(@SQL) > 0
						BEGIN
							SET @SQL = LEFT(@SQL, LEN(@SQL) - 10);

							INSERT INTO #Result
							EXEC sp_executesql @SQL;
						END
				end
				fetch next from tbl_cursor into @schemaname, @tblname;
			end
close tbl_cursor;
deallocate tbl_cursor;


SELECT * FROM #Result
ORDER BY SchemaName, TableName, ColumnName;

	




