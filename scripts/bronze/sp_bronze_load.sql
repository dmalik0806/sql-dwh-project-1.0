/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/
CREATE OR ALTER PROCEDURE bronze.bronze_data_load AS
BEGIN
	BEGIN TRY
		PRINT 'LOAD STARTED FOR CRM BRONZE TABLES';
		TRUNCATE TABLE bronze.crm_cust_info;
		BULK INSERT bronze.crm_cust_info
		FROM 'E:\SQL_Practice\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		PRINT 'CRM_CUST_INFO LOADED';
		TRUNCATE TABLE bronze.crm_prd_info;
		BULK INSERT bronze.crm_prd_info
		FROM 'E:\SQL_Practice\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		PRINT 'CRM_PRD_INFO LOADED';
		TRUNCATE TABLE bronze.crm_sales_details;
		BULK INSERT bronze.crm_sales_details
		FROM 'E:\SQL_Practice\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		PRINT 'CRM_SALES_DETAILS LOADED';
		PRINT 'LOAD COMPLETED FOR CRM BRONZE TABLES';
		PRINT 'LOAD STARTED FOR ERP BRONZE TABLES';
		TRUNCATE TABLE bronze.erp_cust_az12;
		BULK INSERT bronze.erp_cust_az12
		FROM 'E:\SQL_Practice\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		PRINT 'ERP_CUST_AZ12 LOADED';
		TRUNCATE TABLE bronze.erp_loc_a101;
		BULK INSERT bronze.erp_loc_a101
		FROM 'E:\SQL_Practice\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		PRINT 'ERP_LOC_A101 LOADED';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'E:\SQL_Practice\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		PRINT 'ERP_PX_CAT_G1V2 LOADED';
		PRINT 'LOAD COMPLETED FOR ERP BRONZE TABLES';
	END TRY
	BEGIN CATCH
		PRINT 'ERROR OCCURED IN DATA LOAD'
		PRINT ERROR_MESSAGE();
		PRINT ERROR_NUMBER();
		PRINT ERROR_STATE();
	END CATCH
END

