/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

if object_id('bronze.crm_cust_info','U') is not null drop table bronze.crm_cust_info;
create table bronze.crm_cust_info
(
	cst_id	INT,
	cst_key	NVARCHAR(30),
	cst_firstname NVARCHAR(30),
	cst_lastname NVARCHAR(30),	
	cst_marital_status NVARCHAR(5),	
	cst_gndr NVARCHAR(5),	
	cst_create_date DATE
);
go
if object_id('bronze.crm_sales_details','U') is not null drop table bronze.crm_sales_details;
create table bronze.crm_sales_details
(
sls_ord_num NVARCHAR(20),
sls_prd_key	NVARCHAR(20),
sls_cust_id	INT,
sls_order_dt INT,
sls_ship_dt	INT,
sls_due_dt INT,	
sls_sales INT,	
sls_quantity INT,	
sls_price INT
);
go
if object_id('bronze.crm_prd_info','U') is not null drop table bronze.crm_prd_info;
create table bronze.crm_prd_info
(prd_id INT,	
prd_key NVARCHAR(30),	
prd_nm NVARCHAR(60),	
prd_cost INT,
prd_line NVARCHAR(2),	
prd_start_dt DATE,	
prd_end_dt DATE
);
go
if object_id('bronze.erp_loc_a101','U') is not null drop table bronze.erp_loc_a101
create table bronze.erp_loc_a101
(cid NVARCHAR(50),
cntry NVARCHAR(50)
);
go
if object_id('bronze.erp_cust_az12','U') is not null drop table bronze.erp_cust_az12
create table bronze.erp_cust_az12
(cid NVARCHAR(20),
bdate DATE,
gen NVARCHAR(10)
);
go
if object_id('bronze.erp_px_cat_g1v2','U') is not null drop table bronze.erp_px_cat_g1v2
create table bronze.erp_px_cat_g1v2
(id NVARCHAR(10),
cat NVARCHAR(20),
subcat NVARCHAR(30),
maintenance NVARCHAR(5)
);






