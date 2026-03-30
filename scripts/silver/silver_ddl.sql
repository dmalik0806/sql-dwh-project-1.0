/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema based on incomin data from bronze tables, 
    dropping existing tables if they already exist.
	  Run this script to re-define the DDL structure of 'silver' Tables.
===============================================================================
*/
if object_id('silver.crm_cust_info','U') is not null drop table silver.crm_cust_info;
create table silver.crm_cust_info
(
	cst_id	INT,
	cst_key	NVARCHAR(30),
	cst_firstname NVARCHAR(30),
	cst_lastname NVARCHAR(30),	
	cst_marital_status NVARCHAR(5),	
	cst_gender NVARCHAR(5),	
	cst_create_date DATE,
	dwh_create_date datetime default getdate()
);
go
if object_id('silver.crm_prd_info','U') is not null drop table silver.crm_prd_info;
create table silver.crm_prd_info
(prd_id INT,
cat_id NVARCHAR(10),
prd_key NVARCHAR(30),	
prd_nm NVARCHAR(60),	
prd_cost INT,
prd_line NVARCHAR(5),	
prd_start_dt DATE,	
prd_end_dt DATE,
DWH_CREATE_DATE DATETIME DEFAULT GETDATE()
);
go
if object_id('silver.crm_sales_details','U') is not null drop table silver.crm_sales_details;
create table silver.crm_sales_details
(
sls_ord_num NVARCHAR(20),
sls_prd_key	NVARCHAR(20),
sls_cust_id	INT,
sls_order_dt date,
sls_ship_dt	date,
sls_due_dt date,	
sls_sales INT,	
sls_quantity INT,	
sls_price INT,
DWH_CREATE_DATE datetime default getdate()
);
