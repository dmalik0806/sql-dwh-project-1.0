/*
===============================================================================
Stored Procedure: Load Silver Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the silver schema from bronze schema. 
    It performs the following actions:
    - Transforms the bronze tables data before loading data into silver layer.
    - Uses the 'INSERT' command to load data from bronze tables to silver tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC silver.load_silver;
===============================================================================
*/
create or alter procedure silver.load_silver as
begin
set nocount on;
declare @batch_start_time datetime, @batch_end_time datetime,@rec_count int;
	begin try
		PRINT 'Initiating CRM Silver Load'
		PRINT '----------Loading CRM_CUST_INFO-----------------'
		truncate table silver.crm_cust_info;
		insert into silver.crm_cust_info (cst_id,cst_key,cst_firstname,cst_lastname,cst_marital_status,cst_gender,cst_create_date)
		select cst_id,cst_key,cst_firstname,cst_lastname,cst_marital_status,cst_gndr,cst_create_date from
		(select cst_id,
		cst_key,
		trim(cst_firstname) as cst_firstname,
		trim(cst_lastname) as cst_lastname,
		case when cst_marital_status is null then 'N/A' else cst_marital_status end as cst_marital_status,
		case when cst_gndr is null then 'N/A' else cst_gndr end as cst_gndr,
		cst_create_date,
		row_number() over(partition by cst_id order by cst_create_date desc) as rn
		from bronze.crm_cust_info where cst_id is not null)a where rn = 1;
		set @rec_count=@@rowcount
		PRINT 'Load completed for CRM_CUST_INFO, records inserted: '+ cast(@rec_count as varchar);
		PRINT '----------Loading CRM_PRD_INFO-----------------'
		truncate table silver.crm_prd_info;
		insert into silver.crm_prd_info(prd_id,cat_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt)
		select prd_id,cat_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,cast(isnull(prd_end_dt_new,'9999-12-31') as date) as prd_end_dt from
		(select prd_id,
		replace(substring(prd_key,1,5),'-','_') as cat_id,
		substring(prd_key,7,len(prd_key)) as prd_key,
		trim(prd_nm) as prd_nm,
		isnull(prd_cost,0) prd_cost,
		case when prd_line is null then 'N/A' else prd_line end as prd_line,
		prd_start_dt,
		dateadd(day, -1,lead(prd_start_dt) over(partition by prd_key order by prd_start_dt)) as prd_end_dt_new
		from bronze.crm_prd_info)a
		set @rec_count=@@rowcount
		PRINT 'Load completed for CRM_PRD_INFO, records inserted: '+ cast(@rec_count as varchar);
		PRINT '----------Loading CRM_SALES_DETAILS-----------------'
		truncate table silver.crm_sales_details;
		insert into silver.crm_sales_details(sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price)
		select sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price
		from
		(select sls_ord_num,sls_prd_key,sls_cust_id,
		case when sls_order_dt = 0 or len(sls_order_dt) <> 8 then NULL else cast(cast(sls_order_dt as varchar) as date) end as sls_order_dt,
		cast(cast(sls_ship_dt as varchar) as date) sls_ship_dt,
		cast(cast(sls_due_dt as varchar) as date) sls_due_dt,
		case when sls_sales is null or sls_sales <= 0 or sls_sales <> sls_quantity* abs(sls_price) then abs(sls_price)*sls_quantity else sls_sales end as sls_sales,
		sls_quantity,
		case when sls_price is null or sls_price <= 0 then abs(sls_sales)/sls_quantity else sls_price end as sls_price
		from bronze.crm_sales_details)a
		set @rec_count=@@rowcount
		PRINT 'Load completed for CRM_SALES_DETAILS, records inserted: '+ cast(@rec_count as varchar);
		PRINT 'Load completed for CRM Silver tables'
		PRINT 'Initiating ERP Silver load'
		PRINT '-----------Loading ERP_CUST_AZ12--------------'
		truncate table silver.erp_cust_az12;
		insert into silver.erp_cust_az12(cid,bdate,gen)
		select 
		case when cid like 'NAS%' then substring(cid,4,len(cid)) else cid end as cid,
		case when bdate > getdate() then null else bdate end as bdate,
		case when upper(gen) in ('M','MALE') then 'M'
			 when upper(gen) in ('F','FEMALE') then'F'
			 else 'N/A' end as gen
		from bronze.erp_cust_az12;
		set @rec_count=@@rowcount
		PRINT 'Load completed for ERP_CUST_AZ12, records inserted: '+ cast(@rec_count as varchar);
		PRINT '-----------Loading ERP_LOC_A101--------------'
		truncate table silver.erp_loc_a101;
		INSERT INTO silver.erp_loc_a101 (cid,cntry)
		SELECT
			REPLACE(cid, '-', '') AS cid, 
			CASE
				WHEN TRIM(cntry) = 'DE' THEN 'GERMANY'
				WHEN TRIM(cntry) IN ('US', 'USA') THEN 'UNITED STATES'
				WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'N/A'
				ELSE UPPER(TRIM(cntry))
			END AS cntry 
		FROM bronze.erp_loc_a101;
		set @rec_count=@@rowcount
		PRINT 'Load completed for ERP_LOC_A101, records inserted: '+ cast(@rec_count as varchar);
		PRINT '-----------Loading ERP_PX_CAT_G1V2--------------'
		truncate table silver.erp_px_cat_g1v2;
		insert into silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)
		select id,cat,subcat,maintenance from bronze.erp_px_cat_g1v2;
		set @rec_count=@@rowcount
		PRINT 'Load completed for ERP_PX_CAT_G1V2, records inserted: '+ cast(@rec_count as varchar);
	end try
	begin catch
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	end catch
end;

