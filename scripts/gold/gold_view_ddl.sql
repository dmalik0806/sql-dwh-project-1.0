/*
=====================================================================================================
DDL Script: Create Gold views
=====================================================================================================
Usage:
This script will create views for the Gold layer in the data warehouse.
The Gold layer represents the final dimension and fact tables (Star schema)

Each view performs transformations and combines data from silver layer to produce
a clean, enriched and business ready dataset

These views can be used directly for analytics and reporting
=====================================================================================================
*/

create view gold.dim_customer as
select row_number() over(order by a.cst_id) as customer_key
,a.cst_id customer_id
,a.cst_key customer_number
,a.cst_firstname customer_first_name
,a.cst_lastname customer_last_name
,case when a.cst_gender <> 'N/A' then a.cst_gender else coalesce(b.gen,'N/A') end as customer_gender 
,b.bdate customer_dob
,c.cntry customer_country
,a.cst_create_date customer_create_date
from  silver.crm_cust_info a
left join silver.erp_cust_az12 b on a.cst_key=b.cid
left join silver.erp_loc_a101 c on a.cst_key=c.cid;

create view gold.dim_products as
select row_number() over(order by a.prd_key) as product_key
,a.cat_id category_id
,b.cat category
,b.subcat sub_category
,a.prd_id product_id
,a.prd_key product_number
,a.prd_nm product_name
,a.prd_cost product_cost
,a.prd_line product_line
,b.maintenance
,a.prd_start_dt product_start_date
,a.prd_end_dt product_end_date
from silver.crm_prd_info a
left join silver.erp_px_cat_g1v2 b on a.cat_id=b.id
where prd_end_dt = '9999-12-31';

create view gold.fact_sales as
select a.sls_ord_num sales_order_number
,c.product_key
,b.customer_key
,a.sls_order_dt order_date
,a.sls_ship_dt shipping_date
,a.sls_due_dt due_date
,a.sls_sales sales_amount
,a.sls_quantity quantity
,a.sls_price price
from silver.crm_sales_details a
left join gold.dim_customer b on a.sls_cust_id = b.customer_id
left join gold.dim_products c on a.sls_prd_key = c.product_number
