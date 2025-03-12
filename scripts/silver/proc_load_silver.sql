/*
======================================================================================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
======================================================================================================================================
Script Purpose:
	This store procedure performs ETL (Extract, Transform, Load) process
	populate the 'silver' schema tables from the 'bronze' schema.
Actions Performed:
	- Truncate Silver Table
	- Insert transformed and cleansed data from Bronze into Silver Tables.

Parameters:
	None.
	This stored procedure does not accept any parameters or return any values.

Usage example:
	EXEC Silver.load_silver;
======================================================================================================================================
*/

EXEC silver.load_silver;

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '====================================================='
		PRINT 'Loading Silver Layer'
		PRINT '====================================================='

				PRINT '----------------------------------------------';
				PRINT 'LOADING CRM TABLES';
				PRINT '----------------------------------------------';

				-- loading silver.crm_cust_info
		SET @start_time = GETDATE();
					print '>> Truncating Table: silver.crm_cust_info';
					truncate table silver.crm_cust_info;
					print '>> Inserting Data Into: silver.crm_cust_info'
					insert into silver.crm_cust_info (
						cst_id,
						cst_key,
						cst_firstname,
						cst_lastname,
						cst_marital_status,
						cst_gndr,
						cst_create_date)
					select
						cst_id,
						cst_key,
						trim(cst_firstname) as cst_firstname,
						trim(cst_lastname) as cst_lastname,
						case when upper(trim(cst_marital_status)) = 'S' then 'Single'
							when upper(trim(cst_marital_status)) = 'M' then 'Married'
							else 'n/a'
						end cst_marital_status,
						case when upper(trim(cst_gndr)) = 'F' then 'Female'
							when upper(trim(cst_gndr)) = 'M' then 'Male'
							else 'n/a'
						end cst_gndr,
						cst_create_date
					from
						(select *, ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as flag_last
						from bronze.crm_cust_info
						where cst_id is not null
						)t
					where flag_last = 1
					SET @end_time = GETDATE()
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> --------------'


					-- loading silver.crm_prd_info
		SET @start_time = GETDATE();
					print '>> Truncating Table: silver.crm_prd_info';
					truncate table silver.crm_prd_info;
					print '>> Inserting Data Into: silver.crm_prd_info';
					insert into silver.crm_prd_info (
						prd_id,
						cat_id,
						prd_key,
						prd_nm,
						prd_cost,
						prd_line,
						prd_start_dt,
						prd_end_dt
					)
					select
						prd_id,
						replace(substring(prd_key, 1, 5), '-', '_') as cat_id,
						substring(prd_key, 7, len(prd_key)) as prd_key,
						prd_nm,
						isnull(prd_cost, 0) as prd_cost,
						case when upper(trim(prd_line)) = 'M' then 'Mountain'
							when upper(trim(prd_line)) = 'R' then 'Road'
							when upper(trim(prd_line)) = 'S' then 'other Sales'
							when upper(trim(prd_line)) = 'T' then 'Tour'
							else 'n/a'
						end as prd_line,
						cast (prd_start_dt as date) as prd_start_dt,
						cast(LEAD(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 as date) as prd_end_dt
					from bronze.crm_prd_info;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>>---------------------------------------'

						-- loading silver.crm_sales_details
		SET @start_time = GETDATE()
					print '>> Truncating Table: silver.crm_sales_details';
					truncate table silver.crm_sales_details;
					print '>> Inserting Data Into: silver.crm_sales_details';
					insert into silver.crm_sales_details (
						sls_ord_num,
						sls_prd_key,
						sls_cust_id,
						sls_order_dt,
						sls_ship_dt,
						sls_due_dt,
						sls_sales,
						sls_quantity,
						sls_price
					)
					select
						sls_ord_num,
						sls_prd_key,
						sls_cust_id,
						case when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
							else cast(cast(sls_order_dt as varchar) as date)
						end as sls_order_dt,
						case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
							else cast(cast(sls_ship_dt as varchar) as date)
						end as sls_ship_dt,
						case when sls_due_dt = 0 or len(sls_due_dt) != 8 then null
							else cast(cast(sls_due_dt as varchar) as date)
						end as sls_due_dt,
						case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * abs(sls_price)
							then sls_quantity * abs(sls_price)
						else sls_sales
						end as sls_sales,
						sls_quantity,
						case when sls_price is null or sls_price <=0
							then sls_sales / nullif(sls_quantity, 0)
						else sls_price
						end as sls_price
					from bronze.crm_sales_details;
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '>>--------------------------------'

				PRINT '----------------------------------------------';
				PRINT 'LOADING CRM TABLES';
				PRINT '----------------------------------------------';

						--loading silver.erp_cusut_az12
		SET @start_time = GETDATE();
					print '>> Truncating Table: silver.erp_cust_az12';
					truncate table silver.erp_cust_az12;
					print '>> Inserting Data Into: silver.erp_cust_az12'
					insert into silver.erp_cust_az12 (
						cid,
						bdate,
						gen
						)
					select 
						case when cid like 'NAS%' then substring(cid, 4, len(cid)) -- Remove 'NAS' prefix if present
							else cid
						end as cid,
						case when bdate > getdate() then null
							else bdate
						end as bdate, -- set future bdtaes to null
						case when upper(trim(gen)) in ('F', 'Female') then 'Female'
							when upper(trim(gen)) in ('M', 'Male') then 'Male'
							else 'n/a'
						END AS gen -- Normalize gender values and handle unknown cases
					from bronze.erp_cust_az12;
		SET @end_time = GETDATE();
		PRINT '>> Load Dration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '>>------------------------------'

					--loading silver.erp_loc_a101
		SET @start_time = GETDATE();
					print '>> Truncating Table: silver.erp_loc_a101';
					truncate table silver.erp_loc_a101
					print '>> Inseting Data Into: silver.erp_loc_a101'
					insert into silver.erp_loc_a101 (
						cid,
						cntry
						)
					select
						replace(cid, '-', '') cid,
						case when trim(cntry) = 'DE' then 'Germany'
							when trim(cntry) in ('US', 'USA') then 'United States'
							when trim(cntry) = '' or cntry is null then 'n/a'
							else trim(cntry)
						end as cntry
					from bronze.erp_loc_a101;
				SET @end_time = GETDATE();
				PRINT '>> Loading Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds'
				PRINT '>>------------------------------------'

						--loading silver.erp_px_cat_g1v2
		SET @start_time = GETDATE();
					print '>> Truncating Table: silver.erp_px_cat_g1v2';
					truncate table silver.erp_px_cat_g1v2;
					print '>> Inserting Data Into: silver.erp_px_cat_g1v2';
					insert into silver.erp_px_cat_g1v2 (
						id,
						cat,
						subcat,
						maintenance
						)
					select
						id,
						cat,
						subcat,
						maintenance
					from bronze.erp_px_cat_g1v2;
				SET @end_time = GETDATE();
			PRINT '>> Loading Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time)AS NVARCHAR) + ' seconds'
			PRINT '>>------------------------------------------'

				SET @batch_end_time = GETDATE();
				PRINT '========================================='
				PRINT 'Loading SIlver Layer is Completed';
			PRINT '- Total Load Duration: '+ CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
				PRINT '========================================'

			END TRY
			BEGIN CATCH
				PRINT '======================================='
				PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
				PRINT 'Error Message' + ERROR_MESSAGE();
				PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
				PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
				PRINT '======================================='
			END CATCH
END

