exec bronze.load_bronze;

create or alter procedure bronze.load_bronze as
begin
	declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
	begin try
		set @batch_start_time = getdate();
		print '=========================';
		print 'Loading the Bronze Layer';
		print '=========================';

		print '----------------------';
		print 'Loading for CRM sections';
		print '----------------------';

		set @start_time = getdate();
		print '>>> Truncating Table: bronze.crm_cust_info';
		truncate table bronze.crm_cust_info;

		print '>>> Inserting Data Into: bronze.crm_cust_info';
		bulk insert bronze.crm_cust_info
		from 'C:\Users\Edy\OneDrive\Desktop\bara\data warehouse project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ';',
			tablock
		);
		set @end_time = getdate();
		print '>>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds'
		print '---------------------------'

		print '>>> Truncating Table:  bronze.crm_prd_info';
		truncate table bronze.crm_prd_info;

		set @start_time = getdate();
		print '>>> Inserting Data Into: bronze.crm_prd_info';
		bulk insert bronze.crm_prd_info
		from 'C:\Users\Edy\OneDrive\Desktop\bara\data warehouse project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ';',
			tablock
		);
		set @end_time = getdate();
		print '>>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds'
		print '---------------------------'

		print '>>> Truncating Table:  bronze.crm_sales_details';
		truncate table bronze.crm_sales_details;

		set @start_time = getdate();
		print 'Inserting Data Into: bronze.crm_sales_details';
		bulk insert bronze.crm_sales_details
		from 'C:\Users\Edy\OneDrive\Desktop\bara\data warehouse project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
			firstrow = 2,
			fieldterminator = ';',
			tablock
		);
		set @end_time = getdate();
		print '>>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds'
		print '---------------------------'

		print '----------------------';
		print 'Loading for ERP sections';
		print '----------------------';

		print '>>> Truncating Table:  bronze.erp_cust_a212';
		truncate table bronze.erp_cust_a212;

		set @start_time = getdate();
		print 'Inserting Data Into: bronze.erp_cust_a212';
		bulk insert bronze.erp_cust_a212
		from 'C:\Users\Edy\OneDrive\Desktop\bara\data warehouse project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with (
			firstrow = 2,
			fieldterminator = ';',
			tablock
		);
		set @end_time = getdate();
		print '>>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds'
		print '---------------------------'

		print '>>> Truncating Table:  bronze.erp_loc_a101';
		truncate table bronze.erp_loc_a101;

		set @start_time = getdate();
		print 'Inserting Data Into: bronze.erp_loc_a101';
		bulk insert bronze.erp_loc_a101
		from 'C:\Users\Edy\OneDrive\Desktop\bara\data warehouse project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds'
		print '---------------------------'

		print '>>> Truncating Table:  bronze.erp_px_cat_g1v2';
		truncate table bronze.erp_px_cat_g1v2;

		set @start_time = getdate();
		print 'Inserting Data Into: bronze.erp_px_cat_g1v2';
		bulk insert bronze.erp_px_cat_g1v2
		from 'C:\Users\Edy\OneDrive\Desktop\bara\data warehouse project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>>> Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds'
		print '---------------------------'

		set @batch_end_time = getdate();
		print '============================================='
		print 'Loading the Bronze Layer is completed';
		print '- Total duration: ' + cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + 'seconds';

	end try
	begin catch
		print '============================================='
		print 'ERORR OCCURED DURING LOADING THE BRONZE LAYER'
		print 'Error Message' + ERROR_MESSAGE();
		print 'Error Message' + cast (error_number() as nvarchar);
		print 'Erorr Message' + cast (error_state() as nvarchar);
 		print '============================================='
	end catch
end
