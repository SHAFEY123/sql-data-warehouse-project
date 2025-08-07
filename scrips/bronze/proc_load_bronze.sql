/*
====================================================================
Stored Procedure:     Load Bronze Layer (Source → Bronze)
====================================================================
Script Purpose:       
  This stored procedure loads data into the 'bronze' schema from external 
  CSV files.

It performs the following actions:
  - Truncates the bronze tables before loading data.
  - Uses the BULK INSERT command to load data from CSV files into bronze tables.

Parameters:           
    None. This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
====================================================================
*/


create or alter procedure bronze.load_bronze as 
begin 
    declare @start_time datetime, @end_time datetime,@batch_start_time datetime,@batch_end_time datetime;
    begin try 
        set @batch_start_time = GETDATE();
        print'========================================';
        print'Loading the Bronze Layer';
        print'========================================';
        print'----------------------------------------';
        print'Loading CRM tables';
        print'----------------------------------------';

--1
        set @start_time=GETDATE();
        print'>> Truncating table: bronze.crm_cust_info';
        truncate table bronze.crm_cust_info;

        print'>> Inserting  data into: bronze.crm_cust_info';

        bulk insert bronze.crm_cust_info
        from 'C:\Users\shafe\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        with (
	        firstrow = 2,
	        fieldterminator = ',',
	        tablock 
        );
        set @end_time=GETDATE();
        print'>> Load duration: ' + cast(datediff(second, @start_time,@end_time) as nvarchar) +' seconds'
        print'------------------------------------------';

--2

        set @start_time=GETDATE();
        print'>> Truncating table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        print'>> Inserting  data into: bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\shafe\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        set @end_time=GETDATE();
        print'>> Load duration: ' + cast(datediff(second, @start_time,@end_time) as nvarchar) +' seconds'
        print'------------------------------------------';
--3

        set @start_time=GETDATE();
        print'>> Truncating table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        print'>> Inserting  data into: bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\shafe\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        set @end_time=GETDATE();
        print'>> Load duration: ' + cast(datediff(second, @start_time,@end_time) as nvarchar) +' seconds'
        print'------------------------------------------';
--4

        print'----------------------------------------';
        print'Loading ERP tables';
        print'----------------------------------------';

        set @start_time=GETDATE();
        print'>> Truncating table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        print'>> Inserting  data into: bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\shafe\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        set @end_time=GETDATE();
        print'>> Load duration: ' + cast(datediff(second, @start_time,@end_time) as nvarchar) +' seconds'
        print'------------------------------------------';
--5
        set @start_time=GETDATE();
        print'>> Truncating table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        print'>> Inserting  data into: bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\shafe\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        set @end_time=GETDATE();
        print'>> Load duration: ' + cast(datediff(second, @start_time,@end_time) as nvarchar) +' seconds'
        print'------------------------------------------';
--6

        set @start_time=GETDATE();
        print'>> Truncating table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        print'>> Inserting  data into: bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\shafe\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );
        set @end_time=GETDATE();
        print'>> Load duration: ' + cast(datediff(second, @start_time,@end_time) as nvarchar) +' seconds'
        print'------------------------------------------';
        set @batch_end_time = GETDATE();
        print'==========================================';
        print'Loading Bronze Layer Is Completed';
        print'    -Total Load Duration: ' + cast(datediff(second , @batch_start_time, @batch_end_time) as  nvarchar) + ' seconds';
        print'==========================================';
    end try
    begin catch
    print'==========================================='
    print'ERROR OCCURED DURING LOADING BRONZE LAYER'
    print'ERROR MESSAGE' + error_message();
    print'ERROR MESSAGE'  + CAST(error_number() AS NVARCHAR);
    print'ERROR number'  + CAST(error_STATE() as nvarchar);
    print'==========================================='
    end catch
end 
