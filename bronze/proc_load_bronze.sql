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
    exec bronze.load_bronze;
===============================================================================
*/
--exec bronze.load_bronze

create or alter procedure bronze.load_bronze as
    begin
        begin try
            declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;

            set @batch_start_time = getdate()

            print '------------------------------------------------';
		    print 'loading customers table';
		    print '------------------------------------------------';

            set @start_time = getdate()

            print '>> truncating table: bronze.customers';
		    truncate table bronze.customers 
            print '>> inserting data into: bronze.customers';
		    bulk insert bronze.customers 
            from 'c:\sql\olist\customers.csv'
            with (
                fieldterminator = ',',
                rowterminator = '0x0a',
                firstrow = 2,
                tablock
            );
            set @end_time = getdate()
            print '>> load duration: ' + cast( datediff(second, @start_time, @end_time) as nvarchar) + 'seconds'

            print '------------------------------------------------';
		    print 'loading geolocation table';
		    print '------------------------------------------------';

            set @start_time = getdate()
            print '>> truncating table: bronze.geolocation';
            truncate table bronze.geolocation 
            print '>> inserting data into: bronze.geolocation';
		    bulk insert bronze.geolocation 
            from 'c:\sql\olist\geolocation.csv'
            with (
                fieldterminator = ',',
                rowterminator = '0x0a',
                firstrow = 2,
                tablock
            );
            set @end_time = getdate()
            print '>> load duration: ' + cast( datediff(second, @start_time, @end_time) as nvarchar) + 'seconds'

            print '------------------------------------------------';
		    print 'loading order_items table';
		    print '------------------------------------------------';

            set @start_time = getdate()
            print '>> truncating table: bronze.order_items';
            truncate table bronze.order_items 
            print '>> inserting data into: bronze.order_items';
		    bulk insert bronze.order_items 
            from 'c:\sql\olist\order_items.csv'
            with (
                fieldterminator = ',',
                rowterminator = '0x0a',
                firstrow = 2,
                tablock
            );
            set @end_time = getdate()
            print '>> load duration: ' + cast( datediff(second, @start_time, @end_time) as nvarchar) + 'seconds'

            print '------------------------------------------------';
		    print 'loading order_payments table';
		    print '------------------------------------------------';

            set @start_time = getdate()
            print '>> truncating table: bronze.order_payments';
            truncate table bronze.order_payments 
            print '>> inserting data into: bronze.order_payments';
		    bulk insert bronze.order_payments 
            from 'c:\sql\olist\order_payments.csv'
            with (
                fieldterminator = ',',
                rowterminator = '0x0a',
                firstrow = 2,
                tablock
            );
            set @end_time = getdate()
            print '>> load duration: ' + cast( datediff(second, @start_time, @end_time) as nvarchar) + 'seconds'

            print '------------------------------------------------';
		    print 'loading orders table';
		    print '------------------------------------------------';

            set @start_time = getdate()
            print '>> truncating table: bronze.orders';
            truncate table bronze.orders 
            print '>> inserting data into: bronze.orders';
		    bulk insert bronze.orders 
            from 'c:\sql\olist\orders.csv'
            with (
                fieldterminator = ',',
                rowterminator = '0x0a',
                firstrow = 2,
                tablock
            );
            set @end_time = getdate()
            print '>> load duration: ' + cast( datediff(second, @start_time, @end_time) as nvarchar) + 'seconds'

            print '------------------------------------------------';
		    print 'loading products table';
		    print '------------------------------------------------';

            set @start_time = getdate()
            print '>> truncating table: bronze.products';
            truncate table bronze.products 
            print '>> inserting data into: bronze.products';
		    bulk insert bronze.products 
            from 'c:\sql\olist\products.csv'
            with (
                fieldterminator = ',',
                rowterminator = '0x0a',
                firstrow = 2,
                tablock
            );
            set @end_time = getdate()
            print '>> load duration: ' + cast( datediff(second, @start_time, @end_time) as nvarchar) + 'seconds'

            print '------------------------------------------------';
		    print 'loading sellers table';
		    print '------------------------------------------------';

            set @start_time = getdate()
            print '>> truncating table: bronze.sellers';
            truncate table bronze.sellers 
            print '>> inserting data into: bronze.sellers';
		    bulk insert bronze.sellers 
            from 'c:\sql\olist\sellers.csv'
            with (
                fieldterminator = ',',
                rowterminator = '0x0a',
                firstrow = 2,
                tablock
            );
            set @end_time = getdate()
            print '>> load duration: ' + cast( datediff(second, @start_time, @end_time) as nvarchar) + 'seconds'

          print '------------------------------------------------';
		    print 'loading translation table';
		    print '------------------------------------------------';

            set @start_time = getdate()
            print '>> truncating table: bronze.translation';
            truncate table bronze.translation 
            print '>> inserting data into: bronze.translation';
		    bulk insert bronze.translation 
            from 'c:\sql\olist\translation.csv'
            with (
                fieldterminator = ',',
                rowterminator = '0x0a',
                firstrow = 2,
                tablock
            );
            set @end_time = getdate()
            print '>> load duration: ' + cast( datediff(second, @start_time, @end_time) as nvarchar) + 'seconds'

            
            print '------------------------------------------------';
		    print 'loading order_reviews table';
		    print '------------------------------------------------';

            
            truncate table [bronze].[order_reviews]
            bulk insert [bronze].[order_reviews] 
            from 'c:\sql\olist\order_reviews2.csv'
            with (
                format = 'csv',     
                fieldterminator = ',',
                rowterminator = '\n',
                firstrow = 2,
                tablock
            );
            set @end_time = getdate()
            print '>> load duration: ' + cast( datediff(second, @start_time, @end_time) as nvarchar) + 'seconds' */

            set @batch_end_time = getdate()
            print '=========================================='
		    print 'loading bronze layer is completed';
            print '>> total load duration: ' + cast( datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + 'seconds'
            print '=========================================='
		end try


        begin catch
            /* --------------------------------------------------------------------------
               error handling
            -------------------------------------------------------------------------- */
            print('an error has occured')
            print('error message: '+ error_message() )
            print('error number: ' + cast( error_number()  as nvarchar ) ) 
            print('error severity: ' + cast(error_severity() as nvarchar));
            print('error state: ' + cast(error_state() as nvarchar));
            print('error line: ' + cast(error_line() as nvarchar));
            print('error procedure: ' + isnull(error_procedure(), 'n/a'));
        end catch

    end

