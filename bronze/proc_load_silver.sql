/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/
--EXEC Silver.load_silver;

create or alter procedure silver.load_silver as
    begin
        begin try
            declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;

            set @batch_start_time = getdate()

            print '------------------------------------------------';
		    print 'loading customers table';
		    print '------------------------------------------------';

            set @start_time = getdate()

            print '>> truncating table: silver.customers';
		    truncate table silver.customers 
            print '>> inserting data into: silver.customers';
		    insert into silver.customers(
				id, 
				unique_id,
				zip_code_prefix,
				city,
				state
				) 
			select 
				replace(trim(customer_id),'"','')  as id
				, replace(trim(customer_unique_id),'"','') as unique_id
				, replace(trim(customer_zip_code_prefix),'"','') as zip_code_prefix
				, dbo.propercase(customer_city) as city
				, trim(customer_state) as state 
			from bronze.customers
            set @end_time = getdate()
            print '>> load duration: ' + cast( datediff(second, @start_time, @end_time) as nvarchar) + 'seconds'

            print '------------------------------------------------';
		    print 'loading geolocation table';
		    print '------------------------------------------------';

            set @start_time = getdate()
            print '>> truncating table: silver.geolocation';
            truncate table silver.geolocation 
            print '>> inserting data into: silver.geolocation';
			; with geolocation_format as (
				select 
					replace(trim(geolocation_zip_code_prefix),'"','')  as zip_code_prefix
					, round(geolocation_lat,3) as latitude
					, round(geolocation_lng,3) as longitude
					, concat(round(geolocation_lat+0.0,1), ',',round(geolocation_lng+0.0,1)) as lat_lgn 
					, dbo.propercase(lower(regexp_replace(geolocation_city, '[^a-za-z ]', ''))) as city
					, case when len(trim(geolocation_state))> 2 
						then right(trim(geolocation_state),2) 
						else trim(geolocation_state) 
						end as state 
				from bronze.geolocation
			),
			geolocation_cleaned as (
				select distinct 
					lat_lgn, 
					first_value(city) over (
						partition by lat_lgn 
						order by count(*) desc 
						rows unbounded preceding
					) as most_frequent
				from geolocation_format
				group by lat_lgn, city
			)
			insert into silver.geolocation (
					zip_code_prefix
						, latitude
						, longitude
						, city
						, state 
						)
			select t1.zip_code_prefix as zip_code_prefix, 
					t1.latitude as latitude, 
					t1.longitude  as longitude,		
					case when difference(t1.city,t2.city) = 4 
					then t2.city else t1.city end as city,
					t1.state as state
			from geolocation_format t1
			inner join (
				select lat_lgn, most_frequent as city 
				from geolocation_cleaned
			) as t2 
			on t1.lat_lgn = t2.lat_lgn
            set @end_time = getdate()
            print '>> load duration: ' + cast( datediff(second, @start_time, @end_time) as nvarchar) + 'seconds'

            print '------------------------------------------------';
		    print 'loading order_items table';
		    print '------------------------------------------------';

            set @start_time = getdate()
            print '>> truncating table: silver.order_items';
            truncate table silver.order_items 
            print '>> inserting data into: silver.order_items';
		    insert into silver.order_items(
	             order_id 
	            , order_item_id
	            , product_id
	            , seller_id
	            , shipping_limit_date
	            , price
	            , freight_value
            )
            select  
	            replace(trim(order_id),'"','') as order_id
	            , order_item_id
	            , replace(trim(product_id),'"','') as product_id
	            , replace(trim(seller_id),'"','') as seller_id
	            , shipping_limit_date
	            , price
	            , freight_value
            --	, format(price, 'c', 'en-us') as price
            --	, format(freight_value, 'c', 'en-us') as freight_value
            from bronze.order_items

			with tbl_row_num as (
			  select *, row_number() over (partition by order_id order by order_id) as row_num
			  from silver.order_items 
			)
			delete from silver.order_items  
			where order_id in (select order_id from tbl_row_num where row_num > 1);

            set @end_time = getdate()
            print '>> load duration: ' + cast( datediff(second, @start_time, @end_time) as nvarchar) + 'seconds'

            print '------------------------------------------------';
		    print 'loading order_payments table';
		    print '------------------------------------------------';

            set @start_time = getdate()
            print '>> truncating table: silver.order_payments';
            truncate table silver.order_payments 
            print '>> inserting data into: silver.order_payments';
            insert into silver.order_payments(
	             order_id 
	            , payment_sequential 
	            , payment_type
	            , payment_installments
	            , payment_value
            )
            select 
	            replace(trim(order_id),'"','') as order_id
	            , payment_sequential
	            , case when payment_type = 'not_defined' then 'n/a'
	            else dbo.propercase(lower(replace(payment_type, '_', ' '))) 
	            end as payment_type
	            , payment_installments
	            , payment_value
            from bronze.order_payments
            set @end_time = getdate()
            print '>> load duration: ' + cast( datediff(second, @start_time, @end_time) as nvarchar) + 'seconds'

            print '------------------------------------------------';
		    print 'loading orders table';
		    print '------------------------------------------------';

            set @start_time = getdate()
            print '>> truncating table: silver.orders';
            truncate table silver.orders 
            print '>> inserting data into: silver.orders';
            insert into silver.orders
            select 
	            replace(trim(order_id),'"','') as order_id
	            , replace(trim(customer_id),'"','') as customer_id
	            , order_status as order_status 
	            , order_purchase_timestamp as purchase_timestamp
	            , order_approved_at as order_approved_at
	            , order_delivered_carrier_date as delivered_carrier_date
	            , order_delivered_customer_date as delivered_customer_date 
	            , order_estimated_delivery_date as estimated_delivery_date
            from bronze.orders
            set @end_time = getdate()
            print '>> load duration: ' + cast( datediff(second, @start_time, @end_time) as nvarchar) + 'seconds'

            print '------------------------------------------------';
		    print 'loading products table';
		    print '------------------------------------------------';

            set @start_time = getdate()
            print '>> truncating table: silver.products';
            truncate table silver.products 
            print '>> inserting data into: silver.products';
            insert into silver.products(
	            product_id						
	            , product_category_name			
	            , product_name_length			 
	            , product_description_length		
	            , product_photos_qty			
	            , product_weight_g				
	            , product_length_cm				
	            , product_height_cm				
	            , product_width_cm				
            )
            select
	              replace(trim(product_id),'"','') as product_id					
	            , dbo.propercase(replace(product_category_name_english,'_',' ')) as product_category_name		
	            , product_name_lenght as product_name_length	 
	            , product_description_lenght as product_description_length
	            , product_photos_qty			
	            , product_weight_g				
	            , product_length_cm				
	            , product_height_cm				
	            , product_width_cm
            from 
            (
	            select p.*, t.product_category_name_english
	            from bronze.products as p
	            left join bronze.translation as t
	            on p.product_category_name = t.product_category_name
            ) t

            set @end_time = getdate()
            print '>> load duration: ' + cast( datediff(second, @start_time, @end_time) as nvarchar) + 'seconds'

            print '------------------------------------------------';
		    print 'loading sellers table';
		    print '------------------------------------------------';

            set @start_time = getdate()
            print '>> truncating table: silver.sellers';
            truncate table silver.sellers 
            print '>> inserting data into: silver.sellers';
             insert into silver.sellers(
             seller_id						
            , seller_zip_code_prefix		
            , seller_city					
            , seller_state					
            ) 
             select 
	            replace(trim(seller_id),'"','') as seller_id
	            , replace(trim(seller_zip_code_prefix),'"','') as seller_zip_code_prefix
	            , dbo.propercase(  replace(
	            case 
	            when seller_city like 'aparecida' then 'aparecida de goiania' 
	            when seller_city like 'balneario camboriu' then 'balenario camboriu'
	            when seller_city like 'belo horizont' then 'belo horizonte'
	            when seller_city like 'brasilia df' then 'brasilia'
	            when seller_city like 'ferraz de  vasconcelos' then 'ferraz de vasconcelos'
	            when seller_city like 'pinhais%' then 'pinhais' 
	            when seller_city like 'ribeirao pret%' then 'ribeirao preto'
	            when seller_city like 'rio de janeiro%*' then 'rio de janeiro'
	            when seller_city like 'santa barbara%' then 'santa barbara d''oeste'
	            when seller_city like 'sao bernardo do%' then 'sao bernardo do campo'
	            when seller_city like 'sao jose do rio p%' then 'sao jose do rio preto'
	            when seller_city like 'sao jose dos pin%' then 'sao jose dos pinhais'
	            when seller_city like 'sao pa%' then 'sao paulo'
	            else seller_city
	            end 
	            ,'"','')) as seller_city
	            , seller_state 
             from bronze.sellers		   
            set @end_time = getdate()
            print '>> load duration: ' + cast( datediff(second, @start_time, @end_time) as nvarchar) + 'seconds'

                     
            print '------------------------------------------------';
		    print 'loading order_reviews table';
		    print '------------------------------------------------';
            set @start_time = getdate()
            print '>> truncating table: silver.order_reviews';
            truncate table silver.[order_reviews]
            print '>> inserting data into: silver.order_reviews';
            insert into silver.order_reviews(
	             review_id 
	            , order_id 
	            , score
	            , comment_title
	            , comment_message
	            , answer_timestamp
            )
            select 
	            replace(trim(review_id),'"','') as review_id
	            , replace(trim(order_id),'"','') as order_id
	            , review_score as score
	            , review_comment_title as comment_title
	            , review_comment_message as comment_message
	            , review_answer_timestamp as answer_timestamp
            from bronze.order_reviews

			-- delete old review 
			with tbl_row_num as (
				  select *, row_number() over (partition by order_id order by answer_timestamp desc) as row_num
				  from silver.order_reviews 
			)
			delete from silver.order_reviews
			where review_id in (select review_id from tbl_row_num where row_num > 1);

            set @end_time = getdate()
            print '>> load duration: ' + cast( datediff(second, @start_time, @end_time) as nvarchar) + 'seconds' 

            set @batch_end_time = getdate()
            print '=========================================='
		    print 'loading silver layer is completed';
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

