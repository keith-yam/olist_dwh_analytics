/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'OlistAnalytics' after checking if it already exists. 
    If the database exists, it is dropped and recreated. 
    Additionally, this script creates a schema called gold
	
WARNING:
    Running this script will drop the entire 'OlistAnalytics' database if it exists. 
    All data in the database will be permanently deleted. 
    Proceed with caution and ensure you have proper backups before running this script.
*/

USE master;
GO

-- Drop and recreate the 'OlistAnalytics' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'OlistAnalytics')
BEGIN
    ALTER DATABASE OlistAnalytics SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE OlistAnalytics;
END;
GO

-- Create the 'OlistAnalytics' database
CREATE DATABASE OlistAnalytics;
GO

USE OlistAnalytics;
GO

-- Create Schemas

CREATE SCHEMA gold;
GO

CREATE TABLE gold.dim_order_reviews(
	reviews_key  int
    , review_id  nvarchar(50)
    , order_id	 nvarchar(50)
    , score		 int
    , answer_timestamp datetime
);
GO

CREATE TABLE gold.dim_order_payments(
	  payments_key		 int
    , order_id			 nvarchar(50)
    , payment_sequential int
    , payment_type		 nvarchar(50)
    , installments		 int
    , payment_value		 decimal(10, 2)
);
GO

CREATE TABLE gold.dim_sellers(
	  sellers_key			int
    , seller_id				nvarchar(50)
    , zip_code_prefix		nvarchar(50)
    , city					nvarchar(50)
    , state					nvarchar(50)
    , latitude				float	
    , longitude				float
);
GO

CREATE TABLE gold.dim_customers(
	customer_key	    int
    , zip_code_prefix	nvarchar(50)	
    , customer_id		nvarchar(50)
    , unique_id			nvarchar(50)
    , city				nvarchar(50)
    , state				nvarchar(50)
    , latitude			float
    , longitude			float
);
GO

CREATE TABLE gold.dim_products(
	product_key				int
   , product_id				nvarchar(50)
   , category				nvarchar(50)
   , name_length			int
   , description_length		int
   , photos_quantity		int
   , weight_g				int
   , length_cm				int
   , height_cm				int
   , width_cm				int
);
GO

CREATE TABLE gold.fact_orders(
	products_key                int
    , customers_key             int
    , payments_key              int
    , reviews_key               int
    , sellers_key               int
    , order_id                  nvarchar(50)
    , order_status              nvarchar(50)
    , purchase_timestamp        datetime
    , order_approved_at         datetime
    , shipping_limit_date       datetime
    , price                     decimal(10,2)
    , freight_value             decimal(10,2)
    , delivered_carrier_date    datetime
    , delivered_customer_date   datetime
    , estimated_delivery_date   datetime
);
GO

-------------------------------

TRUNCATE TABLE gold.dim_order_reviews;
GO

BULK INSERT gold.dim_order_reviews
FROM 'C:\sql\Olist\Analytics\dim_order_reviews.csv'
WITH (
    FORMAT = 'CSV',
	FIRSTROW = 1,
	FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
	TABLOCK
);
GO

TRUNCATE TABLE gold.dim_order_payments;
GO

BULK INSERT gold.dim_order_payments
FROM 'C:\sql\Olist\Analytics\dim_order_payments.csv'
WITH (
    FORMAT = 'CSV',
	FIRSTROW = 1,
	FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
	TABLOCK
);
GO

TRUNCATE TABLE gold.dim_sellers;
GO

BULK INSERT gold.dim_sellers
FROM 'C:\sql\Olist\Analytics\dim_sellers.csv'
WITH (
    FORMAT = 'CSV',
	FIRSTROW = 1,
	FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
	TABLOCK
);
GO

TRUNCATE TABLE gold.dim_customers;
GO

BULK INSERT gold.dim_customers
FROM 'C:\sql\Olist\Analytics\dim_customers.csv'
WITH (
    FORMAT = 'CSV',
	FIRSTROW = 1,
	FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
	TABLOCK
);
GO


TRUNCATE TABLE gold.dim_products;
GO

BULK INSERT gold.dim_products
FROM 'C:\sql\Olist\Analytics\dim_products.csv'
WITH (
    FORMAT = 'CSV',
	FIRSTROW = 1,
	FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
	TABLOCK
);
GO


TRUNCATE TABLE gold.fact_orders;
GO

BULK INSERT gold.fact_orders
FROM 'C:\sql\Olist\Analytics\fact_orders.csv'
WITH (
    FORMAT = 'CSV',
	FIRSTROW = 1,
	FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
	TABLOCK
);
GO

