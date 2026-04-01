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

IF OBJECT_ID('bronze.customers', 'U') IS NOT NULL 
	DROP TABLE bronze.customers;
GO

CREATE TABLE bronze.customers (
customer_id					NVARCHAR(50)
, customer_unique_id		NVARCHAR(50)
, customer_zip_code_prefix	NVARCHAR(50)
, customer_city				NVARCHAR(50)
, customer_state			NVARCHAR(50)
);

-----------------------------------------------------------

IF OBJECT_ID('bronze.geolocation', 'U') IS NOT NULL 
	DROP TABLE bronze.geolocation;
GO

CREATE TABLE bronze.geolocation (
geolocation_zip_code_prefix		NVARCHAR(50)
, geolocation_lat					FLOAT
, geolocation_lng					FLOAT
, geolocation_city				NVARCHAR(50)
, geolocation_state				NVARCHAR(50)

);

-----------------------------------------------------------

IF OBJECT_ID('bronze.order_items', 'U') IS NOT NULL 
	DROP TABLE bronze.order_items;
GO

CREATE TABLE bronze.order_items (
order_id					NVARCHAR(50)
, order_item_id				INT
, product_id				NVARCHAR(50)
, seller_id					NVARCHAR(50)
, shipping_limit_date		DATETIME
, price						FLOAT
, freight_value				FLOAT
);

IF OBJECT_ID('bronze.order_payments', 'U') IS NOT NULL 
	DROP TABLE bronze.order_payments;
GO

CREATE TABLE bronze.order_payments (
order_id					NVARCHAR(50)
, payment_sequential		INT
, payment_type				NVARCHAR(50)
, payment_installments		INT
, payment_value				NUMERIC
);

IF OBJECT_ID('bronze.order_reviews', 'U') IS NOT NULL 
	DROP TABLE bronze.order_reviews;
GO

CREATE TABLE bronze.order_reviews (
review_id					NVARCHAR(50)
, order_id					NVARCHAR(50) 
, review_score				INT
, review_comment_title		NVARCHAR(50) NULL 
, review_comment_message	NVARCHAR(MAX) NULL 
, review_creation_date		DATETIME NULL   
, review_answer_timestamp	DATETIME NULL 
);

IF OBJECT_ID('bronze.order_reviews_redact', 'V') IS NOT NULL 
	DROP VIEW bronze.order_reviews_redact;
GO

CREATE VIEW bronze.order_reviews_redact AS
     SELECT
		review_id					
		, order_id					
		, review_score				
		, review_creation_date	
		, review_answer_timestamp	
     FROM bronze.order_reviews
GO

IF OBJECT_ID('bronze.orders', 'U') IS NOT NULL 
	DROP TABLE bronze.orders;
GO

CREATE TABLE bronze.orders (
order_id						NVARCHAR(50)
, customer_id					NVARCHAR(50)
, order_status					NVARCHAR(50)
, order_purchase_timestamp		DATETIME
, order_approved_at				DATETIME
, order_delivered_carrier_date	DATETIME	
, order_delivered_customer_date	DATETIME
, order_estimated_delivery_date	DATETIME
);

IF OBJECT_ID('bronze.products', 'U') IS NOT NULL 
	DROP TABLE bronze.products;
GO

CREATE TABLE bronze.products (
product_id						NVARCHAR(50)
, product_category_name			NVARCHAR(50)
, product_name_lenght			INT 
, product_description_lenght	INT	
, product_photos_qty			INT
, product_weight_g				INT
, product_length_cm				INT
, product_height_cm				INT
, product_width_cm				INT
);

IF OBJECT_ID('bronze.sellers', 'U') IS NOT NULL 
	DROP TABLE bronze.sellers;
GO

CREATE TABLE bronze.sellers (
seller_id						NVARCHAR(50)
, seller_zip_code_prefix		NVARCHAR(50)
, seller_city					NVARCHAR(50)
, seller_state					NVARCHAR(50)
);

IF OBJECT_ID('bronze.translation', 'U') IS NOT NULL 
	DROP TABLE bronze.translation;
GO

CREATE TABLE bronze.translation (
product_category_name			NVARCHAR(50)
, product_category_name_english	NVARCHAR(50)	
);

