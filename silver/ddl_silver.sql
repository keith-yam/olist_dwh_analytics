/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	Run this script to re-define the DDL structure of 'silver' Tables
===============================================================================
*/


IF OBJECT_ID('silver.customers', 'U') IS NOT NULL 
	DROP TABLE silver.customers;
GO

CREATE TABLE silver.customers (
id					NVARCHAR(50)
, unique_id			NVARCHAR(50)
, zip_code_prefix	NVARCHAR(50)
, city				NVARCHAR(50)
, state				NVARCHAR(50)
);

-----------------------------------------------------------

IF OBJECT_ID('silver.geolocation', 'U') IS NOT NULL 
	DROP TABLE silver.geolocation;
GO

CREATE TABLE silver.geolocation (
zip_code_prefix		NVARCHAR(50)
, latitude			FLOAT
, longitude			FLOAT 
, lat_lgn			NVARCHAR(50)
, city				NVARCHAR(50)
, state				NVARCHAR(50)

);

-----------------------------------------------------------

IF OBJECT_ID('silver.order_items', 'U') IS NOT NULL 
	DROP TABLE silver.order_items;
GO

CREATE TABLE silver.order_items (
order_id					NVARCHAR(50)
, order_item_id				INT
, product_id				NVARCHAR(50)
, seller_id					NVARCHAR(50)
, shipping_limit_date		DATETIME
, price						DECIMAL(18,2)
, freight_value				DECIMAL(18,2)
);

IF OBJECT_ID('silver.order_payments', 'U') IS NOT NULL 
	DROP TABLE silver.order_payments;
GO

CREATE TABLE silver.order_payments (
order_id					NVARCHAR(50)
, payment_sequential		INT
, payment_type				NVARCHAR(50)
, payment_installments		INT
, payment_value				DECIMAL(18,2)
);

IF OBJECT_ID('silver.order_reviews', 'U') IS NOT NULL 
	DROP TABLE silver.order_reviews;
GO

CREATE TABLE silver.order_reviews (
review_id					NVARCHAR(50)
, order_id					NVARCHAR(50) 
, score				INT
, comment_title		NVARCHAR(50) NULL 
, comment_message	NVARCHAR(MAX) NULL 
, creation_date		DATETIME NULL   
, answer_timestamp	DATETIME NULL 
);

IF OBJECT_ID('silver.order_reviews_redact', 'V') IS NOT NULL 
	DROP VIEW silver.order_reviews_redact;
GO

CREATE VIEW silver.order_reviews_redact AS
     SELECT
		review_id					
		, order_id					
		, review_score				
		, review_creation_date	
		, review_answer_timestamp	
     FROM silver.order_reviews
GO

IF OBJECT_ID('silver.orders', 'U') IS NOT NULL 
	DROP TABLE silver.orders;
GO

CREATE TABLE silver.orders (
order_id						NVARCHAR(50)
, customer_id					NVARCHAR(50)
, order_status					NVARCHAR(50)
, purchase_timestamp		DATETIME
, order_approved_at			DATETIME
, delivered_carrier_date	DATETIME	
, delivered_customer_date	DATETIME
, estimated_delivery_date	DATETIME
);

IF OBJECT_ID('silver.products', 'U') IS NOT NULL 
	DROP TABLE silver.products;
GO

CREATE TABLE silver.products (
product_id						NVARCHAR(50)
, product_category_name			NVARCHAR(50)
, product_name_length			INT 
, product_description_length	INT	
, product_photos_qty			INT
, product_weight_g				INT
, product_length_cm				INT
, product_height_cm				INT
, product_width_cm				INT
);

IF OBJECT_ID('silver.sellers', 'U') IS NOT NULL 
	DROP TABLE silver.sellers;
GO

CREATE TABLE silver.sellers (
seller_id						NVARCHAR(50)
, seller_zip_code_prefix		NVARCHAR(50)
, seller_city					NVARCHAR(50)
, seller_state					NVARCHAR(50)
);

IF OBJECT_ID('silver.translation', 'U') IS NOT NULL 
	DROP TABLE silver.translation;
GO
