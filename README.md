# 🛒 eCommerce Data Warehouse & Analytics Pipeline

This project demonstrates a robust, end-to-end ETL and analytics pipeline that converts raw, fragmented eCommerce data into high-value business intelligence. Leveraging a Medallion Architecture, the pipeline extracts, cleans, and models data to drive strategic decision-making.

## 📊 Project Overview

The core of this project is the Brazilian E-Commerce Public Dataset by Olist, featuring 100k orders spanning 2016 to 2018. This multifaceted dataset provides a 360-degree view of the customer journey, including:
- Logistics: Freight performance and delivery status.
- Financials: Pricing, payment methods, and revenue.
- Customer Experience: Product attributes and sentiment analysis from customer reviews.

## 🏗️ Data Architecture: The Medallion Approach
The project implements a Medallion Architecture within SQL Server to ensure data quality and traceability as it moves from raw files to refined insights.

### 🥉 Bronze Layer (Raw)
**Source**: Ingestion of raw CSV files directly into the SQL Server Database.
**State**: Data is kept in its original form to act as a "Single Source of Truth."

### 🥈 Silver Layer (Refined)
**Process**: Data cleansing, standardization, and normalization.
**Goal**: De-duplicating records and handling null values to prepare data for complex modeling.

### 🥇 Gold Layer (Business)
**Model**: Data is transformed into a Star Schema (Fact and Dimension tables).
**Goal**: High-performance tables optimized for BI tools and executive reporting.

<img width="975" height="503" alt="image" src="https://github.com/user-attachments/assets/f91372da-1164-4cc5-bcdf-68234dad6f99" />

## ERD
<img width="975" height="756" alt="image" src="https://github.com/user-attachments/assets/62fc219f-ae3c-48ab-8ab7-5faf38b5a90c" />

## 📈 Business Intelligence & Analytics
Using advanced SQL-based analytics, this project delivers deep dives into the following key performance indicators (KPIs):
- **Product Performance**: Identifying top-tier and underperforming categories.
- **Sales Trends**: Analyzing historical growth patterns and seasonality.
- **Sales Forecasting**: Projecting future demand based on historical sales.
- **Geospatial Distribution**: Mapping customer and seller density across Brazil to optimize logistics.

Impact: These insights empower stakeholders to identify operational bottlenecks, optimize shipping costs, and improve customer satisfaction scores.

## 🛠️ Tech Stack
**Database**: Microsoft SQL Server
**Architecture**: Medallion (Bronze, Silver, Gold)
**Data Modeling**: Star Schema (Fact/Dimensions)
**Language**: T-SQL (Stored Procedures, CTEs, Window Functions)
