/*
===============================================================================
MoM Forecasting Report
===============================================================================
Purpose:
    - This report predicts future sales using moving average method. 

Report Format:
    1. date yyyy-mm 
    2. actual monthly sales
    3. averaged monthly sales using 3-months moving average
    4. averaged monthly sales using 6-months moving average
    5. running total of actual monthly sales
    6. running total of 3 months averaged sales
    7. running total of 6 months averaged sales
===============================================================================
*/

with extended_date as (
    select dateadd(month, value, '2017-01-01') as month_date
    from generate_series(0, 30) 
), 
month_sales as 
(
select  
    datetrunc(month,purchase_timestamp) as month_date,
    sum(price) as sales 
    from gold.fact_orders
    group by datetrunc(month,purchase_timestamp)
    order by month_date
),
sales_averaging as
(
    select 
        format(e.month_date, 'yyyyMM') as month_date
        , sales as actual_sales
        , avg(sales) over (
            order by e.month_date
            rows between 3 preceding and 1 preceding
         ) as three_months_averaging
        , avg(sales) over (
            order by e.month_date
            rows between 6 preceding and 1 preceding
        ) as six_months_averaging
    from month_sales s
    right join extended_date e
    on s.month_date = e.month_date
),
sales_running_total as
(   
    select 
        month_date as date
        , actual_sales
        , cast( sum(actual_sales) over (order by month_date  rows unbounded preceding) as decimal(10,2)) as running_total_actual
        , three_months_averaging
        , cast( sum(three_months_averaging) over (order by month_date  rows unbounded preceding) as decimal(10,2)) as running_total_three_months
        , six_months_averaging
        , cast( sum(six_months_averaging) over ( order by month_date  rows unbounded preceding) as decimal(10,2)) as running_total_six_months
    from
    sales_averaging
)

-- final query: formatting 
select
    date
    , actual_sales
    , cast( three_months_averaging as decimal(10,2)) as '3 months averaged sales'
    , cast( six_months_averaging as decimal(10,2)) as '6 months averaged sales'
    , case when actual_sales is null then null 
        else running_total_actual end as 'running total of actual sales'
    , case when three_months_averaging is null then null 
        else running_total_three_months end as 'running total of 3 months averaged sales'   
    , case when six_months_averaging is null then null 
        else running_total_six_months end as 'running total of 6 months averaged sales'
from
    sales_running_total
where date < '201810'
order  by date