CREATE DATABASE sales_analysis;
USE sales_analysis;


CREATE TABLE online_sales (
    order_id VARCHAR(50),
    order_date DATE,
    product_id VARCHAR(50),
    amount DECIMAL(10,2),
    customer_id VARCHAR(50)
);



SET GLOBAL local_infile = 1;



LOAD DATA LOCAL INFILE 'C:/Users/prana/Downloads/archive (2)/Online Sales Data.csv'
INTO TABLE online_sales
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


SELECT * FROM online_sales LIMIT 5;


SELECT MIN(order_date), MAX(order_date) FROM online_sales;


SELECT 
    COUNT(*) - COUNT(order_id) AS missing_orders,
    COUNT(*) - COUNT(order_date) AS missing_dates,
    COUNT(*) - COUNT(amount) AS missing_amounts
FROM online_sales;


SELECT
    EXTRACT(YEAR FROM order_date) AS year,
    EXTRACT(MONTH FROM order_date) AS month,
    ROUND(SUM(amount), 2) AS monthly_revenue,
    COUNT(DISTINCT order_id) AS order_volume,
    ROUND(SUM(amount) / COUNT(DISTINCT order_id), 2) AS avg_order_value
FROM
    online_sales
GROUP BY
    EXTRACT(YEAR FROM order_date),
    EXTRACT(MONTH FROM order_date)
ORDER BY
    year ASC,
    month ASC;
    
    WITH monthly_sales AS (
    SELECT
        EXTRACT(YEAR FROM order_date) AS year,
        EXTRACT(MONTH FROM order_date) AS month,
        SUM(amount) AS revenue
    FROM online_sales
    GROUP BY year, month
)
SELECT
    year,
    month,
    revenue,
    LAG(revenue) OVER (ORDER BY year, month) AS prev_month_revenue,
    ROUND((revenue - LAG(revenue) OVER (ORDER BY year, month)) / 
          LAG(revenue) OVER (ORDER BY year, month) * 100, 2) AS growth_pct
FROM monthly_sales
ORDER BY year, month;


SELECT
    EXTRACT(YEAR FROM order_date) AS year,
    EXTRACT(MONTH FROM order_date) AS month,
    SUM(amount) AS monthly_revenue,
    COUNT(DISTINCT order_id) AS order_volume
FROM
    online_sales
GROUP BY
    EXTRACT(YEAR FROM order_date),
    EXTRACT(MONTH FROM order_date)
ORDER BY
    year ASC,
    month ASC;
    
    SELECT
    EXTRACT(YEAR FROM order_date) AS year,
    EXTRACT(MONTH FROM order_date) AS month,
    SUM(amount) AS monthly_revenue,
    COUNT(DISTINCT order_id) AS order_volume
FROM
    online_sales
WHERE
    order_date BETWEEN '2022-01-01' AND '2022-12-31'
GROUP BY
    year, month
ORDER BY
    year, month;
    
    SELECT
    EXTRACT(YEAR FROM order_date) AS year,
    EXTRACT(MONTH FROM order_date) AS month,
    SUM(amount) AS monthly_revenue
FROM
    online_sales
GROUP BY
    year, month
ORDER BY
    monthly_revenue DESC
LIMIT 3;

WITH monthly_stats AS (
    SELECT
        EXTRACT(YEAR FROM order_date) AS year,
        EXTRACT(MONTH FROM order_date) AS month,
        SUM(amount) AS revenue,
        COUNT(DISTINCT order_id) AS volume
    FROM
        online_sales
    GROUP BY
        year, month
)
SELECT
    year,
    month,
    revenue,
    volume,
    LAG(revenue) OVER (ORDER BY year, month) AS prev_month_revenue,
    ROUND((revenue - LAG(revenue) OVER (ORDER BY year, month)) / 
          LAG(revenue) OVER (ORDER BY year, month) * 100, 2) AS revenue_growth_pct,
    ROUND((volume - LAG(volume) OVER (ORDER BY year, month)) / 
          LAG(volume) OVER (ORDER BY year, month) * 100, 2) AS volume_growth_pct
FROM
    monthly_stats
ORDER BY
    year, month;
    
    SELECT
    EXTRACT(YEAR FROM order_date) AS year,
    EXTRACT(QUARTER FROM order_date) AS quarter,
    SUM(amount) AS quarterly_revenue,
    COUNT(DISTINCT order_id) AS order_volume
FROM
    online_sales
GROUP BY
    year, quarter
ORDER BY
    year, quarter;
    
    WITH current_year AS (
    SELECT
        EXTRACT(MONTH FROM order_date) AS month,
        SUM(amount) AS current_year_revenue,
        COUNT(DISTINCT order_id) AS current_year_volume
    FROM
        online_sales
    WHERE
        EXTRACT(YEAR FROM order_date) = EXTRACT(YEAR FROM CURRENT_DATE)
    GROUP BY
        month
),
prev_year AS (
    SELECT
        EXTRACT(MONTH FROM order_date) AS month,
        SUM(amount) AS prev_year_revenue,
        COUNT(DISTINCT order_id) AS prev_year_volume
    FROM
        online_sales
    WHERE
        EXTRACT(YEAR FROM order_date) = EXTRACT(YEAR FROM CURRENT_DATE) - 1
    GROUP BY
        month
)
SELECT
    c.month,
    c.current_year_revenue,
    p.prev_year_revenue,
    ROUND((c.current_year_revenue - p.prev_year_revenue) / p.prev_year_revenue * 100, 2) AS revenue_growth_pct,
    c.current_year_volume,
    p.prev_year_volume,
    ROUND((c.current_year_volume - p.prev_year_volume) / p.prev_year_volume * 100, 2) AS volume_growth_pct
FROM
    current_year c
LEFT JOIN
    prev_year p ON c.month = p.month
ORDER BY
    c.month;
