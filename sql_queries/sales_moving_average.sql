WITH bounds AS (
    SELECT 
        MIN(CAST(transaction_date AS DATE)) as min_dt, 
        MAX(CAST(transaction_date AS DATE)) as max_dt 
    FROM transactions
),
date_range AS (
    SELECT 
        explode(sequence(min_dt, max_dt, interval 1 day)) AS transaction_date
    FROM bounds
)
SELECT 
    dr.transaction_date,
    COALESCE(SUM(t.quantity * p.price), 0) AS total_sales,
    SUM(COALESCE(SUM(t.quantity * p.price), 0)) OVER (
        ORDER BY dr.transaction_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) / 
    COUNT(*) OVER (
        ORDER BY dr.transaction_date 
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS moving_average_sales
FROM 
    date_range dr
LEFT JOIN 
    transactions t ON CAST(t.transaction_date AS DATE) = dr.transaction_date
LEFT JOIN 
    products p ON t.product_id = p.product_id
GROUP BY 
    dr.transaction_date
ORDER BY 
    dr.transaction_date;