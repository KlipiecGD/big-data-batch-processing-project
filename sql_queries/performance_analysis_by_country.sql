SELECT
    u.country,
    COALESCE(SUM(t.quantity * p.price), 0) AS total_sales,
    COUNT(DISTINCT t.transaction_id) AS total_transactions,
    COUNT(DISTINCT t.user_id) AS total_users
FROM
    users u
LEFT JOIN
    transactions t ON u.user_id = t.user_id
LEFT JOIN
    products p ON t.product_id = p.product_id
GROUP BY
    u.country
ORDER BY
    total_sales DESC;