SELECT
    u.user_id,
    u.name,
    SUM(p.price * t.quantity) AS total_spent
FROM
    transactions t
JOIN
    users u ON t.user_id = u.user_id
JOIN
    products p ON t.product_id = p.product_id
GROUP BY
    u.user_id, u.name
ORDER BY
    total_spent DESC
LIMIT 10;