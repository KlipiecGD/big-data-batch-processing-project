WITH ranked_products AS (
    SELECT
        p.category,
        p.product_id,
        p.name,
        SUM(t.quantity) AS total_quantity_sold,
        RANK() OVER (PARTITION BY p.category ORDER BY SUM(t.quantity) DESC) AS sales_rank
    FROM
        products p
    JOIN
        transactions t ON p.product_id = t.product_id
    GROUP BY
        p.category, p.product_id, p.name
)
SELECT
    category,
    product_id,
    name,
    total_quantity_sold
FROM
    ranked_products
WHERE
    sales_rank <= 5
ORDER BY
    category, sales_rank;