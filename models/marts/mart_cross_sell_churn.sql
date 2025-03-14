
WITH product_purchases AS (
    SELECT 
        t.customer_id,
        COUNT(DISTINCT t.product_id) AS product_count 
    FROM {{ ref('stg_TRANSACTION') }} t
    JOIN {{ ref('stg_CUSTOMER') }} c ON t.customer_id = c.customer_id
    GROUP BY t.customer_id
),
product_churn AS (
    SELECT 
        t.customer_id,
        COUNT(DISTINCT t.product_id) AS churned_product_count 
    FROM {{ ref('stg_TRANSACTION') }} t
    JOIN {{ ref('stg_CUSTOMER') }} c ON t.customer_id = c.customer_id
    WHERE t.revenue_type = 1 
    GROUP BY t.customer_id
)
SELECT 
    p.customer_id,
    p.product_count,
    COALESCE(pc.churned_product_count, 0) AS churned_product_count
FROM product_purchases p
LEFT JOIN product_churn pc USING (customer_id)
ORDER BY p.product_count DESC, churned_product_count DESC