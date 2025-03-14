SELECT 
    DATE_TRUNC('month', t.payment_date) AS month,
    SUM(CASE WHEN t.revenue_type = 1 THEN t.revenue ELSE 0 END) AS lost_revenue 
FROM {{ ref('stg_TRANSACTION') }} t
JOIN {{ ref('stg_CUSTOMER') }} c ON t.customer_id = c.customer_id
GROUP BY month
ORDER BY month
