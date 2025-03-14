WITH customer_revenue AS (
    SELECT 
        t.customer_id,
        c.customer_name,
        SUM(CAST(t.revenue AS DECIMAL(10,2))) AS total_revenue,
        COUNT(*) AS transaction_count 
    FROM {{ ref('stg_TRANSACTION') }} t
    LEFT JOIN {{ ref('stg_CUSTOMER') }} c ON t.customer_id = c.customer_id
    GROUP BY t.customer_id, c.customer_name
),

ranked_customers AS (
    SELECT 
        customer_id,
        customer_name,
        total_revenue,
        transaction_count, 
        RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank
    FROM customer_revenue
)

SELECT * FROM ranked_customers
