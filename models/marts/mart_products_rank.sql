WITH product_revenue AS (
    SELECT 
        t.product_id,
        p.product_family,
        p.product_sub_family,
        SUM(t.revenue) AS total_revenue
    FROM {{ ref('stg_TRANSACTION') }} t
    LEFT JOIN {{ ref('stg_PRODUCTS') }} p ON t.product_id = p.product_id
    GROUP BY t.product_id, p.product_family,p.product_sub_family
),

ranked_products AS (
    SELECT 
        product_id,
        product_family,
        product_sub_family,
        total_revenue,
        DENSE_RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank
    FROM product_revenue
)

SELECT * FROM ranked_products
