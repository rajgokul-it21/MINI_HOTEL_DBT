

WITH revenue_periods AS (
    SELECT 
        t.customer_id,
        SUM(t.revenue) AS total_revenue,
        t.payment_date
    FROM {{ ref('stg_TRANSACTION') }} t
    JOIN {{ ref('stg_CUSTOMER') }} c ON t.customer_id = c.customer_id
    GROUP BY t.customer_id, t.payment_date
),
nrr_grr AS (
    SELECT 
        rp.customer_id,
        SUM(CASE WHEN t.revenue_type = 1 THEN t.revenue ELSE 0 END) AS churn_revenue, 
        SUM(CASE WHEN t.revenue_type = 0 THEN t.revenue ELSE 0 END) AS non_churn_revenue, 
        SUM(t.revenue) AS total_revenue
    FROM revenue_periods rp
    JOIN {{ ref('stg_TRANSACTION') }} t ON rp.customer_id = t.customer_id AND rp.payment_date = t.payment_date
    GROUP BY rp.customer_id
)
SELECT 
    nrr_grr.customer_id,
    (total_revenue + non_churn_revenue - churn_revenue) / total_revenue AS NRR,
    (total_revenue - churn_revenue) / total_revenue AS GRR
FROM nrr_grr