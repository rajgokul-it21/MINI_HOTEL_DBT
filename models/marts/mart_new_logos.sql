
WITH first_purchase AS (
    SELECT 
        t.customer_id,
        MIN(t.payment_date) AS first_purchase_date
    FROM {{ ref('stg_TRANSACTION') }} t
    JOIN {{ ref('stg_CUSTOMER') }} c ON t.customer_id = c.customer_id
    GROUP BY t.customer_id
)
SELECT 
    DATE_TRUNC('year', first_purchase_date) AS fiscal_year,
    COUNT(DISTINCT customer_id) AS new_logos
FROM first_purchase
GROUP BY fiscal_year
ORDER BY fiscal_year
