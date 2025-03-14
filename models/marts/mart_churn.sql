WITH customer_activity AS (
    SELECT 
        t.customer_id,
        MIN(t.payment_date) AS first_purchase_date,
        MAX(t.payment_date) AS last_purchase_date
    FROM {{ ref('stg_TRANSACTION') }} t
    JOIN {{ ref('stg_CUSTOMER') }} c ON t.customer_id = c.customer_id
    GROUP BY t.customer_id
),
latest_date AS (
    SELECT MAX(payment_date) AS max_date
    FROM {{ ref('stg_TRANSACTION') }}
),
churned_customers_lm AS (
    SELECT 
        customer_id,
        last_purchase_date
    FROM customer_activity, latest_date
    WHERE last_purchase_date < DATEADD(month, -1, max_date)
),
new_customers_lm AS (
    SELECT 
        customer_id,
        first_purchase_date
    FROM customer_activity, latest_date
    WHERE first_purchase_date >= DATEADD(month, -1, max_date)
),
churned_customers_l3m AS (
    SELECT 
        customer_id,
        last_purchase_date
    FROM customer_activity, latest_date
    WHERE last_purchase_date < DATEADD(month, -3, max_date)
),
new_customers_l3m AS (
    SELECT 
        customer_id,
        first_purchase_date
    FROM customer_activity, latest_date
    WHERE first_purchase_date >= DATEADD(month, -3, max_date)
),
churned_customers_ltm AS (
    SELECT 
        customer_id,
        last_purchase_date
    FROM customer_activity, latest_date
    WHERE last_purchase_date < DATEADD(month, -12, max_date)
),
new_customers_ltm AS (
    SELECT 
        customer_id,
        first_purchase_date
    FROM customer_activity, latest_date
    WHERE first_purchase_date >= DATEADD(month, -12, max_date)
)
SELECT 
    COUNT(DISTINCT churned_customers_lm.customer_id) AS LM_churned_customers,
    COUNT(DISTINCT new_customers_lm.customer_id) AS LM_new_customers,
    COUNT(DISTINCT churned_customers_l3m.customer_id) AS L3M_churned_customers,
    COUNT(DISTINCT new_customers_l3m.customer_id) AS L3M_new_customers,
    COUNT(DISTINCT churned_customers_ltm.customer_id) AS LTM_churned_customers,
    COUNT(DISTINCT new_customers_ltm.customer_id) AS LTM_new_customers
FROM churned_customers_lm
FULL OUTER JOIN new_customers_lm ON churned_customers_lm.customer_id = new_customers_lm.customer_id
FULL OUTER JOIN churned_customers_l3m ON churned_customers_lm.customer_id = churned_customers_l3m.customer_id
FULL OUTER JOIN new_customers_l3m ON churned_customers_lm.customer_id = new_customers_l3m.customer_id
FULL OUTER JOIN churned_customers_ltm ON churned_customers_lm.customer_id = churned_customers_ltm.customer_id
FULL OUTER JOIN new_customers_ltm ON churned_customers_lm.customer_id = new_customers_ltm.customer_id