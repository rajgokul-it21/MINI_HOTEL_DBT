WITH cleaned_data AS (
    SELECT 
        {{ cast_columns({
            'customer_id': 'INT',
            'revenue_type': 'INT',
            'revenue': 'DECIMAL(10,2)',
            'quantity': 'INT'
        }) }}
        
        product_id,
        payment_month AS payment_date,
        companies AS company_name
    FROM {{ source('sf_source', 'TRANSACTIONS') }}
    WHERE {{ filter_not_null([
        'customer_id', 'product_id', 'payment_month', 'revenue', 'quantity',
        'companies'
    ]) }}
)

SELECT DISTINCT * FROM cleaned_data

-- SELECT 
--             *
--         FROM {{ source('sf_source', 'TRANSACTIONS') }}