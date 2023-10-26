SELECT DISTINCT
    column0 AS purchase_id,
    column1 AS customer_id
FROM
    purchase_customer_mapping_table
WHERE
    column0 != 'Purchase ID' AND
    column1 != 'Customer ID'
