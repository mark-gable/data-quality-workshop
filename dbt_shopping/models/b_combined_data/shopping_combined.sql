SELECT
    purchase_table.purchase_id,
    purchase_table.purchase_date,
    purchase_table.item_purchased,
    purchase_table.category,
    purchase_table.purchase_amount_usd,
    purchase_table.size,
    purchase_table.color,
    purchase_table.season,
    purchase_table.shipping_type,
    purchase_table.discount_applied,
    purchase_table.promo_code_used,
    purchase_table.payment_method,
    purchase_customer_mapping.customer_id,
    customers.age,
    customers.gender,
    customers.location,
    reviews.review_rating
FROM
    {{ref('purchase_table')}}
LEFT JOIN
    {{ref('purchase_customer_mapping')}} ON
    purchase_table.purchase_id = purchase_customer_mapping.purchase_id
LEFT JOIN
    {{ref('customers')}} ON
    purchase_customer_mapping.customer_id = customers.customer_id
LEFT JOIN
    {{ref('reviews')}} ON
    purchase_table.purchase_id = reviews.purchase_id
