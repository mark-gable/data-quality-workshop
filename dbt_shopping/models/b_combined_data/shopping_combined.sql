SELECT
    purchases.purchase_id,
    purchases.purchase_date,
    purchases.item_purchased,
    purchases.category,
    purchases.purchase_amount_usd,
    purchases.size,
    purchases.color,
    purchases.season,
    purchases.shipping_type,
    purchases.discount_applied,
    purchases.promo_code_used,
    purchases.payment_method,
    purchase_customer_mapping.customer_id,
    customers.age,
    customers.gender,
    customers.location,
    reviews.review_rating
FROM
    {{ref('purchases')}}
LEFT JOIN
    {{ref('purchase_customer_mapping')}} ON
    purchases.purchase_id = purchase_customer_mapping.purchase_id
LEFT JOIN
    {{ref('customers')}} ON
    purchase_customer_mapping.customer_id = customers.customer_id
LEFT JOIN
    {{ref('reviews')}} ON
    purchases.purchase_id = reviews.purchase_id
