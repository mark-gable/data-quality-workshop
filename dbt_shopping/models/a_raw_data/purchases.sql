SELECT DISTINCT
    purchase_id,
    purchase_date,
    item_purchased,
    category,
    purchase_amount_usd,
    size,
    color,
    season,
    shipping_type,
    discount_applied,
    promo_code_used,
    payment_method
FROM
    purchase_table
