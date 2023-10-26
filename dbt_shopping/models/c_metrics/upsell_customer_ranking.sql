WITH shopping_metrics_total AS (
    SELECT
        customer_id,
        SUM(purchase_amount_usd) AS total_spent_usd,
        COUNT (DISTINCT purchase_id) AS total_purchases,
        MIN(purchase_date) AS first_purchase_date,
        MAX(purchase_date) AS latest_purchase_date,
        DATE_DIFF('day', MIN(purchase_date), MAX(purchase_date)) AS customer_lifetime_days
    FROM
        {{ref('shopping_combined')}}
    GROUP BY
        customer_id
),

shopping_thirty_days AS (
    SELECT
        *
    FROM
        {{ref('shopping_combined')}}
    WHERE
        purchase_date >= (SELECT MAX(purchase_date) - INTERVAL 30 DAY FROM {{ref('shopping_combined')}})
),

shopping_metrics_thirty_days AS (
    SELECT
        customer_id,
        SUM(purchase_amount_usd) AS thirty_days_total_spent_usd,
        COUNT (DISTINCT purchase_id) AS thirty_days_total_purchases,
        MIN(purchase_date) AS thirty_days_first_purchase_date,
        MAX(purchase_date) AS thirty_days_latest_purchase_date,
        DATE_DIFF('day', MIN(purchase_date), MAX(purchase_date)) AS thirty_days_customer_lifetime_days
    FROM
        shopping_thirty_days
    GROUP BY
        customer_id
),

combined_metric_data AS (
    SELECT
        shopping_metrics_total.customer_id,
        shopping_metrics_total.total_spent_usd,
        shopping_metrics_total.total_purchases,
        shopping_metrics_total.first_purchase_date,
        shopping_metrics_total.latest_purchase_date,
        shopping_metrics_total.customer_lifetime_days,
        IFNULL(shopping_metrics_thirty_days.thirty_days_total_spent_usd, 0) AS thirty_days_total_spent_usd,
        IFNULL(shopping_metrics_thirty_days.thirty_days_total_purchases, 0) AS thirty_days_total_purchases,
        shopping_metrics_thirty_days.thirty_days_first_purchase_date,
        shopping_metrics_thirty_days.thirty_days_latest_purchase_date,
        IFNULL(shopping_metrics_thirty_days.thirty_days_customer_lifetime_days, 0) AS thirty_days_customer_lifetime_days
    FROM
        shopping_metrics_total
    LEFT JOIN
        shopping_metrics_thirty_days ON
        shopping_metrics_total.customer_id = shopping_metrics_thirty_days.customer_id
)

SELECT
    customer_id,
    total_spent_usd,
    total_purchases,
    first_purchase_date,
    latest_purchase_date,
    customer_lifetime_days,
    thirty_days_total_spent_usd,
    thirty_days_total_purchases,
    thirty_days_first_purchase_date,
    thirty_days_latest_purchase_date,
    thirty_days_customer_lifetime_days,
    (
        (((total_spent_usd/(total_purchases+.0001))*(customer_lifetime_days*0.1))*.01) +
        ((thirty_days_total_spent_usd/(thirty_days_total_purchases+.0001))*(thirty_days_customer_lifetime_days*0.1))
    ) AS ranking_heuristic
FROM
    combined_metric_data
ORDER BY
    ranking_heuristic DESC
