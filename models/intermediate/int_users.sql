WITH 
    orders AS (
        SELECT
            user_id,
            COUNT(DISTINCT order_id) AS orders,
            SUM(number_of_items)     AS total_number_of_items,
            MAX(created_at_ts)       AS recent_order,
            MIN(created_at_ts)       AS first_order
        FROM {{ ref('int_orders') }}
        WHERE status NOT IN ('cancelled')
        GROUP BY user_id
    )

SELECT 
    u.id AS user_id,
    u.first_name,
    u.last_name,
    u.email,
    u.age,
    u.gender,
    u.state,
    u.street_address,
    u.postal_code,
    u.city,
    u.country,
    u.traffic_source,
    COALESCE(o.orders, 0)                           AS orders,
    COALESCE(o.total_number_of_items, 0)            AS total_number_of_items,
    DATE_DIFF(recent_order, first_order, DAY)       AS days_between_first_and_recent_order
FROM {{ source('src_thelook_orders', 'users') }} u
LEFT JOIN
    orders o
    ON u.id = o.user_id