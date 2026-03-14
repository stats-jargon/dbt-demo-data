with

    increment_orders AS (
        SELECT
            order_id
        FROM 
            {{ source("src_thelook_orders", "orders") }} src
        {% if is_incremental() %}
        WHERE created_at > (SELECT MAX(created_at_ts) FROM {{ this }})
        {% endif %}
    )

select
    src.order_id,
    src.user_id,
    LOWER(src.status) AS status,
    LOWER(src.gender) AS gender,
    src.created_at AS created_at_ts,
    src.returned_at AS returned_at_ts,
    src.shipped_at AS shipped_at_ts,
    src.delivered_at AS delivered_at_ts,
    src.num_of_item AS number_of_items
FROM 
    {{ source("src_thelook_orders", "orders") }} src
JOIN
    increment_orders io
    ON src.order_id = io.order_id 