WITH
    base AS (
        SELECT
            order_id,
            user_id,
            LOWER(status)   AS status,
            LOWER(gender)   AS gender,
            created_at      AS created_at_ts,
            returned_at     AS returned_at_ts,
            shipped_at      AS shipped_at_ts,
            delivered_at    AS delivered_at_ts,
            num_of_item     AS number_of_items
        FROM {{ source("src_thelook_orders", "orders") }}
    )

SELECT
    {{dbt_utils.generate_surrogate_key(
        [
            'order_id',
            'status',
            'created_at_ts'
        ]
    )}} AS order_member_history_id,
    * 
FROM base