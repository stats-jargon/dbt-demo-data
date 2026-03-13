{% set order_history_key_construct %}
    {{dbt_utils.generate_surrogate_key(
        [
            'order_id',
            'status',
            'created_at'
        ]
    )}}
{% endset %}


WITH
    increment_orders AS (
        SELECT
            {{ order_history_key_construct }} AS order_history_id,
            *,
            CURRENT_TIMESTAMP()               AS order_history_loaded_at
        FROM {{ source("src_thelook_orders", "orders") }} 
        {% if is_incremental %}
        WHERE
            CURRENT_TIMESTAMP() > (SELECT MAX(order_history_loaded_at) FROM {{ this }})
        {% endif %}
    ) 

SELECT
    order_history_id,
    order_id,
    user_id,
    LOWER(status)           AS status,
    LOWER(gender)           AS gender,
    created_at              AS created_at_ts,
    returned_at             AS returned_at_ts,
    shipped_at              AS shipped_at_ts,
    delivered_at            AS delivered_at_ts,
    num_of_item             AS number_of_items,
    order_history_loaded_at
FROM increment_orders