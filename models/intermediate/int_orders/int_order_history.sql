{% set order_history_key_construct %}
    {{ dbt_utils.generate_surrogate_key(
        [
            'src.order_id',
            'src.status',
            'src.created_at'
        ]
    ) }}
{% endset %}


WITH
    increment_order_history AS (
        SELECT
            {{ order_history_key_construct }} AS order_history_id,
            order_id,
            user_id,
            LOWER(status) AS status,
            LOWER(gender) AS gender,
            created_at AS created_at_ts,
            returned_at AS returned_at_ts,
            shipped_at AS shipped_at_ts,
            delivered_at AS delivered_at_ts,
            num_of_item AS number_of_items,
            CURRENT_TIMESTAMP() AS order_history_loaded_at
        FROM {{ source("src_thelook_orders", "orders") }} src
        {% if is_incremental() %}
        WHERE NOT EXISTS (
            SELECT 1
            FROM {{ this }} AS existing
            WHERE existing.order_history_id = {{ order_history_key_construct }}
        )
        {% endif %}
    )

SELECT *
FROM increment_order_history
