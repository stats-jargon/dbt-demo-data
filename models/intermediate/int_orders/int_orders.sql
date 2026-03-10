with
    base as (
        select
            order_id,
            user_id,
            lower(status) as status,
            lower(gender) as gender,
            created_at as created_at_ts,
            returned_at as returned_at_ts,
            shipped_at as shipped_at_ts,
            delivered_at as delivered_at_ts,
            num_of_item as number_of_items
        from {{ source("src_thelook_orders", "orders") }}
    )

select * from base