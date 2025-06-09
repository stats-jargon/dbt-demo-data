with
    base as (
        select
            id,
            order_id,
            user_id,
            product_id,
            lower(status) as status,
            created_at as created_at_ts,
            shipped_at as shipped_at_ts,
            delivered_at as delivered_at_ts,
            returned_at as returned_at_ts,
            sale_price
        from {{ source("src_thelook_orders", "order_items") }}
    )

select * from base