with
    base as (
        select
            id,
            lower(category) as category,
            lower(name) as name,
            lower(brand) as brand,
            lower(department) as department,
            retail_price,
            cost
        from {{ source("src_thelook_orders", "products") }}
    )

select * from base