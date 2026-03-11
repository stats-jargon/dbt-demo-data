SELECT 
    *
FROM {{ source('src_thelook_orders', 'inventory_items') }}