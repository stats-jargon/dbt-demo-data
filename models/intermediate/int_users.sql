SELECT 
    *
FROM {{ source('src_thelook_orders', 'users') }}