SELECT 
    *
FROM {{ source('src_thelook_orders', 'distribution_centers') }}