
WITH
    inc AS (
        SELECT
            COUNT(*) AS row_count,
            COUNT(DISTINCT order_id) AS pk_count,
            COUNT(DISTINCT CASE WHEN status = 'cancelled' THEN order_id END) AS cancelled_count
        from {{ ref('int_orders') }}
    ),

    src AS (
        SELECT
            COUNT(*) AS row_count,
            COUNT(DISTINCT order_id) AS pk_count,
            COUNT(DISTINCT CASE WHEN LOWER(status) = 'cancelled' THEN order_id END) AS cancelled_count
        FROM {{ source("src_thelook_orders", "orders") }}
    )

SELECT *
FROM inc
CROSS JOIN src
WHERE
    1=0 AND (
    inc.row_COUNT != src.row_COUNT
    OR inc.pk_COUNT != src.pk_COUNT
    OR inc.cancelled_count != src.cancelled_count
    )