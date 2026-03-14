
WITH
    inc AS (
        SELECT
            COUNT(*) AS row_count,
            COUNT(DISTINCT order_id) AS pk_count
        from {{ ref('int_orders') }}
    ),

    src AS (
        SELECT
            COUNT(*) AS row_count,
            COUNT(DISTINCT order_id) AS pk_count
        FROM {{ source("src_thelook_orders", "orders") }}
    )

SELECT *
FROM inc
CROSS JOIN src
WHERE
    inc.row_COUNT != src.row_COUNT
    OR inc.pk_COUNT != src.pk_COUNT