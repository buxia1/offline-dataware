INSERT OVERWRITE dwd.dwd_order_detail PARTITION (p${bizdate})
SELECT
    order_id, dt, user_id, product_id, amount, order_time, status
FROM (
    SELECT
        order_id,
        dt,
        user_id,
        product_id,
        ABS(amount) AS amount,
        order_time,
        status,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY ABS(amount) DESC
        ) AS rn
    FROM ods.ods_order
    WHERE user_id IS NOT NULL
      AND dt = STR_TO_DATE('${bizdate}', '%Y%m%d')
) t
WHERE rn = 1;
