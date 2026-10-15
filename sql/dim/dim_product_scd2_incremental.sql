-- ============ 第1步：关闭旧的当前版本 ============
-- 【约束】UPDATE 不能带别名；CTE 不能被后续语句复用 → 推导必须重写一遍
UPDATE dim.dim_product_scd2
SET is_current = 0,
    valid_to = DATE_SUB((
        SELECT MIN(m.snapshot_date) FROM (
            SELECT lg.product_id, lg.snapshot_date
            FROM (
                SELECT l.product_id, l.snapshot_date, l.category, l.price, l.status,
                       LAG(l.category)      OVER (PARTITION BY l.product_id ORDER BY l.snapshot_date) AS prev_category,
                       LAG(l.price)         OVER (PARTITION BY l.product_id ORDER BY l.snapshot_date) AS prev_price,
                       LAG(l.status)        OVER (PARTITION BY l.product_id ORDER BY l.snapshot_date) AS prev_status,
                       LAG(l.snapshot_date) OVER (PARTITION BY l.product_id ORDER BY l.snapshot_date) AS prev_date
                FROM ods.ods_product l
                WHERE l.snapshot_date > '${LAST}'
            ) lg
            LEFT JOIN dim.dim_product_scd2 s
                   ON s.product_id = lg.product_id AND s.is_current = 1
            WHERE ( lg.prev_date IS NULL
                    AND NOT (lg.category <=> s.category AND lg.price <=> s.price AND lg.status <=> s.status) )
               OR ( lg.prev_date IS NOT NULL
                    AND NOT (lg.category <=> lg.prev_category AND lg.price <=> lg.prev_price AND lg.status <=> lg.prev_status) )
        ) m
        WHERE m.product_id = dim.dim_product_scd2.product_id
    ), INTERVAL 1 DAY)
WHERE is_current = 1
  AND EXISTS (
        SELECT 1 FROM (
            SELECT lg.product_id
            FROM (
                SELECT l.product_id, l.snapshot_date, l.category, l.price, l.status,
                       LAG(l.category)      OVER (PARTITION BY l.product_id ORDER BY l.snapshot_date) AS prev_category,
                       LAG(l.price)         OVER (PARTITION BY l.product_id ORDER BY l.snapshot_date) AS prev_price,
                       LAG(l.status)        OVER (PARTITION BY l.product_id ORDER BY l.snapshot_date) AS prev_status,
                       LAG(l.snapshot_date) OVER (PARTITION BY l.product_id ORDER BY l.snapshot_date) AS prev_date
                FROM ods.ods_product l
                WHERE l.snapshot_date > '${LAST}'
            ) lg
            LEFT JOIN dim.dim_product_scd2 s
                   ON s.product_id = lg.product_id AND s.is_current = 1
            WHERE ( lg.prev_date IS NULL
                    AND NOT (lg.category <=> s.category AND lg.price <=> s.price AND lg.status <=> s.status) )
               OR ( lg.prev_date IS NOT NULL
                    AND NOT (lg.category <=> lg.prev_category AND lg.price <=> lg.prev_price AND lg.status <=> lg.prev_status) )
        ) m2
        WHERE m2.product_id = dim.dim_product_scd2.product_id
  );

-- ============ 第2步：插入新版本 ============
INSERT INTO dim.dim_product_scd2
    (product_id, valid_from, valid_to, is_current, product_name, category, brand, price, status, update_time)
WITH lagged AS (
    SELECT l.*,
           LAG(l.category)      OVER (PARTITION BY l.product_id ORDER BY l.snapshot_date) AS prev_category,
           LAG(l.price)         OVER (PARTITION BY l.product_id ORDER BY l.snapshot_date) AS prev_price,
           LAG(l.status)        OVER (PARTITION BY l.product_id ORDER BY l.snapshot_date) AS prev_status,
           LAG(l.snapshot_date) OVER (PARTITION BY l.product_id ORDER BY l.snapshot_date) AS prev_date
    FROM ods.ods_product l
    WHERE l.snapshot_date > '${LAST}'
),
marked AS (
    SELECT lg.product_id, lg.snapshot_date, lg.product_name, lg.category, lg.brand,
           lg.price, lg.status, lg.update_time
    FROM lagged lg
    LEFT JOIN dim.dim_product_scd2 s
           ON s.product_id = lg.product_id AND s.is_current = 1
    WHERE ( lg.prev_date IS NULL
            AND NOT (lg.category <=> s.category AND lg.price <=> s.price AND lg.status <=> s.status) )
       OR ( lg.prev_date IS NOT NULL
            AND NOT (lg.category <=> lg.prev_category AND lg.price <=> lg.prev_price AND lg.status <=> lg.prev_status) )
)
SELECT n.product_id, n.snapshot_date,
       COALESCE(DATE_SUB(LEAD(n.snapshot_date) OVER (PARTITION BY n.product_id ORDER BY n.snapshot_date),
                         INTERVAL 1 DAY), DATE '9999-12-31'),
       CASE WHEN LEAD(n.snapshot_date) OVER (PARTITION BY n.product_id ORDER BY n.snapshot_date) IS NULL
            THEN 1 ELSE 0 END,
       n.product_name, n.category, n.brand, n.price, n.status, n.update_time
FROM marked n;
