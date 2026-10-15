-- ============================================================
-- SCD2 全量重建（清空 + 推导 + 写入，不可分割）
--
-- 【为什么 TRUNCATE 必须在这个文件里】
--   下面是从 ODS 完整推导所有版本的"全量重算式"。
--   INSERT 只追加不删除，所以重跑会叠加出"一个商品多个当前版本"。
--   把 TRUNCATE 和 INSERT 放在同一个文件、一次执行，就不存在"忘记清空"。
--
-- 【为什么 is_current 会失控】
--   主键 (product_id, valid_from) 只保证版本不重复，
--   保证不了"每个商品恰好一个 is_current=1" —— 那是业务语义，只能靠这段 SQL 算对。
-- ============================================================

TRUNCATE TABLE dim.dim_product_scd2;

INSERT INTO dim.dim_product_scd2
    (product_id, valid_from, valid_to, is_current,
     product_name, category, brand, price, status, update_time)
WITH lagged AS (
    SELECT
        product_id, snapshot_date, product_name, category, brand,
        price, status, update_time,
        LAG(category) OVER (PARTITION BY product_id ORDER BY snapshot_date) AS prev_category,
        LAG(price)    OVER (PARTITION BY product_id ORDER BY snapshot_date) AS prev_price,
        LAG(status)   OVER (PARTITION BY product_id ORDER BY snapshot_date) AS prev_status,
        LAG(snapshot_date) OVER (PARTITION BY product_id ORDER BY snapshot_date) AS prev_date
    FROM ods.ods_product
),
marked AS (
    SELECT * FROM lagged
    WHERE prev_date IS NULL
       OR category <> prev_category
       OR price    <> prev_price
       OR status   <> prev_status
)
SELECT
    product_id,
    snapshot_date AS valid_from,
    COALESCE(
        DATE_SUB(LEAD(snapshot_date) OVER (PARTITION BY product_id ORDER BY snapshot_date),
                 INTERVAL 1 DAY),
        DATE '9999-12-31'
    ) AS valid_to,
    CASE WHEN LEAD(snapshot_date) OVER (PARTITION BY product_id ORDER BY snapshot_date) IS NULL
         THEN 1 ELSE 0 END AS is_current,
    product_name, category, brand, price, status, update_time
FROM marked;
