-- ============================================================
-- 商品链路指纹：验证"反复重跑结果不变"（幂等）
--
-- 【为什么不能只看行数】
--   行数一样不代表数据一样：品类改了、金额错了、版本区间挪了，
--   行数都可能纹丝不动。所以要把【每一行的每一列】都算进指纹。
--
-- 【怎么算】
--   crc32(把一行所有关键列拼成字符串)  →  一个整数
--   再 sum() 汇总成整张表的一个指纹
--   任何一行的任何一列变化 → 指纹必变
--
-- 【用法】
--   跑工作流之前记下指纹，跑完再记一次，两次必须完全一致。
-- ============================================================

SELECT 'ods_product' AS 表, count(*) AS 行数,
       COALESCE(sum(crc32(concat_ws('|',
           product_id, snapshot_date, category, brand, price, status, update_time))), 0) AS 指纹
FROM ods.ods_product

UNION ALL

SELECT 'dim_product', count(*),
       COALESCE(sum(crc32(concat_ws('|',
           product_id, category, brand, price, status, update_time))), 0)
FROM dim.dim_product

UNION ALL

SELECT 'dim_product_scd2', count(*),
       COALESCE(sum(crc32(concat_ws('|',
           product_id, valid_from, valid_to, is_current, category, brand, price, status))), 0)
FROM dim.dim_product_scd2

UNION ALL

SELECT 'dwd_order_sku_detail', count(*),
       COALESCE(sum(crc32(concat_ws('|',
           order_id, dt, user_id, product_id, amount, status,
           category, brand, sku_price, valid_from, valid_to))), 0)
FROM dwd.dwd_order_sku_detail;
