-- ============================================================
-- DQC 自检：故意构造坏数据，证明"检查真的能发现问题"
--
-- 【为什么必须做这个】
--   一个从来没失败过的检查，跟没有检查是一样的。
--   必须亲眼看到它在坏数据上报警，才能相信它在生产里管用。
--
-- 【它已经抓到过一次真 bug】
--   第一版的检查② 没拦住"哨兵值版本后面还有版本"这种断裂 ——
--   因为 DATE_ADD(DATE '9999-12-31', INTERVAL 1 DAY) 返回 NULL，
--   而 NULL <> next_from 是 NULL，CASE 落到 ELSE 0，损坏被静默放过。
--   这个自检用例 B 就是为了防止那个盲区回归。
--
-- 【为什么不会污染数据】
--   全部是 UNION ALL 出来的内存临时行，不碰任何真实表。
-- ============================================================


-- ============ 第一部分：① 每商品恰好一个当前版本 ============
SELECT '① 正常：2 个商品各 1 个当前版本' AS 用例, '0' AS expect,
       CAST(count(DISTINCT product_id) AS BIGINT) - CAST(COALESCE(sum(is_current), 0) AS BIGINT) AS actual
FROM (          SELECT 1 AS product_id, 1 AS is_current
      UNION ALL SELECT 2, 1) t

UNION ALL

SELECT '① 故障：商品1 有两个当前版本', '-1',
       CAST(count(DISTINCT product_id) AS BIGINT) - CAST(COALESCE(sum(is_current), 0) AS BIGINT)
FROM (          SELECT 1 AS product_id, 1 AS is_current
      UNION ALL SELECT 1, 1
      UNION ALL SELECT 2, 1) t

UNION ALL

SELECT '① 故障：商品1 一个当前版本都没有', '1',
       CAST(count(DISTINCT product_id) AS BIGINT) - CAST(COALESCE(sum(is_current), 0) AS BIGINT)
FROM (          SELECT 1 AS product_id, 0 AS is_current
      UNION ALL SELECT 2, 1) t;


-- ============ 第二部分：② 版本区间无断裂 ============
-- 四个用例，每个用例的所有行必须共用同一个 cid，否则会被拆成不同分组
WITH cases AS (
             SELECT 'A' AS cid, '正常：09-20→09-21 首尾相接' AS cdesc, '0' AS expect,
                    1 AS pid, DATE '2026-09-20' AS vf, DATE '2026-09-20' AS vt
    UNION ALL SELECT 'A', '正常：09-20→09-21 首尾相接', '0', 1, DATE '2026-09-21', DATE '9999-12-31'

    UNION ALL SELECT 'B', '故障：哨兵值版本后面还有版本', '1', 2, DATE '2026-09-20', DATE '2026-09-20'
    UNION ALL SELECT 'B', '故障：哨兵值版本后面还有版本', '1', 2, DATE '2026-09-21', DATE '9999-12-31'
    UNION ALL SELECT 'B', '故障：哨兵值版本后面还有版本', '1', 2, DATE '2026-09-22', DATE '9999-12-31'

    UNION ALL SELECT 'C', '故障：09-21 到 09-23 之间有空洞', '1', 3, DATE '2026-09-20', DATE '2026-09-21'
    UNION ALL SELECT 'C', '故障：09-21 到 09-23 之间有空洞', '1', 3, DATE '2026-09-23', DATE '9999-12-31'

    UNION ALL SELECT 'D', '故障：09-21 被两个版本同时占用', '1', 4, DATE '2026-09-20', DATE '2026-09-21'
    UNION ALL SELECT 'D', '故障：09-21 被两个版本同时占用', '1', 4, DATE '2026-09-21', DATE '9999-12-31'
),
compared AS (
    SELECT cid, cdesc, expect,
           COALESCE(sum(CASE
                WHEN next_from IS NULL THEN 0
                WHEN vt = DATE '9999-12-31' THEN 1
                WHEN DATE_ADD(vt, INTERVAL 1 DAY) <> next_from THEN 1
                ELSE 0 END), 0) AS actual
    FROM (
        SELECT cid, cdesc, expect, vt,
               LEAD(vf) OVER (PARTITION BY pid ORDER BY vf) AS next_from
        FROM cases
    ) t
    GROUP BY cid, cdesc, expect
)
SELECT cid, cdesc, expect, CAST(actual AS BIGINT) AS actual,
       CASE WHEN CAST(actual AS BIGINT) = CAST(expect AS BIGINT) THEN '✅ 通过' ELSE '❌ 不一致' END AS 结论
FROM compared
ORDER BY cid;


-- ============ 第三部分：③ is_current 与 valid_to 自洽 ============
SELECT '③ 正常：is_current=1 且 valid_to 是哨兵值' AS 用例, '0' AS expect,
       CAST(COALESCE(sum(CASE
                WHEN is_current = 1 AND valid_to <> DATE '9999-12-31' THEN 1
                WHEN is_current = 0 AND valid_to  = DATE '9999-12-31' THEN 1
                ELSE 0 END), 0) AS BIGINT) AS actual
FROM (SELECT 1 AS is_current, DATE '9999-12-31' AS valid_to) t

UNION ALL

SELECT '③ 故障：is_current=1 但 valid_to 不是哨兵值', '1',
       CAST(COALESCE(sum(CASE
                WHEN is_current = 1 AND valid_to <> DATE '9999-12-31' THEN 1
                WHEN is_current = 0 AND valid_to  = DATE '9999-12-31' THEN 1
                ELSE 0 END), 0) AS BIGINT)
FROM (SELECT 1 AS is_current, DATE '2026-09-21' AS valid_to) t

UNION ALL

SELECT '③ 故障：is_current=0 但 valid_to 是哨兵值', '1',
       CAST(COALESCE(sum(CASE
                WHEN is_current = 1 AND valid_to <> DATE '9999-12-31' THEN 1
                WHEN is_current = 0 AND valid_to  = DATE '9999-12-31' THEN 1
                ELSE 0 END), 0) AS BIGINT)
FROM (SELECT 0 AS is_current, DATE '9999-12-31' AS valid_to) t;



-- ============ 第四部分：⑥ 重物化属性一致 ============
-- 在内存里造两张假表（sku / scd2），把 ⑥ 的判定逻辑原样跑一遍。
-- 三个用例必须恰好覆盖两种盲区，否则自检本身就是瞎的。

-- 用例1：正常 —— 属性完全一致 → 期望 0
SELECT '⑥ 正常：属性完全一致' AS 用例, '0' AS expect,
       CAST(COALESCE(sum(CASE WHEN NOT (sku.category <=> s.category
                                       AND sku.brand    <=> s.brand
                                       AND sku.sku_price<=> s.price)
                              THEN 1 ELSE 0 END), 0) AS BIGINT) AS actual
FROM (          SELECT 1 AS product_id, DATE '2026-09-20' AS dt,
                       '数码' AS category, 'A' AS brand, 100 AS sku_price) sku
JOIN (          SELECT 1 AS product_id, DATE '2026-09-20' AS valid_from,
                       DATE '9999-12-31' AS valid_to,
                       '数码' AS category, 'A' AS brand, 100 AS price) s
  ON sku.product_id = s.product_id
 AND sku.dt BETWEEN s.valid_from AND s.valid_to

UNION ALL

-- 用例2：盲区(1) —— SCD2 已改成家电，宽表还留着数码 → 期望 1
SELECT '⑥ 故障：SCD2 改了品类，宽表没重物化', '1',
       CAST(COALESCE(sum(CASE WHEN NOT (sku.category <=> s.category
                                       AND sku.brand    <=> s.brand
                                       AND sku.sku_price<=> s.price)
                              THEN 1 ELSE 0 END), 0) AS BIGINT)
FROM (          SELECT 1 AS product_id, DATE '2026-09-20' AS dt,
                       '数码' AS category, 'A' AS brand, 100 AS sku_price) sku
JOIN (          SELECT 1 AS product_id, DATE '2026-09-20' AS valid_from,
                       DATE '9999-12-31' AS valid_to,
                       '家电' AS category, 'A' AS brand, 100 AS price) s
  ON sku.product_id = s.product_id
 AND sku.dt BETWEEN s.valid_from AND s.valid_to

UNION ALL

-- 用例3：盲区(2) —— 宽表 09-20 的行，SCD2 里版本从 09-21 才开始
--        → 范围 JOIN 不成立 → 宽表那行消失 → 必须靠 LEFT JOIN 抓
SELECT '⑥ 故障：宽表行在 SCD2 里找不到版本', '1',
       CAST(COALESCE(sum(CASE WHEN s.product_id IS NULL THEN 1 ELSE 0 END), 0) AS BIGINT)
FROM (          SELECT 1 AS product_id, DATE '2026-09-20' AS dt,
                       '数码' AS category, 'A' AS brand, 100 AS sku_price) sku
LEFT JOIN (     SELECT 1 AS product_id, DATE '2026-09-21' AS valid_from,
                       DATE '9999-12-31' AS valid_to,
                       '数码' AS category, 'A' AS brand, 100 AS price) s
  ON sku.product_id = s.product_id
 AND sku.dt BETWEEN s.valid_from AND s.valid_to;
