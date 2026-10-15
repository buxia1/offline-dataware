-- ============================================================
-- DQC 自检：故意构造坏数据，证明订单链路的检查真的能发现问题
--
-- 【为什么必须做这个】
--   一个从来没失败过的检查，跟没有检查是一样的。
--   必须亲眼看到它在坏数据上报警，才能相信它在生产里管用。
--
-- 【为什么不会污染数据】
--   全部是 UNION ALL 出来的内存临时行，不碰任何真实表。
--
-- 【三个必须知道的坑】
--   (1) ② 的判定不能照抄主 SQL 的 NOT IN：假数据只有一行时
--       StarRocks 优化器会退化成 nest-loop join：
--         ERROR 1064: nest-loop join not support: NULL_AWARE_LEFT_ANTI_JOIN
--       换成 LEFT JOIN ... IS NULL 又会在内存假表上给出错误结果。
--       → 自检里统一用【标量子查询】判断，语义等价且稳定。
--   (2) 【不要用小数差异做用例】：ABS(10.00 - 10.01) 在常量折叠时
--       会踩到精度边界（>= 0.01 判定不稳定）。自检用例一律用【整数计数】。
--       真实表上的金额比对不受影响（那是真的列，不是字面量）。
--   (3) ④ 在 paid_cnt = 0 时 NULLIF(...) → NULL → ABS(NULL) → NULL，
--       CASE 落到 ELSE 0 —— 错误被【静默放过】。用例 4e 就是这个已知盲区。
-- ============================================================


-- ============ 第一部分：① DWS 汇总与 DWD 一致 ============
SELECT '① 正常：DWS 与 DWD 相符' AS 用例, '0' AS expect,
       CAST(COALESCE(sum(ABS(a.order_cnt - b.c)), 0) AS BIGINT) AS actual
FROM (          SELECT 1 AS user_id, DATE '2026-09-20' AS dt, 3 AS order_cnt) a
JOIN (          SELECT 1 AS user_id, DATE '2026-09-20' AS dt, 3 AS c) b
  ON a.user_id = b.user_id AND a.dt = b.dt

UNION ALL

SELECT '① 故障：order_cnt 少算 1', '1',
       CAST(COALESCE(sum(ABS(a.order_cnt - b.c)), 0) AS BIGINT)
FROM (          SELECT 1 AS user_id, DATE '2026-09-20' AS dt, 2 AS order_cnt) a
JOIN (          SELECT 1 AS user_id, DATE '2026-09-20' AS dt, 3 AS c) b
  ON a.user_id = b.user_id AND a.dt = b.dt

UNION ALL

SELECT '① 故障：paid_cnt 与 DWD 差 2', '2',
       CAST(COALESCE(sum(ABS(a.paid_cnt - b.p)), 0) AS BIGINT)
FROM (          SELECT 1 AS user_id, DATE '2026-09-20' AS dt, 4 AS paid_cnt) a
JOIN (          SELECT 1 AS user_id, DATE '2026-09-20' AS dt, 6 AS p) b
  ON a.user_id = b.user_id AND a.dt = b.dt

UNION ALL

SELECT '① 故障：两个指标同时错（差异应累加）', '3',
       CAST(COALESCE(sum(ABS(a.order_cnt - b.c)), 0)
          + COALESCE(sum(ABS(a.paid_cnt  - b.p)), 0) AS BIGINT)
FROM (          SELECT 1 AS user_id, DATE '2026-09-20' AS dt, 1 AS order_cnt, 2 AS paid_cnt) a
JOIN (          SELECT 1 AS user_id, DATE '2026-09-20' AS dt, 2 AS c,         4 AS p) b
  ON a.user_id = b.user_id AND a.dt = b.dt;


-- ============ 第二部分：② DWS 覆盖 DWD 全部日期 ============
-- ⚠️ 这里【不能】用 LEFT JOIN ... WHERE w.dt IS NULL：
--    在内存假表上 StarRocks 会给出错误结果（实测 v5 明明有 2 行、matched 有 NULL，
--    但 COUNT(*) 仍返回 0）。真实表上用 NOT IN 是正常的，但假表一行时
--    NOT IN 又会退化成 nest-loop join 报错。
--    → 自检里统一用【标量子查询】判断"这天在 DWS 里一条都没有"，语义最直白。
SELECT '② 正常：DWD 每天 DWS 都有' AS 用例, '0' AS expect,
       CAST(count(*) AS BIGINT) AS actual
FROM (SELECT DATE '2026-09-20' AS dt) d
WHERE (SELECT count(*) FROM (SELECT DATE '2026-09-20' AS dt) w WHERE w.dt = d.dt) = 0

UNION ALL

SELECT '② 故障：DWD 有 09-22 而 DWS 没有', '1',
       CAST(count(*) AS BIGINT)
FROM (          SELECT DATE '2026-09-20' AS dt
      UNION ALL SELECT DATE '2026-09-22') d
WHERE (SELECT count(*) FROM (SELECT DATE '2026-09-20' AS dt) w WHERE w.dt = d.dt) = 0

UNION ALL

SELECT '② 故障：DWD 有两天 DWS 都没有', '2',
       CAST(count(*) AS BIGINT)
FROM (          SELECT DATE '2026-09-20' AS dt
      UNION ALL SELECT DATE '2026-09-22'
      UNION ALL SELECT DATE '2026-09-23') d
WHERE (SELECT count(*) FROM (SELECT DATE '2026-09-20' AS dt) w WHERE w.dt = d.dt) = 0;


-- ============ 第三部分：③ ADS 与 DWS 汇总一致 ============
SELECT '③ 正常：ADS 等于 DWS 按天汇总' AS 用例, '0' AS expect,
       CAST(COALESCE(sum(ABS(a.order_cnt - b.c)), 0) AS BIGINT) AS actual
FROM (          SELECT DATE '2026-09-20' AS dt, 5 AS order_cnt) a
JOIN (          SELECT DATE '2026-09-20' AS dt, 5 AS c) b ON a.dt = b.dt

UNION ALL

SELECT '③ 故障：ADS 的 order_cnt 与 DWS 汇总差 2', '2',
       CAST(COALESCE(sum(ABS(a.order_cnt - b.c)), 0) AS BIGINT)
FROM (          SELECT DATE '2026-09-20' AS dt, 3 AS order_cnt) a
JOIN (          SELECT DATE '2026-09-20' AS dt, 5 AS c) b ON a.dt = b.dt

UNION ALL

SELECT '③ 故障：某天 net_amount 与 DWS 不一致', '3',
       CAST(COALESCE(sum(ABS(a.net_amount - b.net)), 0) AS BIGINT)
FROM (          SELECT DATE '2026-09-20' AS dt, 10 AS net_amount) a
JOIN (          SELECT DATE '2026-09-20' AS dt, 13 AS net) b ON a.dt = b.dt;


-- ============ 第四部分：④ 派生指标自洽 ============
-- 正常用例验算：net=100-20=80 ✓；客单价=100/8=12.50 ✓；pay_rate=8/10=0.8000 ✓
SELECT '④ 正常：三个派生指标都对' AS 用例, '0' AS expect,
       CAST(COALESCE(sum(CASE
                WHEN net_amount      <> paid_amount - refund_amount THEN 1
                WHEN avg_order_amount <> ROUND(paid_amount / NULLIF(paid_cnt, 0), 2) THEN 1
                WHEN pay_rate         <> ROUND(paid_cnt / NULLIF(order_cnt, 0), 4) THEN 1
                WHEN paid_cnt > order_cnt THEN 1
                ELSE 0 END), 0) AS BIGINT) AS actual
FROM (          SELECT 100.00 AS paid_amount, 20.00 AS refund_amount, 80.00 AS net_amount,
                       8 AS paid_cnt, 10 AS order_cnt,
                       12.50 AS avg_order_amount, 0.8000 AS pay_rate) a

UNION ALL

SELECT '④ 故障：net_amount 算错（85 应为 80）', '1',
       CAST(COALESCE(sum(CASE
                WHEN net_amount      <> paid_amount - refund_amount THEN 1
                WHEN avg_order_amount <> ROUND(paid_amount / NULLIF(paid_cnt, 0), 2) THEN 1
                WHEN pay_rate         <> ROUND(paid_cnt / NULLIF(order_cnt, 0), 4) THEN 1
                WHEN paid_cnt > order_cnt THEN 1
                ELSE 0 END), 0) AS BIGINT)
FROM (          SELECT 100.00 AS paid_amount, 20.00 AS refund_amount, 85.00 AS net_amount,
                       8 AS paid_cnt, 10 AS order_cnt,
                       12.50 AS avg_order_amount, 0.8000 AS pay_rate) a

UNION ALL

SELECT '④ 故障：客单价算错（13.00 应为 12.50）', '1',
       CAST(COALESCE(sum(CASE
                WHEN net_amount      <> paid_amount - refund_amount THEN 1
                WHEN avg_order_amount <> ROUND(paid_amount / NULLIF(paid_cnt, 0), 2) THEN 1
                WHEN pay_rate         <> ROUND(paid_cnt / NULLIF(order_cnt, 0), 4) THEN 1
                WHEN paid_cnt > order_cnt THEN 1
                ELSE 0 END), 0) AS BIGINT)
FROM (          SELECT 100.00 AS paid_amount, 20.00 AS refund_amount, 80.00 AS net_amount,
                       8 AS paid_cnt, 10 AS order_cnt,
                       13.00 AS avg_order_amount, 0.8000 AS pay_rate) a

UNION ALL

SELECT '④ 故障：pay_rate 用错分母（写成 1.0000）', '1',
       CAST(COALESCE(sum(CASE
                WHEN net_amount      <> paid_amount - refund_amount THEN 1
                WHEN avg_order_amount <> ROUND(paid_amount / NULLIF(paid_cnt, 0), 2) THEN 1
                WHEN pay_rate         <> ROUND(paid_cnt / NULLIF(order_cnt, 0), 4) THEN 1
                WHEN paid_cnt > order_cnt THEN 1
                ELSE 0 END), 0) AS BIGINT)
FROM (          SELECT 100.00 AS paid_amount, 20.00 AS refund_amount, 80.00 AS net_amount,
                       8 AS paid_cnt, 10 AS order_cnt,
                       12.50 AS avg_order_amount, 1.0000 AS pay_rate) a

UNION ALL

SELECT '④ 故障：paid_cnt > order_cnt（逻辑不可能）', '1',
       CAST(COALESCE(sum(CASE
                WHEN net_amount      <> paid_amount - refund_amount THEN 1
                WHEN avg_order_amount <> ROUND(paid_amount / NULLIF(paid_cnt, 0), 2) THEN 1
                WHEN pay_rate         <> ROUND(paid_cnt / NULLIF(order_cnt, 0), 4) THEN 1
                WHEN paid_cnt > order_cnt THEN 1
                ELSE 0 END), 0) AS BIGINT)
FROM (          SELECT 100.00 AS paid_amount, 20.00 AS refund_amount, 80.00 AS net_amount,
                       12 AS paid_cnt, 10 AS order_cnt,
                       12.50 AS avg_order_amount, 1.2000 AS pay_rate) a

UNION ALL

-- ⚠️ 已知盲区：paid_cnt = 0 → NULLIF(paid_cnt,0) → NULL
--    → avg_order_amount <> NULL → NULL（不是 TRUE）→ CASE 落到 ELSE 0
--    这里客单价 999.00 明显错，但实际只报 0
SELECT '④ 已知盲区：paid_cnt=0 时客单价检查失效（expect 1，实际 0）', '1（实际 0=盲区）',
       CAST(COALESCE(sum(CASE
                WHEN net_amount      <> paid_amount - refund_amount THEN 1
                WHEN avg_order_amount <> ROUND(paid_amount / NULLIF(paid_cnt, 0), 2) THEN 1
                WHEN pay_rate         <> ROUND(paid_cnt / NULLIF(order_cnt, 0), 4) THEN 1
                WHEN paid_cnt > order_cnt THEN 1
                ELSE 0 END), 0) AS BIGINT)
FROM (          SELECT 0.00 AS paid_amount, 0.00 AS refund_amount, 0.00 AS net_amount,
                       0 AS paid_cnt, 5 AS order_cnt,
                       999.00 AS avg_order_amount, 0.0000 AS pay_rate) a;


-- ============ 第五部分：⑤ DWD 行数不超过 ODS ============
SELECT '⑤ 正常：DWD 行数少于 ODS（被过滤/去重）' AS 用例, '0' AS expect,
       CAST(COALESCE(sum(CASE WHEN d.c > o.c THEN 1 ELSE 0 END), 0) AS BIGINT) AS actual
FROM (          SELECT DATE '2026-09-20' AS dt, 82 AS c) d
JOIN (          SELECT DATE '2026-09-20' AS dt, 87 AS c) o ON d.dt = o.dt

UNION ALL

SELECT '⑤ 故障：DWD 行数超过 ODS（凭空多出 5 行）', '1',
       CAST(COALESCE(sum(CASE WHEN d.c > o.c THEN 1 ELSE 0 END), 0) AS BIGINT)
FROM (          SELECT DATE '2026-09-20' AS dt, 92 AS c) d
JOIN (          SELECT DATE '2026-09-20' AS dt, 87 AS c) o ON d.dt = o.dt;
