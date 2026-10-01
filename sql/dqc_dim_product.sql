-- ============================================================
-- 商品链路数据质量检查（DQC）
--
-- 【约定】每一行 = 一个"没通过"的检查项。
--         全部通过时返回 0 行。脚本据此决定是否让工作流中断。
--
-- 【为什么必须有这个检查】
--   SCD2 的 is_current 没有任何数据库约束保护它。
--   主键 (product_id, valid_from) 只保证"版本不重复"，
--   保证不了"每个商品恰好一个当前版本" —— 那是业务语义。
--   装载 SQL 算错时，数据是"坏但不报错"的：
--   曾经出现 ratio = 2.00（每个商品两个当前版本），表照样能查，没人发现。
--   唯一能发现它的办法就是主动校验。
--
-- 【为什么 violations 要算成数字，而不是只返回 TRUE/FALSE】
--   数字能告诉你"错得多严重"：差 1 是漏了一个当前版本，
--   差 50 就是整批重复装载了。诊断信息量完全不同。
-- ============================================================

WITH checks AS (
    -- ① 每个商品恰好一个当前版本
    SELECT '① 每商品恰好一个当前版本' AS check_name,
           CAST(count(DISTINCT product_id) AS BIGINT) - CAST(COALESCE(sum(is_current), 0) AS BIGINT) AS violations
    FROM dim.dim_product_scd2

    UNION ALL

    -- ② 版本区间之间既不能重叠，也不能有空洞
    --
    -- ⚠️ 第二行那个判断不能省，否则整个检查有个致命盲区：
    --   DATE_ADD(DATE '9999-12-31', INTERVAL 1 DAY) 返回的是 NULL，不是报错。
    --   而 NULL <> next_from 的结果是 NULL（不是 TRUE），CASE 会落到 ELSE 0 ——
    --   于是"一个声称永久有效的版本后面又跟了一个版本"这种断裂被静默放过。
    --   这和 PITFALLS 里"BETWEEN 遇 NULL 返回 NULL"是同一个陷阱。
    SELECT '② 版本区间无断裂',
           CAST(COALESCE(sum(CASE
                    WHEN next_from IS NULL THEN 0
                    WHEN valid_to = DATE '9999-12-31' THEN 1
                    WHEN DATE_ADD(valid_to, INTERVAL 1 DAY) <> next_from THEN 1
                    ELSE 0 END), 0) AS BIGINT)
    FROM (
        SELECT product_id, valid_to,
               LEAD(valid_from) OVER (PARTITION BY product_id ORDER BY valid_from) AS next_from
        FROM dim.dim_product_scd2
    ) t

    UNION ALL

    -- ③ is_current 与 valid_to 必须自洽（当前版本必须是哨兵值，反之亦然）
    SELECT '③ is_current 与 valid_to 自洽',
           CAST(COALESCE(sum(CASE
                    WHEN is_current = 1 AND valid_to <> DATE '9999-12-31' THEN 1
                    WHEN is_current = 0 AND valid_to  = DATE '9999-12-31' THEN 1
                    ELSE 0 END), 0) AS BIGINT)
    FROM dim.dim_product_scd2

    UNION ALL

    -- ④ 物化对账（行数）：宽表每一行都该来自订单明细，不多不少
    SELECT '④ 物化行数一致',
           CAST(ABS(
               (SELECT count(*) FROM dwd.dwd_order_detail)
             - (SELECT count(*) FROM dwd.dwd_order_sku_detail)
           ) AS BIGINT)

    UNION ALL

    -- ⑤ 物化对账（金额）：JOIN 写错最典型的表现就是金额被放大（一对多）
    SELECT '⑤ 物化金额一致',
           CAST(CASE WHEN ABS(
               (SELECT COALESCE(sum(amount), 0) FROM dwd.dwd_order_detail)
             - (SELECT COALESCE(sum(amount), 0) FROM dwd.dwd_order_sku_detail)
           ) < 0.01 THEN 0 ELSE 1 END AS BIGINT)
)
SELECT check_name, violations
FROM checks
WHERE violations <> 0;
