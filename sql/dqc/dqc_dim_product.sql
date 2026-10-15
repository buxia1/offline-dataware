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

    UNION ALL

    -- ⑥ 重物化对账：宽表里的商品属性，必须等于 SCD2 对该日期算出的属性
    --
    -- 【为什么需要这一项】
    --   现有 ④⑤ 是拿【宽表】比【订单明细】。但订单明细里根本没有
    --   category/brand/price —— 所以这两项压根没检查商品属性。
    --   结果：SCD2 变了而宽表没重物化时，④⑤ 永远通过（行数金额都不变）。
    --
    -- 【两个盲区，各用一条查询盖住】
    --   (1) JOIN 得上但属性值不同   → 数不一致的行
    --   (2) JOIN 不上（找不到版本） → 数孤儿行
    --       第 (2) 种最阴险：范围 JOIN 条件不满足时，宽表那行会
    --       【直接消失】，连比较的机会都没有，计数纹丝不动。
    --
    -- 【为什么用 <=> 而不是 <>】
    --   这一列现在没有 NULL，但 NULL <=> NULL 返回 TRUE（安全的"相等"），
    --   而 NULL <> x 返回 NULL 不是 TRUE → 会把该行【静默漏掉】。
    --   你的 ② 号检查就踩过同款坑（DATE_ADD(哨兵值) 返回 NULL）。
    --
    -- 【为什么用 LEFT JOIN 而不是 NOT EXISTS】
    --   StarRocks 不支持"关联子查询里用非等值谓词"：
    --   NOT EXISTS (... WHERE sku.dt BETWEEN s.valid_from AND s.valid_to)
    --   → ERROR 1064: Not support Non-EQ correlated predicate in correlated subquery
    SELECT '⑥ 重物化属性一致',
           CAST(
             (SELECT count(*)
              FROM dwd.dwd_order_sku_detail sku
              JOIN dim.dim_product_scd2 s
                ON sku.product_id = s.product_id
               AND sku.dt BETWEEN s.valid_from AND s.valid_to
              WHERE NOT (sku.category  <=> s.category
                     AND sku.brand     <=> s.brand
                     AND sku.sku_price <=> s.price))
             +
             (SELECT count(*)
              FROM (
                  SELECT s.product_id AS matched
                  FROM dwd.dwd_order_sku_detail sku
                  LEFT JOIN dim.dim_product_scd2 s
                    ON sku.product_id = s.product_id
                   AND sku.dt BETWEEN s.valid_from AND s.valid_to
              ) t
              WHERE matched IS NULL)
           AS BIGINT))

SELECT check_name, violations
FROM checks
WHERE violations <> 0;
