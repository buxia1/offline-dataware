-- ============================================================
-- 订单链路数据质量检查（DQC）
--
-- 【约定】每一行 = 一个"没通过"的检查项。全部通过时返回 0 行。
--         脚本据此决定是否让工作流中断。
--
-- 【为什么必须有这个检查】
--   订单链路是三级链：ods_order → dwd_order_detail → dws_user_order_day → ads_daily_sales。
--   每一级都是"汇总"，而汇总的错法是【静默】的：
--     某天没汇总 → 那天数据凭空消失，但没有任何报错。
--   商品链路的 DQC 管不到这三张表，所以订单链路一直是裸奔的。
--
-- 【⚠️ 每个分支都必须写 AS violations】
--   UNION ALL 以【第一个 SELECT 的列名】为准。
--   第一项漏了别名 → 整个 CTE 的第二列无名 →
--   最后的 SELECT violations 报 "Column 'violations' cannot be resolved"。
--   报错信息完全不会提到"你少写了别名"，极难定位。
-- ============================================================

WITH checks AS (
    -- ① DWS 汇总必须与 DWD 逐格相符（同一粒度 user_id × dt）
    --
    -- 【为什么能直接对账】
    --   DWS 的粒度是 user_id × dt，而 DWD 按 user_id、dt 分组后是【同一个粒度】。
    --   两边格数相同 → 可以一一对应，一格一格比。
    --   （注意：不能拿 DWS 去比 ADS，粒度不同，会变成④那样的重算）
    SELECT '① DWS 汇总与 DWD 一致' AS check_name,
           CAST(COALESCE(sum(ABS(a.order_cnt     - b.c)),  0)
              + COALESCE(sum(ABS(a.paid_cnt      - b.p)),  0)
              + COALESCE(sum(ABS(a.paid_amount   - b.pa)), 0)
              + COALESCE(sum(ABS(a.refund_amount - b.ra)), 0) AS BIGINT) AS violations
    FROM dws.dws_user_order_day a
    JOIN (
        SELECT user_id, dt,
               count(*) AS c,
               count(CASE WHEN status = 'paid'   THEN 1 END) AS p,
               sum(CASE WHEN status = 'paid'   THEN amount ELSE 0 END) AS pa,
               sum(CASE WHEN status = 'refund' THEN amount ELSE 0 END) AS ra
        FROM dwd.dwd_order_detail
        GROUP BY user_id, dt
    ) b ON a.user_id = b.user_id AND a.dt = b.dt

    UNION ALL

    -- ② DWD 有数据的天，DWS 必须有（抓"整天没汇总"）
    --   dt <= '${AS_OF}'：正常运行传"昨天"，补数时传当次业务日期
    --   → 逐天重建时只检查"已重建到的那天"，不会因为"后面的天还没做"而红灯
    SELECT '② DWS 覆盖 DWD 全部日期' AS check_name,
           CAST(count(*) AS BIGINT) AS violations
    FROM (SELECT DISTINCT dt FROM dwd.dwd_order_detail WHERE dt <= '${AS_OF}') d
    WHERE d.dt NOT IN (SELECT dt FROM dws.dws_user_order_day)

    UNION ALL

    -- ③ ADS 日报必须等于 DWS 按天汇总
    --   ADS 是 dt 粒度，DWS 是 user_id × dt → 先把 DWS 汇总到 dt 再比
    SELECT '③ ADS 与 DWS 汇总一致' AS check_name,
           CAST(COALESCE(sum(ABS(a.order_cnt   - b.c)),   0)
              + COALESCE(sum(ABS(a.paid_cnt    - b.p)),   0)
              + COALESCE(sum(ABS(a.paid_amount - b.pa)),  0)
              + COALESCE(sum(ABS(a.refund_amount - b.ra)),0)
              + COALESCE(sum(ABS(a.net_amount  - b.net)), 0) AS BIGINT) AS violations
    FROM ads.ads_daily_sales a
    JOIN (
        SELECT dt, sum(order_cnt) c, sum(paid_cnt) p,
               sum(paid_amount) pa, sum(refund_amount) ra,
               sum(paid_amount) - sum(refund_amount) net
        FROM dws.dws_user_order_day GROUP BY dt
    ) b ON a.dt = b.dt

    UNION ALL

    -- ④ 派生指标必须能由原子指标重算出来
    SELECT '④ 派生指标自洽' AS check_name,
           CAST(COALESCE(sum(CASE
                    WHEN ABS(net_amount - (paid_amount - refund_amount)) >= 0.01 THEN 1
                    WHEN ABS(avg_order_amount - ROUND(paid_amount / NULLIF(paid_cnt, 0), 2)) >= 0.01 THEN 1
                    WHEN ABS(pay_rate - ROUND(paid_cnt / NULLIF(order_cnt, 0), 4)) >= 0.0001 THEN 1
                    WHEN paid_cnt > order_cnt THEN 1
                    ELSE 0 END), 0) AS BIGINT) AS violations
    FROM ads.ads_daily_sales

    UNION ALL

    -- ⑤ DWD 每天的行数不可能超过 ODS（只过滤 + 去重，绝不产生新行）
    --
    -- 【为什么只查上界，不查下界】
    --   下界"DWD ≥ ODS 的 user_id 非空行数"看着很合理，但【不是不变式】：
    --   dwd_overwrite.sh 用 ROW_NUMBER() 按 order_id 去重，
    --   同一天同一个 order_id 有多行时会被合并 → DWD 会低于那个下界。
    --   实测有 3 天不满足。写成检查会永远红灯。
    SELECT '⑤ DWD 行数不超过 ODS' AS check_name,
           CAST(COALESCE(sum(CASE WHEN d.c > o.c THEN 1 ELSE 0 END), 0) AS BIGINT) AS violations
    FROM (SELECT dt, count(*) c FROM dwd.dwd_order_detail WHERE dt <= '${AS_OF}' GROUP BY dt) d
    JOIN (SELECT dt, count(*) c FROM ods.ods_order      WHERE dt <= '${AS_OF}' GROUP BY dt) o
      ON d.dt = o.dt
    UNION ALL

    -- ⑤a ODS 必须非空
    --    ⑤ 用的是 INNER JOIN：上游全空时【一行都匹配不上】→ violations=0 → 静默绿灯。
    --    而"上游全空"是最严重的数据事故，必须单独拦。
    --    （标量子查询比 IN 单行派生表稳：见 PITFALLS §3.12）
    SELECT '⑤b ODS 覆盖 DWD 全部日期' AS check_name,
           CAST(count(*) AS BIGINT) AS violations
    FROM (SELECT DISTINCT dt FROM dwd.dwd_order_detail WHERE dt <= '${AS_OF}') d
    LEFT JOIN (SELECT DISTINCT dt FROM ods.ods_order WHERE dt <= '${AS_OF}') o ON o.dt = d.dt
    WHERE o.dt IS NULL

    UNION ALL

    -- ⑤b DWD 的每一天，ODS 里必须都有（抓"上游部分丢失"）
    --    LEFT JOIN ... IS NULL 而不是 NOT IN / NOT EXISTS：见 PITFALLS §3.9
    SELECT '⑤b ODS 覆盖 DWD 全部日期' AS check_name,
           CAST(count(*) AS BIGINT) AS violations
    FROM (SELECT DISTINCT dt FROM dwd.dwd_order_detail) d
    LEFT JOIN (SELECT DISTINCT dt FROM ods.ods_order) o ON o.dt = d.dt
    WHERE o.dt IS NULL
)
SELECT check_name, violations
FROM checks
WHERE violations <> 0;
