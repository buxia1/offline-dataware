-- DWS 层：用户 × 天 汇总
-- 在 DS 里作为「SQL 任务 / 非查询」节点 dws_agg 使用
--
-- 【为什么必须有 WHERE dt = ...】
--   汇总层的每次运行只能是"某一天"的运行。
--   如果不加 WHERE（全表聚合）：
--     一旦 DWD 里某天的分区过期被删（dynamic_partition.start = -30 自动删），
--     GROUP BY 就算不出那一天 → INSERT 不产出那批键 →
--     DWS 里那天的旧数据不会报错也不会变空，而是变成"无法重建的孤儿数据"。
--   加了 WHERE 后：算不出来就写 0 行，旧数据原样保留，可重建性得以保持。

INSERT INTO dws.dws_user_order_day
SELECT
    user_id,
    dt,
    COUNT(*) AS order_cnt,
    COUNT(CASE WHEN status = 'paid' THEN 1 END) AS paid_cnt,
    SUM(CASE WHEN status = 'paid' THEN amount ELSE 0 END) AS paid_amount,
    SUM(CASE WHEN status = 'refund' THEN amount ELSE 0 END) AS refund_amount
FROM dwd.dwd_order_detail
WHERE dt = STR_TO_DATE('${system.biz.date}', '%Y%m%d')
GROUP BY user_id, dt;
