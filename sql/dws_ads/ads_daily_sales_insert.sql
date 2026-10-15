-- ADS 层：每日大盘 + 派生指标
-- 在 DS 里作为「SQL 任务 / 非查询」节点 ads_metric 使用
--
-- 【为什么必须有 WHERE dt = ...】
--   与 dws_agg 同理：汇总层必须按天运行，才能保证每天的指标可独立重算。
--   否则一旦 dws 里某天缺失，这里会算出一个"全 0 的行"，
--   再由 PRIMARY KEY(dt) 覆盖掉原本正确的值 —— 静默改坏数据。
--
-- 【派生指标必须先各自 SUM 再运算】
--   原子指标（可加）：order_cnt / paid_amount / refund_amount
--   派生指标（不可加）：avg_order_amount / pay_rate —— 必须在最终粒度现算
--   SUM(paid_amount) - SUM(refund_amount)   ✓
--   SUM(paid_amount - refund_amount)        ✗ 换成比率结果会不同
--
-- 【NULLIF(x, 0) 是除零保护】
--   某天零支付时，分母变 NULL → 结果是 NULL 而不是报错中断作业。

INSERT INTO ads.ads_daily_sales
SELECT
    dt,
    SUM(order_cnt)                                          AS order_cnt,
    SUM(paid_cnt)                                           AS paid_cnt,
    SUM(paid_amount)                                        AS paid_amount,
    SUM(refund_amount)                                      AS refund_amount,
    SUM(paid_amount) - SUM(refund_amount)                   AS net_amount,
    ROUND(SUM(paid_amount) / NULLIF(SUM(paid_cnt), 0), 2)   AS avg_order_amount,
    ROUND(SUM(paid_cnt) / NULLIF(SUM(order_cnt), 0), 4)     AS pay_rate
FROM dws.dws_user_order_day
WHERE dt = STR_TO_DATE('${system.biz.date}', '%Y%m%d')
GROUP BY dt;
