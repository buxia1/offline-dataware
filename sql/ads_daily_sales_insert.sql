INSERT INTO ads.ads_daily_sales
SELECT
    dt,
    SUM(order_cnt)                                  AS order_cnt,
    SUM(paid_cnt)                                   AS paid_cnt,
    SUM(paid_amount)                                AS paid_amount,
    SUM(refund_amount)                              AS refund_amount,
    SUM(paid_amount) - SUM(refund_amount)           AS net_amount,
    ROUND(SUM(paid_amount) / NULLIF(SUM(paid_cnt), 0), 2)   AS avg_order_amount,
    ROUND(SUM(paid_cnt) / NULLIF(SUM(order_cnt), 0), 4)     AS pay_rate
FROM dws.dws_user_order_day
GROUP BY dt;
