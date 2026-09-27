INSERT INTO dws.dws_user_order_day
SELECT
    user_id,
    dt,
    COUNT(*) AS order_cnt,
    COUNT(CASE WHEN status = 'paid' THEN 1 END) AS paid_cnt,
    SUM(CASE WHEN status = 'paid' THEN amount ELSE 0 END) AS paid_amount,
    SUM(CASE WHEN status = 'refund' THEN amount ELSE 0 END) AS refund_amount
FROM dwd.dwd_order_detail
GROUP BY user_id, dt;
