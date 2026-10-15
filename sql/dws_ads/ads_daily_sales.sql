CREATE DATABASE IF NOT EXISTS ads;

CREATE TABLE IF NOT EXISTS ads.ads_daily_sales (
    dt                DATE          COMMENT "日期",
    order_cnt         BIGINT        COMMENT "订单总数",
    paid_cnt          BIGINT        COMMENT "支付订单数",
    paid_amount       DECIMAL(18,2) COMMENT "支付金额",
    refund_amount     DECIMAL(18,2) COMMENT "退款金额",
    net_amount        DECIMAL(18,2) COMMENT "净收入",
    avg_order_amount  DECIMAL(18,2) COMMENT "客单价",
    pay_rate          DECIMAL(10,4) COMMENT "支付率"
) ENGINE = OLAP
PRIMARY KEY(dt)
DISTRIBUTED BY HASH(dt) BUCKETS 3
PROPERTIES ("replication_num" = "1");
