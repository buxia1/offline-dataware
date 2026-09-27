CREATE DATABASE IF NOT EXISTS dws;

CREATE TABLE IF NOT EXISTS dws.dws_user_order_day (
    user_id        BIGINT        COMMENT "用户ID",
    dt             DATE          COMMENT "日期",
    order_cnt      BIGINT        COMMENT "订单总数",
    paid_cnt       BIGINT        COMMENT "已支付订单数",
    paid_amount    DECIMAL(18,2) COMMENT "已支付金额",
    refund_amount  DECIMAL(18,2) COMMENT "退款金额"
) ENGINE = OLAP
PRIMARY KEY(user_id, dt)
DISTRIBUTED BY HASH(user_id) BUCKETS 3
PROPERTIES ("replication_num" = "1");
