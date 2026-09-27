CREATE DATABASE IF NOT EXISTS ods;

CREATE TABLE IF NOT EXISTS ods.ods_order (
    order_id     BIGINT        COMMENT "订单ID",
    user_id      BIGINT        COMMENT "用户ID",
    product_id   BIGINT        COMMENT "商品ID",
    amount       DECIMAL(10,2) COMMENT "金额",
    order_time   DATETIME      COMMENT "下单时间",
    status       VARCHAR(32)   COMMENT "状态",
    dt           DATE          COMMENT "日期"
) ENGINE = OLAP
DUPLICATE KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 3
PROPERTIES ("replication_num" = "1");
