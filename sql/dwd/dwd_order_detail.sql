CREATE DATABASE IF NOT EXISTS dwd;

DROP TABLE IF EXISTS dwd.dwd_order_detail;

CREATE TABLE dwd.dwd_order_detail (
    order_id     BIGINT        COMMENT "订单ID",
    dt           DATE          COMMENT "日期",
    user_id      BIGINT        COMMENT "用户ID",
    product_id   BIGINT        COMMENT "商品ID",
    amount       DECIMAL(10,2) COMMENT "金额",
    order_time   DATETIME      COMMENT "下单时间",
    status       VARCHAR(32)   COMMENT "状态"
) ENGINE = OLAP
DUPLICATE KEY(order_id, dt)
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(order_id) BUCKETS 3
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "3"
);
