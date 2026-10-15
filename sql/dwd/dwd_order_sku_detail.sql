CREATE DATABASE IF NOT EXISTS dwd;

DROP TABLE IF EXISTS dwd.dwd_order_sku_detail;

CREATE TABLE dwd.dwd_order_sku_detail (
    order_id     BIGINT        COMMENT "订单ID",
    dt           DATE          COMMENT "日期（分区列，必须在 key 里）",
    user_id      BIGINT        COMMENT "用户ID",
    product_id   BIGINT        COMMENT "商品ID",
    amount       DECIMAL(10,2) COMMENT "订单金额",
    order_time   DATETIME      COMMENT "下单时间",
    status       VARCHAR(32)   COMMENT "订单状态",
    -- ↓ 新增：从 dim_product_scd2 JOIN 出来的商品属性（订单当天的口径）
    category     VARCHAR(32)   COMMENT "品类（订单当天该商品的品类）",
    brand        VARCHAR(32)   COMMENT "品牌（订单当天）",
    sku_price    DECIMAL(10,2) COMMENT "单价（订单当天，区别于订单金额 amount）",
    -- ↓ 保留版本有效期，便于排查"这笔订单匹配到了哪个版本"
    valid_from   DATE          COMMENT "匹配到的商品版本生效日",
    valid_to     DATE          COMMENT "匹配到的商品版本失效日"
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
