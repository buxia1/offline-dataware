CREATE DATABASE IF NOT EXISTS dim;

-- 商品维度表（普通版：只存当前状态）
--
-- 【为什么用 PRIMARY KEY(product_id)】
--   维度表要的是"每个商品当前长什么样"，一个商品只能有一行。
--   PRIMARY KEY 同键覆盖 = 天然去重 + 只留最新。

CREATE TABLE IF NOT EXISTS dim.dim_product (
    product_id     BIGINT        COMMENT "商品ID",
    product_name   VARCHAR(64)   COMMENT "商品名称",
    category       VARCHAR(32)   COMMENT "品类",
    brand          VARCHAR(32)   COMMENT "品牌",
    price          DECIMAL(10,2) COMMENT "单价",
    status         VARCHAR(32)   COMMENT "状态",
    update_time    DATETIME      COMMENT "上游更新时间"
) ENGINE = OLAP
PRIMARY KEY(product_id)
DISTRIBUTED BY HASH(product_id) BUCKETS 3
PROPERTIES ("replication_num" = "1");
