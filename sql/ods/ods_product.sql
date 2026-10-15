CREATE DATABASE IF NOT EXISTS ods;

-- 商品快照落地层：每天一份全量快照，全部保留
--
-- 【为什么用 DUPLICATE KEY 而不是 PRIMARY KEY】
--   同一个 product_id 每天都会来一行（9-20 一行、9-26 一行）。
--   如果这里用 PRIMARY KEY(product_id)，后一天会覆盖前一天，
--   历史就没了 —— 而历史正是 DIM 层做 SCD2 的原料。
--   DUPLICATE KEY = 只追加、不覆盖，所有快照一行不丢。

CREATE TABLE IF NOT EXISTS ods.ods_product (
    product_id     BIGINT        COMMENT "商品ID",
    product_name   VARCHAR(64)   COMMENT "商品名称",
    category       VARCHAR(32)   COMMENT "品类",
    brand          VARCHAR(32)   COMMENT "品牌",
    price          DECIMAL(10,2) COMMENT "单价",
    status         VARCHAR(32)   COMMENT "状态 on_sale/off_shelf",
    update_time    DATETIME      COMMENT "上游更新时间",
    snapshot_date  DATE          COMMENT "快照日期（数仓观测到它的日期）"
) ENGINE = OLAP
DUPLICATE KEY(product_id)
DISTRIBUTED BY HASH(product_id) BUCKETS 3
PROPERTIES ("replication_num" = "1");
