CREATE DATABASE IF NOT EXISTS dim;

-- 商品维度拉链表（SCD Type 2）
--
-- 【它和 dim_product 的区别】
--   dim_product      : 每个商品 1 行，只有"现在"
--   dim_product_scd2 : 每个商品 N 行，N = 变更次数 + 1，能回答"当时"
--
-- 【为什么主键是 (product_id, valid_from) 而不是 product_id】
--   product_id 在 SCD2 里会重复出现（每个版本一行），不能当主键。
--   加 valid_from 后：同一商品的同一版本唯一 → 支持幂等重跑（重跑覆盖同一行）。
--
-- 【valid_to 用哨兵值 9999-12-31 而不是 NULL】
--   因为 JOIN 条件要写 dt BETWEEN valid_from AND valid_to，
--   BETWEEN 遇到 NULL 返回 NULL，当前版本就永远匹配不上。
--
-- 【为什么可以按 valid_from 分区，普通 dim_product 不行】
--   SCD2 表里有真实的时间语义（有效期区间）→ 分区有意义。
--   普通维度表没有时间列 → 分区只是"抄 ODS"。

CREATE TABLE IF NOT EXISTS dim.dim_product_scd2 (
    product_id     BIGINT        COMMENT "商品ID（自然键）",
    valid_from     DATE          COMMENT "版本生效日",
    valid_to       DATE          COMMENT "版本失效日（含），当前版本=9999-12-31",
    is_current     TINYINT       COMMENT "是否当前版本 1/0",
    product_name   VARCHAR(64)   COMMENT "商品名称",
    category       VARCHAR(32)   COMMENT "品类",
    brand          VARCHAR(32)   COMMENT "品牌",
    price          DECIMAL(10,2) COMMENT "单价",
    status         VARCHAR(32)   COMMENT "状态",
    update_time    DATETIME      COMMENT "上游更新时间"
) ENGINE = OLAP
PRIMARY KEY(product_id, valid_from)
DISTRIBUTED BY HASH(product_id) BUCKETS 3
PROPERTIES ("replication_num" = "1");
