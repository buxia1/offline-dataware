-- =============================================================================
-- 湖侧维度层建表：lake.dim.* （逐列照搬仓侧 dim.* 的 DDL）
-- 仓侧对应：sql/dim_product_scd2.sql / sql/dim_product.sql
-- =============================================================================

CREATE NAMESPACE IF NOT EXISTS lake.dim;

-- SCD2 拉链表：PRIMARY KEY(product_id, valid_from)
-- ★ valid_to 用哨兵 9999-12-31 而不是 NULL —— 否则 `dt BETWEEN valid_from AND valid_to`
--   在开放区间上永远不匹配（仓侧同一约定）
DROP TABLE IF EXISTS lake.dim.dim_product_scd2;
CREATE TABLE lake.dim.dim_product_scd2 (
    product_id   BIGINT        COMMENT '商品ID',
    valid_from   DATE          COMMENT '版本生效日',
    valid_to     DATE          COMMENT '版本失效日（末版 = 9999-12-31 哨兵）',
    is_current   INT           COMMENT '是否当前版本（1/0）',
    product_name VARCHAR(64)   COMMENT '商品名',
    category     VARCHAR(32)   COMMENT '品类',
    brand        VARCHAR(32)   COMMENT '品牌',
    price        DECIMAL(10,2) COMMENT '价格',
    status       VARCHAR(32)   COMMENT '状态',
    update_time  TIMESTAMP_NTZ COMMENT 'ETL 写入时刻'
) USING iceberg
TBLPROPERTIES ('format-version' = '2');

-- 当前版快照：一商品一行
DROP TABLE IF EXISTS lake.dim.dim_product;
CREATE TABLE lake.dim.dim_product (
    product_id   BIGINT        COMMENT '商品ID',
    product_name VARCHAR(64)   COMMENT '商品名',
    category     VARCHAR(32)   COMMENT '品类',
    brand        VARCHAR(32)   COMMENT '品牌',
    price        DECIMAL(10,2) COMMENT '价格',
    status       VARCHAR(32)   COMMENT '状态',
    update_time  TIMESTAMP_NTZ COMMENT 'ETL 写入时刻'
) USING iceberg
TBLPROPERTIES ('format-version' = '2');
