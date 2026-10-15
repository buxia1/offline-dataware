-- =============================================================================
-- 湖侧 DWD 层建表：lake.dwd.* （逐列照搬仓侧 dwd.* 的 DDL）
--
-- 三张表都是 ETL 产物、不是落地原始数据，所以：
--   · 不用 dt 分区也能跑，但 detail/sku 都带 dt 且查询几乎总是按天切 → 保留 dt 分区
--   · lifecycle 是累积快照（一单一行、无 dt），不分区
--   · 每次重建由 lake_dwd_build.py 用 DELETE + append 完成（Iceberg 无 TRUNCATE）
--
-- 仓侧对应 DDL：sql/dwd_order_detail.sql / sql/dwd_order_sku_detail.sql / sql/dwd_order_lifecycle.sql
-- =============================================================================

CREATE NAMESPACE IF NOT EXISTS lake.dwd;

-- ---------------------------------------------------------------- DWD 明细
-- 仓侧：DUPLICATE KEY(order_id, dt) / RANGE(dt) / BUCKETS 3
-- 湖侧：dt 分区；排序与桶是 StarRocks 特有的，Iceberg 侧不需要
DROP TABLE IF EXISTS lake.dwd.dwd_order_detail;
CREATE TABLE lake.dwd.dwd_order_detail (
    order_id   BIGINT        COMMENT '订单ID',
    dt         DATE          COMMENT '日期（分区列）',
    user_id    BIGINT        COMMENT '用户ID（已过滤 NULL）',
    product_id BIGINT        COMMENT '商品ID',
    amount     DECIMAL(10,2) COMMENT '金额（已取绝对值）',
    order_time TIMESTAMP_NTZ COMMENT '下单时间',
    status     VARCHAR(32)   COMMENT '状态'
) USING iceberg
PARTITIONED BY (dt)
TBLPROPERTIES ('format-version' = '2');

-- ------------------------------------------------------- DWD SKU（SCD2 时点）
-- 仓侧：DUPLICATE KEY(order_id, dt) / RANGE(dt) / BUCKETS 3
DROP TABLE IF EXISTS lake.dwd.dwd_order_sku_detail;
CREATE TABLE lake.dwd.dwd_order_sku_detail (
    order_id   BIGINT        COMMENT '订单ID',
    dt         DATE          COMMENT '日期（分区列）',
    user_id    BIGINT        COMMENT '用户ID',
    product_id BIGINT        COMMENT '商品ID',
    amount     DECIMAL(10,2) COMMENT '金额（已取绝对值）',
    order_time TIMESTAMP_NTZ COMMENT '下单时间',
    status     VARCHAR(32)   COMMENT '状态',
    category   VARCHAR(32)   COMMENT '★ 下单时点（dt）的商品品类，来自 SCD2',
    brand      VARCHAR(32)   COMMENT '★ 下单时点的品牌',
    sku_price  DECIMAL(10,2) COMMENT '★ 下单时点的价格',
    valid_from DATE          COMMENT '★ 命中的 SCD2 版本生效日',
    valid_to   DATE          COMMENT '★ 命中的 SCD2 版本失效日'
) USING iceberg
PARTITIONED BY (dt)
TBLPROPERTIES ('format-version' = '2');

-- ------------------------------------------------------- DWD 累积快照
-- 仓侧：PRIMARY KEY(order_id) / BUCKETS 3 / 无 dt 无分区
DROP TABLE IF EXISTS lake.dwd.dwd_order_lifecycle;
CREATE TABLE lake.dwd.dwd_order_lifecycle (
    order_id        BIGINT        COMMENT '订单ID',
    user_id         BIGINT        COMMENT '用户ID（可能 NULL）',
    product_id      BIGINT        COMMENT '商品ID',
    amount          DECIMAL(10,2) COMMENT '金额',
    order_time      TIMESTAMP_NTZ COMMENT '★ 下单时刻（event_type=order 的 MAX）',
    pay_time        TIMESTAMP_NTZ COMMENT '★ 支付时刻',
    ship_time       TIMESTAMP_NTZ COMMENT '★ 发货时刻',
    finish_time     TIMESTAMP_NTZ COMMENT '★ 完成时刻',
    cancel_time     TIMESTAMP_NTZ COMMENT '★ 取消时刻',
    current_stage   VARCHAR(16)   COMMENT '★ 当前阶段 = 最晚发生的那个事件',
    pay_lag_hours   INT           COMMENT '★ 下单→支付小时数',
    ship_lag_days   INT           COMMENT '★ 下单→发货天数',
    finish_lag_days INT           COMMENT '★ 下单→完成天数',
    last_event_time TIMESTAMP_NTZ COMMENT '★ 最晚事件时刻',
    update_time     TIMESTAMP_NTZ COMMENT 'ETL 写入时刻'
) USING iceberg
TBLPROPERTIES ('format-version' = '2');
