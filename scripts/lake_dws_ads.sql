-- =============================================================================
-- 湖侧 DWS / ADS 层建表：lake.dws.* / lake.ads.*
--
-- 口径**逐字照搬**仓侧：
--   sql/dws_user_order_day_insert.sql   （DWS：user × day 汇总）
--   sql/ads_daily_sales_insert.sql      （ADS：每日大盘 + 派生指标）
--
-- 与仓侧 DDL 的唯一差异：仓侧两张表是 StarRocks PRIMARY KEY 模型、无分区；
-- 湖侧改成「dt 分区 + 全量覆盖写」，理由：
--   · 汇总层每行天然带 dt，按天分区后单天可独立重算（对应仓侧 SQL 里
--     "必须有 WHERE dt = ..." 的可重建性论证）
--   · Iceberg 分区表配 partitionOverwriteMode=dynamic 时，重算某天只替换该天，
--     效果与仓侧 PK 表的 upsert 等价
--   · 全量重建用 DELETE + append（Iceberg 无 TRUNCATE）
--
-- 目标粒度：
--   lake.dws.dws_user_order_day   1 行 = 1 个 user_id × 1 天
--   lake.ads.ads_daily_sales      1 行 = 1 天
-- =============================================================================

CREATE NAMESPACE IF NOT EXISTS lake.dws;
CREATE NAMESPACE IF NOT EXISTS lake.ads;

-- ---------------------------------------------------------------- DWS
-- 仓侧：PRIMARY KEY(user_id, dt) / BUCKETS 3 / 无分区
DROP TABLE IF EXISTS lake.dws.dws_user_order_day;
CREATE TABLE lake.dws.dws_user_order_day (
    user_id       BIGINT        COMMENT '用户ID',
    dt            DATE          COMMENT '日期（分区列）',
    order_cnt     BIGINT        COMMENT '订单总数',
    paid_cnt      BIGINT        COMMENT '已支付订单数',
    paid_amount   DECIMAL(18,2) COMMENT '已支付金额',
    refund_amount DECIMAL(18,2) COMMENT '退款金额'
) USING iceberg
PARTITIONED BY (dt)
TBLPROPERTIES ('format-version' = '2');

-- ---------------------------------------------------------------- ADS
-- 仓侧：PRIMARY KEY(dt) / BUCKETS 3 / 无分区
-- ★ 派生指标（avg_order_amount / pay_rate）在最终粒度现算，不能从 DWS 直接相加
DROP TABLE IF EXISTS lake.ads.ads_daily_sales;
CREATE TABLE lake.ads.ads_daily_sales (
    dt               DATE          COMMENT '日期（分区列）',
    order_cnt        BIGINT        COMMENT '订单总数',
    paid_cnt         BIGINT        COMMENT '支付订单数',
    paid_amount      DECIMAL(18,2) COMMENT '支付金额',
    refund_amount    DECIMAL(18,2) COMMENT '退款金额',
    net_amount       DECIMAL(18,2) COMMENT '净收入 = 支付 - 退款',
    avg_order_amount DECIMAL(18,2) COMMENT '客单价 = 支付金额 / 支付订单数',
    pay_rate         DECIMAL(10,4) COMMENT '支付率 = 支付订单数 / 订单总数'
) USING iceberg
PARTITIONED BY (dt)
TBLPROPERTIES ('format-version' = '2');
