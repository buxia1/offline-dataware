-- =============================================================================
-- ODS 层：订单「事件流」表（累积快照事实表的数据源）
--
-- 为什么要新表而不是给 ods.ods_order 加两列？
--   ① ods.ods_order 是「订单宽消息」的落点，被 dwd_order_detail 链路消费；
--      往里塞事件会一行变多行，DWD 的 ROW_NUMBER 去重语义就变了（status 取值不再确定）。
--   ② 累积快照是「同一实体的另一个视角」，独立链路 → 旧链路零回归。
--
-- 为什么是 DUPLICATE KEY（追加）而不是 PRIMARY KEY（更新）？
--   Spark 用 startingOffsets=earliest 全量重读 Kafka → 每跑一次都会重复追加。
--   ODS 保持「追加层」不变，重复交给下游聚合消化
--   （装载用 MAX/MIN(event_time) GROUP BY order_id → 天然幂等）。
--
-- 为什么先不分区？
--   数据量小（每天 ~100 单 → 100 多个事件），分区只会增加复杂度。
--   dt 列仍然保留：它是「事件发生日」，装载 SQL 按 dt 过滤时一眼能看懂。
-- =============================================================================

CREATE DATABASE IF NOT EXISTS ods;

CREATE TABLE IF NOT EXISTS ods.ods_order_event (
    order_id   BIGINT        COMMENT "订单ID",
    user_id    BIGINT        COMMENT "用户ID（脏数据：可能为 NULL）",
    product_id BIGINT        COMMENT "商品ID",
    amount     DECIMAL(10,2) COMMENT "金额（脏数据：可能为负）",
    order_time DATETIME      COMMENT "下单时间（订单级属性，每个事件都重复带）",
    event_type VARCHAR(16)   COMMENT "事件类型：order / pay / ship / finish / cancel",
    event_time DATETIME      COMMENT "事件发生时间（里程碑时间戳）",
    dt         DATE          COMMENT "事件发生日 = to_date(event_time)"
) ENGINE = OLAP
DUPLICATE KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 3
PROPERTIES ("replication_num" = "1");

-- 核对
-- DESC ods.ods_order_event;
-- SELECT event_type, count(*) FROM ods.ods_order_event GROUP BY event_type ORDER BY 2 DESC;
