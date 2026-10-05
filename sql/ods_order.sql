-- =============================================================================
-- ODS 层：订单表（订单快照链路的数据源）
--
-- 【本次改造：DUPLICATE KEY(order_id) → PRIMARY KEY(order_id)】
--
-- 老版本是 DUPLICATE KEY（追加层），重复消息会累积多行：
--   实测事故前 ods_order 有 4000 行，而 Kafka 里只有 1000 条消息
--   —— 因为 Spark 用 startingOffsets=earliest 全量重读，跑了 4 遍就追加了 4 倍。
--
-- 为了消掉重复，DS 工作流里挂了一个 truncate_ods 节点【先清表再灌】——
-- 那恰好就是 2026-10-05 静默清库的机制：
--   truncate（表清空）→ Spark 读到 0 行（Kafka 消息已过期）→ 灌回 0 行 → 报成功。
--
-- 改成 PRIMARY KEY(order_id) 之后：
--   ① INSERT 就是 UPSERT → 重复消息自动折叠，天然幂等
--   ② 不再需要 truncate → ODS 变成真正的「只追加、不删除」层
--   ③ 历史订单不会被误删（这是 ODS 作为"重建数据来源"的基本要求）
--   ④ Spark 的 startingOffsets=earliest 不用改（重复读也幂等）
--
-- 【代价（要清楚）】
--   ODS 不再保留"原始重复"。下游 dwd_overwrite.sh 里的 ROW_NUMBER 去重
--   因此变成冗余 —— 留着无害，它是"跨天同一 order_id"的兜底。
--   注意：主键是 order_id（不含 dt），所以【同一个订单只属于一天】。
--   改造后的生成器保证了这点（order_id 由日期推导）。
--
-- 【为什么不用 "按天分区 + INSERT OVERWRITE PARTITION" 实现幂等】
--   Spark 走 JDBC 写入，无法指定"覆盖某个分区"；要那样做就得再加一张
--   staging 表多一跳。主键 UPSERT 用一行 DDL 就达到同样效果。
-- =============================================================================

CREATE DATABASE IF NOT EXISTS ods;

-- ⚠️ 表模型变了，CREATE TABLE IF NOT EXISTS 不会修改已存在的表 → 必须显式重建。
--    当前 ods.ods_order 是 0 行（事故后），DROP 没有数据损失。
--    如果你的 ODS 里还有数据，先备份再执行！
DROP TABLE IF EXISTS ods.ods_order;

CREATE TABLE IF NOT EXISTS ods.ods_order (
    order_id     BIGINT        NOT NULL  COMMENT "订单ID（主键，一个订单一行）",
    user_id      BIGINT                  COMMENT "用户ID（脏数据：可能为 NULL）",
    product_id   BIGINT                  COMMENT "商品ID",
    amount       DECIMAL(10,2)           COMMENT "金额（脏数据：可能为负）",
    order_time   DATETIME                COMMENT "下单时间",
    status       VARCHAR(32)             COMMENT "状态：paid / refund / cancel",
    dt           DATE                    COMMENT "日期 = to_date(order_time)"
) ENGINE = OLAP
PRIMARY KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 3
PROPERTIES (
    "replication_num" = "1",
    "enable_persistent_index" = "true",
    "compression" = "LZ4"
);

-- 核对：
-- DESC ods.ods_order;          -- Key 列应显示 true（主键）
-- SHOW CREATE TABLE ods.ods_order;
