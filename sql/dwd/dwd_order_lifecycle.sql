-- =============================================================================
-- DWD 层：订单累积快照事实表（Accumulating Snapshot Fact Table）
--
-- 【它是什么】
--   一行 = 一个订单的【全生命周期】。每发生一个里程碑就回填一列。
--   NULL 的里程碑 = 还没发生。「卡住」就是永远 NULL —— 这正是累积快照的核心价值。
--
-- 【和 dwd_order_detail 的关系：同一实体的两个视角，不要直接 JOIN 出报表】
--   （见 docs/dimension-modeling.md §10.6）
--     dwd_order_detail    : 快照式，按天分区，同一订单可能多行
--     dwd_order_lifecycle : 状态式，一个订单一行，会被反复回填
--
-- 【为什么是 PRIMARY KEY(order_id) 而不是 DUPLICATE KEY】
--   累积快照的核心操作是「按订单回填」，主键模型才有「按主键快速定位一行」的能力；
--   而且主键模型下 INSERT 就是 UPSERT → 增量回填天然幂等（见装载 SQL 的说明）。
--
-- 【为什么先不分区】
--   数据量小；而且 StarRocks 要求分区列出现在排序键里 ——
--   先不分区可以让 order_id 独占排序键，将来要按天分区时再改。
--
-- 【为什么保留五个独立里程碑列，而不是 event_type + event_time 的窄表】
--   ① 累积快照的定义就是「宽表 + 每个里程碑一列」
--   ② 不依赖任何编码规则（比如 order_id % 100）→ 换真实数据集（Olist）零改表
--
-- 【为什么顺带建一个视图】
--   「由 ODS 推导出期望的累积快照」这段逻辑要用三次：
--     ① 装载（INSERT）
--     ② 防线③ 正向对账（期望表 EXCEPT 实际表）
--     ③ 防线③ 反向对账
--   写成视图 = 单一真相源。否则三处重复，改一处忘两处 → 对账自己就漂了。
--   代价：StarRocks 的视图是逻辑视图（不物化），每次查询重算 —— 数据量小，无所谓。
-- =============================================================================

CREATE DATABASE IF NOT EXISTS dwd;

CREATE TABLE IF NOT EXISTS dwd.dwd_order_lifecycle (
    order_id        BIGINT        NOT NULL          COMMENT "订单ID（主键，一个订单一行）",
    user_id         BIGINT                          COMMENT "用户ID（脏数据：可能为 NULL）",
    product_id      BIGINT                          COMMENT "商品ID",
    amount          DECIMAL(10,2)                   COMMENT "订单金额（脏数据：可能为负）",

    -- ↓ 五个里程碑：每发生一个就回填一列；NULL = 还没发生
    order_time      DATETIME                        COMMENT "下单时间",
    pay_time        DATETIME                        COMMENT "支付时间",
    ship_time       DATETIME                        COMMENT "发货时间",
    finish_time     DATETIME                        COMMENT "完成时间",
    cancel_time     DATETIME                        COMMENT "取消时间",

    -- ↓ 派生列：当前阶段 + 各阶段时长（累积快照的典型派生）
    current_stage   VARCHAR(16)                     COMMENT "当前阶段 = 时间上最后发生的那个事件：order/pay/ship/finish/cancel",
    pay_lag_hours   INT                             COMMENT "下单到支付的小时数；未支付 = NULL（卡单的 lag 就是 NULL）",
    ship_lag_days   INT                             COMMENT "下单到发货的天数；未发货 = NULL",
    finish_lag_days INT                             COMMENT "下单到完成的天数；未完成 = NULL",
    last_event_time DATETIME                        COMMENT "最后一次状态变化时间（用来监控哪些订单动过）",
    update_time     DATETIME                        COMMENT "最后一次回填时间"
) ENGINE = OLAP
PRIMARY KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 3
PROPERTIES (
    "replication_num" = "1",
    "enable_persistent_index" = "true",
    "compression" = "LZ4"
);

-- -----------------------------------------------------------------------------
-- 视图：由 ODS 事件流推导出「期望的累积快照」
--
-- 两个必须解释的写法：
--   ① MAX(CASE WHEN event_type = 'x' THEN event_time END)
--      就是「这个订单的 x 里程碑时间」，没发生则为 NULL。
--      不用 LEAST/MIN 判空：StarRocks 里 LEAST(NULL, x) / GREATEST(NULL, x) 返回 NULL，
--      会静默算错（交接文档已实测）。
--   ② GREATEST(...) 必须用 COALESCE 兜底
--      直接写 GREATEST(order_time, pay_time, ...) 只要有一个里程碑是 NULL，
--      结果就是 NULL → current_stage 全部退化成 'order'。这是本视图最容易踩的坑。
-- -----------------------------------------------------------------------------
DROP VIEW IF EXISTS dwd.v_order_lifecycle_expected;

CREATE VIEW dwd.v_order_lifecycle_expected AS
WITH agg AS (
    SELECT order_id,
           MAX(user_id)                                             AS user_id,
           MAX(product_id)                                          AS product_id,
           MAX(amount)                                              AS amount,
           MAX(CASE WHEN event_type = 'order'  THEN event_time END) AS order_time,
           MAX(CASE WHEN event_type = 'pay'    THEN event_time END) AS pay_time,
           MAX(CASE WHEN event_type = 'ship'   THEN event_time END) AS ship_time,
           MAX(CASE WHEN event_type = 'finish' THEN event_time END) AS finish_time,
           MAX(CASE WHEN event_type = 'cancel' THEN event_time END) AS cancel_time
    FROM ods.ods_order_event
    GROUP BY order_id
),
derived AS (
    SELECT agg.*,
           GREATEST(COALESCE(order_time,  '1970-01-01 00:00:00'),
                    COALESCE(pay_time,    '1970-01-01 00:00:00'),
                    COALESCE(ship_time,   '1970-01-01 00:00:00'),
                    COALESCE(finish_time, '1970-01-01 00:00:00'),
                    COALESCE(cancel_time, '1970-01-01 00:00:00')) AS last_event_time
    FROM agg
)
SELECT d.order_id,
       d.user_id,
       d.product_id,
       d.amount,
       d.order_time,
       d.pay_time,
       d.ship_time,
       d.finish_time,
       d.cancel_time,
       -- 当前阶段 = 时间上最后发生的那个事件（同时刻并列时取终态）
       CASE WHEN d.cancel_time = d.last_event_time THEN 'cancel'
            WHEN d.finish_time = d.last_event_time THEN 'finish'
            WHEN d.ship_time   = d.last_event_time THEN 'ship'
            WHEN d.pay_time    = d.last_event_time THEN 'pay'
            ELSE 'order' END            AS current_stage,
       TIMESTAMPDIFF(HOUR, d.order_time, d.pay_time)  AS pay_lag_hours,
       DATEDIFF(d.ship_time,   d.order_time)          AS ship_lag_days,
       DATEDIFF(d.finish_time, d.order_time)          AS finish_lag_days,
       d.last_event_time
FROM derived d;

-- 建完立刻核对（无需数据也能跑）：
-- DESC dwd.dwd_order_lifecycle;
-- SELECT current_stage, count(*) FROM dwd.v_order_lifecycle_expected GROUP BY current_stage ORDER BY 1;
