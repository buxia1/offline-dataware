-- =============================================================================
-- 湖层 ODS：Iceberg 建表（ods_order / ods_order_event）
--
--   sql/lake_ods.sql
--
-- 【这一步做什么】
--   在【湖】里建两张与仓内 ods.ods_order / ods.ods_order_event 对齐的 Iceberg 表。
--   只建表，**不灌数据**（灌数据见 scripts/lake_ods_replay.py）。
--
-- 【为什么"湖表要和仓表对齐"】
--   湖是"唯一真源"（single source of truth），仓退化为加速层。
--   对齐之后：① DQC 重算对账（湖算出 vs 仓算出）才能逐属性比
--            ② 将来"仓挂了从湖重建"是纯 SQL 直搬，不需要写转换
--   对齐的口径 = 【列名、列类型、语义】三项，不要求引擎/键模型一致。
--
-- -----------------------------------------------------------------------------
-- 【⚠️⚠️ 头号坑：时间字段必须写 TIMESTAMP_NTZ，不能写 TIMESTAMP】
--
--   实测（Spark 3.5.1 + iceberg-spark-runtime 1.9.0）：
--     spark.sql.timestampType = TIMESTAMP_LTZ      ← Spark 3.5 的默认值
--     DDL 写 TIMESTAMP     → Iceberg 落成 timestamptz（带时区）→ 【错】
--     DDL 写 TIMESTAMP_NTZ → Iceberg 落成 timestamp  （无时区）→ 【对】
--
--   为什么"写 TIMESTAMP 会错"是个隐蔽 bug：
--     · 数据是东八区【墙钟时间】字符串："2026-09-20 03:13:23"，本身不含时区信息
--     · 写成 timestamptz 后，这个值被理解成"某个时刻"，语义变了
--     · Spark 自己读【看不出问题】（它按 session 时区 Asia/Shanghai 渲染，
--       读回来还是 03:13:23）→ 只测 Spark 会完全漏掉这个 bug
--     · StarRocks 按 UTC 渲染 → 【偏 -8 小时】：
--         写入 2026-09-20 03:13:23 → SR 读到 2026-09-19 19:13:23
--         写入 2026-09-20 00:00:00 → SR 读到 2026-09-19 16:00:00
--     · 后果：dt、event_time、下游所有时间与分区全错，且是【静默】错
--
--   实测对照表（同一份数据，两种写法）：
--     ┌─────────────────┬──────────────────┬──────────────┬──────────────────────┐
--     │ DDL 写法        │ Iceberg 实际类型 │ Spark 读回   │ StarRocks 读         │
--     ├─────────────────┼──────────────────┼──────────────┼──────────────────────┤
--     │ TIMESTAMP       │ timestamptz  ❌  │ 正确         │ 偏 -8h            ❌ │
--     │ TIMESTAMP_NTZ   │ timestamp    ✅  │ 正确         │ 完全正确          ✅ │
--     └─────────────────┴──────────────────┴──────────────┴──────────────────────┘
--
--   【教训（比这个 bug 本身重要）】
--     验证湖表不能只让 Spark 自己读——必须【让 StarRocks 也读一遍并逐值比对】。
--     两个引擎的渲染口径不同，单侧验证会把时区错误放过去。
--
-- -----------------------------------------------------------------------------
-- 【StarRocks 读湖必须手动刷新】
--
--   SR 读 Iceberg 有元数据缓存，Spark 写入新数据后【不会自动感知】。
--   实测（StarRocks 3.5.0，增量追加两轮）：
--     写第 4 行 → 不刷新读到 3 ｜ REFRESH 后读到 4  ✅
--     写第 5 行 → 不刷新读到 4 ｜ REFRESH 后读到 5  ✅
--   → 灌数脚本（scripts/lake_ods_replay.py）末尾必须跟一条刷新：
--       REFRESH EXTERNAL TABLE lake.ods.ods_order;
--       REFRESH EXTERNAL TABLE lake.ods.ods_order_event;
--     否则查湖会以为"没灌进去"。
--
--   ⚠️ 本版本只支持表级刷新：REFRESH EXTERNAL CATALOG 会报语法错误
--      （Unexpected input 'CATALOG', the most similar input is {'TABLE'}）
--
--   ⚠️ 有数据之后改表结构用 ALTER TABLE ... ADD COLUMN，不要 DROP 重建。
-- -----------------------------------------------------------------------------
--
-- 【另外三个设计决定】
--
--   ① 事件表的 dt = to_date(event_time)，不是 to_date(order_time)
--      实测反例（order_id=20260920000）：
--        order_time = 2026-09-20 21:55:55   ← 订单级属性，所有事件重复携带
--        ship 事件 event_time = 2026-09-22 21:55:55  → dt 应为 09-22
--      用错就变成"所有事件都落在下单日"，分区语义全错、下游累积快照全错。
--      订单快照表 dt = to_date(order_time)（它的事件就是下单本身）。
--
--   ② order_id 允许 NULL（仓里是 NOT NULL）—— ⚠️ 唯一一处刻意不对齐
--      实测：当前 800 + 2379 行里 order_id 全非空（各 0 个 NULL）。
--      但仍声明可空，理由：
--        · 湖层的职责是【忠实留痕】。若上游某天发出 order_id=null 的消息，
--          声明 NOT NULL 会让整批写入直接失败（湖里连"证据"都没有）；
--          声明可空则照收，再由 DQC 报警 —— 符合"湖留脏数据、DQC 判脏"的分工。
--        · 仓侧 NOT NULL 是"约束"；湖侧允许是"留痕"。两者目的不同。
--      代价：DQC 必须有一条 order_id IS NULL 的检查，否则脏数据会静默流进 DWD。
--
--   ③ status / event_type 用 VARCHAR(n) 声明，但 Iceberg 实际类型是 string
--      实测：Iceberg 没有"带长度的字符串"类型，VARCHAR(1073741824) 是
--      StarRocks 读 string 时的显示值。无法与仓侧 VARCHAR(32)/VARCHAR(16) 对齐
--      —— 这一列的类型对齐是【做不到】的，属已知差异。
--      影响：无。DQC 对账比的是【值】，不是 varchar 长度。
--
--   [补充] 其他属性保持默认：
--     · format-version 不写 → 实测落成 2，StarRocks 3.5 支持
--       显式写 "format-version"="2" 也可，但写死反而挡住将来升 v3
--     · write.parquet.compression-codec 实测落成 zstd
--       仓侧 ods 表用 LZ4，但 Parquet 没有 LZ4 编解码器，无法对齐。
--       zstd 比 gzip 压得更小、速度也够，StarRocks 能读，保持默认即可。
--
-- 【执行方式】spark-sql -f（实测可用）
--   docker exec spark /opt/spark/bin/spark-sql \
--     --master 'local[1]' --driver-memory 1g \
--     --jars /opt/offline-dw/jars/iceberg-spark-runtime-3.5_2.12-1.9.0.jar \
--     --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
--     --conf spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog \
--     --conf spark.sql.catalog.lake.type=hadoop \
--     --conf spark.sql.catalog.lake.warehouse=file:///opt/offline-dw/lake/warehouse \
--     --conf spark.sql.session.timeZone=Asia/Shanghai \
--     -f /opt/offline-dw/sql/lake_ods.sql
--
-- 【⚠️ 路径必须用 file:///opt/offline-dw/lake/warehouse】
--   Iceberg 会把绝对路径写进元数据。spark 与 starrocks 两个容器【挂的是宿主同一目录、
--   且容器内路径同为 /opt/offline-dw/lake】—— 这是刻意的，否则 StarRocks 读不到：
--   曾用 /data/deploy/starrocks/lake 导致
--     ERROR 1064: File does not exist: .../snap-xxx.avro
--   （表能被发现，但一读就报文件不存在 —— 元数据里的绝对路径在 SR 侧不存在）
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 0) 命名空间：库 = ods（与仓侧 ods 库同名，便于对照）
-- -----------------------------------------------------------------------------
CREATE NAMESPACE IF NOT EXISTS lake.ods;


-- -----------------------------------------------------------------------------
-- 1) 订单快照：lake.ods.ods_order
--    Kafka 消息形态（实测原文）：
--      {"order_id":20260920538,"user_id":31,"product_id":13,"amount":199.5,
--       "order_time":"2026-09-20 01:19:30","status":"paid"}
--    JSON 里【没有 dt】→ 由 order_time 派生（与仓侧 Routine Load 的 dt = to_date(order_time) 一致）
--
--    ⚠️ 建表模式：DROP + CREATE（项目一贯做法，见 sql/ods_order.sql）
--       ⚠️⚠️ 但【湖表只能用一次】！建好之后不要再执行本文件（见文件头"硬约束"）。
--          有数据后改 schema 请走 ALTER TABLE ... ADD COLUMN。
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS lake.ods.ods_order;

CREATE TABLE lake.ods.ods_order (
    order_id     BIGINT        COMMENT "订单ID（湖侧允许 NULL，仓侧是 NOT NULL —— 见文件头②）",
    user_id      BIGINT        COMMENT "用户ID（脏数据：可能为 NULL，实测 37/800）",
    product_id   BIGINT        COMMENT "商品ID",
    amount       DECIMAL(10,2) COMMENT "金额（脏数据：可能为负，实测 19/800）",
    order_time   TIMESTAMP_NTZ COMMENT "⚠️ 下单时间（东八区墙钟时间）—— 必须是 NTZ，见文件头",
    status       VARCHAR(32)   COMMENT "状态：paid / refund / cancel（Iceberg 里落成 string）",
    dt           DATE          COMMENT "日期 = to_date(order_time)；Iceberg 隐藏分区"
)
USING iceberg
PARTITIONED BY (dt);


-- -----------------------------------------------------------------------------
-- 2) 订单事件流：lake.ods.ods_order_event
--    Kafka 消息形态（实测原文）：
--      {"order_id":20260920036,"user_id":106,"product_id":24,"amount":99.8,
--       "order_time":"2026-09-20 03:13:23","event_type":"order",
--       "event_time":"2026-09-20 03:13:23"}
--
--    ⚠️ 仓侧这张表是 DUPLICATE KEY（不做去重），湖侧同样是纯追加语义：
--       Iceberg 建表未加任何主键/唯一约束 → 重放两次就是两份数据（不会自动折叠）。
--       幂等靠【写入侧】保证（脚本按 dt 动态覆盖），不靠表模型。
--    ⚠️ dt 取 event_time（见文件头①）—— 本表最容易写错的一处。
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS lake.ods.ods_order_event;

CREATE TABLE lake.ods.ods_order_event (
    order_id     BIGINT        COMMENT "订单ID（湖侧允许 NULL，仓侧也是 NULL）",
    user_id      BIGINT        COMMENT "用户ID（脏数据：可能为 NULL，实测 133/2379）",
    product_id   BIGINT        COMMENT "商品ID",
    amount       DECIMAL(10,2) COMMENT "金额（脏数据：可能为负，实测 49/2379）",
    order_time   TIMESTAMP_NTZ COMMENT "⚠️ 下单时间（订单级属性，每个事件都重复带）",
    event_type   VARCHAR(16)   COMMENT "事件类型：order / pay / ship / finish / cancel（落成 string）",
    event_time   TIMESTAMP_NTZ COMMENT "⚠️ 事件发生时间（里程碑时间戳）",
    dt           DATE          COMMENT "事件发生日 = to_date(event_time)；Iceberg 隐藏分区"
)
USING iceberg
PARTITIONED BY (dt);


-- =============================================================================
-- 验证（建完表立刻核对 —— 全部为实测可用的命令）
-- =============================================================================

-- ① 两张表存在
-- SHOW TABLES IN lake.ods;
--   → 期望：ods_order、ods_order_event（无 probe_* 探针残留）

-- ② ★ 表结构必须与仓侧对齐（逐列比）
-- DESCRIBE TABLE EXTENDED lake.ods.ods_order;
-- DESCRIBE TABLE EXTENDED lake.ods.ods_order_event;
--   对照仓侧：
--   docker exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "DESC ods.ods_order; DESC ods.ods_order_event;"
--   期望列序一致：order_id, user_id, product_id, amount, order_time, [status|event_type, event_time], dt

-- ③ ★★ 最关键的一条：Iceberg 元数据里的真实类型必须是 timestamp（不是 timestamptz）
--   （Spark 的 DESCRIBE 会【掩盖】这个问题，必须直接读元数据 JSON）
--   python3 -c "
--   import json,glob
--   f=sorted(glob.glob('lake/warehouse/ods/ods_order/metadata/*.metadata.json'))[-1]
--   d=json.load(open(f))
--   print([(x['name'],x['type']) for x in d['schemas'][0]['fields']])"
--   → 期望：order_time / event_time 都是 'timestamp'
--            若出现 'timestamptz' → DDL 写成了 TIMESTAMP，立刻改回 TIMESTAMP_NTZ 重建

-- ④ ★ 分区规格必须是 ident(dt)，且 location 是 file:///opt/offline-dw/...
--   python3 -c "
--   import json,glob
--   f=sorted(glob.glob('lake/warehouse/ods/ods_order/metadata/*.metadata.json'))[-1]
--   d=json.load(open(f))
--   print(d['location'], [(x['name'],x['transform']) for x in d['partition-specs'][0]['fields']])"
--   → 期望：file:///opt/offline-dw/lake/warehouse/ods/ods_order  [('dt', 'identity')]

-- ⑤ ★ 反向验：StarRocks 侧能否发现并真读这两张表（仓→湖通道打通）
-- docker exec starrocks mysql -P9030 -h127.0.0.1 -uroot -B -e "
--   SHOW TABLES FROM lake.ods;
--   SELECT count(*) FROM lake.ods.ods_order;
--   SELECT count(*) FROM lake.ods.ods_order_event;"
--   → 刚建完表还没数据时应返回 NULL（不是报错）；灌完数据应返回 800 / 2379

-- ⑥ 灌数据后【必做】的跨引擎时间对账（防时区 bug 复发）
-- docker exec starrocks mysql -P9030 -h127.0.0.1 -uroot -B -e "
--   SELECT order_id, order_time, dt FROM lake.ods.ods_order ORDER BY order_id LIMIT 3;
--   SELECT order_id, order_time, dt FROM ods.ods_order      ORDER BY order_id LIMIT 3;"
--   → 两侧的 order_time / dt 必须【逐值相同】。差 8 小时就是又踩了 timestamptz 坑。


-- =============================================================================
-- 回滚
-- =============================================================================
-- DROP TABLE IF EXISTS lake.ods.ods_order;
-- DROP TABLE IF EXISTS lake.ods.ods_order_event;
-- DROP NAMESPACE IF EXISTS lake.ods;      -- 仅当库为空
-- rm -rf lake/warehouse/ods                -- 元数据没清干净时的兜底（湖目录是空的才可删）
