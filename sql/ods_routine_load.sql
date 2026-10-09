-- =============================================================================
-- ODS 层：改用 StarRocks Routine Load 摄入（替代 Spark 批作业）
--
--   sql/ods_routine_load.sql
--
-- 【这一步换掉的是什么】
--   换掉的是 ODS 的【摄入方式】，不是把清洗/join 搬进 StarRocks。
--   ODS 这一步本来就没有清洗（只 JSON 解析 + 类型转换 + 派生 dt），
--   DWD 三层（dwd_overwrite / dwd_sku_load / dwd_order_lifecycle_load）
--   本来就在 StarRocks 里，【一行都不用改】。
--
-- 【为什么换】
--   ① 实测：旧路径空跑也要 13.7 秒（读到 0 行照样花）—— 全是 Spark 作业固定开销
--   ② 位点表 + Python 摄入脚本 + Shell 外壳（含 4 道防线）全部可退役
--   ③ Routine Load 支持 Exactly-Once，位点由引擎维护且与数据同事务提交
--      —— 手工位点表本质是在重新实现这个能力（10-05 事故就是手工逻辑的漏洞）
--
-- 【⚠️ 三个必须记住的写法（实测/官方文档核实）】
--   ① kafka_offsets 是【逗号分隔】，且与 kafka_partitions【按顺序一一对应】
--   ② 绝不能写 OFFSET_BEGINNING：事件表是 DUPLICATE KEY，
--      重灌会把 2379 行变成 4758 行（不会自动折叠！）
--   ③ max_filter_ratio 默认是 1（=不生效），必须显式设 "0"
--      → 否则坏数据被【静默过滤】；设 0 则一条坏数据就暂停作业
--
-- 【⚠️ 执行前提】
--   先采集当前真实 offset（会随时间前进，别照抄下面注释里的值）：
--     for t in ods_order ods_order_event; do
--       docker exec kafka bash -c "/opt/kafka/bin/kafka-get-offsets.sh \
--         --bootstrap-server localhost:9092 --topic \$t"
--     done
--   2026-10-08 13:40 实测值：
--     ods_order_event : 0:783  1:825  2:771
--     ods_order       : 0:606  1:591  2:603
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1) 事件流：ods.ods_order_event
--    消息形态（实测原文）：
--      {"order_id":20260920036,"user_id":106,"product_id":24,"amount":99.8,
--       "order_time":"2026-09-20 03:13:23","event_type":"order",
--       "event_time":"2026-09-20 03:13:23"}
--    JSON 里【没有 dt】→ 由 event_time 派生（与旧 Spark 脚本一致）
--    时间字段是【字符串】不是 epoch → 可直接 CAST 成 DATETIME
-- -----------------------------------------------------------------------------
CREATE ROUTINE LOAD ods.ods_order_event_load ON ods_order_event
COLUMNS(
    -- ⚠️ 先声明全部映射列，再声明派生列（顺序不能反）
    order_id,
    user_id,
    product_id,
    amount,
    order_time,
    event_type,
    event_time,
    dt = to_date(event_time)
)
PROPERTIES(
    "format"                    = "json",
    "jsonpaths"                 = "[\"$.order_id\",\"$.user_id\",\"$.product_id\",\"$.amount\",\"$.order_time\",\"$.event_type\",\"$.event_time\"]",
    -- 一条消息一个 JSON 对象（不是数组）→ 不要设 strip_outer_array
    "max_filter_ratio"          = "0",
    "log_rejected_record_num"   = "-1",
    "desired_concurrent_number" = "3",
    "max_batch_interval"        = "20",
    -- ⚠️ 实测：不显式指定时，JobProperties 里的 timezone 是 "Etc/UTC"，
    --    不是文档写的默认 Asia/Shanghai。数据本身就是东八区墙钟时间，
    --    显式指定与项目口径一致，避免将来用时间函数时算错。
    "timezone"                  = "Asia/Shanghai"
)
FROM KAFKA(
    "kafka_broker_list" = "kafka:29092",
    "kafka_topic"       = "ods_order_event",
    "kafka_partitions"  = "0,1,2",
    -- ⚠️ 与上面的分区【按顺序】对应；改成你采集到的最新值
    "kafka_offsets"     = "783,825,771"
);


-- -----------------------------------------------------------------------------
-- 2) 订单快照：ods.ods_order
--    消息形态：
--      {"order_id":20260920538,"user_id":31,"product_id":13,"amount":199.5,
--       "order_time":"2026-09-20 01:19:30","status":"paid"}
--
--    ⚠️ 这条链路原来【全量重读】：topic 里 1800 条消息，表里只有 800 行
--       —— 历史上重复读了 2.25 倍，靠 PRIMARY KEY(order_id) 折叠才没显形。
--       改 Routine Load 后，位点由引擎维护，"靠模型兜底"就不需要了。
-- -----------------------------------------------------------------------------
CREATE ROUTINE LOAD ods.ods_order_load ON ods_order
COLUMNS(
    order_id,
    user_id,
    product_id,
    amount,
    order_time,
    status,
    dt = to_date(order_time)
)
PROPERTIES(
    "format"           = "json",
    "jsonpaths"        = "[\"$.order_id\",\"$.user_id\",\"$.product_id\",\"$.amount\",\"$.order_time\",\"$.status\"]",
    "max_filter_ratio" = "0"
)
FROM KAFKA(
    "kafka_broker_list" = "kafka:29092",
    "kafka_topic"       = "ods_order",
    "kafka_partitions"  = "0,1,2",
    "kafka_offsets"     = "606,591,603"
);


-- =============================================================================
-- 验证（执行后逐条核对）
-- =============================================================================

-- ① 作业状态：应为 RUNNING
-- SHOW ROUTINE LOAD FROM ods\G
-- 关注字段：State / Progress / MaxOffset / ErrorLogUrls / ReasonOfStateChanged

-- ② 表行数必须【不变】：ods_order=800、ods_order_event=2379、lifecycle=900
-- SELECT (SELECT count(*) FROM ods.ods_order)          AS ods_order,
--        (SELECT count(*) FROM ods.ods_order_event)    AS ods_event,
--        (SELECT count(*) FROM dwd.dwd_order_lifecycle) AS lifecycle;

-- ③ 事件流三重一致：必须 2379 / 2379 / 2379
-- SELECT (SELECT count(*) FROM ods.ods_order_event) rows_,
--        (SELECT count(DISTINCT concat(order_id,'-',event_type)) FROM ods.ods_order_event) pairs,
--        (SELECT sum(next_offset) FROM ods.ods_kafka_offset) offset_sum;

-- ④ 累积快照阶段分布：cancel=80 finish=180 order=111 pay=279 ship=250
-- SELECT current_stage, count(*) FROM dwd.dwd_order_lifecycle GROUP BY current_stage ORDER BY 2 DESC;

-- ⑤ 两条 DQC 都应 EXIT=0
-- bash scripts/dqc_order_chain.sh;  echo "订单 EXIT=$?"
-- bash scripts/dqc_dim_product.sh;  echo "商品 EXIT=$?"

-- ⑥ 坏数据行为测试（期望 PAUSED，而不是静默丢弃）：
--   往 topic 发一条非法 JSON，比如 "abcd"，然后看 State 是否变成 PAUSED
--   echo 'abcd' | docker exec -i kafka bash -c \
--     '/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic ods_order_event'
--   测完记得 RESUME：
--   RESUME ROUTINE LOAD FOR ods.ods_order_event_load;


-- =============================================================================
-- 运维命令
-- =============================================================================
-- SHOW ROUTINE LOAD FROM ods\G                                  -- 看状态/进度/错误
-- SHOW ROUTINE LOAD TASK FROM ods WHERE JobName='ods_order_event_load';  -- 看任务
-- PAUSE  ROUTINE LOAD FOR ods.ods_order_event_load;
-- RESUME ROUTINE LOAD FOR ods.ods_order_event_load;
-- STOP   ROUTINE LOAD FOR ods.ods_order_event_load;             -- 停止（不可恢复）
-- ALTER  ROUTINE LOAD FOR ods.ods_order_event_load
--   PROPERTIES("max_batch_interval"="30");


-- =============================================================================
-- 回滚（⚠️ 旧脚本先别删，那是唯一退路）
-- =============================================================================
-- STOP ROUTINE LOAD FOR ods.ods_order_event_load;
-- STOP ROUTINE LOAD FOR ods.ods_order_load;
-- 若事件表被污染（行数翻倍）：
--   TRUNCATE TABLE ods.ods_order_event;
--   TRUNCATE TABLE ods.ods_kafka_offset;
--   bash scripts/ods_order_event_ingest.sh --reset     -- 用旧 Spark 路径重建
