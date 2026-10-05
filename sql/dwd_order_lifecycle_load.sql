-- =============================================================================
-- 累积快照装载（增量回填）
--
-- 占位符 ${FROM_DT}：只回填「在 ${FROM_DT} 及之后发生过事件」的那些订单。
--     正常按天跑 : ${FROM_DT} = 业务日期   → 只碰今天有变化的订单（真增量）
--     补数       : ${FROM_DT} = 要补那天   → 那天及之后受影响的订单一起刷新（自愈）
--     全量重建   : ${FROM_DT} = 1970-01-01 （外壳脚本先 TRUNCATE）
--
-- ⚠️ 这个占位符必须由 scripts/dwd_order_lifecycle_load.sh 替换后再执行。
--    直接 docker exec ... mysql < 本文件 是【静默 no-op】（PITFALLS §7.2）：
--    CAST('${FROM_DT}' AS DATE) = NULL，而 dt >= NULL 恒为 NULL（不是 TRUE）
--    → 0 行命中、不报错、退出码 0。已实测复现。
--
-- 【为什么用 INSERT 而不是 UPDATE —— 这一点和最初的设计说明不同，看仔细】
--   表是 PRIMARY KEY(order_id) 模型，而 StarRocks 主键模型下 **INSERT 就是 UPSERT**
--   （按主键定位：有则覆盖、无则插入）。所以「事务级增量」的正确表达是
--     「只 INSERT 有变化的那些订单」
--   而不是
--     「UPDATE 已存在的行 + INSERT 新的行」
--   好处：
--     ① 一条语句。不用先判断哪些订单已存在（省掉 LEFT JOIN ... IS NULL / NOT IN 那套）
--     ② 天然幂等：同一批重跑 → 同样的值覆盖同样的行
--     ③ 避开 PITFALLS §3.14（UPDATE 不接受表别名；SET 里得写 7 个关联子查询）
--     ④ 教学点没丢：主键模型的写路径同样是「按主键定位再改」，代价模型一致
--          （见 docs/dimension-modeling.md §10.7）
--
-- 【为什么是 dt >= 而不是 dt =】
--   正常按天跑时两者等价；但补数某天时 >= 会把「那天及之后」受影响的订单一并刷新，
--   于是「漏跑一天」不会留下永久缺口。真正的兜底是外壳脚本的防线③（全表对账）。
--
-- 【为什么 CTE 只有一个 scope_orders】
--   推导逻辑已经全部放进视图 dwd.v_order_lifecycle_expected（单一真相源）。
--   另外注意：CTE 只作用于紧随其后的那一条语句（PITFALLS §3.15），
--   所以整段回填必须是【一条 INSERT ... WITH ... SELECT】。
-- =============================================================================

INSERT INTO dwd.dwd_order_lifecycle
    (order_id, user_id, product_id, amount,
     order_time, pay_time, ship_time, finish_time, cancel_time,
     current_stage, pay_lag_hours, ship_lag_days, finish_lag_days,
     last_event_time, update_time)
WITH scope_orders AS (
    SELECT DISTINCT order_id
    FROM ods.ods_order_event
    WHERE dt >= '${FROM_DT}'
)
SELECT v.order_id,
       v.user_id,
       v.product_id,
       v.amount,
       v.order_time,
       v.pay_time,
       v.ship_time,
       v.finish_time,
       v.cancel_time,
       v.current_stage,
       v.pay_lag_hours,
       v.ship_lag_days,
       v.finish_lag_days,
       v.last_event_time,
       NOW() AS update_time
FROM dwd.v_order_lifecycle_expected v
JOIN scope_orders s ON s.order_id = v.order_id;
