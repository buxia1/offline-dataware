-- ============================================================
-- dqc_lake_ods.sql —— 湖侧 ODS（Kafka→湖 唯一入口）的 6 项质量闸门
-- 占位符 ${DT} / ${THR_ORDER} / ${THR_EVENT} 由 scripts/dqc_lake_ods.sh 替换
-- 每条检查输出 4 列：check_id / check_name / ok(1|0) / detail
--
-- ⚠️ 两个 STARROCKS 实测坑（2026-10-19 验证）：
--   1. 外部表按分区条件过滤后若「一个分区都没匹配上」，标量子查询会返回 NULL
--      而不是 0 —— 所以每个 count 都必须包 IFNULL(...,0)。
--      这恰好是最危险的场景（整天数据没来），不包就会把 NULL 当正常值漏过去。
--   2. ratio 这类除法，分母为 0 时得 NULL —— 用 NULLIF 兜到 99，让它必然判红。
-- ============================================================
-- C1 业务日非空：今天这一天到底有没有数据进来（防「静默空跑」）
SELECT 'C1' AS check_id, '业务日非空' AS check_name,
       ((IFNULL((SELECT count(*) FROM lake.ods.ods_order       WHERE dt = '${DT}'), 0) > 0)
    AND (IFNULL((SELECT count(*) FROM lake.ods.ods_order_event WHERE dt = '${DT}'), 0) > 0)) AS ok,
       concat('orders=', IFNULL((SELECT count(*) FROM lake.ods.ods_order       WHERE dt = '${DT}'), 0),
              ' events=', IFNULL((SELECT count(*) FROM lake.ods.ods_order_event WHERE dt = '${DT}'), 0)) AS detail
UNION ALL
-- C2 量级下限：订单 / 事件数不得低于 0.7 × 前 7 天日均（断崖式下跌要抓）
SELECT 'C2', '量级下限（0.7×前7天日均）',
       ((IFNULL((SELECT count(*) FROM lake.ods.ods_order       WHERE dt = '${DT}'), 0) >= ${THR_ORDER})
    AND (IFNULL((SELECT count(*) FROM lake.ods.ods_order_event WHERE dt = '${DT}'), 0) >= ${THR_EVENT})),
       concat('thr_order=', ${THR_ORDER}, ' thr_event=', ${THR_EVENT},
              ' orders=', IFNULL((SELECT count(*) FROM lake.ods.ods_order       WHERE dt = '${DT}'), 0),
              ' events=', IFNULL((SELECT count(*) FROM lake.ods.ods_order_event WHERE dt = '${DT}'), 0))
UNION ALL
-- C3 订单主键唯一：一天内 order_id 不得重复
SELECT 'C3', '订单主键唯一',
       (IFNULL((SELECT count(*) - count(DISTINCT order_id) FROM lake.ods.ods_order WHERE dt = '${DT}'), 0) = 0),
       concat('dup=', IFNULL((SELECT count(*) - count(DISTINCT order_id) FROM lake.ods.ods_order WHERE dt = '${DT}'), 0))
UNION ALL
-- C4 事件类型枚举合法：新出现的事件类型要么是生成器升级，要么是脏数据
SELECT 'C4', '事件类型枚举合法',
       (IFNULL((SELECT count(*) FROM lake.ods.ods_order_event
                 WHERE dt = '${DT}'
                   AND event_type NOT IN ('order','pay','ship','finish','cancel')), 0) = 0),
       concat('bad=', IFNULL((SELECT count(*) FROM lake.ods.ods_order_event
                               WHERE dt = '${DT}'
                                 AND event_type NOT IN ('order','pay','ship','finish','cancel')), 0))
UNION ALL
-- C5 事件重复率：允许重复（原样落地）但必须可控
--    基线：重放日 1.0000（重放自身去重）/ 历史回填日 1.0525；阈值 1.10；缺数据时 99 必红
SELECT 'C5', '事件重复率≤1.10',
       (IFNULL((SELECT round(1.0 * count(*) / NULLIF(count(DISTINCT concat(order_id, '-', event_type)), 0), 4)
                  FROM lake.ods.ods_order_event WHERE dt = '${DT}'), 99) <= 1.10),
       concat('ratio=', IFNULL((SELECT round(1.0 * count(*) / NULLIF(count(DISTINCT concat(order_id, '-', event_type)), 0), 4)
                                  FROM lake.ods.ods_order_event WHERE dt = '${DT}'), 99))
UNION ALL
-- C6 近 7 天分区连续：任何一天整段缺失（不只是今天）都要抓
SELECT 'C6', '近7天分区连续',
       (IFNULL((SELECT count(DISTINCT dt) FROM lake.ods.ods_order
                 WHERE dt > date_sub('${DT}', 7) AND dt <= '${DT}'), 0) = 7),
       concat('days=', IFNULL((SELECT count(DISTINCT dt) FROM lake.ods.ods_order
                                WHERE dt > date_sub('${DT}', 7) AND dt <= '${DT}'), 0), '/7');
