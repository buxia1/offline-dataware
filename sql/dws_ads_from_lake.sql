-- =============================================================================
-- 仓侧 DWS / ADS 两层物化（StarRocks 侧聚合，从湖侧 DWD 取数）
--
--   SQL 文件 : sql/dws_ads_from_lake.sql
--   宿主 runner: scripts/run_dws_ads_from_lake.sh
--
-- -----------------------------------------------------------------------------
-- 【分层契约】
--   湖侧负责 ods -> dim -> dwd（湖侧到 DWD 为止）；
--   仓侧 StarRocks 只负责 dws / ads 两层；
--   仓侧必须从湖侧 lake.dwd.* 取数【物化】，而不是直连湖侧做即席查询；
--   聚合发生在 StarRocks 侧 —— INSERT ... SELECT ... FROM lake.dwd.dwd_order_detail。
--
-- 【依赖顺序（不可调换）】
--   1) 先按区间清掉 dws / ads 的旧值；
--   2) dws 从 lake.dwd.dwd_order_detail 聚合（用户 x 天）；
--   3) ads 从【仓侧 dws.dws_user_order_day】聚合（天）——
--      ads 必须读 DWS 而不是再读一次湖，否则「DWS -> ADS」的口径链断掉。
--
-- 【占位符】
--   '${D1}' / '${D2}' 是 YYYY-MM-DD 字符串字面量，由 runner 用 sed 替换后执行。
--   本文件【不依赖】DolphinScheduler 的 ${system.biz.date}（那是 YYYYMMDD +
--   STR_TO_DATE('${system.biz.date}', '%Y%m%d') 的写法），这是与旧调度脚本的关键差异。
--
-- 【为什么 DELETE + INSERT，而不是 INSERT OVERWRITE】
--   dws.dws_user_order_day 与 ads.ads_daily_sales 都是 PRIMARY KEY 模型、都不带分区，
--   INSERT OVERWRITE 无法只重写一个日期区间；只能先区间 DELETE、再按相同键重写。
--   两次运行同区间是幂等的（键相同则覆盖）。
--
-- 【为什么 DELETE 必须写 >= AND <= 而不是 BETWEEN】
--   StarRocks 的 DELETE 对谓词形态有限制，实测用 BETWEEN 直接报错：
--     Where clause only supports compound predicate, binary predicate,
--     is_null predicate and in predicate
--   BETWEEN 不在支持列表里，必须展开成  dt >= '...' AND dt <= '...' 。
--
-- 【口径来源（逐字照搬，只允许两处差异）】
--   sql/dws_ads/dws_user_order_day_insert.sql   -> DWS 与 ADS 的聚合表达式
--   sql/dws_ads/ads_daily_sales_insert.sql
--   允许的差异仅：来源表由仓侧 dwd.dwd_order_detail 换成 lake.dwd.dwd_order_detail；
--                 过滤由「= 某一天」换成「区间 >= AND <=」。
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1. 清理区间旧值（先删后插，保证重跑幂等）
--    注意：dt 是 DATE，与 'YYYY-MM-DD' 字符串比较由 StarRocks 隐式转换。
-- ---------------------------------------------------------------------------
DELETE FROM dws.dws_user_order_day WHERE dt >= '${D1}' AND dt <= '${D2}';
DELETE FROM ads.ads_daily_sales    WHERE dt >= '${D1}' AND dt <= '${D2}';

-- ---------------------------------------------------------------------------
-- 2. DWS：用户 x 天 汇总（来源 = 湖侧 lake.dwd.dwd_order_detail）
--      order_cnt     = COUNT(*)                                      -- 订单总数
--      paid_cnt      = COUNT(CASE WHEN status = 'paid' THEN 1 END)   -- 已支付订单数
--      paid_amount   = SUM(CASE WHEN status = 'paid'   THEN amount ELSE 0 END)
--      refund_amount = SUM(CASE WHEN status = 'refund' THEN amount ELSE 0 END)
--    粒度 GROUP BY user_id, dt —— 与 dws.dws_user_order_day 的 PRIMARY KEY 对齐。
--    金额分支都带 ELSE 0：避免 SUM 忽略 NULL 与「条件不命中」在语义上出现歧义。
-- ---------------------------------------------------------------------------
INSERT INTO dws.dws_user_order_day
SELECT
    user_id,
    dt,
    COUNT(*) AS order_cnt,
    COUNT(CASE WHEN status = 'paid' THEN 1 END) AS paid_cnt,
    SUM(CASE WHEN status = 'paid' THEN amount ELSE 0 END) AS paid_amount,
    SUM(CASE WHEN status = 'refund' THEN amount ELSE 0 END) AS refund_amount
FROM lake.dwd.dwd_order_detail
WHERE dt >= '${D1}' AND dt <= '${D2}'
GROUP BY user_id, dt;

-- ---------------------------------------------------------------------------
-- 3. ADS：每日大盘 + 派生指标（来源 = 仓侧 dws.dws_user_order_day，必须在第 2 步之后）
--    原子指标（可加）：order_cnt / paid_cnt / paid_amount / refund_amount
--    派生指标（不可加，必须先各自 SUM 再相除）：
--      net_amount       = SUM(paid_amount) - SUM(refund_amount)
--      avg_order_amount = ROUND(SUM(paid_amount) / NULLIF(SUM(paid_cnt), 0), 2)
--      pay_rate         = ROUND(SUM(paid_cnt)    / NULLIF(SUM(order_cnt), 0), 4)
--    NULLIF(x, 0) 是除零保护：分母为 0 -> 结果为 NULL，而不是让作业报错中断。
-- ---------------------------------------------------------------------------
INSERT INTO ads.ads_daily_sales
SELECT
    dt,
    SUM(order_cnt)                                          AS order_cnt,
    SUM(paid_cnt)                                           AS paid_cnt,
    SUM(paid_amount)                                        AS paid_amount,
    SUM(refund_amount)                                      AS refund_amount,
    SUM(paid_amount) - SUM(refund_amount)                   AS net_amount,
    ROUND(SUM(paid_amount) / NULLIF(SUM(paid_cnt), 0), 2)   AS avg_order_amount,
    ROUND(SUM(paid_cnt) / NULLIF(SUM(order_cnt), 0), 4)     AS pay_rate
FROM dws.dws_user_order_day
WHERE dt >= '${D1}' AND dt <= '${D2}'
GROUP BY dt;
