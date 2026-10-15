-- 订单明细 + 商品属性（当时口径）
--
-- 【为什么用 SCD2 而不是普通维度表】
--   订单 dt=2026-09-20 时商品5是"数码"，9-21 起是"家电"。
--   用 dim_product（只有当前状态）会把 9-20 的订单算成"家电" → 口径错。
--   用 dim_product_scd2 + dt BETWEEN valid_from AND valid_to → 拿到"当天"的品类。
--   ⚠️ 09-26 的快照与 09-21 完全相同（gen_mock_products.py 的品类用
--      today.toordinal() 算，两天相差 5 天 → 索引偏移 5 % 5 = 0 → 品类不变）
--      → 不产生新版本，所以商品5只有 2 个版本，不是 3 个。
--      **版本数 = 真正发生变更的次数 + 1，不等于快照天数。**
--
-- 【为什么这里必须 JOIN】
--   DWD 的职责就是"把事实和维度关联好存下来"，
--   这样下游查询不用每次付范围 JOIN 的代价（实测 1分41秒 → 毫秒）。
--
-- 【为什么用 INSERT OVERWRITE】
--   和 dwd_overwrite.sh 同理：DUPLICATE KEY + 分区表，
--   INSERT OVERWRITE 原子覆盖整个分区，重跑不翻倍。
--
-- 【执行方式】
--   分区名是标识符，拼不出来 → 必须用 Shell + 文本替换（PITFALLS #2.1）
--   所以这个 SQL 由 shell 脚本逐天调用，每次替换 ${DF} 和 ${D}

INSERT OVERWRITE dwd.dwd_order_sku_detail PARTITION (p${D})
SELECT
    o.order_id,
    o.dt,
    o.user_id,
    o.product_id,
    o.amount,
    o.order_time,
    o.status,
    s.category,
    s.brand,
    s.price AS sku_price,
    s.valid_from,
    s.valid_to
FROM dwd.dwd_order_detail o
JOIN dim.dim_product_scd2 s
  ON  o.product_id = s.product_id
 AND  o.dt BETWEEN s.valid_from AND s.valid_to
WHERE o.dt = '${DF}';
