-- DIM 层：商品维度表装载（普通维度表，只留每个商品的最新状态）
--
-- 【为什么不需要任何日期过滤】
--   DIM 要的是"每个商品现在长什么样"。
--   "最新"是跨所有 snapshot_date 比出来的，不是某一天。
--   一旦加了 WHERE snapshot_date = 'xxx'，就只剩一天数据，没法比较了。
--
-- 【为什么用 INSERT INTO 而不是 INSERT OVERWRITE】
--   dim_product 是 PRIMARY KEY 表，同键自动覆盖，天然幂等。
--   DWD 用 OVERWRITE 是因为它是 DUPLICATE KEY + 分区表，没有主键覆盖能力。
--
-- 【为什么 ORDER BY 用 snapshot_date 而不是 update_time】
--   snapshot_date = 数仓"看到"这条数据的日期（批次时间）
--   update_time   = 上游"声称"改动的时间（业务时间，可能延迟/不准）
--   数仓的快照边界必须以批次为准。
--
-- 【不写 WHERE rn = 1 之外的条件】
--   PARTITION BY product_id + ORDER BY 保证每个商品只有一行 rn=1。

INSERT INTO dim.dim_product
    (product_id, product_name, category, brand, price, status, update_time)
SELECT
    product_id,
    product_name,
    category,
    brand,
    price,
    status,
    update_time
FROM (
    SELECT
        product_id,
        product_name,
        category,
        brand,
        price,
        status,
        update_time,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY snapshot_date DESC        -- 最新的快照排第一
        ) AS rn
    FROM ods.ods_product
    WHERE product_id IS NOT NULL
) t
WHERE rn = 1;
