-- =============================================================================
-- 湖侧商品快照建表：lake.ods.ods_product
--
-- 用途：承载 data/dim/product_snapshot_YYYYMMDD.csv（由 scripts/lake/lake_ods_product_load.py 导入）
--       供湖侧维度链 scripts/lake/lake_dim_build.py 使用
--             lake.ods.ods_product -> lake.dim.dim_product_scd2 -> lake.dim.dim_product
--
-- 用法：bash scripts/lake/run_ods_product.sh ddl
--
-- ⚠️ 这里用 CREATE TABLE IF NOT EXISTS，**不是** DROP + CREATE：
--    本表在环境里已经存在且有 4,500 行数据（2026-09-20 ~ 2026-12-18）。
--    DROP 会把它们全删掉、且只能靠重灌 90 个 CSV 恢复。要重建请显式手工执行 DROP。
--
-- ⚠️ snapshot_date 不在 CSV 里 —— 由文件名解析（product_snapshot_20260920.csv → 2026-09-20），
--    与仓侧 sql/ods/ods_product.sql 的列序逐列对齐；Iceberg 里它是隐藏分区。
-- =============================================================================

CREATE NAMESPACE IF NOT EXISTS lake.ods;

CREATE TABLE IF NOT EXISTS lake.ods.ods_product (
    product_id    BIGINT        COMMENT '商品ID',
    product_name  VARCHAR(64)   COMMENT '商品名',
    category      VARCHAR(32)   COMMENT '品类',
    brand         VARCHAR(32)   COMMENT '品牌',
    price         DECIMAL(10,2) COMMENT '价格',
    status        VARCHAR(32)   COMMENT '状态：on_sale 等',
    update_time   TIMESTAMP_NTZ COMMENT '快照生成时刻（CSV 自带）',
    snapshot_date DATE          COMMENT '快照日 = 从文件名解析；Iceberg 隐藏分区'
)
USING iceberg
PARTITIONED BY (snapshot_date);

-- 验证：
--   docker exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "DESC lake.ods.ods_product;"
--   → 期望 8 列，snapshot_date 为分区键
