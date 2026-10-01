-- 补 dwd_order_sku_detail 的历史分区
-- 原因：dynamic_partition.start 只负责"保留"，不负责"创建"历史分区（PITFALLS #3.2）
-- 注意：动态分区表不允许手工 ADD PARTITION，必须先关掉（PITFALLS #3.3）

ALTER TABLE dwd.dwd_order_sku_detail SET ('dynamic_partition.enable' = 'false');

ALTER TABLE dwd.dwd_order_sku_detail ADD PARTITION p20260920 VALUES [('2026-09-20'), ('2026-09-21'));
ALTER TABLE dwd.dwd_order_sku_detail ADD PARTITION p20260921 VALUES [('2026-09-21'), ('2026-09-22'));
ALTER TABLE dwd.dwd_order_sku_detail ADD PARTITION p20260926 VALUES [('2026-09-26'), ('2026-09-27'));
ALTER TABLE dwd.dwd_order_sku_detail ADD PARTITION p20260927 VALUES [('2026-09-27'), ('2026-09-28'));

ALTER TABLE dwd.dwd_order_sku_detail SET ('dynamic_partition.enable' = 'true');
