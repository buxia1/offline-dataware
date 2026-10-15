-- ⚠️ 已作废（2026-10-03）：不要再用这个文件。
--     补 dwd_order_sku_detail 的分区已由 scripts/dwd_sku_load.sh 自动完成
--     （对比"有数据的天" vs "现有分区"，缺的自动 ADD PARTITION，并用 trap 恢复开关）。
--     保留本文件仅为记录历史做法。
--
-- 补 dwd_order_sku_detail 的历史分区
-- 原因：dynamic_partition.start 只负责"保留"，不负责"创建"历史分区（PITFALLS #3.2）
-- 注意：动态分区表不允许手工 ADD PARTITION，必须先关掉（PITFALLS #3.3）
--      ⚠️ 关掉之后必须自己记得开回来 —— 这个开关没有任何东西会自动恢复（PITFALLS §3.3）

ALTER TABLE dwd.dwd_order_sku_detail SET ('dynamic_partition.enable' = 'false');

ALTER TABLE dwd.dwd_order_sku_detail ADD PARTITION p20260920 VALUES [('2026-09-20'), ('2026-09-21'));
ALTER TABLE dwd.dwd_order_sku_detail ADD PARTITION p20260921 VALUES [('2026-09-21'), ('2026-09-22'));
ALTER TABLE dwd.dwd_order_sku_detail ADD PARTITION p20260926 VALUES [('2026-09-26'), ('2026-09-27'));
ALTER TABLE dwd.dwd_order_sku_detail ADD PARTITION p20260927 VALUES [('2026-09-27'), ('2026-09-28'));

ALTER TABLE dwd.dwd_order_sku_detail SET ('dynamic_partition.enable' = 'true');
