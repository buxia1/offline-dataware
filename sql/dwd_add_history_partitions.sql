-- 先关掉动态分区，否则不允许手工加分区
ALTER TABLE dwd.dwd_order_detail SET ("dynamic_partition.enable" = "false");

-- 补齐历史分区
ALTER TABLE dwd.dwd_order_detail ADD PARTITION p20260915 VALUES [('2026-09-15'), ('2026-09-16'));
ALTER TABLE dwd.dwd_order_detail ADD PARTITION p20260916 VALUES [('2026-09-16'), ('2026-09-17'));
ALTER TABLE dwd.dwd_order_detail ADD PARTITION p20260917 VALUES [('2026-09-17'), ('2026-09-18'));
ALTER TABLE dwd.dwd_order_detail ADD PARTITION p20260918 VALUES [('2026-09-18'), ('2026-09-19'));
ALTER TABLE dwd.dwd_order_detail ADD PARTITION p20260919 VALUES [('2026-09-19'), ('2026-09-20'));
ALTER TABLE dwd.dwd_order_detail ADD PARTITION p20260920 VALUES [('2026-09-20'), ('2026-09-21'));
ALTER TABLE dwd.dwd_order_detail ADD PARTITION p20260921 VALUES [('2026-09-21'), ('2026-09-22'));
ALTER TABLE dwd.dwd_order_detail ADD PARTITION p20260922 VALUES [('2026-09-22'), ('2026-09-23'));
ALTER TABLE dwd.dwd_order_detail ADD PARTITION p20260923 VALUES [('2026-09-23'), ('2026-09-24'));
ALTER TABLE dwd.dwd_order_detail ADD PARTITION p20260924 VALUES [('2026-09-24'), ('2026-09-25'));
ALTER TABLE dwd.dwd_order_detail ADD PARTITION p20260925 VALUES [('2026-09-25'), ('2026-09-26'));
ALTER TABLE dwd.dwd_order_detail ADD PARTITION p20260926 VALUES [('2026-09-26'), ('2026-09-27'));
