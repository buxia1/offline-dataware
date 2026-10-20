#!/usr/bin/env bash
# 恢复 Iceberg warehouse（S6）。会停 spark + starrocks；原 lake 目录改名保留（不删）。
# 用法: bash scripts/backup/restore_lake.sh <lake_warehouse.tar.gz> --yes
set -euo pipefail
ROOT=${ROOT:-/home/l/offline-dw}; COMPOSE="$ROOT/docker-compose.yml"
TAR=${1:?用法: restore_lake.sh <lake_warehouse.tar.gz> --yes}
[ "${2:-}" = "--yes" ] || { echo "拒绝执行：会替换 lake/ 并重启 spark+starrocks。确认请加 --yes"; exit 1; }
[ -f "$TAR" ] || { echo "FATAL: 文件不存在 $TAR" >&2; exit 1; }
tar -tzf "$TAR" >/dev/null || { echo "FATAL: tar 已损坏" >&2; exit 1; }
tar -tzf "$TAR" | head -1 | grep -q '^lake/' || { echo "FATAL: 这个 tar 不是 lake/ 根打包的" >&2; exit 1; }
SAFE="$ROOT/lake.bak.$(date +%Y%m%d_%H%M%S)"

echo "[1/6] 停 spark + starrocks（湖的读写方）"
docker compose -f "$COMPOSE" stop spark starrocks >/dev/null
echo "[2/6] 原目录改名保留 -> $SAFE"; mv "$ROOT/lake" "$SAFE"
echo "[3/6] 解包"; tar -xzf "$TAR" -C "$ROOT"
[ -d "$ROOT/lake/warehouse" ] || { echo "FATAL: 解包后 lake/warehouse 不存在" >&2; exit 1; }
echo "[4/6] 起 starrocks + spark"
docker compose -f "$COMPOSE" start starrocks spark >/dev/null
for i in $(seq 1 180); do
  docker exec starrocks mysql -P9030 -h 127.0.0.1 -uroot -e 'SELECT 1;' >/dev/null 2>&1 && { echo "      FE 已就绪（${i}s）"; break; }
  [ "$i" = "180" ] && { echo "FATAL: FE 180s 未就绪" >&2; exit 1; }
  sleep 1
done
echo "[5/6] 重建 external catalog"; bash "$ROOT/scripts/lake/sr_catalog_reset.sh"
echo "[6/6] 10 张湖表行数（与 §8 基线对照）"
docker exec starrocks mysql -P9030 -h 127.0.0.1 -uroot -e "
  SELECT 'ods.ods_order' t, COUNT(*) c FROM lake.ods.ods_order
  UNION ALL SELECT 'ods.ods_order_event', COUNT(*) FROM lake.ods.ods_order_event
  UNION ALL SELECT 'ods.ods_product', COUNT(*) FROM lake.ods.ods_product
  UNION ALL SELECT 'dim.dim_product', COUNT(*) FROM lake.dim.dim_product
  UNION ALL SELECT 'dim.dim_product_scd2', COUNT(*) FROM lake.dim.dim_product_scd2
  UNION ALL SELECT 'dwd.dwd_order_detail', COUNT(*) FROM lake.dwd.dwd_order_detail
  UNION ALL SELECT 'dwd.dwd_order_sku_detail', COUNT(*) FROM lake.dwd.dwd_order_sku_detail
  UNION ALL SELECT 'dwd.dwd_order_lifecycle', COUNT(*) FROM lake.dwd.dwd_order_lifecycle
  UNION ALL SELECT 'dws.dws_user_order_day', COUNT(*) FROM lake.dws.dws_user_order_day
  UNION ALL SELECT 'ads.ads_daily_sales', COUNT(*) FROM lake.ads.ads_daily_sales;"
echo "完成。验收：bash scripts/recon_lake_wh.sh（退出码 2 = 不一致）"
echo "回滚：停 spark+starrocks -> mv lake lake.restored.fail -> mv $SAFE lake -> 起 -> sr_catalog_reset.sh"
