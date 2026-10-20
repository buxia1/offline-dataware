#!/usr/bin/env bash
# 恢复 StarRocks FE 元数据镜像（S5）。会停 starrocks；原 image/bdb 改名保留（不删除）。
# 用法: bash scripts/backup/restore_fe.sh <fe_image.tar.gz> --yes
# ★ 关键：必须先移走 meta/bdb —— 否则 FE 启动会 replay 比 image 更新的 journal，回滚看起来"没生效"
set -euo pipefail
ROOT=${ROOT:-/home/l/offline-dw}; COMPOSE="$ROOT/docker-compose.yml"
TAR=${1:?用法: restore_fe.sh <fe_image.tar.gz> --yes}
[ "${2:-}" = "--yes" ] || { echo "拒绝执行：会替换 FE 元数据并重启 starrocks。确认请加 --yes"; exit 1; }
[ -f "$TAR" ] || { echo "FATAL: 文件不存在 $TAR" >&2; exit 1; }
tar -tzf "$TAR" >/dev/null || { echo "FATAL: tar 已损坏" >&2; exit 1; }
FE_META="$ROOT/starrocks/fe/meta"; IMG="$FE_META/image"; TS=$(date +%Y%m%d_%H%M%S)
FIRST=$(tar -tzf "$TAR" | head -1)

echo "[1/6] 停 starrocks"
docker compose -f "$COMPOSE" stop starrocks >/dev/null

echo "[2/6] 另存当前 image / bdb"
mkdir -p "$FE_META/image.bak.$TS"; cp -a "$IMG/." "$FE_META/image.bak.$TS/" 2>/dev/null || echo "      （当前 image 为空，跳过）"
if [ -d "$FE_META/bdb" ]; then mv "$FE_META/bdb" "$FE_META/bdb.bak.$TS"; echo "      bdb -> bdb.bak.$TS"; fi

echo "[3/6] 解包新的 image"
mkdir -p "$IMG"; rm -rf "$IMG"/*
case "$FIRST" in
  image/|image/*) tar -xzf "$TAR" -C "$FE_META" ;;
  *)              tar -xzf "$TAR" -C "$IMG" ;;
esac

echo "[4/6] 起 starrocks 并等 9030"
docker compose -f "$COMPOSE" start starrocks >/dev/null
for i in $(seq 1 180); do
  docker exec starrocks mysql -P9030 -h 127.0.0.1 -uroot -e 'SELECT 1;' >/dev/null 2>&1 && { echo "      FE 已就绪（${i}s）"; break; }
  [ "$i" = "180" ] && { echo "FATAL: 180s 未就绪；看 docker logs starrocks，别急着删 image.bak.$TS" >&2; exit 1; }
  sleep 1
done

echo "[5/6] 金丝雀"
docker exec starrocks sh -lc '
  echo "clusterId: $(grep -i clusterId /data/deploy/starrocks/fe/meta/image/VERSION 2>/dev/null || echo NA)"
  echo "image 文件数: $(ls -1 /data/deploy/starrocks/fe/meta/image/v2 | wc -l)"
  echo "bdb 文件数:   $(ls -1 /data/deploy/starrocks/fe/meta/bdb 2>/dev/null | wc -l)"
  echo "be cluster_id: $(cat /data/deploy/starrocks/be/storage/cluster_id 2>/dev/null || echo NA)"'
docker exec starrocks mysql -P9030 -h 127.0.0.1 -uroot -e "SHOW PROC '/frontends'\G" 2>/dev/null \
  | grep -E '^\s*(ClusterId|Role|ReplayedJournalId|Version):' || true

echo "[6/6] 重建 external catalog + 抽检"
bash "$ROOT/scripts/lake/sr_catalog_reset.sh"
docker exec starrocks mysql -P9030 -h 127.0.0.1 -uroot -e "
  SHOW CATALOGS;
  SELECT 'lake.ods.ods_order' t, COUNT(*) c FROM lake.ods.ods_order
  UNION ALL SELECT 'lake.dwd.dwd_order_detail', COUNT(*) FROM lake.dwd.dwd_order_detail
  UNION ALL SELECT 'ads.ads_daily_sales', COUNT(*) FROM ads.ads_daily_sales;"
echo "完成。基线见 docs/backup-restore-and-backfill.md §8。"
echo "回滚：停 starrocks -> rm -rf $IMG/* -> cp -a $FE_META/image.bak.$TS/. $IMG/ -> mv $FE_META/bdb.bak.$TS $FE_META/bdb -> 起 starrocks"
