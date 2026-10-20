#!/usr/bin/env bash
# 恢复 DS 元库（S4）。覆盖生产库 dolphinscheduler，先自动另存当前库。
# 用法: bash scripts/backup/restore_ds.sh <ds_dolphinscheduler.sql.gz> --yes
set -euo pipefail
ROOT=${ROOT:-/home/l/offline-dw}; COMPOSE="$ROOT/docker-compose.yml"
DUMP=${1:?用法: restore_ds.sh <ds_dolphinscheduler.sql.gz> --yes}
[ "${2:-}" = "--yes" ] || { echo "拒绝执行：会用 $DUMP 覆盖生产元库。确认请加 --yes"; exit 1; }
[ -f "$DUMP" ] || { echo "FATAL: 文件不存在 $DUMP" >&2; exit 1; }
gzip -t "$DUMP" || { echo "FATAL: dump 已损坏" >&2; exit 1; }

echo "[1/6] 另存当前元库"
SAFE=/tmp/ds_before_restore_$(date +%Y%m%d_%H%M%S).sql.gz
docker exec mysql mysqldump -uroot -proot123 --single-transaction --quick \
  --routines --triggers --events --no-tablespaces dolphinscheduler | gzip -9 > "$SAFE"
echo "      $SAFE ($(du -h "$SAFE" | cut -f1))"

echo "[2/6] 停 dolphinscheduler"
docker compose -f "$COMPOSE" stop dolphinscheduler >/dev/null

echo "[3/6] 重建空库 + 导入"
docker exec mysql mysql -uroot -proot123 -e \
  "DROP DATABASE IF EXISTS dolphinscheduler; CREATE DATABASE dolphinscheduler DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;"
zcat "$DUMP" | docker exec -i mysql mysql -uroot -proot123 dolphinscheduler

echo "[4/6] 内容核对"
tables=$(docker exec mysql mysql -uroot -proot123 -N -e \
  "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='dolphinscheduler';")
defs=$(docker exec mysql mysql -uroot -proot123 -N -e \
  "SELECT COUNT(*) FROM dolphinscheduler.t_ds_process_definition;")
sched=$(docker exec mysql mysql -uroot -proot123 -N -e \
  "SELECT COUNT(*) FROM dolphinscheduler.t_ds_schedules WHERE release_state=1;")
echo "      tables=$tables (期望 65)  process_defs=$defs  online_schedules=$sched (期望 1)"
[ "$tables" = "65" ] || { echo "FATAL: 表数不对，保持停机，先排查" >&2; exit 1; }

echo "[5/6] 起 dolphinscheduler"
docker compose -f "$COMPOSE" start dolphinscheduler >/dev/null
for i in $(seq 1 120); do
  curl -fsS -m 2 http://localhost:12345/dolphinscheduler/actuator/health >/dev/null 2>&1 && { echo "      DS 已就绪（${i}s）"; break; }
  [ "$i" = "120" ] && { echo "FATAL: DS 120s 未就绪" >&2; exit 1; }
  sleep 1
done

echo "[6/6] 核对关键对象"
docker exec mysql mysql -uroot -proot123 dolphinscheduler -e \
  "SELECT id,name,version,release_state FROM t_ds_process_definition WHERE release_state=1;
   SELECT id,release_state,crontab,warning_type,warning_group_id FROM t_ds_schedules WHERE id=7;"
echo "完成。★ 恢复后定时可能是 OFFLINE（DS 导出/导入的固有行为），去 UI 确认并手工上线一次；"
echo "然后手工触发一次 offline_dataware_lake 验证 11/11。"
