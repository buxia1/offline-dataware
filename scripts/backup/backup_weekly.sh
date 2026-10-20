#!/usr/bin/env bash
# 每周冷备：Iceberg warehouse + 离线二进制/仓库包 + 可选 Kafka 原始消息
# 用法: bash scripts/backup/backup_weekly.sh [YYYY-MM-DD] [--with-kafka]
# 约定：周日 03:30 之后跑（02:00 批量与 lake_maintain expire 已结束）
set -euo pipefail
ROOT=${ROOT:-/home/l/offline-dw}; OUT=${OUT:-$ROOT/backups}
DAY=${1:-$(date +%F)}; DEST="$OUT/weekly/$DAY"
RETAIN_DAYS=${RETAIN_DAYS:-28}; MIRROR_DIR=${MIRROR_DIR:-/mnt/d/offline-dw-backups}
COMPOSE="$ROOT/docker-compose.yml"; WITH_KAFKA=0
for a in "$@"; do case "$a" in --with-kafka) WITH_KAFKA=1 ;; esac; done
log(){ printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }
die(){ printf '[%s] FATAL: %s\n' "$(date +%H:%M:%S)" "$*" >&2; exit 1; }
[ -d "$ROOT/lake/warehouse" ] || die "lake/warehouse 不存在"
mkdir -p "$DEST/tmp"; trap 'rm -rf "$DEST/tmp"' EXIT

# 1. 湖数据本体（122.5 MiB / 5918 条目 / 10 张表）
log "1/5 Iceberg warehouse -> lake_warehouse.tar.gz"
tar -czf "$DEST/tmp/lake_warehouse.tar.gz" -C "$ROOT" lake
tar -tzf "$DEST/tmp/lake_warehouse.tar.gz" >/dev/null || die "lake tar 校验失败"
entries=$(tar -tzf "$DEST/tmp/lake_warehouse.tar.gz" | wc -l)
vhl=$(tar -tzf "$DEST/tmp/lake_warehouse.tar.gz" | grep -c '/metadata/version-hint\.text$' || true)
log "   $(du -sh "$DEST/tmp/lake_warehouse.tar.gz" | cut -f1) / ${entries} 条目 / version-hint ${vhl} 个（应 10）"

# 2. Kafka 元数据 + offset 基线（不停 Kafka；--describe 不显示 offset，必须 get-offsets）
log "2/5 Kafka topics / offsets -> kafka_topics.txt"
{
  docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
  echo '--- describe ---'
  docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe
  echo '--- get-offsets ---'
  docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092
} > "$DEST/tmp/kafka_topics.txt" 2>&1 || log "   WARN: Kafka 元数据查询失败（容器未起？）"

# 3. 离线二进制 + 仓库包（全是 gitignored，恢复必需）
log "3/5 配置/jar/离线二进制 -> conf_jars.tar.gz"
tar -czf "$DEST/tmp/conf_jars.tar.gz" -C "$ROOT" \
  starrocks/fe/conf spark/jars docker-compose.yml ds/libs ds/bin/docker 2>/dev/null \
  || tar -czf "$DEST/tmp/conf_jars.tar.gz" -C "$ROOT" starrocks/fe/conf spark/jars docker-compose.yml
tar -tzf "$DEST/tmp/conf_jars.tar.gz" >/dev/null || die "conf_jars tar 校验失败"
log "   git bundle（--all，可离线 clone）"
git -C "$ROOT" bundle create "$DEST/tmp/repo.bundle" --all 2>/dev/null || \
  { git -C "$ROOT" bundle create "$DEST/tmp/repo.bundle" HEAD 2>/dev/null || log "   WARN: bundle 失败"; }

# 4. Kafka 数据（可选；热备 tar 恒 rc=1，所以必须先停）
if [ "$WITH_KAFKA" = "1" ]; then
  log "4/5 Kafka 数据 -> kafka_logs.tar.gz（先停 Kafka）"
  docker stop kafka >/dev/null   # 不用 docker compose：DS 容器里只有 docker CLI
  trap 'docker start kafka >/dev/null 2>&1 || true; rm -rf "$DEST/tmp"' EXIT
  tar -czf "$DEST/tmp/kafka_logs.tar.gz" -C "$ROOT" kafka/logs || die "kafka tar 失败"
  tar -tzf "$DEST/tmp/kafka_logs.tar.gz" >/dev/null || die "kafka tar 无法读出"
  docker start kafka >/dev/null
  trap 'rm -rf "$DEST/tmp"' EXIT
  log "   $(du -sh "$DEST/tmp/kafka_logs.tar.gz" | cut -f1)"
else
  log "4/5 跳过 Kafka 数据（需要时加 --with-kafka，会停 Kafka ~30s）"
fi

# 5. 归档
log "5/5 归档 + manifest"
mv "$DEST/tmp"/* "$DEST"/; rmdir "$DEST/tmp"
( cd "$DEST" && find . -maxdepth 1 -type f ! -name manifest.sha256 -printf '%P\n' \
    | sort | xargs -r sha256sum > manifest.sha256 && sha256sum -c manifest.sha256 >/dev/null )
log "完成: $DEST ($(du -sh "$DEST" | cut -f1))"
if [ -d "$MIRROR_DIR" ] && [ -n "${MIRROR:-0}" ]; then
  mkdir -p "$MIRROR_DIR/weekly"; rm -rf "$MIRROR_DIR/weekly/$DAY"
  cp -a "$DEST" "$MIRROR_DIR/weekly/"; log "已镜像到 $MIRROR_DIR/weekly/$DAY（9p 丢 owner/mode，仅副本）"
fi
find "$OUT/weekly" -mindepth 1 -maxdepth 1 -type d -mtime +"$RETAIN_DAYS" -exec rm -rf {} + 2>/dev/null || true

if [ "$(id -u)" = "0" ]; then chown -R 1000:1000 "$OUT" 2>/dev/null || true; fi
