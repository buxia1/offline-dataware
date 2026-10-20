#!/usr/bin/env bash
# 每日冷备：DS 元库 + StarRocks FE 镜像 + 小状态 + 生成器输入 + git 指纹
# 用法: bash scripts/backup/backup_daily.sh [YYYY-MM-DD]
# 环境变量: ROOT(默认 /home/l/offline-dw) OUT RETAIN_DAYS(7)
#          MIRROR_DIR(/mnt/d/offline-dw-backups) MIRROR=1 时同步一份到 Windows 可见盘
set -euo pipefail
ROOT=${ROOT:-/home/l/offline-dw}; OUT=${OUT:-$ROOT/backups}
DAY=${1:-$(date +%F)}; DEST="$OUT/daily/$DAY"
RETAIN_DAYS=${RETAIN_DAYS:-7}; MIRROR_DIR=${MIRROR_DIR:-/mnt/d/offline-dw-backups}
FE_META="$ROOT/starrocks/fe/meta"; IMG_V2="$FE_META/image/v2"
log(){ printf '[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }
die(){ printf '[%s] FATAL: %s\n' "$(date +%H:%M:%S)" "$*" >&2; exit 1; }
[ -d "$ROOT" ] || die "ROOT 不存在: $ROOT"
mkdir -p "$DEST/tmp"; trap 'rm -rf "$DEST/tmp"' EXIT

# 1. DS 元库（绝不要加 --databases）
log "1/6 DS 元库 -> ds_dolphinscheduler.sql.gz"
docker exec mysql mysqldump -uroot -proot123 --single-transaction --quick \
  --routines --triggers --events --no-tablespaces dolphinscheduler \
  | gzip -9 > "$DEST/tmp/ds_dolphinscheduler.sql.gz"
gzip -t "$DEST/tmp/ds_dolphinscheduler.sql.gz" || die "ds dump gzip 校验失败"

# 2. FE 镜像：实体在 meta/image/v2/image.<journalId>，ALTER 是异步的（最晚 72s 落盘）
log "2/6 StarRocks FE image -> fe_image.tar.gz"
latest_img(){ find "$IMG_V2" -maxdepth 1 -name 'image.[0-9]*' -printf '%f\n' 2>/dev/null | sort -V | tail -1; }
before=$(latest_img || true)
docker exec starrocks mysql -P9030 -h 127.0.0.1 -uroot -e 'ALTER SYSTEM CREATE IMAGE;' >/dev/null
new=""
for _ in $(seq 1 180); do
  cur=$(latest_img || true)
  if [ -n "$cur" ] && [ "$cur" != "$before" ]; then new=$cur; break; fi
  sleep 1
done
[ -n "$new" ] || die "FE image 未生成（180s 内）；看 docker logs starrocks | grep checkpoint"
log "   新 image: v2/$new"
tar -czf "$DEST/tmp/fe_image.tar.gz" -C "$FE_META" image
tar -tzf "$DEST/tmp/fe_image.tar.gz" >/dev/null || die "fe_image tar 校验失败"
{
  printf 'image_version=%s\n' "$new"
  printf 'image_sha256=%s\n' "$(sha256sum "$IMG_V2/$new" | cut -d' ' -f1)"
  printf 'checksum_file=%s\n' "$(cat "$IMG_V2/checksum.${new#image.}" 2>/dev/null || echo N/A)"
  docker exec starrocks mysql -P9030 -h 127.0.0.1 -uroot -e "SHOW PROC '/frontends'\G" 2>/dev/null \
    | grep -E '^\s*(ClusterId|Role|ReplayedJournalId|Version):' || true
} > "$DEST/tmp/fe_image.txt"

# 3. 小状态：台账 + 告警脚本 + BE cluster_id（金丝雀判据）
log "3/6 台账 / be cluster_id / 告警脚本 -> state.tar.gz"
ST=("ds/logs/ops/ledger.csv" "ds/alerts"); files=()
for p in "${ST[@]}"; do [ -e "$ROOT/$p" ] && files+=("$p"); done
if [ ${#files[@]} -gt 0 ]; then tar -czf "$DEST/tmp/state.tar.gz" -C "$ROOT" "${files[@]}"; fi
docker exec starrocks cat /data/deploy/starrocks/be/storage/cluster_id \
  > "$DEST/tmp/be_cluster_id.txt" 2>/dev/null || log "   WARN: 取不到 be/storage/cluster_id"

# 4. 生成器输入
log "4/6 生成器输入 -> data.tar.gz"
tar -czf "$DEST/tmp/data.tar.gz" -C "$ROOT" data
tar -tzf "$DEST/tmp/data.tar.gz" >/dev/null || die "data tar 校验失败"

# 5. git 指纹
log "5/6 git 指纹 -> git.txt"
{ git -C "$ROOT" rev-parse HEAD 2>/dev/null || echo "no-git"; git -C "$ROOT" status --short 2>/dev/null || true; } > "$DEST/tmp/git.txt"

# 6. 归档 + manifest
log "6/6 归档 + manifest"
mv "$DEST/tmp"/* "$DEST"/; rmdir "$DEST/tmp"
( cd "$DEST" && find . -maxdepth 1 -type f ! -name manifest.sha256 -printf '%P\n' \
    | sort | xargs -r sha256sum > manifest.sha256 && sha256sum -c manifest.sha256 >/dev/null )
log "完成: $DEST ($(du -sh "$DEST" | cut -f1))"

if [ -d "$MIRROR_DIR" ] && [ -n "${MIRROR:-0}" ]; then
  mkdir -p "$MIRROR_DIR/daily"; rm -rf "$MIRROR_DIR/daily/$DAY"
  cp -a "$DEST" "$MIRROR_DIR/daily/"; log "已镜像到 $MIRROR_DIR/daily/$DAY"
fi
find "$OUT/daily" -mindepth 1 -maxdepth 1 -type d -mtime +"$RETAIN_DAYS" -exec rm -rf {} + 2>/dev/null || true
log "保留策略: 现有 $(find "$OUT/daily" -mindepth 1 -maxdepth 1 -type d | wc -l) 份 / >${RETAIN_DAYS} 天已清理"

if [ "$(id -u)" = "0" ]; then chown -R 1000:1000 "$OUT" 2>/dev/null || true; fi
