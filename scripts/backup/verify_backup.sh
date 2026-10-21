#!/usr/bin/env bash
# 备份完整性校验：manifest + gzip + tar 结构 + FE image CRC32 自证；--deep 做 DS dump 内容校验
# 用法: bash scripts/backup/verify_backup.sh <backup_dir> [--deep]
# 能证明：文件没被截断/篡改、压缩流完好、tar 可读、DS dump 能导进另一个库、FE image 自身 crc 对得上
# 不能证明：备份与生产"同一时刻一致"（那是 backup_daily.sh 的职责）
set -euo pipefail
DIR=${1:?用法: verify_backup.sh <backup_dir> [--deep]}
DEEP=0; [ "${2:-}" = "--deep" ] && DEEP=1
ok=0; bad=0
say(){ printf '%-6s %s\n' "$1" "$2"; }
fail(){ say FAIL "$1"; bad=$((bad+1)); }
[ -f "$DIR/manifest.sha256" ] || { echo "FATAL: 没有 manifest.sha256，这份备份不可信" >&2; exit 1; }

echo "== 1. sha256 清单 =="
( cd "$DIR" && sha256sum -c manifest.sha256 --quiet ) || { echo "FATAL: manifest 校验失败" >&2; exit 1; }
say PASS manifest

echo "== 2. tar / gzip 结构 =="
shopt -s nullglob
for f in "$DIR"/*.tar.gz; do
  if tar -tzf "$f" >/dev/null 2>&1; then
    say PASS "$(basename "$f") ($(tar -tzf "$f" | wc -l) 条目)"; ok=$((ok+1))
  else fail "$(basename "$f")"; fi
done
for f in "$DIR"/*.gz; do
  case "$f" in *.tar.gz) continue ;; esac
  if gzip -t "$f" 2>/dev/null; then say PASS "$(basename "$f")"; ok=$((ok+1)); else fail "$(basename "$f")"; fi
done
for f in "$DIR"/*.csv "$DIR"/*.txt "$DIR"/*.bundle; do
  [ -s "$f" ] && { say PASS "$(basename "$f") ($(du -h "$f" | cut -f1))"; ok=$((ok+1)); } || fail "$(basename "$f")"
done

echo "== 3. 语义抽检 =="
if [ -f "$DIR/fe_image.tar.gz" ]; then
  T=$(mktemp -d); trap 'rm -rf "$T"' EXIT
  tar -xzf "$DIR/fe_image.tar.gz" -C "$T"
  img=$(find "$T" -name 'image.[0-9]*' -type f | head -1)
  cs=$(find "$T" -name 'checksum.*' -type f | head -1)
  if [ -n "$img" ] && [ -n "$cs" ]; then
    want=$(tr -d '[:space:]' < "$cs")
    if command -v python3 >/dev/null 2>&1; then
      real=$(python3 -c "import zlib,sys;print(zlib.crc32(open(sys.argv[1],'rb').read())&0xffffffff)" "$img")
      [ "$real" = "$want" ] && { say PASS "fe_image crc32=$real"; ok=$((ok+1)); } \
        || fail "fe_image crc32 不符：算出 $real，文件写 $want"
    else
      # 没有 python3（如 DS standalone 容器）→ 用同一份备份里 fe_image.txt 记录的 sha256 交叉验证
      rec=$(sed -n 's/^image_sha256=//p' "$DIR/fe_image.txt" 2>/dev/null | head -1)
      act=$(sha256sum "$img" | cut -d' ' -f1)
      if [ -n "$rec" ] && [ "$rec" = "$act" ]; then say PASS "fe_image sha256=$act"; ok=$((ok+1));
      else fail "fe_image sha256 与 fe_image.txt 不符"; fi
    fi
  else fail "fe_image 里找不到 image.<id>/checksum.<id>"; fi
  rm -rf "$T"; trap - EXIT
fi
if [ -f "$DIR/lake_warehouse.tar.gz" ]; then
  vhl=$(tar -tzf "$DIR/lake_warehouse.tar.gz" | grep -c '/metadata/version-hint\.text$' || true)
  [ "$vhl" = "10" ] && { say PASS "lake 10 张表 version-hint=10"; ok=$((ok+1)); } \
    || fail "lake version-hint=$vhl（期望 10，可能有表缺失）"
fi

if [ "$DEEP" = "1" ]; then
  DUMP="$DIR/ds_dolphinscheduler.sql.gz"
  [ -f "$DUMP" ] || { echo "FATAL: --deep 需要 ds_dolphinscheduler.sql.gz" >&2; exit 1; }
  echo "== 4. DS dump 内容校验（scratch 库 ds_restore_probe，不碰生产） =="
  docker exec mysql mysql -uroot -proot123 -e 'DROP DATABASE IF EXISTS ds_restore_probe; CREATE DATABASE ds_restore_probe;'
  zcat "$DUMP" | docker exec -i mysql mysql -uroot -proot123 ds_restore_probe   # 必须 -i
  q(){ docker exec mysql mysql -uroot -proot123 -N -e "$1" 2>/dev/null; }
  tables=$(q "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='ds_restore_probe';")
  defs=$(q "SELECT COUNT(*) FROM ds_restore_probe.t_ds_process_definition;")
  sched=$(q "SELECT COUNT(*) FROM ds_restore_probe.t_ds_schedules;")
  online=$(q "SELECT COUNT(*) FROM ds_restore_probe.t_ds_schedules WHERE release_state=1;")
  cron=$(q "SELECT crontab FROM ds_restore_probe.t_ds_schedules WHERE release_state=1;" | tr '\n' ' ')
  echo "tables=$tables (期望 65)  process_defs=$defs (期望>=5)  schedules=$sched (期望>=5)  上线=$online (期望>=1)  crontab=[$cron]"
  [ "$tables" = "65" ] || fail "DS 表数 $tables != 65"
  [ "${defs:-0}" -ge 5 ] || fail "流程定义数 $defs < 5"
  [ "${sched:-0}" -ge 5 ] || fail "定时数 $sched < 5"
  [ "${online:-0}" -ge 1 ] || fail "上线定时数 $online < 1"
  case "$cron" in
    *"0 0 2 * * ? *"*) ;;
    *) fail "主链路 02:00 定时不在上线列表里: $cron" ;;
  esac
  docker exec mysql mysql -uroot -proot123 -e 'DROP DATABASE ds_restore_probe;'
fi
echo "== 结果: PASS=$ok FAIL=$bad =="
[ "$bad" = "0" ]
