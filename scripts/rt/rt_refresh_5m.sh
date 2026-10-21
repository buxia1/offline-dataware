#!/usr/bin/env bash
# =============================================================================
# A0 实时化：每 5 分钟把「今天」的 dt 分区幂等覆盖一次（零新增内存/容器）
#   · 复用 lake/lake_ods_replay.sh（按 dt 覆盖写，天然幂等）
#   · 必须自己拿串行锁：否则会和 02:00 批链路抢 spark 容器 2g 上限
#   · 压到 DRIVER_MEM=700m / local[2]（5 分钟一次，1g+local[4] 会顶满容器）
# =============================================================================
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
PROJ="$(cd "$HERE/../.." && pwd)"
SPARK_CT="${SPARK_CT:-spark}"
LOCK_DIR="${LOCK_DIR:-/tmp/.spark_job.lock}"      # ★ 容器内路径
source "$PROJ/scripts/lake/_spark_lib.sh"

spark_ensure_running
LOCK_WAIT_SECONDS="${LOCK_WAIT_SECONDS:-0}"   # 5 分钟一轮：忙就跳过，绝不和 02:00 抢
if ! spark_acquire_lock; then
  echo "另一 Spark 作业占锁，本次跳过"; exit 0
fi
DATE="$(TZ=Asia/Shanghai date +%F)"
DRIVER_MEM="${DRIVER_MEM:-700m}" SPARK_MASTER="${SPARK_MASTER:-local[2]}" \
  bash "$PROJ/scripts/lake/lake_ods_replay.sh" --date "$DATE"
