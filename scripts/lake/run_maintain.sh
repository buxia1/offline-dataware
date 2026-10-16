#!/usr/bin/env bash
# =============================================================================
# 湖表维护：清 Iceberg 历史快照（每表保留最近 N 个）+ 自验
#
# ★ 方案 C 家族：与 run_dim.sh / run_dwd.sh 同一套约定（常驻容器 spark + 串行锁）
#   路径推导：HERE = 本脚本所在目录（scripts/lake）；PROJ = 仓库根（HERE/../..）
#
#   与 lake_maintain.py 的分工：py 负责「盘点 + expire + 自验」，本脚本负责
#   容器存活检查、串行锁、日志过滤、把 py 的退出码原样传出去（DS 靠它判红绿）。
#
# 用法：
#   bash scripts/lake/run_maintain.sh                       # 只读盘点，零写入（约 15s）
#   bash scripts/lake/run_maintain.sh --expire              # 真清理，默认每表保留 5 个快照
#   bash scripts/lake/run_maintain.sh --expire --keep 3     # 换保留个数
#   bash scripts/lake/run_maintain.sh --tables dwd_order_detail,ods_product --expire
#   （不加 --expire 时不会删任何东西）
#
# 环境变量：
#   SPARK_CT    常驻 Spark 容器名（默认 spark）
#   LOCK_DIR    串行锁目录（默认 /tmp/.spark_job.lock，容器内路径）
#   DRIVER_MEM  driver 堆内存（默认 1g）
#
# 退出码（DS 节点就是靠它变红/变绿）：
#   0  成功（含只读盘点「无快照可清」）
#   1  验收不通过（行数变了，或清理后快照数仍 > keep+1）
#   2  用法/表名错误
#   69 spark 容器不在运行    70 锁目录不可写
# =============================================================================
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJ="$(cd "$HERE/../.." && pwd)"
SPARK_CT="${SPARK_CT:-spark}"
LOCK_DIR="${LOCK_DIR:-/tmp/.spark_job.lock}"   # 容器内路径（DS 里 ./scripts 是只读挂载）
FILTER='^[0-9]{2}/|WARN |SLF4J|^Using Spark|Setting default|To adjust|Spark Web UI|Spark master|HiveConf|ObjectStore'

# shellcheck source=_spark_lib.sh
source "$HERE/_spark_lib.sh"
spark_ensure_running

echo "########## 湖表维护：$* ##########"
echo "  PROJ=$PROJ"
echo "  SPARK_CT=$SPARK_CT"

spark_acquire_lock

# py 的退出码要原样传出去，所以这里临时关掉 -e / pipefail
set +e
docker exec "$SPARK_CT" "${SPARK_SUBMIT[@]}" \
  /opt/offline-dw/scripts/lake/lake_maintain.py "$@" 2>&1 \
  | grep -vE "$FILTER"
rc=${PIPESTATUS[0]}
set -e

if [ "$rc" -ne 0 ]; then
  echo "!!! 湖表维护 FAILED（退出码 $rc）" >&2
  exit "$rc"
fi

echo "########## DONE ##########"
