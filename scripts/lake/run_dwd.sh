#!/usr/bin/env bash
# =============================================================================
# 湖侧 DWD 层构建（lake.ods.* -> lake.dwd.*）
#
# ★ 方案 C（2026-10-15）：复用常驻容器 spark，不再起临时容器
#   - 去掉 docker run / docker cp：仓库 scripts/ sql/ lake/ 都已挂进 spark 容器
#   - 好处：不再依赖「宿主路径 vs 容器路径」，不再每次拉镜像/起容器，DS 里直接 docker exec
#   - 代价：内存用 spark 容器的 mem_limit（硬上限），所以下面加了串行锁
#
#   路径推导（自包含，仓库 cp 到任意位置都能跑）：
#     HERE = 本脚本所在目录（scripts/lake）
#     PROJ = 仓库根（HERE/../..）
#
# 用法：
#   bash scripts/lake/run_dwd.sh dry   all
#   bash scripts/lake/run_dwd.sh build all
#   bash scripts/lake/run_dwd.sh build dwd_order_detail 2026-09-20 2026-09-20   # 单日（幂等）
#   bash scripts/lake/run_dwd.sh build dwd_order_lifecycle
#   bash scripts/lake/run_dwd.sh ddl                                            # 只建表
#
#   ★ 按天增量（给 DS 每天跑的就是这条）：
#   bash scripts/lake/run_dwd.sh incremental all                # 区间自己算（水位 = 目标表 MAX(dt)）
#   bash scripts/lake/run_dwd.sh incremental all --lookback 3   # 回捞最近 3 天（迟到事件）
#   bash scripts/lake/run_dwd.sh incremental dwd_order_detail 2026-12-01 2026-12-18  # 手工修区间
#
# 环境变量：
#   SPARK_CT    常驻 Spark 容器名（默认 spark）
#   LOCK_DIR    串行锁目录（默认 $PROJ/scripts/.spark_job.lock，宿主与 DS 共享）
#   DRIVER_MEM  driver 堆内存（默认 1g；整表 rebuild 若 OOM 就调大，同时抬 compose mem_limit）
#
# 幂等由 lake_dwd_build.py 保证：写前按本次范围 DELETE、写后回读自检（不一致 exit 2）。
# 增量模式额外做「按天闭合检查」（DWD 每天行数 vs ODS 现算值；默认只 WARN）。
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

MODE_ARG="${1:-build}"
TABLE="${2:-all}"
D1="${3:-}"
D2="${4:-}"

# --lookback N：只对 incremental 有意义；可写在第 3 位，也可写在末尾
LOOKBACK=""
if [ "$D1" = "--lookback" ]; then
  LOOKBACK="${D2:-}"
  D1=""; D2=""
fi
if [ "${5:-}" = "--lookback" ]; then
  LOOKBACK="${6:-}"
fi
if [ -n "$LOOKBACK" ] && ! [[ "$LOOKBACK" =~ ^[0-9]+$ ]]; then
  echo "--lookback 需要非负整数，收到：$LOOKBACK" >&2; exit 64
fi

case "$MODE_ARG" in
  dry)         BUILD_MODE="--mode dry" ;;
  build)       BUILD_MODE="--mode append" ;;    # append 前会按本次范围 DELETE（幂等）
  rebuild)     BUILD_MODE="--mode rebuild" ;;   # 清理逻辑与 build 相同，仅语义命名
  incremental) BUILD_MODE="--mode incremental" ;;
  ddl)         BUILD_MODE="" ;;
  *) echo "未知模式：$MODE_ARG（可用 dry|build|rebuild|incremental|ddl）" >&2; exit 64 ;;
esac

RANGE=""
[ -n "$D1" ] && RANGE="--start $D1 --end ${D2:-$D1}"
[ -n "$LOOKBACK" ] && RANGE="$RANGE --lookback $LOOKBACK"

echo "########## 湖侧 DWD：table=$TABLE mode=$MODE_ARG ${RANGE:-（全量）} ##########"
echo "  HERE=$HERE"
echo "  PROJ=$PROJ"
echo "  SPARK_CT=$SPARK_CT"

spark_acquire_lock

if [ "$MODE_ARG" = "ddl" ]; then
  echo "########## 建表：sql/lake/lake_dwd.sql（DROP + CREATE，会清空三张表） ##########"
  docker exec "$SPARK_CT" "${SPARK_SQL[@]}" -f /opt/offline-dw/sql/lake/lake_dwd.sql 2>&1 \
    | grep -vE "$FILTER" | grep -E "Time taken|Error" || true
else
  echo "########## 构建 DWD（内置幂等清理 + 写入后自检） ##########"
  docker exec "$SPARK_CT" "${SPARK_SUBMIT[@]}" \
    /opt/offline-dw/scripts/lake/lake_dwd_build.py --table "$TABLE" $BUILD_MODE $RANGE 2>&1 \
    | grep -vE "$FILTER" || { echo "!!! $TABLE FAILED"; exit 1; }
fi

echo ""
echo "########## 湖侧 DWD 行数 + 水位（水位 = 增量下次从这里往后算） ##########"
docker exec "$SPARK_CT" "${SPARK_SQL[@]}" \
  -e "SELECT 'dwd_order_detail' t, count(*) n, cast(max(dt) as string) watermark FROM lake.dwd.dwd_order_detail
      UNION ALL SELECT 'dwd_order_sku_detail', count(*), cast(max(dt) as string) FROM lake.dwd.dwd_order_sku_detail
      UNION ALL SELECT 'dwd_order_lifecycle', count(*),
        (SELECT cast(max(dt) as string) FROM lake.ods.ods_order_event) FROM lake.dwd.dwd_order_lifecycle;" \
  2>&1 | grep -vE "$FILTER" || true

echo "########## DONE ##########"
