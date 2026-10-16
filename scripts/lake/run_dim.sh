#!/usr/bin/env bash
# =============================================================================
# 湖侧维度层构建：lake.ods.ods_product -> lake.dim.dim_product_scd2 -> lake.dim.dim_product
#
# ★ 方案 C（2026-10-15）：自包含 + 复用常驻容器 spark
#   - 去掉了 SCRATCH：原来要把 SQL / py 从 Windows（/mnt/d/...）挂进临时容器，
#     现在直接用仓库挂载里的 sql/lake/lake_dim.sql 和 scripts/lake/lake_dim_build.py
#   - 不再起临时容器，也不再 docker cp
#
#   路径推导：HERE = 本脚本所在目录（scripts/lake）；PROJ = 仓库根（HERE/../..）
#
# 用法：
#   bash scripts/lake/run_dim.sh dry     # 只算不写
#   bash scripts/lake/run_dim.sh build   # 灌数（内部先 DELETE 再写，幂等）
#   bash scripts/lake/run_dim.sh ddl     # 只建表
#
# 环境变量：
#   SPARK_CT    常驻 Spark 容器名（默认 spark）
#   LOCK_DIR    串行锁目录（默认 $PROJ/scripts/.spark_job.lock，宿主与 DS 共享）
#   DRIVER_MEM  driver 堆内存（默认 1g）
#
# ⚠️ 前置数据：lake.ods.ods_product 必须已有数据。仓库里目前【没有】它的建表 SQL，
#    也没有任何把 data/dim/*.csv 灌进湖侧的脚本（现状是 4500 行、来源不可复现）。
#    若要这条链可复现，需先补 scripts/lake/lake_ods_product_load.py（见交接说明）。
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

MODE="${1:-build}"
case "$MODE" in
  dry|build|ddl) ;;
  *) echo "未知模式：$MODE（可用 dry|build|ddl）" >&2; exit 64 ;;
esac

echo "########## 湖侧维度层：mode=$MODE ##########"
echo "  PROJ=$PROJ"
echo "  SPARK_CT=$SPARK_CT"

spark_acquire_lock

if [ "$MODE" = "ddl" ] || [ "$MODE" = "build" ]; then
  echo "########## 建表：sql/lake/lake_dim.sql（DROP + CREATE 两张 lake.dim 表） ##########"
  docker exec "$SPARK_CT" "${SPARK_SQL[@]}" -f /opt/offline-dw/sql/lake/lake_dim.sql 2>&1 \
    | grep -vE "$FILTER" | grep -E "Time taken|Error" || true
fi

if [ "$MODE" = "build" ] || [ "$MODE" = "dry" ]; then
  DRY=""
  [ "$MODE" = "dry" ] && DRY="--mode dry"
  echo "########## 构建维度（--table all $DRY） ##########"
  docker exec "$SPARK_CT" "${SPARK_SUBMIT[@]}" \
    /opt/offline-dw/scripts/lake/lake_dim_build.py --table all $DRY 2>&1 \
    | grep -vE "$FILTER" || { echo "!!! dim build FAILED"; exit 1; }
fi

echo "########## 湖侧维度层行数 ##########"
docker exec "$SPARK_CT" "${SPARK_SQL[@]}" -e "
  SELECT 'scd2' t, count(*) n FROM lake.dim.dim_product_scd2
  UNION ALL SELECT 'current', count(*) FROM lake.dim.dim_product;" \
  2>&1 | grep -vE "$FILTER" || true

echo "########## DONE ##########"
