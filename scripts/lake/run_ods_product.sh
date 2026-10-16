#!/usr/bin/env bash
# =============================================================================
# 湖侧商品快照导入：data/dim/product_snapshot_*.csv -> lake.ods.ods_product
#
# 为什么要有它：lake.dim 链的源表 lake.ods.ods_product 原先既没有建表 SQL、
#   也没有导入脚本（4500 行是手工灌的，不可复现）。本脚本把它补成一条命令。
#
# 用法：
#   bash scripts/lake/run_ods_product.sh ddl                          # 建表（表已存在时是 no-op）
#   bash scripts/lake/run_ods_product.sh all                          # 导入 data/dim 下全部快照
#   bash scripts/lake/run_ods_product.sh range 2026-12-01 2026-12-18  # 导入区间（含端点）
#   bash scripts/lake/run_ods_product.sh file /opt/offline-dw/data/dim/product_snapshot_20261218.csv
#   bash scripts/lake/run_ods_product.sh dry                          # 只解析不写（默认全部）
#
# 环境变量：SPARK_CT（默认 spark）、LOCK_DIR、DRIVER_MEM（默认 1g）
#
# ⚠️ 导入完成后，湖侧 dim 链要跟着重跑：bash scripts/lake/run_dim.sh build
# =============================================================================
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJ="$(cd "$HERE/../.." && pwd)"
SPARK_CT="${SPARK_CT:-spark}"
LOCK_DIR="${LOCK_DIR:-$PROJ/scripts/.spark_job.lock}"
FILTER='^[0-9]{2}/|WARN |SLF4J|^Using Spark|Setting default|To adjust|Spark Web UI|Spark master|HiveConf|ObjectStore'

# shellcheck source=_spark_lib.sh
source "$HERE/_spark_lib.sh"
spark_ensure_running

MODE="${1:-all}"
shift || true

case "$MODE" in
  ddl)   ;;
  all)   ARGS=(--all) ;;
  dry)   ARGS=(--all --mode dry) ;;
  range) [ "$#" -ge 2 ] || { echo "用法：$0 range <D1> <D2>" >&2; exit 64; }
         ARGS=(--start "$1" --end "$2") ;;
  file)  [ "$#" -ge 1 ] || { echo "用法：$0 file <csv绝对路径>" >&2; exit 64; }
         ARGS=(--file "$1") ;;
  *) echo "未知模式：$MODE（可用 ddl|all|range|file|dry）" >&2; exit 64 ;;
esac

echo "########## 湖侧商品快照导入：mode=$MODE ${ARGS[*]:-} ##########"
echo "  PROJ=$PROJ"
echo "  SPARK_CT=$SPARK_CT"

spark_acquire_lock

if [ "$MODE" = "ddl" ]; then
  echo "########## 建表：sql/lake/lake_ods_product.sql ##########"
  docker exec "$SPARK_CT" "${SPARK_SQL[@]}" -f /opt/offline-dw/sql/lake/lake_ods_product.sql 2>&1 \
    | grep -vE "$FILTER" | grep -E "Time taken|Error" || true
else
  docker exec "$SPARK_CT" "${SPARK_SUBMIT[@]}" \
    /opt/offline-dw/scripts/lake/lake_ods_product_load.py "${ARGS[@]}" 2>&1 \
    | grep -vE "$FILTER" || { echo "!!! product load FAILED"; exit 1; }
fi

echo ""
echo "########## 湖侧 ods_product 现状（StarRocks 端需 REFRESH 后才能看到 Spark 刚写的数据）##########"
docker exec -i starrocks mysql -P9030 -h127.0.0.1 -uroot --batch --raw --skip-column-names -e "
  REFRESH EXTERNAL TABLE lake.ods.ods_product;
  SELECT count(*), count(DISTINCT snapshot_date), min(snapshot_date), max(snapshot_date)
  FROM lake.ods.ods_product;" 2>&1 || true

echo "########## DONE ##########"
