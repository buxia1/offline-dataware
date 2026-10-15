#!/usr/bin/env bash
# =============================================================================
# 湖侧 DWD 层构建（lake.ods.* -> lake.dwd.*）
#
# ★ 自包含：所有路径都从本脚本位置推导，不依赖 Windows scratch：
#     HERE = 本脚本所在目录（scripts/lake）
#     PROJ = 仓库根（HERE/../..）
#   把整个仓库 cp -r 到任意路径后，同样能直接跑（含 dry 模式）。
#
# 用法：
#   bash scripts/lake/run_dwd.sh dry   all
#   bash scripts/lake/run_dwd.sh build all
#   bash scripts/lake/run_dwd.sh build dwd_order_detail 2026-09-20 2026-09-20   # 单日（幂等）
#   bash scripts/lake/run_dwd.sh build dwd_order_lifecycle
#   bash scripts/lake/run_dwd.sh ddl                                            # 只建表
#
# 幂等由 lake_dwd_build.py 保证：写前按本次范围 DELETE、写后回读自检（不一致 exit 2）。
# =============================================================================
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJ="$(cd "$HERE/../.." && pwd)"

CT="lake_dwd_tmp"
FILTER='^[0-9]{2}/|WARN |SLF4J|^Using Spark|Setting default|To adjust|Spark Web UI|Spark master|HiveConf|ObjectStore'

cleanup() { docker rm -f "$CT" >/dev/null 2>&1 || true; }
trap cleanup EXIT
cleanup

MODE_ARG="${1:-build}"
TABLE="${2:-all}"
D1="${3:-}"
D2="${4:-}"

case "$MODE_ARG" in
  dry)     BUILD_MODE="--mode dry" ;;
  build)   BUILD_MODE="--mode append" ;;    # append 前会按本次范围 DELETE（幂等）
  rebuild) BUILD_MODE="--mode rebuild" ;;   # 清理逻辑与 build 相同，仅语义命名
  ddl)     BUILD_MODE="" ;;
  *) echo "未知模式：$MODE_ARG（可用 dry|build|rebuild|ddl）" >&2; exit 64 ;;
esac

RANGE=""
[ -n "$D1" ] && RANGE="--start $D1 --end ${D2:-$D1}"

echo "########## 湖侧 DWD：table=$TABLE mode=$MODE_ARG ${RANGE:-（全量）} ##########"
echo "  HERE=$HERE"
echo "  PROJ=$PROJ"

docker run --rm -d --name "$CT" \
  --network offline-dw_default --memory 5g \
  -v "$PROJ/lake:/opt/offline-dw/lake" \
  -v "$PROJ/spark/jars:/opt/offline-dw/jars:ro" \
  -v "$PROJ/sql:/opt/offline-dw/sql:ro" \
  --entrypoint sleep apache/spark:3.5.1 infinity >/dev/null

SPARK_SQL=(/opt/spark/bin/spark-sql --master local[2] --driver-memory 1g \
  --jars /opt/offline-dw/jars/iceberg-spark-runtime-3.5_2.12-1.9.0.jar \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.lake.type=hadoop \
  --conf spark.sql.catalog.lake.warehouse=file:///opt/offline-dw/lake/warehouse)

if [ "$MODE_ARG" = "ddl" ]; then
  echo "########## 建表：sql/lake/lake_dwd.sql（DROP + CREATE，会清空三张表） ##########"
  docker exec "$CT" "${SPARK_SQL[@]}" -f /opt/offline-dw/sql/lake/lake_dwd.sql 2>&1 \
    | grep -vE "$FILTER" | grep -E "Time taken|Error" || true
else
  echo "########## 构建 DWD（内置幂等清理 + 写入后自检） ##########"
  docker exec "$CT" mkdir -p /tmp/o
  docker cp "$HERE/lake_dwd_build.py" "$CT:/tmp/o/build.py"
  docker exec "$CT" /opt/spark/bin/spark-submit \
    --master 'local[2]' --driver-memory 3g \
    --jars /opt/offline-dw/jars/iceberg-spark-runtime-3.5_2.12-1.9.0.jar \
    /tmp/o/build.py --table "$TABLE" $BUILD_MODE $RANGE 2>&1 \
    | grep -vE "$FILTER" || { echo "!!! $TABLE FAILED"; exit 1; }
fi

echo ""
echo "########## 湖侧 DWD 行数 ##########"
docker exec "$CT" "${SPARK_SQL[@]}" \
  -e "SELECT 'dwd_order_detail' t, count(*) n FROM lake.dwd.dwd_order_detail
      UNION ALL SELECT 'dwd_order_sku_detail', count(*) FROM lake.dwd.dwd_order_sku_detail
      UNION ALL SELECT 'dwd_order_lifecycle', count(*) FROM lake.dwd.dwd_order_lifecycle;" \
  2>&1 | grep -vE "$FILTER" || true

echo "########## DONE ##########"
