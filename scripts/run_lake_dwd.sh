#!/usr/bin/env bash
# =============================================================================
# 湖侧 DWD 层构建（lake.ods.* -> lake.dwd.*）
#
# 用法：
#   bash run_lake_dwd.sh dry dwd_order_detail            # 只算不写
#   bash run_lake_dwd.sh build dwd_order_detail
#   bash run_lake_dwd.sh build dwd_order_sku_detail
#   bash run_lake_dwd.sh build dwd_order_lifecycle
#   bash run_lake_dwd.sh build all
#   bash run_lake_dwd.sh build dwd_order_detail 2026-09-20 2026-09-20   # 单日
# =============================================================================
set -euo pipefail

PROJ="${PROJ:-/home/l/offline-dw}"
SCRATCH="${SCRATCH:-/mnt/d/develop/workspace/deepseek_harness_temp}"
CT="lake_dwd_tmp"
FILTER='^[0-9]{2}/|WARN |SLF4J|^Using Spark|Setting default|To adjust|Spark Web UI|Spark master|HiveConf|ObjectStore'

cleanup() { docker rm -f "$CT" >/dev/null 2>&1 || true; }
trap cleanup EXIT
cleanup

MODE_ARG="${1:-build}"
TABLE="${2:-all}"
D1="${3:-}"
D2="${4:-}"

DWD_FLAG=""
[ "$MODE_ARG" = "dry" ] && DWD_FLAG="--dry-run"

RANGE=""
[ -n "$D1" ] && RANGE="--start $D1 --end ${D2:-$D1}"

echo "########## 湖侧 DWD：table=$TABLE mode=$MODE_ARG ${RANGE:-（全量）} ##########"

docker run --rm -d --name "$CT" \
  --network offline-dw_default --memory 5g \
  -v "$PROJ/lake:/opt/offline-dw/lake" \
  -v "$PROJ/spark/jars:/opt/offline-dw/jars:ro" \
  -v "$SCRATCH:/opt/offline-dw/scratch:ro" \
  -v "$PROJ/ds/libs/mysql-connector-java-8.0.30.jar:/opt/spark/jars/mysql-connector-java-8.0.30.jar:ro" \
  --entrypoint sleep apache/spark:3.5.1 infinity >/dev/null

docker exec "$CT" mkdir -p /tmp/o
docker cp "$SCRATCH/lake_dwd_build.py" "$CT:/tmp/o/build.py"

docker exec "$CT" /opt/spark/bin/spark-submit \
  --master 'local[2]' --driver-memory 3g \
  --jars /opt/offline-dw/jars/iceberg-spark-runtime-3.5_2.12-1.9.0.jar \
  /tmp/o/build.py --table "$TABLE" --mode append $DWD_FLAG $RANGE 2>&1 \
  | grep -vE "$FILTER" || { echo "!!! $TABLE FAILED"; exit 1; }

echo ""
echo "########## 湖侧 DWD 行数 ##########"
docker exec "$CT" /opt/spark/bin/spark-sql \
  --master 'local[2]' --driver-memory 1g \
  --jars /opt/offline-dw/jars/iceberg-spark-runtime-3.5_2.12-1.9.0.jar \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.lake.type=hadoop \
  --conf spark.sql.catalog.lake.warehouse=file:///opt/offline-dw/lake/warehouse \
  -e "SELECT 'dwd_order_detail' t, count(*) n FROM lake.dwd.dwd_order_detail
      UNION ALL SELECT 'dwd_order_sku_detail', count(*) FROM lake.dwd.dwd_order_sku_detail
      UNION ALL SELECT 'dwd_order_lifecycle', count(*) FROM lake.dwd.dwd_order_lifecycle;" \
  2>&1 | grep -vE "$FILTER" || true

echo "########## DONE ##########"
