#!/usr/bin/env bash
# =============================================================================
# 湖侧维度层构建：lake.ods.ods_product -> lake.dim.dim_product_scd2 -> lake.dim.dim_product
#
# 用法：
#   bash run_lake_dim.sh dry       # 只算不写
#   bash run_lake_dim.sh build     # 灌数
#   bash run_lake_dim.sh ddl       # 只建表
# =============================================================================
set -euo pipefail

PROJ="${PROJ:-/home/l/offline-dw}"
SCRATCH="${SCRATCH:-/mnt/d/develop/workspace/deepseek_harness_temp}"
CT="lake_dim_tmp"
FILTER='^[0-9]{2}/|WARN |SLF4J|^Using Spark|Setting default|To adjust|Spark Web UI|Spark master|HiveConf|ObjectStore'

cleanup() { docker rm -f "$CT" >/dev/null 2>&1 || true; }
trap cleanup EXIT
cleanup

MODE="${1:-build}"

docker run --rm -d --name "$CT" \
  --network offline-dw_default --memory 4g \
  -v "$PROJ/lake:/opt/offline-dw/lake" \
  -v "$PROJ/spark/jars:/opt/offline-dw/jars:ro" \
  -v "$SCRATCH:/opt/offline-dw/scratch:ro" \
  --entrypoint sleep apache/spark:3.5.1 infinity >/dev/null

SPARK_SQL="/opt/spark/bin/spark-sql --master local[2] --driver-memory 1g \
  --jars /opt/offline-dw/jars/iceberg-spark-runtime-3.5_2.12-1.9.0.jar \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.lake.type=hadoop \
  --conf spark.sql.catalog.lake.warehouse=file:///opt/offline-dw/lake/warehouse"

if [ "$MODE" = "ddl" ] || [ "$MODE" = "build" ]; then
  echo "########## 建表 ##########"
  docker exec "$CT" bash -c "$SPARK_SQL -f /opt/offline-dw/scratch/lake_dim.sql" 2>&1 \
    | grep -vE "$FILTER" | grep -E "Time taken|Error" || true
fi

if [ "$MODE" = "build" ] || [ "$MODE" = "dry" ]; then
  DRY=""
  [ "$MODE" = "dry" ] && DRY="--mode dry"
  echo "########## 构建维度 ##########"
  docker exec "$CT" mkdir -p /tmp/o
  docker cp "$SCRATCH/lake_dim_build.py" "$CT:/tmp/o/build.py"
  docker exec "$CT" /opt/spark/bin/spark-submit \
    --master 'local[2]' --driver-memory 2g \
    --jars /opt/offline-dw/jars/iceberg-spark-runtime-3.5_2.12-1.9.0.jar \
    /tmp/o/build.py --table all $DRY 2>&1 | grep -vE "$FILTER" || true
fi

echo "########## 湖侧维度层行数 ##########"
docker exec "$CT" bash -c "$SPARK_SQL -e \"
  SELECT 'scd2' t, count(*) n FROM lake.dim.dim_product_scd2
  UNION ALL SELECT 'current', count(*) FROM lake.dim.dim_product;\"" 2>&1 | grep -vE "$FILTER" || true

echo "########## DONE ##########"
