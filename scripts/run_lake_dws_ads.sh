#!/usr/bin/env bash
# =============================================================================
# 湖侧 DWS / ADS 构建：lake.dwd.dwd_order_detail -> lake.dws.* / lake.ads.*
#
# 用法：
#   bash run_lake_dws_ads.sh ddl     # 建表
#   bash run_lake_dws_ads.sh dry     # 只算不写（打印头尾各 3 天）
#   bash run_lake_dws_ads.sh build   # 全量重建
# =============================================================================
set -euo pipefail
PROJ="${PROJ:-/home/l/offline-dw}"
LOCAL=/mnt/d/develop/workspace/deepseek_harness_temp
CT="lake_dws_ads_tmp"
MODE="${1:-build}"
FILTER='^[0-9]{2}/|WARN |SLF4J|^Using Spark|Setting default|To adjust|Spark Web UI|Spark master|HiveConf|ObjectStore'

cleanup() { docker rm -f "$CT" >/dev/null 2>&1 || true; }
trap cleanup EXIT
cleanup

cp "$LOCAL/lake_dws_ads.sql" "$PROJ/scripts/lake_dws_ads.sql"
cp "$LOCAL/lake_dws_ads_build.py" "$PROJ/scripts/lake_dws_ads_build.py"

docker run --rm -d --name "$CT" --network offline-dw_default --memory 5g \
  -v "$PROJ/lake:/opt/offline-dw/lake" \
  -v "$PROJ/spark/jars:/opt/offline-dw/jars:ro" \
  -v "$PROJ/scripts:/opt/offline-dw/app:ro" \
  --entrypoint sleep apache/spark:3.5.1 infinity >/dev/null

SPARK="--jars /opt/offline-dw/jars/iceberg-spark-runtime-3.5_2.12-1.9.0.jar"
CATS="--conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions --conf spark.sql.catalog.lake=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.lake.type=hadoop --conf spark.sql.catalog.lake.warehouse=file:///opt/offline-dw/lake/warehouse"

if [ "$MODE" = "ddl" ]; then
  echo "########## 建表：lake.dws / lake.ads ##########"
  docker exec "$CT" bash -c "/opt/spark/bin/spark-sql --master local[2] --driver-memory 2g $SPARK $CATS \
    -f /opt/offline-dw/app/lake_dws_ads.sql" 2>&1 | grep -vE "$FILTER" || true
  echo "########## 建表后表清单 ##########"
  docker exec "$CT" bash -c "/opt/spark/bin/spark-sql --master local[2] --driver-memory 2g $SPARK $CATS \
    -e 'SHOW TABLES IN lake.dws; SHOW TABLES IN lake.ads;'" 2>&1 | grep -vE "$FILTER" || true
  echo "DDL_EXIT=0"
  exit 0
fi

ARGS="--table all --mode rebuild"
[ "$MODE" = "dry" ] && ARGS="--table all --mode dry"

echo "########## 构建：$ARGS ##########"
docker exec "$CT" bash -c "/opt/spark/bin/spark-submit --master 'local[2]' --driver-memory 3g $SPARK \
  $CATS /opt/offline-dw/app/lake_dws_ads_build.py $ARGS" 2>&1 | grep -vE "$FILTER" || true

echo
echo "########## 湖侧结果 ##########"
docker exec "$CT" bash -c "/opt/spark/bin/spark-sql --master local[2] --driver-memory 2g $SPARK $CATS -e \"
  SELECT count(*) rows_, count(DISTINCT dt) days_, sum(order_cnt) oc, sum(paid_cnt) pc,
         sum(paid_amount) pa, sum(refund_amount) ra FROM lake.dws.dws_user_order_day;
  SELECT count(*) rows_, count(DISTINCT dt) days_, sum(order_cnt) oc, sum(paid_cnt) pc,
         sum(paid_amount) pa, sum(refund_amount) ra, sum(net_amount) na
    FROM lake.ads.ads_daily_sales;
  SELECT dt, order_cnt, paid_cnt, paid_amount, refund_amount, net_amount, avg_order_amount, pay_rate
    FROM lake.ads.ads_daily_sales ORDER BY dt LIMIT 3;\"" 2>&1 | grep -vE "$FILTER" || true

echo "########## 仓侧对照 ##########"
cat > /tmp/lake_vs_wh.sql <<'SQL'
SELECT count(*) rows_, count(DISTINCT dt) days_, sum(order_cnt) oc, sum(paid_cnt) pc,
       sum(paid_amount) pa, sum(refund_amount) ra FROM dws.dws_user_order_day;
SELECT count(*) rows_, count(DISTINCT dt) days_, sum(order_cnt) oc, sum(paid_cnt) pc,
       sum(paid_amount) pa, sum(refund_amount) ra, sum(net_amount) na FROM ads.ads_daily_sales;
SELECT dt, order_cnt, paid_cnt, paid_amount, refund_amount, net_amount, avg_order_amount, pay_rate
  FROM ads.ads_daily_sales ORDER BY dt LIMIT 3;
SQL
docker exec -i starrocks mysql -P9030 -h127.0.0.1 -uroot -t < /tmp/lake_vs_wh.sql
echo "EXIT=0"
