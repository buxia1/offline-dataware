#!/bin/bash
# DWD 明细 90 天全量：湖层 Iceberg → 清洗 → StarRocks.dwd_order_detail（Stream Load）
set -euo pipefail
PROJ=/home/l/offline-dw
LOCAL=/mnt/d/develop/workspace/deepseek_harness_temp

cp "$LOCAL/dwd_order_detail_streamload.py" "$PROJ/scripts/dwd_order_detail_streamload.py"

docker rm -f dwd_full_tmp >/dev/null 2>&1 || true

docker run --rm --name dwd_full_tmp \
  --network offline-dw_default --memory 5g \
  -v "$PROJ/lake:/opt/offline-dw/lake" \
  -v "$PROJ/spark/jars:/opt/offline-dw/jars:ro" \
  -v "$PROJ/scripts:/opt/offline-dw/app:ro" \
  -v "$PROJ/spark/jars/mysql-connector-java-8.0.30.jar:/opt/spark/jars/mysql-connector-java-8.0.30.jar:ro" \
  --entrypoint /opt/spark/bin/spark-submit apache/spark:3.5.1 \
  --master 'local[2]' --driver-memory 3g \
  --jars /opt/offline-dw/jars/iceberg-spark-runtime-3.5_2.12-1.9.0.jar \
  /opt/offline-dw/app/dwd_order_detail_streamload.py \
  --start 2026-09-20 --end 2026-12-18 2>&1 \
  | grep -vE "^[0-9]{2}/[0-9]{2}/[0-9]{2} |^WARN |^Using Spark's default|log4j"
echo "EXIT=${PIPESTATUS[0]}"
