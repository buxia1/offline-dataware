#!/usr/bin/env bash
# 运行累积快照 dwd.dwd_order_lifecycle 的 Spark 下沉作业
#
#   bash scripts/legacy_step2/run_lifecycle.sh dry     # 只算不写：验证行数 + 落盘自检
#   bash scripts/legacy_step2/run_lifecycle.sh full    # TRUNCATE 后全量重灌
#
# ★ 为什么清理/TRUNCATE 放在宿主侧：Spark 容器里没有 docker，
#   作业本身只连 StarRocks 的 9030/8040，删不了表。
set -euo pipefail

MODE="${1:-dry}"
PROJ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
NAME="lc_${MODE}_tmp"

docker rm -f "$NAME" >/dev/null 2>&1 || true

ARGS=(--label-suffix full)
[[ "$MODE" == "dry" ]] && ARGS=(--dry-run)

echo "########## 预检：目标表当前行数（TRUNCATE 前）##########"
printf 'SELECT count(*) AS lc_rows_before FROM dwd.dwd_order_lifecycle;\n' > /tmp/lc_pre.sql
docker exec -i starrocks mysql -P9030 -h127.0.0.1 -uroot -t < /tmp/lc_pre.sql

if [[ "$MODE" == "full" ]]; then
  echo "########## TRUNCATE dwd.dwd_order_lifecycle ##########"
  # upsert 删不掉"已不在快照里"的订单，所以全量重建必须先清空
  printf 'TRUNCATE TABLE dwd.dwd_order_lifecycle;\nSELECT count(*) AS after_truncate FROM dwd.dwd_order_lifecycle;\n' > /tmp/lc_trunc.sql
  docker exec -i starrocks mysql -P9030 -h127.0.0.1 -uroot -t < /tmp/lc_trunc.sql
fi

echo "########## 起 Spark 容器 $NAME ##########"
docker run --rm -d --name "$NAME" \
  --network offline-dw_default \
  --memory 5g \
  -v /home/l/offline-dw/lake:/opt/offline-dw/lake \
  -v /home/l/offline-dw/spark/jars:/opt/offline-dw/jars:ro \
  -v "$PROJ/scripts:/opt/offline-dw/scripts:ro" \
  -v "$PROJ/spark/jars/mysql-connector-java-8.0.30.jar:/opt/spark/jars/mysql-connector-java-8.0.30.jar:ro" \
  --entrypoint sleep apache/spark:3.5.1 infinity >/dev/null

sleep 2
docker cp "$PROJ/scripts/dwd_order_lifecycle_streamload.py" \
  "$NAME:/opt/offline-dw/lc.py" >/dev/null

echo "########## spark-submit ($MODE) ##########"
set +e
time docker exec "$NAME" /opt/spark/bin/spark-submit \
  --master 'local[2]' --driver-memory 3g \
  --jars /opt/offline-dw/jars/iceberg-spark-runtime-3.5_2.12-1.9.0.jar \
  /opt/offline-dw/lc.py "${ARGS[@]}"
RC=$?
set -e
echo "SPARK_EXIT=$RC"

docker rm -f "$NAME" >/dev/null 2>&1 || true
exit $RC
