#!/bin/bash
# =============================================================
# SKU 明细链路：湖层 Iceberg ⋈ SCD2 商品维度 → 仓侧 dwd_order_sku_detail
#
# 宿主侧负责「分区 + 幂等清理」，容器侧只负责算和写（容器里没有 docker）。
# 用法：
#   bash run_dwd_sku_full.sh --dry-run
#   bash run_dwd_sku_full.sh                      # 全量 90 天
# =============================================================
set -euo pipefail

PROJ=/home/l/offline-dw
LOCAL=/mnt/d/develop/workspace/deepseek_harness_temp
START="${START:-2026-09-20}"
END="${END:-2026-12-18}"

DRY=0
for a in "$@"; do [ "$a" = "--dry-run" ] && DRY=1; done

cp "$LOCAL/dwd_order_sku_streamload.py" "$PROJ/scripts/dwd_order_sku_streamload.py"

sqlish() { docker exec -i starrocks mysql -P9030 -h127.0.0.1 -uroot "$@"; }

if [ "$DRY" = "0" ]; then
  echo "########## ① 幂等清理：先删掉区间内的旧分区数据 ##########"
  BEFORE=$(sqlish -N -B -e "SELECT count(*) FROM dwd.dwd_order_sku_detail")
  echo "清理前行数 = $BEFORE"
  # ★ StarRocks 的 DELETE 不接受 BETWEEN（实测报 "Where clause only supports compound
  #   predicate, binary predicate, is_null predicate and in predicate"），必须写成
  #   dt >= 'a' AND dt <= 'b'（前面 DWD 明细那支脚本里已经踩过同一个坑）。
  sqlish -e "DELETE FROM dwd.dwd_order_sku_detail WHERE dt >= '$START' AND dt <= '$END'"
  AFTER=$(sqlish -N -B -e "SELECT count(*) FROM dwd.dwd_order_sku_detail")
  echo "清理后行数 = $AFTER"

  echo
  echo "########## ② 确保分区存在（关开关 → 逐条 ADD → 恢复开关）##########"
  EXISTING=$(sqlish -N -B -e "SHOW PARTITIONS FROM dwd.dwd_order_sku_detail" | cut -f2)
  MISSING=""
  d="$START"
  while :; do
    D="$(date -d "$d" +%Y%m%d)"
    echo "$EXISTING" | grep -qx "p${D}" || MISSING="$MISSING $D"
    [ "$d" = "$END" ] && break
    d="$(date -d "$d + 1 day" +%Y-%m-%d)"
  done
  if [ -n "$MISSING" ]; then
    COUNT=$(echo "$MISSING" | wc -w)
    echo "缺 $COUNT 个分区，补齐中..."
    drop_dyn() {
      sqlish -e "ALTER TABLE dwd.dwd_order_sku_detail SET ('dynamic_partition.enable' = 'true')" >/dev/null 2>&1 || true
    }
    trap drop_dyn EXIT
    {
      echo "ALTER TABLE dwd.dwd_order_sku_detail SET ('dynamic_partition.enable' = 'false');"
      for D in $MISSING; do
        DF2="${D:0:4}-${D:4:2}-${D:6:2}"
        NEXT=$(date -d "$DF2 + 1 day" +%Y-%m-%d)
        echo "ALTER TABLE dwd.dwd_order_sku_detail ADD PARTITION p${D} VALUES [('${DF2}'), ('${NEXT}'));"
      done
      echo "ALTER TABLE dwd.dwd_order_sku_detail SET ('dynamic_partition.enable' = 'true');"
    } | docker exec -i starrocks mysql -P9030 -h127.0.0.1 -uroot >/dev/null 2>&1
    trap - EXIT
    drop_dyn
    echo "✅ 分区补齐"
  else
    echo "分区已齐全"
  fi
else
  echo "########## DRY-RUN：不动分区、不写数据 ##########"
fi

echo
echo "########## ③ Spark 作业 ##########"
docker rm -f sku_full_tmp >/dev/null 2>&1 || true

ARGS="--start $START --end $END"
[ "$DRY" = "1" ] && ARGS="$ARGS --dry-run"

docker run --rm --name sku_full_tmp \
  --network offline-dw_default --memory 5g \
  -v "$PROJ/lake:/opt/offline-dw/lake" \
  -v "$PROJ/spark/jars:/opt/offline-dw/jars:ro" \
  -v "$PROJ/scripts:/opt/offline-dw/app:ro" \
  -v "$PROJ/spark/jars/mysql-connector-java-8.0.30.jar:/opt/spark/jars/mysql-connector-java-8.0.30.jar:ro" \
  --entrypoint /opt/spark/bin/spark-submit apache/spark:3.5.1 \
  --master 'local[2]' --driver-memory 3g \
  --jars /opt/offline-dw/jars/iceberg-spark-runtime-3.5_2.12-1.9.0.jar \
  /opt/offline-dw/app/dwd_order_sku_streamload.py $ARGS 2>&1 \
  | grep -vE "^[0-9]{2}/[0-9]{2}/[0-9]{2} |^WARN |^Using Spark's default|log4j"

echo
echo "########## ④ 复核 ##########"
sqlish -t -e "
SELECT count(*) AS sku_rows,
       count(DISTINCT dt) AS days_n,
       min(dt) AS min_d, max(dt) AS max_d,
       sum(amount) AS amount_sum,
       sum(sku_price) AS sku_price_sum
  FROM dwd.dwd_order_sku_detail
 WHERE dt BETWEEN '$START' AND '$END';
"
echo "EXIT=0"
