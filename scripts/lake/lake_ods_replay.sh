#!/usr/bin/env bash
# =============================================================================
# 湖层 ODS 灌数：Kafka → Iceberg（按天覆盖，幂等）
#
#   bash scripts/lake_ods_replay.sh --date 2026-09-20
#   bash scripts/lake_ods_replay.sh --date 2026-09-20 --dry-run
#
# 与仓侧 scripts/legacy_warehouse/dwd_overwrite.sh 的关系：
#   · 参数风格【一样】：--date 只跑一天，who 循环由外面（DS/人工）做
#   · 幂等【一样】：重跑同一天结果不变
#   · 实现【不同】：仓侧靠 INSERT OVERWRITE PARTITION(...)，湖侧靠 dynamic 分区
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SPARK_JAR_DIR="${PROJECT_ROOT}/spark/jars"

DATE=""
DRY_RUN=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --date)   DATE="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    -h|--help) sed -n '2,12p' "$0"; exit 0 ;;
    *) echo "未知参数: $1" >&2; exit 2 ;;
  esac
done

[[ "${DATE}" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] \
  || { echo "❌ --date 格式错误，应为 YYYY-MM-DD，当前='${DATE}'" >&2; exit 2; }

echo "=============================================================="
echo " 湖层 ODS 灌数   date=${DATE}   dry_run=${DRY_RUN}"
echo "=============================================================="

JARS=""
for j in iceberg-spark-runtime-3.5_2.12-1.9.0.jar \
         spark-sql-kafka-0-10_2.12-3.5.1.jar \
         spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
         kafka-clients-3.4.1.jar \
         commons-pool2-2.11.1.jar ; do
  docker exec "${SPARK_CT:-spark}" test -f "/opt/offline-dw/jars/${j}" \
    || { echo "❌ 缺 jar（容器 ${SPARK_CT:-spark} 内 /opt/offline-dw/jars）: ${j}" >&2; exit 3; }
  JARS="${JARS},/opt/offline-dw/jars/${j}"
done
JARS="${JARS#,}"

SPARK_ARGS=(
  --master "${SPARK_MASTER:-local[4]}"
  --driver-memory "${DRIVER_MEM:-1g}"
  --conf "spark.sql.shuffle.partitions=${SPARK_SHUFFLE:-32}"
  --conf "spark.default.parallelism=${SPARK_SHUFFLE:-32}"
  --jars "${JARS}"
)

# ⚠️ --dry-run 必须放在 .py 之后（应用参数区）。
#    spark-submit 只把自己认识的选项放在 .py 之前，放在前面会报
#    "Error: Unrecognized option: --dry-run"（2026-10-13 实测踩过）
APP_ARGS=(--date "${DATE}")
[[ ${DRY_RUN} -eq 1 ]] && APP_ARGS+=(--dry-run)

docker exec spark /opt/spark/bin/spark-submit "${SPARK_ARGS[@]}" \
  /opt/offline-dw/scripts/lake/lake_ods_replay.py "${APP_ARGS[@]}"

docker exec spark chown -R 185:185 /opt/offline-dw/lake/warehouse 2>/dev/null || true

echo "--------------------------------------------------------------"
echo " 刷新 StarRocks 外部表"
docker exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e \
  "REFRESH EXTERNAL TABLE lake.ods.ods_order;
   REFRESH EXTERNAL TABLE lake.ods.ods_order_event;"
docker exec starrocks mysql -P9030 -h127.0.0.1 -uroot -B -e "
SELECT 'orders' AS tb, dt, count(*) AS n FROM lake.ods.ods_order GROUP BY dt
UNION ALL
SELECT 'events', dt, count(*) FROM lake.ods.ods_order_event GROUP BY dt
ORDER BY tb, dt;"

[[ ${DRY_RUN} -eq 1 ]] && echo "（dry-run：以上为当前湖内既有数据，本次未写入）"
echo "✅ 完成 date=${DATE}"
