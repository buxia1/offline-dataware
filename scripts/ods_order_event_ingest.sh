#!/bin/bash
# =============================================================================
# 事件流摄入外壳（增量版，2026-10-07 重构）
#
#   bash scripts/ods_order_event_ingest.sh              # 增量：只读位点之后的新消息
#   bash scripts/ods_order_event_ingest.sh --reset      # 回补：清空事件表 + 清位点，从 earliest 重灌
#
# 【为什么默认必须是增量】
#   原来的"先 TRUNCATE 再从 Kafka 全量重灌"有个致命前提：**Kafka 里必须有完整的全部历史**。
#   但 Kafka 有 retention（本项目 168 小时 = 7 天），消息过期后：
#     · topic 全空   → 清表后灌进 0 行
#     · topic 被裁一半 → 清表后灌进**残缺**数据，而且"topic 非空"的检查拦不住
#   这正是 2026-10-05 事故的成因。增量读位置后，**永远不重读历史，retention 就无关了**。
#
# 【--reset 是回补手段，不是日常路径】
#   它必须能回答"Kafka 里到底有没有我们需要的全部数据"，见下面两道防线。
#
# 【四道防线】
#   ① topic 为空 → 拒绝执行（--reset 时尤其重要，否则等于清库）
#   ② 进度不得超过 Kafka 现存最早 offset（retention 防线）
#   ③ 摄入后行数必须 == 位点表之和（写进去的和记账的要一致）
#   ④ 本次新增行数必须 == topic 消息增量（容错重跑：0 == 0 也算通过）
# =============================================================================
set -e
cd "$(dirname "$0")/.."

KAFKA_BIN=/opt/kafka/bin/kafka-get-offsets.sh
TOPIC=ods_order_event
PYSQL="docker exec starrocks mysql -P9030 -h127.0.0.1 -uroot -N -B -e"
MYSQL="docker exec starrocks mysql -P9030 -h127.0.0.1 -uroot"
SPARK_JOB=/opt/offline-dw/scripts/ods_order_event_to_starrocks.py

MODE="${1:---inc}"
case "$MODE" in
    --inc|--incremental) RESET=0 ;;
    --reset|--full)      RESET=1 ;;
    *)
        echo "❌ 未知参数: $MODE"
        echo "用法: bash scripts/ods_order_event_ingest.sh [--inc | --reset]"
        exit 1
        ;;
esac

topic_total() {
    docker exec kafka bash -c "$KAFKA_BIN --bootstrap-server localhost:9092 --topic $TOPIC $1" \
        2>/dev/null | awk -F: -v t="$TOPIC" '$1==t {s+=$3} END {print s+0}'
}

echo "================================"
echo "模式         : $MODE"
echo "topic        : $TOPIC"

# ---- 防线①：topic 为空 → 拒绝执行 ----
TOPIC_TOTAL=$(topic_total "")
TOPIC_START=$(topic_total "--time -2")     # 现存最早 offset 之和
echo "topic 消息数 : $TOPIC_TOTAL   （现存最早 offset 之和 $TOPIC_START）"
if [ "$TOPIC_TOTAL" = "0" ]; then
    echo "❌ topic 为空，拒绝执行（否则 --reset 会把 ods_order_event 清成空表 = 静默清库）"
    exit 1
fi

# ---- 防线②：进度不得超过现存范围（只在增量模式下有意义）----
# 位点表里各分区的 next_offset 之和；若它 < 现存最早之和，说明没读的消息已被删
OFFSET_SUM=$($PYSQL "SELECT COALESCE(sum(next_offset),0) FROM ods.ods_kafka_offset")
echo "位点表进度   : $OFFSET_SUM"

if [ "$RESET" = "0" ] && [ "$OFFSET_SUM" != "0" ] && [ "$OFFSET_SUM" -lt "$TOPIC_START" ]; then
    echo "❌ 位点进度($OFFSET_SUM) < Kafka 现存最早($TOPIC_START)"
    echo "   → 我们还没读的消息已被 retention 删除。"
    echo "   → 处理：调大 Kafka log.retention.hours，或从上游重放这段时间；"
    echo "     确认 Kafka 数据完整后再用 --reset 重建。"
    exit 1
fi

# ---- --reset：清空事件表 + 清位点（位点表为空 = 下次从 earliest 读）----
if [ "$RESET" = "1" ]; then
    echo "⚠️  --reset：清空 ods_order_event 和 ods_kafka_offset（位点清零 = 下次从 earliest 重灌）"
    $MYSQL -e "TRUNCATE TABLE ods.ods_order_event; TRUNCATE TABLE ods.ods_kafka_offset;"
    # ⚠️ 清完之后必须【重新读】位点：否则下面的"期望增量"会拿清零前的旧值去算，必然报假错
    OFFSET_SUM=$($PYSQL "SELECT COALESCE(sum(next_offset),0) FROM ods.ods_kafka_offset")
fi

ROWS_BEFORE=$($PYSQL "SELECT count(*) FROM ods.ods_order_event")
echo "摄入前事件表 : $ROWS_BEFORE 行"

# ---- 摄入（Spark 脚本自己负责：读位点 → 只读新消息 → 写 ODS → 回写位点）----
echo
echo "=== 执行摄入 ==="
docker exec spark /opt/spark/bin/spark-submit --master 'local[2]' \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.mysql:mysql-connector-j:8.4.0 \
  $SPARK_JOB

# ---- 摄入后校验 ----
ROWS_AFTER=$($PYSQL "SELECT count(*) FROM ods.ods_order_event")
OFFSET_SUM_AFTER=$($PYSQL "SELECT COALESCE(sum(next_offset),0) FROM ods.ods_kafka_offset")
DELTA=$((ROWS_AFTER - ROWS_BEFORE))

echo
echo "================================"
echo "摄入前/后行数 : $ROWS_BEFORE → $ROWS_AFTER   （本次新增 $DELTA 行）"
echo "位点表之和    : $OFFSET_SUM → $OFFSET_SUM_AFTER"

FAIL=0

# ---- 防线③：写进去的和记账的要一致 ----
if [ "$ROWS_AFTER" != "$OFFSET_SUM_AFTER" ]; then
    echo "❌ 行数($ROWS_AFTER) ≠ 位点表之和($OFFSET_SUM_AFTER)"
    echo "   → 写入与记账不一致：要么 ODS 写失败但位点已推进（会丢消息），"
    echo "     要么写入了但位点没更新（下次会重复读）。"
    echo "   → 恢复：确认 Kafka 完整后 bash scripts/ods_order_event_ingest.sh --reset"
    FAIL=1
fi

# ---- 防线④：本次新增行数必须 == topic 消息增量 ----
# 容错重跑（Spark 成功但外壳在检查前被杀）时 delta=0，topic 增量也是 0 → 仍应通过
EXPECT=$((TOPIC_TOTAL - OFFSET_SUM))
if [ "$DELTA" != "$EXPECT" ]; then
    echo "❌ 本次新增 $DELTA 行 ≠ topic 增量 $EXPECT"
    echo "   → 说明有消息没落库（丢消息）或多落了（重复）"
    FAIL=1
fi

if [ "$FAIL" != "0" ]; then
    echo
    echo "工作流将中断在此处。"
    exit 1
fi

echo "✅ 摄入完成（行数与位点一致，本次新增 $DELTA 行）"
