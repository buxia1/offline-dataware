#!/bin/bash
# =============================================================================
# 累积快照事实表 dwd.dwd_order_lifecycle 装载 —— 统一入口
#
#   bash scripts/dwd_order_lifecycle_load.sh                # 增量（起点 = ODS 里最新事件日）
#   bash scripts/dwd_order_lifecycle_load.sh 20260920       # 增量，指定业务日期
#   bash scripts/dwd_order_lifecycle_load.sh --full         # 全量重建（TRUNCATE + 从 ODS 完整重推）
#
# 【为什么默认必须是增量】
#   累积快照的价值是「回填变化的那几行」。全表重算在数据量上来后没有意义，
#   但【全量重建必须留得下来】—— 它是增量逻辑被改坏时唯一的恢复手段
#   （同理 SCD2：见 scripts/dim_product_scd2_load.sh）。
#
# 【为什么起点默认取「ODS 最新事件日」而不是 wall clock】
#   mock 数据的业务日期和历史无关（数据是 2026-09，今天不是）。
#   取 ODS 最新事件日 = 「按数据自己的时间轴推进」，可重放、可复现。
#   DS 里跑就显式传 ${system.biz.date}。
#
# 【四道防线（缺一不可）】
#   ① 范围内没有订单 → 明确打印并退出（不是悄悄什么都不做）
#   ② 替换后校验占位符已消失 + 起点格式合法（否则 dt >= NULL → 0 行命中 → 静默 no-op）
#   ③ 全表【双向 EXCEPT 对账】= 0
#      —— 累积快照最怕的是「某列忘了回填」，那种错【行数完全正常】，只有逐列对账能抓到
#   ④ 不变式：行数 = 唯一订单数、order_time 非空、lag 与里程碑列自洽
# =============================================================================
set -e

cd "$(dirname "$0")/.."

SQL_FILE="sql/dwd_order_lifecycle_load.sql"
TABLE="dwd.dwd_order_lifecycle"
VIEW="dwd.v_order_lifecycle_expected"

MYSQL="docker exec -i starrocks mysql -P9030 -h127.0.0.1 -uroot"
QUERY="docker exec starrocks mysql -P9030 -h127.0.0.1 -uroot -N -B -e"

MODE="${1:---inc}"

case "$MODE" in
    --inc|--incremental)   ARG_DT="${2:-}" ;;
    --full|--full-rebuild) ARG_DT="" ;;
    *)
        echo "❌ 未知参数: $MODE"
        echo "用法: bash scripts/dwd_order_lifecycle_load.sh [YYYYMMDD | --inc [YYYYMMDD] | --full]"
        exit 1
        ;;
esac

if [ ! -f "$SQL_FILE" ]; then
    echo "❌ 找不到 SQL 文件: $SQL_FILE"
    exit 1
fi

# ---- 确定回填起点 ----
if [ "$MODE" = "--full" ] || [ "$MODE" = "--full-rebuild" ]; then
    FROM_DT="1970-01-01"
elif [ -n "$ARG_DT" ]; then
    # 接受 20260920 和 2026-09-20 两种写法
    FROM_DT=$(printf '%s' "$ARG_DT" | sed -E 's/^([0-9]{4})([0-9]{2})([0-9]{2})$/\1-\2-\3/')
else
    FROM_DT=$($QUERY "SELECT COALESCE(MAX(dt), '1970-01-01') FROM ods.ods_order_event")
fi

echo "================================"
echo "模式     : $MODE"
echo "回填起点 : $FROM_DT"

# ---- 防线②的前置：起点必须合法（防「空值 → dt >= '' → CAST 成 NULL → 0 行命中」）----
if ! printf '%s' "$FROM_DT" | grep -Eq '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'; then
    echo "❌ 回填起点不合法: '$FROM_DT'（应为 YYYY-MM-DD）—— 拒绝执行"
    echo "   否则会替换成 dt >= '' → 静默 no-op 且退出码 0（PITFALLS §7.2）"
    exit 1
fi

# ---- 防线①：范围内没有订单 → 明确退出 ----
SCOPE=$($QUERY "SELECT count(DISTINCT order_id) FROM ods.ods_order_event WHERE dt >= '$FROM_DT'")
echo "范围内订单数: $SCOPE"
if [ "$SCOPE" = "0" ]; then
    echo "无订单需要回填，退出"
    exit 0
fi

# ---- 全量模式：先清空 ----
if [ "$MODE" = "--full" ] || [ "$MODE" = "--full-rebuild" ]; then
    echo "⚠️  全量重建：先 TRUNCATE $TABLE（这是增量逻辑坏掉时的恢复手段）"
    $QUERY "TRUNCATE TABLE $TABLE"
fi

# ---- 防线②：替换占位符，并校验真的替换掉了 ----
TMP_SQL=$(mktemp /tmp/lifecycle_load.XXXXXX.sql)
CHECK_SQL=$(mktemp /tmp/lifecycle_check.XXXXXX.sql)
trap 'rm -f "$TMP_SQL" "$CHECK_SQL"' EXIT

sed "s/\${FROM_DT}/${FROM_DT}/g" "$SQL_FILE" > "$TMP_SQL"

if grep -q '\${FROM_DT}' "$TMP_SQL"; then
    echo "❌ 占位符 \${FROM_DT} 未被替换 —— 拒绝执行（否则会静默 no-op）"
    exit 1
fi

echo
echo "=== 执行回填（主键模型：INSERT 即 UPSERT）==="
$MYSQL < "$TMP_SQL"
echo "✅ 回填执行完成"

# ---- 防线③：全表双向对账 ----
cat > "$CHECK_SQL" <<'SQL'
SELECT count(*) AS diff_rows FROM (
  (SELECT order_id, user_id, product_id, amount, order_time, pay_time, ship_time,
          finish_time, cancel_time, current_stage, pay_lag_hours, ship_lag_days,
          finish_lag_days, last_event_time
   FROM dwd.dwd_order_lifecycle
   EXCEPT
   SELECT order_id, user_id, product_id, amount, order_time, pay_time, ship_time,
          finish_time, cancel_time, current_stage, pay_lag_hours, ship_lag_days,
          finish_lag_days, last_event_time
   FROM dwd.v_order_lifecycle_expected)
  UNION ALL
  (SELECT order_id, user_id, product_id, amount, order_time, pay_time, ship_time,
          finish_time, cancel_time, current_stage, pay_lag_hours, ship_lag_days,
          finish_lag_days, last_event_time
   FROM dwd.v_order_lifecycle_expected
   EXCEPT
   SELECT order_id, user_id, product_id, amount, order_time, pay_time, ship_time,
          finish_time, cancel_time, current_stage, pay_lag_hours, ship_lag_days,
          finish_lag_days, last_event_time
   FROM dwd.dwd_order_lifecycle)
) t;
SQL

DIFF=$(docker exec -i starrocks mysql -P9030 -h127.0.0.1 -uroot -N -B < "$CHECK_SQL" | tail -1)

# ---- 防线④：不变式 ----
ROWS=$($QUERY "SELECT count(*) FROM $TABLE")
EXPECT=$($QUERY "SELECT count(*) FROM $VIEW")
IDS=$($QUERY "SELECT count(DISTINCT order_id) FROM $TABLE")
NO_ORDER_TIME=$($QUERY "SELECT count(*) FROM $TABLE WHERE order_time IS NULL")
BAD_LAG=$($QUERY "SELECT count(*) FROM $TABLE
                  WHERE (pay_lag_hours IS NOT NULL AND pay_time IS NULL)
                     OR (pay_lag_hours IS NULL AND pay_time IS NOT NULL)")
BAD_STAGE=$($QUERY "SELECT count(*) FROM $TABLE WHERE current_stage IS NULL")

echo
echo "=== 不变式校验 ==="
printf "  期望行数(视图)              : %s\n" "$EXPECT"
printf "  实际行数                    : %s\n" "$ROWS"
printf "  唯一订单数                  : %s\n" "$IDS"
printf "  order_time 为空             : %s   （必须 0）\n" "$NO_ORDER_TIME"
printf "  pay_lag 与 pay_time 不自洽  : %s   （必须 0）\n" "$BAD_LAG"
printf "  current_stage 为空          : %s   （必须 0）\n" "$BAD_STAGE"
printf "  双向对账差异行数            : %s   （必须 0）\n" "$DIFF"
echo "  当前阶段分布："
$QUERY "SELECT current_stage, count(*) FROM $TABLE GROUP BY current_stage ORDER BY 1" | sed 's/^/      /'

FAIL=0
if [ "$DIFF" != "0" ]; then
    echo "❌ 双向对账有 $DIFF 行差异 —— 增量逻辑坏了（或有列没回填）"
    echo "   恢复手段: bash scripts/dwd_order_lifecycle_load.sh --full"
    FAIL=1
fi
if [ "$ROWS" != "$EXPECT" ]; then
    echo "❌ 行数($ROWS) ≠ 视图行数($EXPECT) —— 少回填了订单，或回放顺序错了（必须先跑早的那天）"
    FAIL=1
fi
if [ "$ROWS" != "$IDS" ]; then
    echo "❌ 行数($ROWS) ≠ 唯一订单数($IDS) —— 主键表不该出现重复，说明建表语句不是 PRIMARY KEY 模型"
    FAIL=1
fi
if [ "$NO_ORDER_TIME" != "0" ]; then
    echo "❌ 有订单没有 order_time —— 事件流缺 order 事件"
    FAIL=1
fi
if [ "$BAD_LAG" != "0" ]; then
    echo "❌ pay_lag_hours 与 pay_time 不自洽（一个有一个没有）"
    FAIL=1
fi
if [ "$BAD_STAGE" != "0" ]; then
    echo "❌ current_stage 为空 —— 派生 CASE 没兜住"
    FAIL=1
fi

if [ "$FAIL" != "0" ]; then
    echo
    echo "工作流将中断在此处。"
    exit 1
fi

echo
echo "✅ 全部通过（行数一致、逐列对账为 0、阶段分布正常）"
