#!/bin/bash
# =============================================================================
# Routine Load 健康检查
#
#   bash scripts/check_routine_load.sh          # 检查，异常返回 1（供 DS 告警）
#   bash scripts/check_routine_load.sh --list   # 只列状态，永远返回 0
#
# 【为什么必须要有这个脚本】
#   换成 Routine Load 后，摄入从「每天 02:30 跑一次」变成「常驻作业」。
#   常驻作业有个新风险：它挂了【不会有人自动发现】——
#   DS 里再没有"摄入节点"会变红，Kafka 数据就这么静静地不进来。
#   所以必须有一个定时检查，异常时 exit 1，交给 DS 告警组。
#
# 【怎么用】
#   在 DS 里建一个工作流，定时（建议每 30 分钟）执行本脚本，
#   配 warning_type=2 + 告警组 2（与现有三个工作流一致）。
#   ⚠️ 失败策略用 END（数据/健康检查失败重试无意义，与工作流三一致）。
# =============================================================================
set -u

MYSQL="docker exec starrocks mysql -P9030 -h127.0.0.1 -uroot"

JOB_EVENT=ods_order_event_load
JOB_ORDER=ods_order_load
DB=ods

# 期望值（改造前实测，2026-10-08）
EXPECT_EVENT=2379
EXPECT_ORDER=800

LIST_ONLY=0
[ "${1:-}" = "--list" ] && LIST_ONLY=1

FAIL=0

get_state() {  # $1 = 作业名
    # ⚠️ SHOW ROUTINE LOAD 的真实列序（3.5.0 实测）：
    #    1=Id 2=Name 3=CreateTime 4=PauseTime 5=EndTime 6=DbName 7=TableName
    #    8=State 9=DataSourceType 10=CurrentTaskNum 11=JobProperties ...
    #    → State 是第 8 列，不是第 3 列
    $MYSQL -N -B -e "SHOW ROUTINE LOAD FROM $DB" 2>/dev/null \
      | awk -F'\t' -v j="$1" '$2==j {print $8}'
}

# 竖排兜底：某些版本 SHOW ROUTINE LOAD 输出为多行
get_state_vertical() {
    $MYSQL -e "SHOW ROUTINE LOAD FROM $DB\G" 2>/dev/null \
      | awk -v j="$1" '
          /^ *Name: /   { name=$2 }
          /^ *State: /  { if (name==j) { print $2; exit } }'
}

echo "================================================================"
echo " Routine Load 健康检查  ($(date '+%Y-%m-%d %H:%M:%S'))"
echo "================================================================"

for J in "$JOB_EVENT" "$JOB_ORDER"; do
    S=$(get_state "$J")
    [ -z "$S" ] && S=$(get_state_vertical "$J")

    if [ -z "$S" ]; then
        printf '  ❌ %-24s 作业不存在（或查询失败）\n' "$J"
        FAIL=1
        continue
    fi

    case "$S" in
        RUNNING)
            printf '  ✅ %-24s RUNNING\n' "$J"
            ;;
        NEED_SCHEDULE)
            # 刚创建/刚恢复时的短暂中间态，会自动转 RUNNING
            printf '  ⚠️  %-24s NEED_SCHEDULE（初始化中，稍后应变 RUNNING）\n' "$J"
            ;;
        PAUSED)
            printf '  ❌ %-24s PAUSED —— 作业被暂停，数据不再进入！\n' "$J"
            FAIL=1
            ;;
        STOPPED)
            printf '  ❌ %-24s STOPPED —— 作业已停止\n' "$J"
            FAIL=1
            ;;
        CANCELLED)
            printf '  ❌ %-24s CANCELLED —— 作业已取消\n' "$J"
            FAIL=1
            ;;
        *)
            printf '  ❌ %-24s %s —— 非运行态\n' "$J" "$S"
            FAIL=1
            ;;
    esac
done

# ---- 行数核对：摄入停了行数不涨，行数异常说明逻辑有问题 ----
E=$($MYSQL -N -B -e "SELECT count(*) FROM $DB.ods_order_event" 2>/dev/null)
O=$($MYSQL -N -B -e "SELECT count(*) FROM $DB.ods_order" 2>/dev/null)
printf '  %-27s %s\n' "ods_order_event 行数" "$E"
printf '  %-27s %s\n' "ods_order 行数" "$O"

# 只对"翻倍"这种明确故障报警；行数正常增长（有新数据）不算错
if [ -n "$E" ] && [ "$E" -ge $(( EXPECT_EVENT * 2 )) ] 2>/dev/null; then
    printf '  ❌ 行数异常膨胀（≥%d）—— 疑似重灌/重复摄入\n' $(( EXPECT_EVENT * 2 ))
    FAIL=1
fi

# ---- 事件流不变式：行数 == (order_id,event_type) 去重对数 ----
PAIRS=$($MYSQL -N -B -e \
  "SELECT count(DISTINCT concat(order_id,'-',event_type)) FROM $DB.ods_order_event" 2>/dev/null)
if [ -n "$E" ] && [ -n "$PAIRS" ]; then
    if [ "$E" = "$PAIRS" ]; then
        printf '  ✅ %-24s 行数 == 去重对数 (%s)\n' "事件表不变式" "$E"
    else
        printf '  ❌ %-24s 行数(%s) ≠ 去重对数(%s) —— 有重复行\n' "事件表不变式" "$E" "$PAIRS"
        FAIL=1
    fi
fi

# ---- 错误信息（有则打印，便于定位）----
if [ "$FAIL" != "0" ]; then
    echo
    echo "--- 作业详情（排查用）---"
    $MYSQL -e "SHOW ROUTINE LOAD FROM $DB\G" 2>/dev/null \
      | grep -E "Name:|State:|ReasonOfStateChanged:|ErrorLogUrls:|OtherMsg:|Progress:" \
      | sed 's/^/    /'
fi

echo "================================================================"
if [ "$LIST_ONLY" = "1" ]; then
    echo " (--list 模式：不报错)"
    exit 0
fi

if [ "$FAIL" = "0" ]; then
    echo " ✅ 全部正常"
    exit 0
else
    echo " ❌ 有异常，DS 应告警"
    exit 1
fi
