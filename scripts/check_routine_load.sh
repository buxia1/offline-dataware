#!/bin/bash
# =============================================================================
# Routine Load 健康检查（v2 · 2026-10-18）
#
#   bash scripts/check_routine_load.sh          # 检查，异常返回 1（供 DS 告警）
#   bash scripts/check_routine_load.sh --list   # 只列状态，永远返回 0
#
# 可调环境变量（不改脚本也能在 DS 节点里覆盖）：
#   LAG_MAX    单分区允许的最大 OffsetLag（默认 5000；追平时≈0~1）
#   DUP_MAX    事件表允许的最大重复率百分比（默认 120，即 1.20×）
#
# 【为什么必须要有这个脚本】
#   换成 Routine Load 后，摄入从「每天 02:30 跑一次」变成「常驻作业」。
#   常驻作业有个新风险：它挂了【不会有人自动发现】——
#   DS 里再没有"摄入节点"会变红，Kafka 数据就这么静静地不进来。
#   所以必须有一个定时检查，异常时 exit 1，交给 DS 告警组。
#
# 【怎么用】
#   在 DS 里建一个工作流，定时（建议每 30 分钟）执行本脚本，
#   配 warning_type=2 + 告警组 2（与现有工作流一致）。
#   ⚠️ 失败策略用 END（健康检查失败重试无意义）。
#
# 【v2 修掉的两条过期判据（2026-10-18 实测）】
#   ① 旧的「行数异常膨胀（≥ EXPECT_EVENT*2）」把改造前（2026-10-08）的 2379 写死了；
#      放量到 1 万单/天 × 90 天后事件表是 3,120,421 行，必然每 30 分钟误报一次。
#      → 改为看 Routine Load 自己的 OffsetLag（摄入是否追平）；行数只打印、不判定。
#   ② 旧的「行数 == (order_id,event_type) 去重对数」不成立：事件表是 DUPLICATE KEY，
#      重放/重投产生的重复行是设计内行为（实测 90 天每天都有重复，重复率 1.05×，
#      单个 key 最多 4 条），整表比 3,120,421 vs 2,969,579 永远不等。
#      → 改为「重复率」软阈值（默认 ≤1.20×，异常重灌会立刻冲高）；
#        真正该相等的不变式挪到 ods_order（900,000 行 == 900,000 个 order_id，实测成立）。
# =============================================================================
set -u

MYSQL="docker exec starrocks mysql -P9030 -h127.0.0.1 -uroot"

JOB_EVENT=ods_order_event_load
JOB_ORDER=ods_order_load
DB=ods

LAG_MAX=${LAG_MAX:-5000}
DUP_MAX=${DUP_MAX:-120}

LIST_ONLY=0
[ "${1:-}" = "--list" ] && LIST_ONLY=1

FAIL=0

# ---- 从 SHOW ROUTINE LOAD 竖排输出里取某个作业的某个字段 ----
# $1 = 作业名, $2 = 字段名（State / OffsetLag / ErrorLogUrls / OtherMsg / Progress）
get_field() {
    $MYSQL -E -e "SHOW ROUTINE LOAD FROM $DB" 2>/dev/null \
      | awk -v j="$1" -v f="$2" '
          /^ *Name: / { name=$2 }
          name==j {
            pat = "^ *" f ": "
            if ($0 ~ pat) { sub(pat, ""); print; exit }
          }'
}

# 竖排兜底：某些版本 SHOW ROUTINE LOAD 输出非 -E 形态
get_field_vertical() {
    $MYSQL -e "SHOW ROUTINE LOAD FROM $DB\G" 2>/dev/null \
      | awk -v j="$1" -v f="$2" '
          /^ *Name: / { name=$2 }
          name==j {
            pat = "^ *" f ": "
            if ($0 ~ pat) { sub(pat, ""); print; exit }
          }'
}

# JSON 形态的 {"0":"1","1":"1"} → 最大整数
json_max() {
    printf '%s' "$1" | tr -d '{}"' | tr ',' '\n' \
      | awk -F: 'NF==2 && $2 ~ /^[0-9]+$/ { if ($2+0 > m) m=$2+0 } END { if (m=="") exit 1; print m }'
}

echo "================================================================"
echo " Routine Load 健康检查  ($(date '+%Y-%m-%d %H:%M:%S'))"
echo "================================================================"

# ---- 1) 作业状态 ----
for J in "$JOB_EVENT" "$JOB_ORDER"; do
    S=$(get_field "$J" State)
    [ -z "${S:-}" ] && S=$(get_field_vertical "$J" State)

    if [ -z "${S:-}" ]; then
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

# ---- 2) 摄入是否追平（OffsetLag）----
for J in "$JOB_EVENT" "$JOB_ORDER"; do
    LAG=$(get_field "$J" OffsetLag)
    [ -z "${LAG:-}" ] && LAG=$(get_field_vertical "$J" OffsetLag)

    if [ -z "${LAG:-}" ]; then
        printf '  ⚠️  %-24s 读不到 OffsetLag（版本差异，跳过滞后判定）\n' "$J 滞后"
        continue
    fi

    MAXLAG=$(json_max "$LAG" 2>/dev/null) || MAXLAG=""
    if [ -z "$MAXLAG" ]; then
        printf '  ⚠️  %-24s OffsetLag 解析失败：%s\n' "$J 滞后" "$LAG"
        continue
    fi

    if [ "$MAXLAG" -gt "$LAG_MAX" ]; then
        printf '  ❌ %-24s 最大 OffsetLag=%s > %s —— 摄入落后，检查 Kafka/作业\n' \
               "$J 滞后" "$MAXLAG" "$LAG_MAX"
        FAIL=1
    else
        printf '  ✅ %-24s 最大 OffsetLag=%s（≤%s，追平）\n' "$J 滞后" "$MAXLAG" "$LAG_MAX"
    fi
done

# ---- 3) 行数（只打印，不判定；绝对阈值会随数据量过期）----
E=$($MYSQL -N -B -e "SELECT count(*) FROM $DB.ods_order_event" 2>/dev/null)
O=$($MYSQL -N -B -e "SELECT count(*) FROM $DB.ods_order" 2>/dev/null)
printf '  %-27s %s\n' "ods_order_event 行数" "${E:-?}"
printf '  %-27s %s\n' "ods_order 行数" "${O:-?}"

# ---- 4) ods_order 不变式：行数 == 去重后的 order_id 数（实测成立，0 重复）----
O_UNIQ=$($MYSQL -N -B -e "SELECT count(DISTINCT order_id) FROM $DB.ods_order" 2>/dev/null)
if [ -n "${O:-}" ] && [ -n "${O_UNIQ:-}" ]; then
    if [ "$O" = "$O_UNIQ" ]; then
        printf '  ✅ %-24s 行数 == distinct order_id (%s)\n' "ods_order 不变式" "$O"
    else
        printf '  ❌ %-24s 行数(%s) ≠ distinct order_id(%s) —— 有重复订单行\n' \
               "ods_order 不变式" "$O" "$O_UNIQ"
        FAIL=1
    fi
fi

# ---- 5) 事件表重复率（软阈值：重放/重投本就有重复，异常重灌才会冲高）----
if [ -n "${E:-}" ]; then
    E_UNIQ=$($MYSQL -N -B -e \
      "SELECT count(DISTINCT concat(dt,'-',order_id,'-',event_type)) FROM $DB.ods_order_event" 2>/dev/null)
    if [ -n "${E_UNIQ:-}" ] && [ "$E_UNIQ" -gt 0 ] 2>/dev/null; then
        RATIO=$(awk -v a="$E" -v b="$E_UNIQ" 'BEGIN{printf "%.2f", a/b}')
        OVER=$(awk -v a="$E" -v b="$E_UNIQ" -v lim="$DUP_MAX" 'BEGIN{print (a*100 > b*lim) ? 1 : 0}')
        if [ "$OVER" = "1" ]; then
            printf '  ❌ %-24s 重复率 %s×（阈值 %s×）—— 疑似重灌/重复摄入\n' \
                   "事件表重复率" "$RATIO" "$(awk -v l="$DUP_MAX" 'BEGIN{printf "%.2f", l/100}')"
            FAIL=1
        else
            printf '  ✅ %-24s %s×（阈值 %s×，去重行数 %s）\n' \
                   "事件表重复率" "$RATIO" "$(awk -v l="$DUP_MAX" 'BEGIN{printf "%.2f", l/100}')" "$E_UNIQ"
        fi
    fi
fi

# ---- 6) 导入错误日志（有值说明有被过滤的行，值得人看一眼）----
for J in "$JOB_EVENT" "$JOB_ORDER"; do
    URLS=$(get_field "$J" ErrorLogUrls)
    if [ -n "${URLS:-}" ]; then
        printf '  ⚠️  %-24s 有导入错误日志：%s\n' "$J 错误" "$URLS"
    fi
done

# ---- 失败时打印作业详情，便于定位 ----
if [ "$FAIL" != "0" ]; then
    echo
    echo "--- 作业详情（排查用）---"
    $MYSQL -E -e "SHOW ROUTINE LOAD FROM $DB" 2>/dev/null \
      | grep -E "Name:|State:|ReasonOfStateChanged:|ErrorLogUrls:|OtherMsg:|Progress:|OffsetLag:" \
      | sed 's/^ */    /'
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
