#!/bin/bash
# 订单链路 DQC
#
#   bash scripts/dqc_order_chain.sh              # 全表口径（手工核对用）
#   bash scripts/dqc_order_chain.sh 20260925     # 只查到这天（补数时由 DS 传入）
#
# 【为什么需要日期参数】
#   ②/⑤/⑤b 是【全表不变量】，而补数是【逐天重建】——中间态永远不一致。
#   不收窄范围的话，补数第 1 天就会 DQC 失败 → 实例 FAILURE → 串行补数中断 →
#   重建永远做不完（实测踩过）。
set -e

cd "$(dirname "$0")/.."
SQL_FILE="sql/dqc_order_chain.sql"

MYSQL="docker exec -i starrocks mysql -P9030 -h127.0.0.1 -uroot"
QUERY="docker exec starrocks mysql -P9030 -h127.0.0.1 -uroot -N -B -e"

# ---- 可选的业务日期（YYYYMMDD）；不传 = 全表 ----
ARG="${1:-}"
if [ -n "$ARG" ]; then
    AS_OF=$(printf '%s' "$ARG" | sed -E 's/^([0-9]{4})([0-9]{2})([0-9]{2})$/\1-\2-\3/')
else
    AS_OF="9999-12-31"
fi

if [ ! -f "$SQL_FILE" ]; then
    echo "❌ 找不到 SQL 文件: $SQL_FILE"
    exit 1
fi

echo "=== 当前状态（检查范围 dt <= $AS_OF）==="
$QUERY "SELECT count(*) AS dwd_rows, count(DISTINCT dt) AS dwd_days,
               COALESCE(sum(amount),0) AS dwd_amount
        FROM dwd.dwd_order_detail WHERE dt <= '$AS_OF';"
$QUERY "SELECT count(*) AS dws_rows, count(DISTINCT dt) AS dws_days
        FROM dws.dws_user_order_day WHERE dt <= '$AS_OF';"

# ---- 防线：替换占位符，并校验真的替换掉了（PITFALLS §7.2）----
TMP_SQL=$(mktemp /tmp/dqc_order_chain.XXXXXX.sql)
trap 'rm -f "$TMP_SQL"' EXIT

sed "s/\${AS_OF}/${AS_OF}/g" "$SQL_FILE" > "$TMP_SQL"
if grep -q '\${AS_OF}' "$TMP_SQL"; then
    echo "❌ 占位符 \${AS_OF} 未被替换 —— 拒绝执行（否则会静默查错范围）"
    exit 1
fi

echo "=== 校验（全部通过才有 0 行）==="
VIOLATIONS=$($MYSQL -N -B < "$TMP_SQL")

if [ -z "$VIOLATIONS" ]; then
    echo "✅ 全部通过"
else
    echo "❌ 以下检查未通过："
    echo "$VIOLATIONS"
    echo
    echo "工作流将中断在此处，防止坏数据流向下游。"
    exit 1
fi
