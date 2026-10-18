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
SQL_FILE="sql/dqc/dqc_order_chain.sql"

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

# ---- 湖侧外部表：读之前必须先 REFRESH ----
# StarRocks 读 Iceberg 用的是【FE 缓存的元数据】。湖侧被 Spark 重写、或被维护脚本
# expire_snapshots 清掉旧快照之后，缓存里的元数据可能仍指向已删除的 snap-*.avro
# —— 实测 2026-10-18 就报过 `ERROR 1064 ... File does not exist: .../snap-xxx.avro`，
# 而且不刷新时也可能「对着旧快照假绿灯」。与 run_dws_ads_from_lake.sh / recon_lake_wh.sh 同一套做法。
for t in lake.dwd.dwd_order_detail lake.ods.ods_order; do
    ok=0
    for attempt in 1 2 3; do
        if docker exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "REFRESH EXTERNAL TABLE $t" >/dev/null 2>&1; then
            echo "  REFRESH $t ✅"
            ok=1
            break
        fi
        echo "  REFRESH $t 第 $attempt 次失败，1 秒后重试…"
        sleep 1
    done
    if [ "$ok" -ne 1 ]; then
        echo "  REFRESH $t ❌ —— 拒绝在缓存元数据上做校验（否则可能对着旧快照假绿灯）"
        exit 1
    fi
done

echo "=== 当前状态（检查范围 dt <= $AS_OF）==="
$QUERY "SELECT count(*) AS dwd_rows, count(DISTINCT dt) AS dwd_days,
               COALESCE(sum(amount),0) AS dwd_amount
        FROM lake.dwd.dwd_order_detail WHERE dt <= '$AS_OF';"
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
