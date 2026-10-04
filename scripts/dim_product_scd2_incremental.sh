#!/bin/bash
# =============================================================================
# SCD2 拉链表【增量维护】—— 只处理比现有最新版本更新的快照
#
# 背景：sql/dim_product_scd2_load.sql 是【全量重建】（TRUNCATE + 全量重推），
#       快照天数一多就变慢。这个脚本改成增量：只 UPDATE 关闭旧的当前版本、
#       再 INSERT 新版本。
#
# 运行环境：DolphinScheduler 的 Shell 任务节点（脚本内部要 docker exec）
#
# 【为什么 SQL 里要放 ${LAST} 占位符，而不是写 (SELECT MAX(...)) 】
#   起点显式可见，排查时一眼能看出"这次从哪天开始处理"。
#   代价是必须有这个脚本做替换 —— 直接用 mysql < 文件 跑是【静默 no-op】：
#       '${LAST}' 转 DATE 是 NULL，而 snapshot_date > NULL 恒为 NULL（不是 TRUE），
#       一行都命中不了，而且不报错。
#
# 【三道防线（缺一不可）】
#   ① 无新快照 → 明确打印并退出（不是悄悄什么都不做）
#   ② 替换后校验占位符已消失（否则立刻失败，而不是静默 no-op）
#   ③ 跑完做不变式校验（区间无断裂 + 每商品恰好一个当前版本）
# =============================================================================
set -e

cd "$(dirname "$0")/.."     # 切到仓库根目录，容器内是 /opt/offline-dw
SQL_FILE="sql/dim_product_scd2_incremental.sql"

MYSQL="docker exec -i starrocks mysql -P9030 -h127.0.0.1 -uroot"
QUERY="docker exec starrocks mysql -P9030 -h127.0.0.1 -uroot -N -B -e"

if [ ! -f "$SQL_FILE" ]; then
    echo "❌ 找不到 SQL 文件: $SQL_FILE"
    exit 1
fi

# ---- 起点：SCD2 现有的最新版本日 ----
LAST=$($QUERY "SELECT COALESCE(MAX(valid_from), '1970-01-01') FROM dim.dim_product_scd2")
echo "SCD2 起点（已处理到）: $LAST"

# ---- 上游：ODS 最新快照日 ----
ODS_MAX=$($QUERY "SELECT COALESCE(MAX(snapshot_date), '1970-01-01') FROM ods.ods_product")
echo "ODS 最新快照        : $ODS_MAX"

# ---- 防线①：没有新快照就明确退出（不是静默什么都不做）----
if [ "$ODS_MAX" \< "$LAST" ] || [ "$ODS_MAX" = "$LAST" ]; then
    echo "无新快照，无需处理"
    exit 0
fi

# ---- 有多少新快照 ----
NEW_DAYS=$($QUERY "SELECT count(DISTINCT snapshot_date) FROM ods.ods_product WHERE snapshot_date > '$LAST'")
echo "待处理快照天数      : $NEW_DAYS"
echo

# ---- 防线②：替换占位符，并校验真的替换掉了 ----
TMP_SQL=$(mktemp /tmp/scd2_inc.XXXXXX.sql)
trap 'rm -f "$TMP_SQL"' EXIT

sed "s/\${LAST}/${LAST}/g" "$SQL_FILE" > "$TMP_SQL"

if grep -q '\${LAST}' "$TMP_SQL"; then
    echo "❌ 占位符 \${LAST} 未被替换 —— 拒绝执行（否则会静默 no-op）"
    exit 1
fi

echo "=== 执行增量（UPDATE 关闭旧版本 + INSERT 新版本）==="
$MYSQL < "$TMP_SQL"
echo "✅ 增量执行完成"
echo

# ---- 防线③：不变式校验 ----
echo "=== 不变式校验 ==="

ROWS=$($QUERY "SELECT count(*) FROM dim.dim_product_scd2")
PIDS=$($QUERY "SELECT count(DISTINCT product_id) FROM dim.dim_product_scd2")
CUR=$($QUERY "SELECT COALESCE(sum(is_current), 0) FROM dim.dim_product_scd2")
GAPS=$($QUERY "
    SELECT count(*) FROM (
        SELECT product_id, valid_to,
               LEAD(valid_from) OVER (PARTITION BY product_id ORDER BY valid_from) AS next_from
        FROM dim.dim_product_scd2
    ) t
    WHERE next_from IS NOT NULL
      AND valid_to <> DATE '9999-12-31'
      AND DATE_ADD(valid_to, INTERVAL 1 DAY) <> next_from")

echo "  行数            : $ROWS"
echo "  商品数          : $PIDS"
echo "  当前版本数      : $CUR   （必须等于商品数）"
echo "  区间断裂数      : $GAPS  （必须为 0）"

FAIL=0
if [ "$CUR" != "$PIDS" ]; then
    echo "❌ 当前版本数($CUR) ≠ 商品数($PIDS) —— 有商品没有当前版本，或有多个"
    FAIL=1
fi
if [ "$GAPS" != "0" ]; then
    echo "❌ 区间有断裂 —— 那些天的订单会匹配不上维度（孤儿行）"
    FAIL=1
fi

if [ "$FAIL" != "0" ]; then
    echo
    echo "工作流将中断在此处。修复方式：跑全量重建 sql/dim_product_scd2_load.sql"
    exit 1
fi

echo "✅ 全部通过（区间连续、每商品恰好一个当前版本）"
