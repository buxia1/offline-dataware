#!/bin/bash
# 订单链路 DQC —— 修正版（状态查询改用订单链路的表：dwd_order_detail / dt）
set -e

SQL_FILE="$(dirname "$0")/../sql/dqc_order_chain.sql"
if [ ! -f "$SQL_FILE" ]; then
    echo "❌ 找不到 SQL 文件: $SQL_FILE"
    exit 1
fi

echo "=== 当前状态 ==="
docker exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "
SELECT count(*) AS dwd_rows, count(DISTINCT dt) AS dwd_days, COALESCE(sum(amount),0) AS dwd_amount
FROM dwd.dwd_order_detail;"

docker exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "
SELECT count(*) AS dws_rows, count(DISTINCT dt) AS dws_days
FROM dws.dws_user_order_day;"

echo "=== 校验（全部通过才有 0 行）==="
VIOLATIONS=$(docker exec -i starrocks mysql -P9030 -h127.0.0.1 -uroot -N -B < "$SQL_FILE")

if [ -z "$VIOLATIONS" ]; then
    echo "✅ 全部通过"
else
    echo "❌ 以下检查未通过："
    echo "$VIOLATIONS"
    echo
    echo "工作流将中断在此处，防止坏数据流向下游。"
    exit 1
fi
