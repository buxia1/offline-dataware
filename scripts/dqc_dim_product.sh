#!/bin/bash
# 商品链路数据质量检查（DQC）—— 给 DolphinScheduler 当节点用
#
# 【为什么是脚本而不是 SQL 节点】
#   SQL 节点只能"打印"结果，没法让工作流失败。
#   校验的意义在于"坏了就拦住"，所以必须有条件判断 + exit 1。
#
# 【为什么 SQL 放在 sql/ 文件里】
#   和逐天物化同一个理由：校验逻辑只写一份，避免节点内容与文件漂移。
set -e

SQL_FILE="$(dirname "$0")/../sql/dqc_dim_product.sql"
if [ ! -f "$SQL_FILE" ]; then
    echo "❌ 找不到 SQL 文件: $SQL_FILE"
    exit 1
fi

echo "=== 当前状态 ==="
docker exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "
SELECT count(*) AS rows_, count(DISTINCT product_id) AS pids,
       COALESCE(sum(is_current),0) AS cur,
       round(COALESCE(sum(is_current),0)/NULLIF(count(DISTINCT product_id),0),2) AS ratio
FROM dim.dim_product_scd2;"

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
