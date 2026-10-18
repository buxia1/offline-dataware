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

SQL_FILE="$(dirname "$0")/../sql/dqc/dqc_dim_product.sql"
if [ ! -f "$SQL_FILE" ]; then
    echo "❌ 找不到 SQL 文件: $SQL_FILE"
    exit 1
fi

# ---- 湖侧外部表：读之前必须先 REFRESH ----
# StarRocks 读 Iceberg 用的是【FE 缓存的元数据】。湖侧被 Spark 重写、或被维护脚本
# expire_snapshots 清掉旧快照之后，缓存里的元数据可能仍指向已删除的 snap-*.avro
# —— 实测 2026-10-18 就报过 `ERROR 1064 ... File does not exist: .../snap-xxx.avro`，
# 而且不刷新时也可能「对着旧快照假绿灯」。与 run_dws_ads_from_lake.sh / recon_lake_wh.sh 同一套做法。
for t in lake.dim.dim_product_scd2 lake.dwd.dwd_order_sku_detail lake.dwd.dwd_order_detail; do
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

echo "=== 当前状态 ==="
docker exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "
SELECT count(*) AS rows_, count(DISTINCT product_id) AS pids,
       COALESCE(sum(is_current),0) AS cur,
       round(COALESCE(sum(is_current),0)/NULLIF(count(DISTINCT product_id),0),2) AS ratio
FROM lake.dim.dim_product_scd2;"

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
