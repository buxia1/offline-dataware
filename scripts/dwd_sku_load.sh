#!/bin/bash
# 全量重物化 dwd_order_sku_detail：逐天关联订单 + 下单当天的商品属性
#
# 【为什么必须覆盖全部天，而不是只处理一天】
#   dim_product_scd2 是 TRUNCATE 全量重建的。重建一次，版本区间可能变，
#   所有天的匹配结果都可能不同 → 必须重算 dwd_order_detail 里出现过的每一天。
#
# 【为什么日期从库里查，不写死】
#   写死的日期列表在新增一天订单后会静默漏掉那一天 —— 不报错，只是少算。
#
# 【为什么 SQL 不内联在本脚本里】
#   内联会让同一段 SQL 存在两处（这里 + sql/dwd_order_sku_detail_load.sql），
#   改了一份忘了另一份就会跑出错误结果。
#   本脚本只负责：替换 ${D}/${DF} 两个占位符 + 逐天执行。
set -e

# 相对脚本自身定位 SQL 文件 —— 宿主和容器里都能找到，不依赖当前目录
SQL_FILE="$(dirname "$0")/../sql/dwd_order_sku_detail_load.sql"
if [ ! -f "$SQL_FILE" ]; then
    echo "❌ 找不到 SQL 文件: $SQL_FILE"
    exit 1
fi

# ---- 要处理哪些天：从 dwd_order_detail 动态取（只会拿到真正有数据的天）----
DAYS=$(docker exec starrocks mysql -P9030 -h127.0.0.1 -uroot -N -B \
       -e "SELECT DISTINCT dt FROM dwd.dwd_order_detail ORDER BY dt")

if [ -z "$DAYS" ]; then
    echo "❌ dwd_order_detail 里没有任何数据，先跑订单链路"
    exit 1
fi

echo "待处理日期："
echo "$DAYS"
echo

# ---- 确保分区存在 ----
# 【为什么需要这一段】
#   INSERT OVERWRITE ... PARTITION (p<日期>) 要求分区已经存在；
#   而动态分区只创建"未来"、不创建历史（PITFALLS §3.2）。
#   所以补数进来的历史日期必须先补分区 —— 以前靠手工，容易忘，忘了就失败。
#
# 【为什么必须用 trap】
#   补分区前要关掉 dynamic_partition.enable（PITFALLS §3.3）。
#   如果中途失败，set -e 会立刻退出，开关就永远停在 false ——
#   将来新分区不再自动创建，而且【不报错】。
#   所以要注册一个"无论怎么退出都执行"的恢复动作。
restore_dynamic_partition() {
    docker exec starrocks mysql -P9030 -h127.0.0.1 -uroot \
      -e "ALTER TABLE dwd.dwd_order_sku_detail SET ('dynamic_partition.enable' = 'true')" \
      >/dev/null 2>&1 || true
}

EXISTING=$(docker exec starrocks mysql -P9030 -h127.0.0.1 -uroot -N -B \
           -e "SHOW PARTITIONS FROM dwd.dwd_order_sku_detail" | cut -f2)

MISSING=""
for DF in $DAYS; do
    D=$(echo "$DF" | tr -d '-')
    # grep -x = 整行匹配。不加 -x 的话，"p2026092" 会误匹配 "p20260922"
    echo "$EXISTING" | grep -qx "p${D}" || MISSING="$MISSING $D"
done

if [ -z "$MISSING" ]; then
    echo "分区已齐全"
else
    echo "需要补的分区："
    for D in $MISSING; do echo "  p${D}"; done
    echo

    # 只在真要动分区时才注册恢复动作 —— 让 99% 的正常运行【完全不碰】开关
    trap restore_dynamic_partition EXIT

    # 拼成一段 DDL 一次喂给 mysql（和下面逐天物化是同一个套路）
    # VALUES 是左闭右开：上界必须写【下一天】（PITFALLS #4）
    {
        echo "ALTER TABLE dwd.dwd_order_sku_detail SET ('dynamic_partition.enable' = 'false');"
        for D in $MISSING; do
            DF2="${D:0:4}-${D:4:2}-${D:6:2}"
            NEXT=$(date -d "$DF2 + 1 day" +%Y-%m-%d)
            echo "ALTER TABLE dwd.dwd_order_sku_detail ADD PARTITION p${D} VALUES [('${DF2}'), ('${NEXT}'));"
        done
        echo "ALTER TABLE dwd.dwd_order_sku_detail SET ('dynamic_partition.enable' = 'true');"
    } | docker exec -i starrocks mysql -P9030 -h127.0.0.1 -uroot

    echo "✅ 分区已补齐"
fi
echo

for DF in $DAYS; do
    D=$(echo "$DF" | tr -d '-')
    echo "处理 $DF  →  分区 p${D} ..."

    sed -e "s/\${D}/${D}/g" -e "s/\${DF}/${DF}/g" "$SQL_FILE" \
      | docker exec -i starrocks mysql -P9030 -h127.0.0.1 -uroot

    echo "  ✅ 完成"
done

echo

echo "全部完成"
