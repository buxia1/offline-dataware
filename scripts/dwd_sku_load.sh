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

for DF in $DAYS; do
    D=$(echo "$DF" | tr -d '-')
    echo "处理 $DF  →  分区 p${D} ..."

    sed -e "s/\${D}/${D}/g" -e "s/\${DF}/${DF}/g" "$SQL_FILE" \
      | docker exec -i starrocks mysql -P9030 -h127.0.0.1 -uroot

    echo "  ✅ 完成"
done

echo

echo "全部完成"
