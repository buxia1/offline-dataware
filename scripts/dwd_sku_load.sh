#!/bin/bash
# 逐天把 dwd_order_detail + dim_product_scd2 关联，写入 dwd_order_sku_detail
set -e

for D in 20260920 20260921 20260926 20260927; do
    DF="${D:0:4}-${D:4:2}-${D:6:2}"
    echo "处理 $DF ..."

    docker exec starrocks mysql -P9030 -h127.0.0.1 -uroot -e "
INSERT OVERWRITE dwd.dwd_order_sku_detail PARTITION (p${D})
SELECT
    o.order_id, o.dt, o.user_id, o.product_id, o.amount, o.order_time, o.status,
    s.category, s.brand, s.price AS sku_price, s.valid_from, s.valid_to
FROM dwd.dwd_order_detail o
JOIN dim.dim_product_scd2 s
  ON  o.product_id = s.product_id
 AND  o.dt BETWEEN s.valid_from AND s.valid_to
WHERE o.dt = '${DF}';"
done

echo "全部完成"
