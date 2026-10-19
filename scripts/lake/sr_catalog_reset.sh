#!/usr/bin/env bash
# =============================================================================
# 重置 StarRocks 的 lake catalog —— 清掉 Iceberg 元数据缓存
#
# 为什么需要：
#   sql/lake/lake_dim.sql 每天 DROP TABLE IF EXISTS + CREATE 重建 lake.dim 两张表。
#   DROP 会把旧 metadata 目录 purge 掉，新表的 Iceberg 元数据版本又从 v1 重新编号
#   （路径与旧表完全相同、内容不同）。StarRocks FE 的 Iceberg 元数据缓存按文件路径
#   缓存内容（内存缓存，iceberg_metadata_memory_cache_expiration_seconds 约 24h），
#   重建后缓存命中旧内容 → 查询去读一个已被删掉的 snap-xxxx.avro →
#   `ERROR 1064 ... File does not exist: .../snap-xxxx.avro`。
#   `REFRESH EXTERNAL TABLE` 清不掉这层缓存（2026-10-19 实测无效），
#   只有 DROP CATALOG + CREATE CATALOG 能真正清掉（实测立即恢复）。
#
# 安全性：只重建 StarRocks 里的 catalog 定义（纯元数据缓存），不碰湖里的数据文件。
#   重建瞬间若有并发查询在用 lake.* 会报错 → 放串行流程里、紧跟 dim 重建之后跑。
#
# 用法：bash scripts/lake/sr_catalog_reset.sh
# 环境变量：SR_CT（默认 starrocks）、LAKE_WAREHOUSE（默认 /opt/offline-dw/lake/warehouse）
# =============================================================================
set -euo pipefail

SR_CT="${SR_CT:-starrocks}"
LAKE_WAREHOUSE="${LAKE_WAREHOUSE:-/opt/offline-dw/lake/warehouse}"

sr_sql() {
  docker exec "$SR_CT" mysql -h127.0.0.1 -P9030 -uroot -N -B -e "$1"
}

echo "########## 重置 lake catalog（清 StarRocks Iceberg 元数据缓存） ##########"
sr_sql "DROP CATALOG IF EXISTS lake"
sr_sql "CREATE EXTERNAL CATALOG lake PROPERTIES (
  \"iceberg.catalog.warehouse\" = \"$LAKE_WAREHOUSE\",
  \"type\" = \"iceberg\",
  \"iceberg.catalog.type\" = \"hadoop\"
)"

# 校验：重置后必须能查通 dim 表（ddl 模式刚建表是 0 行，也允许，只要求能查）
# 判据 = 「能查通」，不是「有行数」：空表（刚 CREATE 完还没灌数）在 StarRocks 里 count(*) 可能返回 NULL
if ! sr_sql "SELECT * FROM lake.dim.dim_product_scd2 LIMIT 0" >/dev/null 2>&1; then
  echo "!!! catalog 重置后读不到 lake.dim.dim_product_scd2"; exit 1
fi
n="$(sr_sql "SELECT count(*) FROM lake.dim.dim_product_scd2" | tail -1 | tr -d '\r')"
echo "  校验：lake.dim.dim_product_scd2 = ${n:-0} 行（空表为 NULL，可查即通过）"

echo "✅ lake catalog 已重置（Iceberg 元数据缓存已清空）"
