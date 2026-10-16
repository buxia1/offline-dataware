#!/bin/bash
# =============================================================================
# 商品快照导入 ODS —— Stream Load 脚本
#
# 用法：
#     bash scripts/load_product_to_ods.sh data/dim/product_snapshot_20260920.csv
#
# 作用：把 gen_mock_products.py 产出的 CSV 推进 ods.ods_product
#
# 【为什么要脚本化，不能手敲】
#   1. snapshot_date 是关键参数，手敲容易错（错成 2026-09-20 会静默变 1997）
#      脚本从文件名自动解析，消除手误
#   2. 可以交给 DolphinScheduler 定时执行
#   3. 进 Git，可追溯
#
# 【为什么用 Stream Load 而不是 INSERT】
#   数据在文件系统上，不在库内。库内搬运才能用 INSERT ... SELECT。
# =============================================================================

set -e

# ---- 目标 StarRocks 主机 ----
# 宿主上直接跑：默认 localhost
# 在容器里跑（DS 调度时）：传 SR_HOST=starrocks
SR_HOST="${SR_HOST:-localhost}"

# ---- 参数校验 ----
CSV="$1"
if [ -z "$CSV" ]; then
    echo "用法: bash $0 <csv文件路径>"
    exit 1
fi
if [ ! -f "$CSV" ]; then
    echo "文件不存在: $CSV"
    exit 1
fi

# ---- 从文件名解析日期 ----
# data/dim/product_snapshot_20260920.csv
#   basename         → product_snapshot_20260920.csv
#   去掉后缀          → product_snapshot_20260920
#   取最后 8 位       → 20260920
D=$(basename "$CSV" .csv | rev | cut -c1-8 | rev)
DF="${D:0:4}-${D:4:2}-${D:6:2}"

# ---- 导入标签（每次运行唯一）----
# 标签是 StarRocks"同一次导入只生效一次"的保护，存在 FE 元数据里，
# TRUNCATE 清不掉它，默认保留 3 天（label_keep_max_second=259200）。
# 固定标签会导致重跑被 "Label Already Exists" 拒绝 → 工作流无法重跑。
# 本链路每次都是"清空 + 全量重灌"，重跑结果必然一致，不需要标签防重复。
LABEL="ods_product_${D}_$(date +%s)"


echo "CSV 文件   : $CSV"
echo "快照日期   : $D  →  $DF"
echo "目标主机   : $SR_HOST:8040"
echo "导入标签   : $LABEL"

# ---- Stream Load ----
# 【每个 header 的作用】
#   Authorization     : root + 空密码 的 base64（cm9vdDo=）
#   Expect            : 关掉 curl 的 100-continue 探测，避免某些代理卡住
#   format            : 文件格式，csv 或 json
#   column_separator  : 列分隔符
#   skip_header       : 跳过第 1 行表头（不加的话表头会被当数据，报错）
#   strict_mode       : 严格模式，类型转换失败的行会被过滤掉而不是塞脏值
#   max_filter_ratio  : 允许过滤比例，0 = 一行都不许错，错了整体失败
#   columns           : 【关键】声明"文件列 → 表列"的映射
#                       前 7 个是普通列名，按 CSV 顺序对应
#                       最后 snapshot_date='...' 是常量表达式：不从文件读，直接给值
#                       ⚠️ 单引号不能省！写成 snapshot_date=2026-09-20 会被当算术式
#                          算成 2026-9-20 = 1997
#   label             : 事务标签。同一个 label 重复提交会被拒绝（幂等保护）
#   -T                : upload-file，把文件内容 PUT 上去
RESP=$(curl -s -L -X PUT \
  "http://${SR_HOST}:8040/api/ods/ods_product/_stream_load" \
  -H "Authorization: Basic cm9vdDo=" \
  -H "Expect: 100-continue" \
  -H "format: csv" \
  -H "column_separator: ," \
  -H "skip_header: 1" \
  -H "strict_mode: true" \
  -H "max_filter_ratio: 0" \
  -H "columns: product_id, product_name, category, brand, price, status, update_time, snapshot_date='${DF}'" \
  -H "label: ${LABEL}" \
  -T "$CSV")

echo "$RESP"

# ---- 检查结果 ----
# ⚠️ curl 返回 0 不代表导入成功！必须看返回体里的 Status 字段。
#    这正是"导入成功但 0 行数据"那个坑的根源。
STATUS=$(echo "$RESP" | python3 -c "
import sys, json
try:
    print(json.load(sys.stdin).get('Status','UNKNOWN'))
except Exception:
    print('PARSE_FAIL')
")
LOADED=$(echo "$RESP" | python3 -c "
import sys, json
try:
    print(json.load(sys.stdin).get('NumberLoadedRows',-1))
except Exception:
    print(-1)
")

if [ "$STATUS" != "Success" ]; then
    echo ""
    echo "❌ 导入失败: Status=$STATUS"
    exit 1
fi

echo ""
echo "✅ 导入成功，写入 ${LOADED} 行"
