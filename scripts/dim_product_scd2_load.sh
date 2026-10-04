#!/bin/bash
# =============================================================================
# 商品 SCD2 拉链表装载 —— 统一入口
#
#   bash dim_product_scd2_load.sh            # 默认：增量维护
#   bash dim_product_scd2_load.sh --full     # 强制全量重建（TRUNCATE + 重推）
#   bash dim_product_scd2_load.sh --inc      # 显式增量（等同默认）
#
# 【为什么要这个包装脚本，而不是让 DS 节点直接指向两个脚本之一】
#   ① 默认必须是增量（快照天数多了全量会越来越慢）
#   ② 但【全量重建必须留得下来】—— 它是增量逻辑被改坏时唯一的恢复手段
#      （见 PITFALLS §3.16：增量写错会静默多版本，那时只能靠全量重建回正）
#   ③ 开关放在脚本参数里 → 恢复时不用改 DS 节点、不用导出导入工作流
#
# 【两个 SQL 的分工】
#   sql/dim_product_scd2_incremental.sql  （增量）
#       UPDATE 关闭旧的当前版本 + INSERT 新版本
#       需要外壳替换 ${LAST}，见 scripts/dim_product_scd2_incremental.sh
#   sql/dim_product_scd2_load.sql         （全量）
#       TRUNCATE + 全量重推，永远从 ODS 完整推导
#
# 运行环境：DolphinScheduler 的 Shell 任务节点（脚本内部要 docker exec）
# =============================================================================
set -e

cd "$(dirname "$0")/.."     # 切到仓库根目录，容器内是 /opt/offline-dw

MODE="${1:---inc}"

case "$MODE" in
    --inc|--incremental)
        echo "=== 模式：增量维护（默认）==="
        exec bash scripts/dim_product_scd2_incremental.sh
        ;;
    --full|--full-rebuild)
        echo "=== 模式：全量重建（TRUNCATE + 从 ODS 完整重推）==="
        echo "⚠️  会清空 dim.dim_product_scd2 后重建 —— 这是增量逻辑坏掉时的恢复手段"
        docker exec -i starrocks mysql -P9030 -h127.0.0.1 -uroot < sql/dim_product_scd2_load.sql
        echo "✅ 全量重建完成"
        ;;
    *)
        echo "❌ 未知参数: $MODE"
        echo "用法: bash scripts/dim_product_scd2_load.sh [--inc|--full]"
        exit 1
        ;;
esac
