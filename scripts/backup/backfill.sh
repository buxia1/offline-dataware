#!/usr/bin/env bash
# 按 dt 补数：湖侧重放(可选) -> 闸门 -> dim -> dwd -> 仓侧物化 -> DQC -> 对账
# 用法: bash scripts/backfill.sh 2026-10-15 [--with-replay] [--skip-recon]
# 注意: 手工补数不会写台账；要留痕就手工追加一行、notes 写 backfill（避免污染 7 天判据）
set -euo pipefail
ROOT=${ROOT:-/home/l/offline-dw}
DT=${1:?用法: backfill.sh YYYY-MM-DD [--with-replay] [--skip-recon]}; D8=${DT//-/}
WITH_REPLAY=0; SKIP_RECON=0; shift || true
for a in "$@"; do case "$a" in
  --with-replay) WITH_REPLAY=1 ;; --skip-recon) SKIP_RECON=1 ;;
  *) echo "未知参数: $a" >&2; exit 1 ;; esac; done
cd "$ROOT"; step(){ printf '\n[%s] ==== %s ====\n' "$(date +%H:%M:%S)" "$*"; }

if [ "$WITH_REPLAY" = 1 ]; then
  step "1 湖侧 ODS 重放 $DT（需该天消息仍在 Kafka 30 天窗口内）"
  bash scripts/lake/lake_ods_replay.sh --date "$DT"
else
  step "1 跳过 ODS 重放（lake.ods 已有该天；若缺/坏，加 --with-replay）"
fi
step "2 湖侧 ODS 闸门（6 项绝对断言）"; bash scripts/dqc_lake_ods.sh "$D8"
step "3 维度重建"; bash scripts/lake/run_dim.sh build
step "4 DWD 增量（--lookback 3 覆盖迟到事件）"; bash scripts/lake/run_dwd.sh incremental all --lookback 3
step "5 仓侧 dws/ads 物化（单天区间，幂等）"; bash scripts/run_dws_ads_from_lake.sh --range "$DT" "$DT"
step "6 DQC"; bash scripts/dqc_order_chain.sh "$D8"; bash scripts/dqc_dim_product.sh
if [ "$SKIP_RECON" = 0 ]; then
  step "7 湖↔仓对账 26 项"
  set +e; bash scripts/recon_lake_wh.sh; rc=$?; set -e
  if [ "$rc" = 2 ]; then echo "FATAL: 对账不一致（退出码 2）" >&2; exit 1; fi
  [ "$rc" = 0 ] || { echo "FATAL: 对账脚本异常（退出码 $rc）" >&2; exit 1; }
fi
cat <<'EOF'

补数完成。三个必须记住的点：
1) 台账由工作流第 11 节点写，手工补数不会留痕；要留痕就手工追加一行、notes 写 backfill。
2) 补的是 lake.ods 时，该天 dwd 必须重算（DWD 只在单天内去重，坑 #5）——第 4 步已覆盖。
3) 补数窗口 = Kafka 保留 30 天；超窗日期只能从 warehouse 备份恢复（手册 §4 S3）。
EOF
