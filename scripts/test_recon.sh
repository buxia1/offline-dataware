#!/usr/bin/env bash
# =============================================================================
# recon_lake_wh.sh 的对抗性测试：注入故障 → 确认 gate 真的会红 → 还原
#
# 为什么必须有这个测试：一个「只读、永远 OK」的对账脚本毫无价值。
# 这里的每个用例都是「先把数据弄坏，看脚本是否报错，再修回去看是否转绿」。
#
# 用例：
#   T1 基线                      期望 EXIT=0，26 项通过
#   T2 破坏仓侧 dws 一天（区间内） 期望 EXIT=2（C 段对称差 / D' 基线）
#   T3 删掉 ads 一天              期望 EXIT=2（D 段覆盖 / 行数）
#   T4 子区间模式                 期望 EXIT=0，且跳过全量基线段
#   T5 破坏湖侧 DWD 闭合          默认 EXIT=0 但告警；--strict EXIT=2
#   T6 全部还原后复跑             期望 EXIT=0
#
# 用法：bash test_recon.sh
# =============================================================================
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
RECON="$HERE/recon_lake_wh.sh"
PROJ="/home/l/offline-dw"

PASS=0
FAIL=0
run() { # run <标签> <期望退出码> <grep 必须命中> <grep 必须不命中(可空)> <参数...>
  local label="$1" want="$2" must="$3" mustnot="$4"; shift 4
  local out rc
  out="$(bash "$RECON" "$@" 2>&1)"; rc=$?
  local ok=1 why=""
  [ "$rc" = "$want" ] || { ok=0; why="退出码 $rc != $want"; }
  if [ -n "$must" ] && ! printf '%s' "$out" | grep -q "$must"; then
    ok=0; why="$why; 未命中 /$must/"
  fi
  if [ -n "$mustnot" ] && printf '%s' "$out" | grep -q "$mustnot"; then
    ok=0; why="$why; 不该命中 /$mustnot/"
  fi
  if [ "$ok" = 1 ]; then
    echo "  [PASS] $label（EXIT=$rc）"; PASS=$((PASS + 1))
  else
    echo "  [FAIL] $label -> $why"; FAIL=$((FAIL + 1))
    printf '%s\n' "$out" | grep -E '\[FAIL\]|\[WARN\]|####' | head -12 | sed 's/^/         /'
  fi
}

sr() { docker exec -i starrocks mysql -P9030 -h127.0.0.1 -uroot --batch --raw --skip-column-names -e "$1"; }
restore_day() { bash "$PROJ/scripts/run_dws_ads_from_lake.sh" --range "$1" "$1" >/dev/null 2>&1; }

echo "########## T1 基线 ##########"
run "T1 基线应通过" 0 "对账通过" ""

echo "########## T2 破坏仓侧 dws 一天（2026-12-18） ##########"
sr "UPDATE dws.dws_user_order_day SET order_cnt = order_cnt - 1000, paid_amount = paid_amount - 100 WHERE dt = '2026-12-18'"
run "T2 破坏后必须报红" 2 "dws_mismatch" ""
restore_day 2026-12-18
run "T2 还原后应转绿" 0 "对账通过" ""

echo "########## T3 删掉 ads 一天（2026-12-18） ##########"
sr "DELETE FROM ads.ads_daily_sales WHERE dt = '2026-12-18'"
run "T3 删行后必须报红" 2 "ads_mismatch" ""
restore_day 2026-12-18
run "T3 还原后应转绿" 0 "对账通过" ""

echo "########## T4 子区间模式（2026-12-01 ~ 2026-12-02） ##########"
# 子区间只校验区间内 → 检查数从 26 降到 11，且 D' 段必须明确「已跳过」
run "T4 子区间应通过且跳过全量基线" 0 "对账通过（11 项）" "" --range 2026-12-01 2026-12-02

echo "########## T5 破坏湖侧 DWD 闭合（插入一行 2026-12-18） ##########"
sr "INSERT INTO lake.dwd.dwd_order_detail (order_id, dt, user_id, product_id, amount, order_time, status) VALUES (2026121899999, '2026-12-18', 99999999, 1001, 123.45, '2026-12-18 10:00:00', 'paid')" \
  || echo "  [WARN] 插入失败，T5 跳过"
# 注意：插入这一行会同时造成 C 段对称差 / D 段行数漂移（湖侧多一行、仓侧还没算），
# 所以整脚本本来就该 EXIT=2。T5a 要验的是「闭合这一项只告警、不计入失败项」。
run "T5a 默认只告警（闭合不计入失败项）" 2 "按天闭合不一致：1 天（默认只告警" "按天闭合不一致：1 天（--strict"
run "T5b --strict 必须把闭合计为失败" 2 "按天闭合不一致：1 天（--strict 下计为失败）" "" --strict

echo "  -- 还原湖侧 DWD（run_dwd.sh build 单日，幂等）--"
bash "$PROJ/scripts/lake/run_dwd.sh" build dwd_order_detail 2026-12-18 2026-12-18 2>&1 | grep -E 'RESULT|ERROR' | sed 's/^/         /'
sr "REFRESH EXTERNAL TABLE lake.dwd.dwd_order_detail" >/dev/null 2>&1
restore_day 2026-12-18

echo "########## T6 全部还原后复跑 ##########"
run "T6 收尾应通过" 0 "对账通过" ""

echo
echo "========== 汇总：PASS=$PASS FAIL=$FAIL =========="
echo "湖侧 dwd_order_detail = $(sr "SELECT COUNT(*) FROM lake.dwd.dwd_order_detail")（应 854844）"
echo "合成用户残留 = $(sr "SELECT COUNT(*) FROM lake.dwd.dwd_order_detail WHERE user_id = 99999999")（应 0）"
echo "仓侧 dws/ads = $(sr "SELECT COUNT(*) FROM dws.dws_user_order_day")/$(sr "SELECT COUNT(*) FROM ads.ads_daily_sales")（应 18000/90）"
[ "$FAIL" = 0 ] || exit 2
exit 0
