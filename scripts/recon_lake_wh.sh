#!/usr/bin/env bash
# =============================================================================
# 湖 ↔ 仓 对账（只读，可独立调度）
#
# 定位：run_dws_ads_from_lake.sh 的自检只覆盖【本次回刷的窗口】，窗口外的漂移它看不见
#       （C2 段只在默认增量模式下额外探一段）。本脚本是独立的、全区间、只读的对账作业，
#       建议排在每日物化【之后】作为一个独立节点；非 0 退出即触发 DS 告警。
#
# 四层校验（分层是有意的，别只看最后一层）：
#   A 水位 / 新鲜度    湖侧三张 DWD 的行数·天数·水位；仓侧 dws/ads 的覆盖区间
#   B 湖侧闭合         DWD 每天行数 vs 由 ODS【现算】的期望 —— 独立源！
#                      这是唯一能发现「湖仓两边一起错」（例如 DWD 自己漏了一天）的检查；
#                      对称差永远发现不了这种错。
#   C 湖↔仓 对称差     仓侧 dws/ads 落库值 vs 由 lake.dwd 现算的期望，整行逐字段比对
#   D 仓侧覆盖 / 基线  行数硬 gate、区间有无空洞、全区间指标基线
#
# 退出码：0 全绿 / 2 对账不一致（→ DS 告警）/ 1 用法或环境错误
#
# 用法：
#   bash scripts/recon_lake_wh.sh                  # 全区间（D1=FULL_MIN，D2=湖侧最新日）
#   bash scripts/recon_lake_wh.sh --range D1 D2    # 只对账某区间
#   bash scripts/recon_lake_wh.sh --fast           # 跳过 B（湖侧闭合，最贵的那段）
#   bash scripts/recon_lake_wh.sh --strict         # 湖侧闭合不一致也算失败（默认只 WARN）
#   bash scripts/recon_lake_wh.sh --allow-lag 1    # 容忍仓侧比湖侧晚 N 天（默认 0 = 必须追平）
#   bash scripts/recon_lake_wh.sh --json           # 只输出 KEY|VALUE（给下游节点/告警解析）
#
# ★ 为什么开头一定要 REFRESH：StarRocks 读 Iceberg 用的是【缓存的元数据】。湖侧被 Spark
#   写过之后不刷新，StarRocks 会继续返回旧快照 —— 那会让本脚本「拿旧快照当基准」，
#   于是两边一起错、对账还全绿。详见 docs/PITFALLS.md 3.18。
#
# ★ 本脚本【只读】：除了 REFRESH EXTERNAL TABLE（读路径的元数据刷新）之外不写任何数据。
# =============================================================================
set -euo pipefail

CT="starrocks"
MYSQL_BASE=(docker exec -i "$CT" mysql -P9030 -h127.0.0.1 -uroot --batch --raw --skip-column-names)
mysql_q()    { "${MYSQL_BASE[@]}" -e "$1"; }
mysql_file() { "${MYSQL_BASE[@]}" < "$1"; }

log()  { printf '%s\n' "$*"; }
warn() { printf '[WARN] %s\n' "$*" >&2; }
fail() { printf '[FAIL] %s\n' "$*" >&2; }
die()  { printf '[ERROR] %s\n' "$*" >&2; exit 1; }

_TMPD="$(mktemp -d "${TMPDIR:-/tmp}/recon_lake_wh.XXXXXX")"
cleanup_tmp() { rm -rf "$_TMPD"; }
trap cleanup_tmp EXIT INT TERM
PROBE_SQL="$_TMPD/probe.sql"
AB_SQL="$_TMPD/ab.sql"
CD_SQL="$_TMPD/cd.sql"

# ---------------------------------------------------------------- 常量 / 基线
# 全区间物化后的验收基线。只在请求区间 == [FULL_MIN, FULL_MAX] 时才作为 gate 启用，
# 否则只打印、不判定（数据长过 FULL_MAX 之后这些常量会自然失效，别让它们挡住日常对账）。
FULL_MIN="2026-09-20"
FULL_MAX="2026-12-18"
EXP_DWS_ROWS=18000
EXP_DWS_DAYS=90
EXP_OC=854844
EXP_PA="119749476.90"
EXP_RA="14069110.40"
EXP_ADS_ROWS=90
EXP_ADS_DAYS=90
EXP_PC=726511
EXP_NA="105680366.50"
EXP_AOA_SUM="14834.44"
EXP_PR_SUM="76.4891"

# 湖侧外部表：读之前必须先 REFRESH。critical=1 刷新失败 → die（本脚本的基准全来自它们，
# 拿旧快照继续跑 = 假绿）。格式：表名|是否关键|失败提示
LAKE_TABLES=(
  "lake.dwd.dwd_order_detail|1|D2 水位、C 段对称差、D 段期望行数全部来自它"
  "lake.dwd.dwd_order_sku_detail|1|B 段闭合检查用到"
  "lake.dwd.dwd_order_lifecycle|1|B 段闭合检查用到"
  "lake.ods.ods_order|1|B 段 detail/sku 的期望来源（独立源，不能用旧快照）"
  "lake.ods.ods_order_event|1|B 段 lifecycle 的期望来源（独立源，不能用旧快照）"
)

# ------------------------------------------------------------------ 参数解析
RANGE_LO=""
RANGE_HI=""
FAST=0
STRICT=0
LAG_ALLOW=0
JSON=0
while [ "$#" -gt 0 ]; do
  case "$1" in
    --range)     [ "$#" -ge 3 ] || die "--range 需要两个日期：--range D1 D2"
                 RANGE_LO="$2"; RANGE_HI="$3"; shift 2 ;;
    --fast)      FAST=1 ;;
    --strict)    STRICT=1 ;;
    --allow-lag) [ "$#" -ge 2 ] || die "--allow-lag 需要天数"
                 LAG_ALLOW="$2"; shift ;;
    --json)      JSON=1 ;;
    -h|--help)   sed -n '2,40p' "$0"; exit 0 ;;
    *)           die "未知参数：$1（-h 看用法）" ;;
  esac
  shift
done

case "$LAG_ALLOW" in ''|*[!0-9]*) die "--allow-lag 需要非负整数，收到：$LAG_ALLOW" ;; esac

add_days()   { date -d "$1 $2 days" +%F; }
days_between() { echo $(( ( $(date -d "$2" +%s) - $(date -d "$1" +%s) ) / 86400 )); }
is_date()    { [[ "$1" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; }

[ -n "$RANGE_LO" ] && { is_date "$RANGE_LO" || die "--range 的 D1 不是 YYYY-MM-DD：$RANGE_LO"; }
[ -n "$RANGE_HI" ] && { is_date "$RANGE_HI" || die "--range 的 D2 不是 YYYY-MM-DD：$RANGE_HI"; }

# ---------------------------------------------------------- 0. 环境 + 刷新湖表
docker inspect -f '{{.State.Running}}' "$CT" >/dev/null 2>&1 \
  || die "容器 $CT 不存在。先 docker compose up -d starrocks"

[ "$JSON" = 1 ] || log "########## 湖 ↔ 仓 对账（只读） ##########"
[ "$JSON" = 1 ] || log "  刷新湖侧外部表元数据（StarRocks 读 Iceberg 走缓存快照）…"
for item in "${LAKE_TABLES[@]}"; do
  IFS='|' read -r t critical note <<< "$item"
  if mysql_q "REFRESH EXTERNAL TABLE $t" >/dev/null 2>&1; then
    continue
  fi
  if [ "$critical" = 1 ]; then
    die "关键外部表刷新失败：$t
    —— $note
    REFRESH 失败时 StarRocks 仍会返回【缓存里的旧快照】，继续对账会「两边一起错」还显示通过。
    先确认湖侧可读、external catalog lake 正常：
      docker exec -i starrocks mysql -P9030 -h127.0.0.1 -uroot -e 'REFRESH EXTERNAL TABLE $t'"
  fi
  warn "刷新外部表失败：$t —— $note"
done

# ------------------------------------------- 1. 探水位（用来算默认对账区间）
cat > "$PROBE_SQL" <<'SQL'
SELECT CONCAT('p_lake_min|', COALESCE(CAST(MIN(dt) AS STRING),'-')) FROM lake.dwd.dwd_order_detail;
SELECT CONCAT('p_lake_max|', COALESCE(CAST(MAX(dt) AS STRING),'-')) FROM lake.dwd.dwd_order_detail;
SELECT CONCAT('p_ods_max|',  COALESCE(CAST(MAX(dt) AS STRING),'-')) FROM lake.ods.ods_order;
SELECT CONCAT('p_evt_max|',  COALESCE(CAST(MAX(dt) AS STRING),'-')) FROM lake.ods.ods_order_event;
SELECT CONCAT('p_wh_dws_min|', COALESCE(CAST(MIN(dt) AS STRING),'-')) FROM dws.dws_user_order_day;
SELECT CONCAT('p_wh_dws_max|', COALESCE(CAST(MAX(dt) AS STRING),'-')) FROM dws.dws_user_order_day;
SELECT CONCAT('p_wh_ads_max|', COALESCE(CAST(MAX(dt) AS STRING),'-')) FROM ads.ads_daily_sales;
SQL
PROBE_OUT="$(mysql_file "$PROBE_SQL")" || die "探水位失败（SQL 执行错误，先确认 external catalog lake 正常）"

pget() { printf '%s\n' "$PROBE_OUT" | grep "^$1|" | head -n1 | cut -d'|' -f2 || true; }

LAKE_MIN="$(pget p_lake_min)"
LAKE_MAX="$(pget p_lake_max)"
ODS_MAX="$(pget p_ods_max)"
EVT_MAX="$(pget p_evt_max)"
WH_DWS_MIN="$(pget p_wh_dws_min)"
WH_DWS_MAX="$(pget p_wh_dws_max)"
WH_ADS_MAX="$(pget p_wh_ads_max)"

[ "$LAKE_MIN" = "-" ] || [ -n "$LAKE_MIN" ] || die "读不到 lake.dwd.dwd_order_detail 的水位"
[ "$LAKE_MAX" = "-" -o -z "$LAKE_MAX" ] && die "lake.dwd.dwd_order_detail 是空表，没有可对账的数据"

if [ -n "$RANGE_LO" ]; then
  D1="$RANGE_LO"; D2="$RANGE_HI"
else
  D1="$FULL_MIN"; D2="$LAKE_MAX"
fi
[ "$D1" '<' "$D2" ] || [ "$D1" = "$D2" ] || die "区间倒挂：D1=$D1 > D2=$D2"

FULL_RANGE=0
[ "$D1" = "$FULL_MIN" ] && [ "$D2" = "$FULL_MAX" ] && FULL_RANGE=1

[ "$JSON" = 1 ] || log "  对账区间：$D1 ~ $D2$( [ "$FULL_RANGE" = 1 ] && printf '（= 全量基线区间）' )"

# ------------------------------------------------------------------ A + B
{
  cat <<SQL
-- A. 水位 / 新鲜度
SELECT CONCAT('lake_detail|', COUNT(*), '|', COUNT(DISTINCT dt), '|', COALESCE(CAST(MIN(dt) AS STRING),'-'), '|', COALESCE(CAST(MAX(dt) AS STRING),'-'), '|', COALESCE(CAST(SUM(amount) AS STRING),'-')) FROM lake.dwd.dwd_order_detail;
SELECT CONCAT('lake_sku|', COUNT(*), '|', COUNT(DISTINCT dt), '|', COALESCE(CAST(SUM(amount) AS STRING),'-')) FROM lake.dwd.dwd_order_sku_detail;
SELECT CONCAT('lake_lifecycle|', COUNT(*), '|', COUNT(DISTINCT order_id), '|', COALESCE(CAST(SUM(amount) AS STRING),'-')) FROM lake.dwd.dwd_order_lifecycle;
SELECT CONCAT('ods_order|', COUNT(*), '|', COALESCE(CAST(MAX(dt) AS STRING),'-')) FROM lake.ods.ods_order;
SELECT CONCAT('ods_event|', COUNT(*), '|', COALESCE(CAST(MAX(dt) AS STRING),'-')) FROM lake.ods.ods_order_event;
SQL
  if [ "$FAST" = 0 ]; then
    cat <<SQL
-- B. 湖侧闭合：DWD 每天行数 vs 由 ODS【现算】的期望（独立源）
SELECT CONCAT('clo_detail|', COUNT(*)) FROM (
  SELECT dt FROM (
      SELECT dt, COUNT(*) n FROM lake.dwd.dwd_order_detail WHERE dt >= '$D1' AND dt <= '$D2' GROUP BY dt
      UNION ALL
      SELECT dt, SUM(CASE WHEN user_id IS NOT NULL THEN 1 ELSE 0 END) n
        FROM lake.ods.ods_order WHERE dt >= '$D1' AND dt <= '$D2' GROUP BY dt
  ) t GROUP BY dt HAVING COUNT(*) <> 2 OR MIN(n) <> MAX(n)
) z;
SELECT CONCAT('clo_sku|', COUNT(*)) FROM (
  SELECT dt FROM (
      SELECT dt, COUNT(*) n FROM lake.dwd.dwd_order_sku_detail WHERE dt >= '$D1' AND dt <= '$D2' GROUP BY dt
      UNION ALL
      SELECT dt, SUM(CASE WHEN user_id IS NOT NULL THEN 1 ELSE 0 END) n
        FROM lake.ods.ods_order WHERE dt >= '$D1' AND dt <= '$D2' GROUP BY dt
  ) t GROUP BY dt HAVING COUNT(*) <> 2 OR MIN(n) <> MAX(n)
) z;
SELECT CONCAT('clo_lifecycle|', COUNT(*)) FROM (
  SELECT dt FROM (
      SELECT e.dt dt, COUNT(DISTINCT d.order_id) n
        FROM lake.ods.ods_order_event e JOIN lake.dwd.dwd_order_lifecycle d ON d.order_id = e.order_id
       WHERE e.dt >= '$D1' AND e.dt <= '$D2' GROUP BY e.dt
      UNION ALL
      SELECT dt, COUNT(DISTINCT order_id) n
        FROM lake.ods.ods_order_event WHERE dt >= '$D1' AND dt <= '$D2' GROUP BY dt
  ) t GROUP BY dt HAVING COUNT(*) <> 2 OR MIN(n) <> MAX(n)
) z;
-- B'. 不一致的具体是哪些天（前 10 天，便于精确回刷）
SELECT CONCAT('clo_bad|detail|', dt, '|', SUM(CASE WHEN s = 'd' THEN n ELSE 0 END), '|', SUM(CASE WHEN s = 'o' THEN n ELSE 0 END)) FROM (
    SELECT dt, COUNT(*) n, 'd' s FROM lake.dwd.dwd_order_detail WHERE dt >= '$D1' AND dt <= '$D2' GROUP BY dt
    UNION ALL
    SELECT dt, SUM(CASE WHEN user_id IS NOT NULL THEN 1 ELSE 0 END) n, 'o' s
      FROM lake.ods.ods_order WHERE dt >= '$D1' AND dt <= '$D2' GROUP BY dt
) t GROUP BY dt
HAVING SUM(CASE WHEN s = 'd' THEN n ELSE 0 END) <> SUM(CASE WHEN s = 'o' THEN n ELSE 0 END)
ORDER BY dt LIMIT 10;
SELECT CONCAT('clo_bad|lifecycle|', dt, '|', SUM(CASE WHEN s = 'd' THEN n ELSE 0 END), '|', SUM(CASE WHEN s = 'o' THEN n ELSE 0 END)) FROM (
    SELECT e.dt dt, COUNT(DISTINCT d.order_id) n, 'd' s
      FROM lake.ods.ods_order_event e JOIN lake.dwd.dwd_order_lifecycle d ON d.order_id = e.order_id
     WHERE e.dt >= '$D1' AND e.dt <= '$D2' GROUP BY e.dt
    UNION ALL
    SELECT dt, COUNT(DISTINCT order_id) n, 'o' s
      FROM lake.ods.ods_order_event WHERE dt >= '$D1' AND dt <= '$D2' GROUP BY dt
) t GROUP BY dt
HAVING SUM(CASE WHEN s = 'd' THEN n ELSE 0 END) <> SUM(CASE WHEN s = 'o' THEN n ELSE 0 END)
ORDER BY dt LIMIT 10;
SQL
  fi
} > "$AB_SQL"

# ------------------------------------------------------------------ C + D
{
  cat <<SQL
-- C. 仓侧 vs 由 lake.dwd 现算：整行逐字段对称差（只看落库值，不看湖侧有没有行）
-- 整表覆盖（给新鲜度 / ads-dws 互相比对用）
SELECT CONCAT('wh_dws_all|', COALESCE(CAST(MIN(dt) AS STRING),'-'), '|', COALESCE(CAST(MAX(dt) AS STRING),'-'), '|', COUNT(*), '|', COUNT(DISTINCT dt))
  FROM dws.dws_user_order_day;
SELECT CONCAT('wh_ads_all|', COALESCE(CAST(MIN(dt) AS STRING),'-'), '|', COALESCE(CAST(MAX(dt) AS STRING),'-'), '|', COUNT(*), '|', COUNT(DISTINCT dt))
  FROM ads.ads_daily_sales;
-- 区间内覆盖（给「有无空洞」用；必须按区间取，否则 --range 模式会拿整表天数去比区间跨度）
SELECT CONCAT('wh_dws_rng|', COALESCE(CAST(MIN(dt) AS STRING),'-'), '|', COALESCE(CAST(MAX(dt) AS STRING),'-'), '|', COUNT(*), '|', COUNT(DISTINCT dt))
  FROM dws.dws_user_order_day WHERE dt >= '$D1' AND dt <= '$D2';
SELECT CONCAT('wh_ads_rng|', COALESCE(CAST(MIN(dt) AS STRING),'-'), '|', COALESCE(CAST(MAX(dt) AS STRING),'-'), '|', COUNT(*), '|', COUNT(DISTINCT dt))
  FROM ads.ads_daily_sales WHERE dt >= '$D1' AND dt <= '$D2';

SELECT CONCAT('dws_mismatch|', COUNT(*)) FROM (
  SELECT 1 FROM (
      SELECT user_id, dt, order_cnt, paid_cnt, paid_amount, refund_amount
        FROM dws.dws_user_order_day WHERE dt >= '$D1' AND dt <= '$D2'
      UNION ALL
      SELECT user_id, dt,
             COUNT(*) AS order_cnt,
             COUNT(CASE WHEN status = 'paid' THEN 1 END) AS paid_cnt,
             SUM(CASE WHEN status = 'paid' THEN amount ELSE 0 END) AS paid_amount,
             SUM(CASE WHEN status = 'refund' THEN amount ELSE 0 END) AS refund_amount
        FROM lake.dwd.dwd_order_detail WHERE dt >= '$D1' AND dt <= '$D2'
       GROUP BY user_id, dt
  ) t
  GROUP BY user_id, dt, order_cnt, paid_cnt, paid_amount, refund_amount
  HAVING COUNT(*) <> 2
) z;
SELECT CONCAT('ads_mismatch|', COUNT(*)) FROM (
  SELECT 1 FROM (
      SELECT dt, order_cnt, paid_cnt, paid_amount, refund_amount, net_amount, avg_order_amount, pay_rate
        FROM ads.ads_daily_sales WHERE dt >= '$D1' AND dt <= '$D2'
      UNION ALL
      SELECT dt,
             SUM(order_cnt), SUM(paid_cnt), SUM(paid_amount), SUM(refund_amount),
             SUM(paid_amount) - SUM(refund_amount),
             ROUND(SUM(paid_amount) / NULLIF(SUM(paid_cnt), 0), 2),
             ROUND(SUM(paid_cnt) / NULLIF(SUM(order_cnt), 0), 4)
        FROM (
              SELECT user_id, dt,
                     COUNT(*) AS order_cnt,
                     COUNT(CASE WHEN status = 'paid' THEN 1 END) AS paid_cnt,
                     SUM(CASE WHEN status = 'paid' THEN amount ELSE 0 END) AS paid_amount,
                     SUM(CASE WHEN status = 'refund' THEN amount ELSE 0 END) AS refund_amount
                FROM lake.dwd.dwd_order_detail WHERE dt >= '$D1' AND dt <= '$D2'
               GROUP BY user_id, dt
        ) u
       GROUP BY dt
  ) t
  GROUP BY dt, order_cnt, paid_cnt, paid_amount, refund_amount, net_amount, avg_order_amount, pay_rate
  HAVING COUNT(*) <> 2
) z;

-- C'. 漂移落在哪些天（前 20 天 → 直接给出该回刷的区间）
SELECT CONCAT('dws_bad_day|', dt, '|', COUNT(*)) FROM (
  SELECT dt FROM (
      SELECT user_id, dt, order_cnt, paid_cnt, paid_amount, refund_amount
        FROM dws.dws_user_order_day WHERE dt >= '$D1' AND dt <= '$D2'
      UNION ALL
      SELECT user_id, dt,
             COUNT(*) AS order_cnt,
             COUNT(CASE WHEN status = 'paid' THEN 1 END) AS paid_cnt,
             SUM(CASE WHEN status = 'paid' THEN amount ELSE 0 END) AS paid_amount,
             SUM(CASE WHEN status = 'refund' THEN amount ELSE 0 END) AS refund_amount
        FROM lake.dwd.dwd_order_detail WHERE dt >= '$D1' AND dt <= '$D2'
       GROUP BY user_id, dt
  ) t
  GROUP BY user_id, dt, order_cnt, paid_cnt, paid_amount, refund_amount
  HAVING COUNT(*) <> 2
) z GROUP BY dt ORDER BY dt LIMIT 20;
SELECT CONCAT('ads_bad_day|', dt, '|', COUNT(*)) FROM (
  SELECT dt FROM (
      SELECT dt, order_cnt, paid_cnt, paid_amount, refund_amount, net_amount, avg_order_amount, pay_rate
        FROM ads.ads_daily_sales WHERE dt >= '$D1' AND dt <= '$D2'
      UNION ALL
      SELECT dt,
             SUM(order_cnt), SUM(paid_cnt), SUM(paid_amount), SUM(refund_amount),
             SUM(paid_amount) - SUM(refund_amount),
             ROUND(SUM(paid_amount) / NULLIF(SUM(paid_cnt), 0), 2),
             ROUND(SUM(paid_cnt) / NULLIF(SUM(order_cnt), 0), 4)
        FROM (
              SELECT user_id, dt,
                     COUNT(*) AS order_cnt,
                     COUNT(CASE WHEN status = 'paid' THEN 1 END) AS paid_cnt,
                     SUM(CASE WHEN status = 'paid' THEN amount ELSE 0 END) AS paid_amount,
                     SUM(CASE WHEN status = 'refund' THEN amount ELSE 0 END) AS refund_amount
                FROM lake.dwd.dwd_order_detail WHERE dt >= '$D1' AND dt <= '$D2'
               GROUP BY user_id, dt
        ) u
       GROUP BY dt
  ) t
  GROUP BY dt, order_cnt, paid_cnt, paid_amount, refund_amount, net_amount, avg_order_amount, pay_rate
  HAVING COUNT(*) <> 2
) z GROUP BY dt ORDER BY dt LIMIT 20;

-- D. 行数硬 gate：只用 COUNT（不可能为 NULL，不会像 CONCAT 那样整行消失）
SELECT CONCAT('exp_dws_rows|', COUNT(*)) FROM (
    SELECT user_id, dt FROM lake.dwd.dwd_order_detail WHERE dt >= '$D1' AND dt <= '$D2' GROUP BY user_id, dt
) g;
SELECT CONCAT('exp_ads_rows|', COUNT(DISTINCT dt)) FROM lake.dwd.dwd_order_detail WHERE dt >= '$D1' AND dt <= '$D2';
SELECT CONCAT('wh_dws_rows|', COUNT(*)) FROM dws.dws_user_order_day WHERE dt >= '$D1' AND dt <= '$D2';
SELECT CONCAT('wh_ads_rows|', COUNT(*)) FROM ads.ads_daily_sales WHERE dt >= '$D1' AND dt <= '$D2';

-- D'. 整表指标（FULL_RANGE 时对照基线用）
SELECT CONCAT('wh_dws_whole|', COUNT(*), '|', COUNT(DISTINCT dt), '|', SUM(order_cnt), '|', SUM(paid_cnt), '|', SUM(paid_amount), '|', SUM(refund_amount))
  FROM dws.dws_user_order_day;
SELECT CONCAT('wh_ads_whole|', COUNT(*), '|', COUNT(DISTINCT dt), '|', SUM(order_cnt), '|', SUM(paid_cnt), '|', SUM(paid_amount), '|', SUM(refund_amount), '|', SUM(net_amount), '|', SUM(avg_order_amount), '|', SUM(pay_rate))
  FROM ads.ads_daily_sales;
SQL
} > "$CD_SQL"

AB_OUT="$(mysql_file "$AB_SQL")" || die "A/B 段 SQL 执行失败"
CD_OUT="$(mysql_file "$CD_SQL")" || die "C/D 段 SQL 执行失败"
ALL_OUT="$(printf '%s\n%s\n' "$AB_OUT" "$CD_OUT")"

# ------------------------------------------- 1b. 维度链（尽力而为，不参与 gate）
# 为什么单独跑：2026-10-16 实测 lake.dim 的两张表在 StarRocks 侧读到的是【陈旧元数据】，
# 指向一个已被清理的旧快照文件，报 "File does not exist: .../metadata/snap-....avro"，
# 且 REFRESH EXTERNAL TABLE 清不掉（同一份数据用 Spark 或新建 catalog 读都正常）。
# 这种环境问题不该让对账整体失败，所以单独执行、失败只告警。
# 详见 docs/PITFALLS.md 3.18 / 3.19。
DIM_SQL="$_TMPD/dim.sql"
cat > "$DIM_SQL" <<'SQL'
SELECT CONCAT('lake_dim_scd2|', COUNT(*)) FROM lake.dim.dim_product_scd2;
SELECT CONCAT('lake_dim_current|', COUNT(*)) FROM lake.dim.dim_product;
SELECT CONCAT('lake_ods_product|', COUNT(*), '|', COUNT(DISTINCT snapshot_date)) FROM lake.ods.ods_product;
SQL
if DIM_OUT="$(mysql_file "$DIM_SQL" 2>&1)"; then DIM_RC=0; else DIM_RC=$?; fi

get() { printf '%s\n' "$ALL_OUT" | grep "^$1|" | head -n1 || true; }
# fld <key> <n>：取该行第 n 个「值」（key 是第 1 个字段，所以值是第 n+1 个字段）
fld() { local l; l="$(get "$1")"; [ -n "$l" ] && printf '%s' "$l" | cut -d'|' -f"$(( $2 + 1 ))" || true; }

SEC() { [ "$JSON" = 1 ] || { log ""; log "-- $* --"; }; }
LINE(){ [ "$JSON" = 1 ] || log "    $*"; }
OK()  { [ "$JSON" = 1 ] || log "    [OK]   $*"; }
BAD() { fail "  $*"; }
JSONK(){ [ "$JSON" = 1 ] && printf '%s\n' "$*" || true; }

FAILED=0
CNT_OK=0
ck_eq()   { if [ "$2" = "$3" ]; then OK "$1 = $2"; CNT_OK=$((CNT_OK+1)); else BAD "$1 不一致：期望 $3，实际 $2"; FAILED=$((FAILED+1)); fi; }
ck_zero() { if [ "$2" = "0" ]; then OK "$1 = 0"; CNT_OK=$((CNT_OK+1)); else BAD "$1 = $2（期望 0）"; FAILED=$((FAILED+1)); fi; }

# ------------------------------------------------------------------ A 段
SEC "A. 水位 / 新鲜度"
LINE "湖侧 dwd_order_detail : $(fld lake_detail 1) 行 / $(fld lake_detail 2) 天 / $(fld lake_detail 3) ~ $(fld lake_detail 4) / sum(amount)=$(fld lake_detail 5)"
LINE "湖侧 dwd_order_sku    : $(fld lake_sku 1) 行 / $(fld lake_sku 2) 天 / sum(amount)=$(fld lake_sku 3)"
LINE "湖侧 dwd_lifecycle    : $(fld lake_lifecycle 1) 行 / 去重订单 $(fld lake_lifecycle 2) / sum(amount)=$(fld lake_lifecycle 3)"
LINE "湖侧 ods_order        : $(fld ods_order 1) 行 / max(dt)=$(fld ods_order 2)"
LINE "湖侧 ods_order_event  : $(fld ods_event 1) 行 / max(dt)=$(fld ods_event 2)"
if [ "$DIM_RC" = 0 ]; then
  dget() { printf '%s\n' "$DIM_OUT" | grep "^$1|" | head -n1 | cut -d'|' -f"$(( $2 + 1 ))" || true; }
  LINE "湖侧 dim_product_scd2 : $(dget lake_dim_scd2 1) 行 / dim_product(当前) $(dget lake_dim_current 1) 行 / ods_product $(dget lake_ods_product 1) 行 / $(dget lake_ods_product 2) 天"
else
  LINE "湖侧维度链           : [WARN] 读不到（StarRocks 侧 lake.dim.* 元数据陈旧；不参与下面的 gate）"
  LINE "                        原始错误：$(printf '%s' "$DIM_OUT" | grep -m1 'ERROR' || printf '%s' "$DIM_OUT" | head -n1)"
  LINE "                        数据本身没坏（Spark 读得到）。修法：重建外部 catalog 或重启 FE："
  LINE "                          docker exec -i starrocks mysql -P9030 -h127.0.0.1 -uroot -e 'DROP CATALOG lake' \\"
  LINE "                            && docker exec -i starrocks mysql -P9030 -h127.0.0.1 -uroot -e 'CREATE EXTERNAL CATALOG lake PROPERTIES (\"type\"=\"iceberg\",\"iceberg.catalog.type\"=\"hadoop\",\"iceberg.catalog.warehouse\"=\"/opt/offline-dw/lake/warehouse\")'"
fi

LINE "仓侧 dws              : $(fld wh_dws_all 1) ~ $(fld wh_dws_all 2) / $(fld wh_dws_all 3) 行 / $(fld wh_dws_all 4) 天"
LINE "仓侧 ads              : $(fld wh_ads_all 1) ~ $(fld wh_ads_all 2) / $(fld wh_ads_all 3) 行 / $(fld wh_ads_all 4) 天"

JSONK "JSON_A|dws_rows=$(fld wh_dws_all 3)|ads_rows=$(fld wh_ads_all 3)|lake_max=$LAKE_MAX|wh_dws_max=$WH_DWS_MAX"

# 湖侧 ODS 水位落后（ODS 还没落今天的数据）→ 提示，不算失败
if [ "$ODS_MAX" != "$LAKE_MAX" ] && [ "$EVT_MAX" != "$LAKE_MAX" ]; then
  LINE "    提示：湖侧 ODS 水位（order=$ODS_MAX / event=$EVT_MAX）与 DWD 水位（$LAKE_MAX）不同 —— 若刚重放过 ODS 分区，需重跑 DWD 增量（run_dwd.sh incremental all）"
fi

# ------------------------------------------------------------------ B 段
if [ "$FAST" = 1 ]; then
  SEC "B. 湖侧闭合 —— 已按 --fast 跳过"
else
  SEC "B. 湖侧闭合（DWD 每天行数 vs 由 ODS 现算的期望）"
  CLO_BAD=0
  for pair in "clo_detail|dwd_order_detail" "clo_sku|dwd_order_sku_detail" "clo_lifecycle|dwd_order_lifecycle"; do
    k="${pair%%|*}"; label="${pair##*|}"
    v="$(fld "$k" 1)"
    if [ "$v" = "0" ]; then
      OK "$label 按天闭合：区间内每天都与 ODS 现算一致"
      CNT_OK=$((CNT_OK+1))
    else
      CLO_BAD=$((CLO_BAD+1))
      if [ "$STRICT" = 1 ]; then
        BAD "$label 按天闭合不一致：$v 天（--strict 下计为失败）"
        FAILED=$((FAILED+1))
      else
        warn "$label 按天闭合不一致：$v 天（默认只告警；--strict 会算失败）"
        LINE "    [WARN] $label 有 $v 天与 ODS 现算不符"
      fi
    fi
  done
  if [ "$CLO_BAD" != 0 ]; then
    LINE "    不一致的天（前 10，格式：表|天|DWD行数|ODS期望）："
    printf '%s\n' "$AB_OUT" | grep '^clo_bad|' | sed 's/^/      /' || true
    LINE "    常见原因：该天 ODS 分区刚被重放但 DWD 还没重算（正常瞬态）→ 重跑："
    LINE "      bash scripts/lake/run_dwd.sh incremental all --lookback 3"
  fi
fi

# ------------------------------------------------------------------ C 段
SEC "C. 湖 ↔ 仓 整行对称差（仓侧落库值 vs 由 lake.dwd 现算，区间 $D1 ~ $D2）"
ck_zero "dws_mismatch（漂移组数）" "$(fld dws_mismatch 1)"
ck_zero "ads_mismatch（漂移组数）" "$(fld ads_mismatch 1)"

BAD_DAYS="$(printf '%s\n' "$ALL_OUT" | grep -E '^(dws|ads)_bad_day\|' | cut -d'|' -f2 | sort -u || true)"
if [ -n "$BAD_DAYS" ]; then
  LINE "    漂移按天分布（前 20，格式：表|天|漂移组数）："
  printf '%s\n' "$ALL_OUT" | grep -E '^(dws|ads)_bad_day\|' | sed 's/^/      /' || true
  BAD_LO="$(printf '%s\n' "$BAD_DAYS" | head -n1)"
  BAD_HI="$(printf '%s\n' "$BAD_DAYS" | tail -n1)"
  [ "$BAD_HI" '<' "$LAKE_MAX" ] && BAD_HI="$LAKE_MAX"
  LINE "    修复（按天 DELETE+INSERT，区间取最早漂移日 ~ 湖侧最新日）："
  LINE "      bash scripts/run_dws_ads_from_lake.sh --range $BAD_LO $BAD_HI"
else
  LINE "    无任何天出现漂移"
fi

# ------------------------------------------------------------------ D 段
SEC "D. 仓侧覆盖 / 行数 gate"
ck_eq "dws 行数（区间内，仓侧 vs 湖侧现算）" "$(fld wh_dws_rows 1)" "$(fld exp_dws_rows 1)"
ck_eq "ads 行数（区间内，仓侧 vs 湖侧现算）" "$(fld wh_ads_rows 1)" "$(fld exp_ads_rows 1)"

LAG="?"
if [ -n "$WH_DWS_MAX" ] && [ "$WH_DWS_MAX" != "-" ]; then
  LAG="$(days_between "$WH_DWS_MAX" "$LAKE_MAX")"
fi
if [ "$LAG" != "?" ] && [ "$LAG" -le "$LAG_ALLOW" ]; then
  OK "仓侧 dws 已追平（MAX=$WH_DWS_MAX，湖侧 $LAKE_MAX，lag=${LAG} 天，允许 $LAG_ALLOW 天）"
  CNT_OK=$((CNT_OK+1))
else
  BAD "仓侧 dws 落后湖侧：仓侧 MAX=${WH_DWS_MAX:-空} vs 湖侧 $LAKE_MAX（lag=${LAG} 天，允许 $LAG_ALLOW）—— 物化还没跑？本对账必须排在物化【之后】"
  FAILED=$((FAILED+1))
fi
[ "$WH_ADS_MAX" = "$WH_DWS_MAX" ] && OK "仓侧 ads MAX(dt) 与 dws 一致（$WH_ADS_MAX）" \
  || { BAD "仓侧 ads MAX(dt)=${WH_ADS_MAX:-空} 与 dws MAX=${WH_DWS_MAX:-空} 不一致（ads 是从仓侧 dws 聚合的，dws 有洞 ads 就少算）"; FAILED=$((FAILED+1)); }

# 区间内空洞：区间内的 COUNT(DISTINCT dt) 必须等于有效重叠段的跨度天数
#   有效重叠段 = [max(区间内 MIN(dt), D1), min(区间内 MAX(dt), D2)]
#   如果仓侧区间下沿晚于 D1（例如仓侧某天还没物化），下面 dws 行数 gate 已经会先报出来；
#   这里只判「连成一片」这件事，报的是「中间少了一天」这种洞。
if [ -n "$(fld wh_dws_rng 1)" ] && [ "$(fld wh_dws_rng 1)" != "-" ]; then
  lo="$(fld wh_dws_rng 1)"; hi="$(fld wh_dws_rng 2)"
  [ "$lo" '<' "$D1" ] && lo="$D1"
  [ "$hi" '>' "$D2" ] && hi="$D2"
  span=$(( $(days_between "$lo" "$hi") + 1 ))
  ck_eq "dws 区间内无空洞（$lo ~ $hi，跨度 $span 天）" "$(fld wh_dws_rng 4)" "$span"
  ck_eq "dws 区间内天数 == 湖侧现算天数" "$(fld wh_dws_rng 4)" "$(fld exp_ads_rows 1)"
  alo="$(fld wh_ads_rng 1)"; ahi="$(fld wh_ads_rng 2)"
  if [ -n "$alo" ] && [ "$alo" != "-" ]; then
    aspan=$(( $(days_between "$alo" "$ahi") + 1 ))
    ck_eq "ads 区间内无空洞（$alo ~ $ahi，跨度 $aspan 天）" "$(fld wh_ads_rng 4)" "$aspan"
  else
    BAD "ads 在区间 $D1 ~ $D2 内没有任何数据"; FAILED=$((FAILED+1))
  fi
else
  BAD "dws 在区间 $D1 ~ $D2 内没有任何数据"; FAILED=$((FAILED+1))
fi

if [ "$FULL_RANGE" = 1 ]; then
  SEC "D'. 全区间基线（请求区间 == $FULL_MIN ~ $FULL_MAX）"
  ck_eq "dws 整表行数" "$(fld wh_dws_whole 1)" "$EXP_DWS_ROWS"
  ck_eq "dws 整表天数" "$(fld wh_dws_whole 2)" "$EXP_DWS_DAYS"
  ck_eq "dws sum(order_cnt)" "$(fld wh_dws_whole 3)" "$EXP_OC"
  ck_eq "dws sum(paid_cnt)" "$(fld wh_dws_whole 4)" "$EXP_PC"
  ck_eq "dws sum(paid_amount)" "$(fld wh_dws_whole 5)" "$EXP_PA"
  ck_eq "dws sum(refund_amount)" "$(fld wh_dws_whole 6)" "$EXP_RA"
  ck_eq "ads 整表行数" "$(fld wh_ads_whole 1)" "$EXP_ADS_ROWS"
  ck_eq "ads 整表天数" "$(fld wh_ads_whole 2)" "$EXP_ADS_DAYS"
  ck_eq "ads sum(order_cnt)" "$(fld wh_ads_whole 3)" "$EXP_OC"
  ck_eq "ads sum(paid_cnt)" "$(fld wh_ads_whole 4)" "$EXP_PC"
  ck_eq "ads sum(paid_amount)" "$(fld wh_ads_whole 5)" "$EXP_PA"
  ck_eq "ads sum(refund_amount)" "$(fld wh_ads_whole 6)" "$EXP_RA"
  ck_eq "ads sum(net_amount)" "$(fld wh_ads_whole 7)" "$EXP_NA"
  ck_eq "ads sum(avg_order_amount)" "$(fld wh_ads_whole 8)" "$EXP_AOA_SUM"
  ck_eq "ads sum(pay_rate)" "$(fld wh_ads_whole 9)" "$EXP_PR_SUM"
else
  SEC "D'. 全区间基线 —— 已跳过（本次区间 $D1 ~ $D2 != $FULL_MIN ~ $FULL_MAX）"
fi

# ------------------------------------------------------------------ 结论
JSONK "RECON_RANGE|$D1|$D2"
if [ "$FAILED" -eq 0 ]; then
  [ "$JSON" = 1 ] || { log ""; log "########## 对账通过（$CNT_OK 项） ##########"; }
  printf 'RECON_RESULT|OK|checks=%d|range=%s|%s\n' "$CNT_OK" "$D1~$D2" "$(date +%F' '%T)"
  exit 0
fi
fail "########## 对账失败：$FAILED 项不一致（通过 $CNT_OK 项） ##########"
printf 'RECON_RESULT|FAIL|failed=%d|checks=%d|range=%s|%s\n' "$FAILED" "$CNT_OK" "$D1~$D2" "$(date +%F' '%T)"
exit 2
