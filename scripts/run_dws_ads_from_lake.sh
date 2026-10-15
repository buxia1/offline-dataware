#!/usr/bin/env bash
# =============================================================================
# 仓侧 DWS / ADS 物化 runner（聚合发生在 StarRocks 侧）
#
#   lake.dwd.dwd_order_detail
#         |  StarRocks INSERT ... SELECT ... GROUP BY user_id, dt
#         v
#   dws.dws_user_order_day      （仓侧 DWS，PRIMARY KEY(user_id, dt)）
#         |  StarRocks INSERT ... SELECT ... GROUP BY dt
#         v
#   ads.ads_daily_sales         （仓侧 ADS，PRIMARY KEY(dt)）
#
# 用法：
#   bash scripts/run_dws_ads_from_lake.sh [--dry-run] [--auto] [--range D1 D2]
#                                        [--lookback N] [--truncate]
#
#     （不带参数）     【默认】自动增量：区间由水位算出来，见下方「区间怎么来」
#     --auto          同默认（显式写法，便于调度里自解释）
#     --range D1 D2    手工指定区间（YYYY-MM-DD）；只给 D1 时 D2 取同一天
#     --lookback N     自动增量时往前多算 N 天（默认 3，用于迟到事件回捞）
#     --truncate       全量重灌：先 TRUNCATE 两张仓表，再把 lake.dwd 全区间灌一遍
#     --dry-run, -n    只做【只读】校验：湖侧区间行数/天数/金额、仓侧 dws/ads 现状；
#                      不 TRUNCATE、不 DELETE、不 INSERT，不写库
#
# 区间怎么来（auto 模式）：
#     D1 = 仓侧 dws.dws_user_order_day 的 MAX(dt) 往前推 (lookback-1) 天
#     D2 = 湖侧 lake.dwd.dwd_order_detail 的 MAX(dt)
#     仓侧表为空时 D1 = 2026-09-20（全量起点）
#     例：仓侧已到 12-18、湖侧已到 12-19、lookback=2 -> 重算 12-17 ~ 12-19
#     —— 为什么要往前多算一天：迟到事件会把「已物化过的那几天」改掉，
#        而 StarRocks PK 表只能按本次区间 DELETE，漏掉的那天会留着旧值。
#
# 退出码：
#   0  成功，且自检与湖侧逐项一致
#   1  用法错误 / 环境缺失 / SQL 执行失败
#   2  自检数字与湖侧不一致（已打印差异）
#
# 环境：
#   容器 starrocks，mysql -P9030 -h127.0.0.1 -uroot（无密码），external catalog = lake
#
# -----------------------------------------------------------------------------
# 【为什么日常是「区间 DELETE + INSERT」而不是 TRUNCATE】
#   仓侧这两张都是 PRIMARY KEY 模型。PK 是 upsert 语义：DELETE 只能删掉
#   「本次计算能产出的键」，删不掉「旧表里有、但本次结果里已经没有」的键 ——
#   冒烟/探针残留行（如 user_id >= 90000000 的测试行）会永久留在表里。
#   所以**首次建表 / 怀疑有残留**时要用 --truncate 全量重灌一次；
#   日常调度走 auto 增量（只动 D1~D2，其余天原样保留）。
#
# 【auto 增量的前提：仓侧已物化的是一个「从起点起连续」的区间】
#   ads 是从【仓侧 dws】聚合的，不是再读一次湖。所以只要仓侧 dws 中间有洞，
#   ads 对应那天就会算少 —— 而且两边对称差检查都发现不了（两边一起错）。
#   因此 auto 模式带一条硬 gate：D1 不允许早于仓侧 dws 现有的 MIN(dt)。
#   （回刷历史请显式 --range，并自己确认回刷范围与已有数据连续。）
#
# 【PROJ 只靠脚本自身位置推导】
#   脚本从 WSL 内运行，路径全部由 BASH_SOURCE 推导，不写死任何 Windows 挂载盘
#   从 Windows 侧调用：wsl.exe -e bash -c 'bash /home/l/offline-dw/scripts/<本脚本>'
#
# 【为什么 SQL 走文件重定向而不是 -e "..."】
#   文件里有 '$' 占位符、单引号、中文注释，塞进 docker exec ... -e "..." 会被 shell
#   先吃掉一层引号 —— 实测会掉进 mysql 的 --help 输出。因此：sed 渲染到文件，
#   再 `docker exec -i starrocks mysql ... < 文件`。
# =============================================================================
set -euo pipefail

# ---------------------------------------------------------------- 路径与常量
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJ="$(cd "$HERE/.." && pwd)"

SQL_SRC="$PROJ/sql/dws_ads_from_lake.sql"          # 占位符 SQL（交付物 1）

# 渲染产物用 mktemp 生成「每次运行唯一」的名字，并挂 trap 清理。
# 以前是固定的 /tmp/dws_ads_from_lake.*.sql：跑完不删（留垃圾），
# 两个并发调度实例还会互相覆盖渲染结果、执行到对方的 SQL。
_TMPD="$(mktemp -d "${TMPDIR:-/tmp}/dws_ads_from_lake.XXXXXX")"
SQL_TMP="$_TMPD/step.sql"                           # sed 渲染后的待执行 SQL
DRY_SQL="$_TMPD/dryrun.sql"                         # dry-run 的渲染预览
CLEAN_SQL="$_TMPD/clean.sql"                        # TRUNCATE 清场语句
CHK_SQL="$_TMPD/check.sql"                          # 自检 / 对照 SQL
REQ_SQL="$_TMPD/required.sql"                       # 任务指定的两条整表自检
cleanup_tmp() { rm -rf "$_TMPD"; }
trap cleanup_tmp EXIT INT TERM

CT="starrocks"
MYSQL_BASE=(docker exec -i "$CT" mysql -P9030 -h127.0.0.1 -uroot --batch --raw --skip-column-names)

# 湖侧已知全量基线（用于 dry-run 的期望行数对账）
FULL_MIN="2026-09-20"
FULL_MAX="2026-12-18"
FULL_ROWS="854844"

# auto 模式默认往前多算的天数（迟到事件回捞窗口）
#   ★ 2026-10-15 由 2 改成 3（复核实测的缺陷）：
#     lookback=2 的实际覆盖面是 [D2-1, D2]，而事件生成器 LATE_EVENT_RATE 会把
#     event_time 回拨到 3 天前 —— 也就是「最新日之前 2~3 天到货的事件」落在窗口外，
#     默认增量永远不重算它，而且**自检全绿**（gate 只看窗口内）。
#     改成 3 后覆盖面 = [D2-2, D2]，恰好覆盖生成器的最大迟到跨度。
#     想调整请用 --lookback N；这只是"自动补几天"的默认值，不是硬限制。
DEF_LOOKBACK=3

# 湖侧外部表：读之前必须先 REFRESH（StarRocks 读 Iceberg 用的是缓存元数据）。
#   critical=1 的表刷新失败 → 直接 die：下面所有水位/期望/对称差都源自它们，
#   刷新失败会「用旧快照算区间 + 用旧快照算期望 → 两边一起错、自检全绿 → 照旧期望写库」。
#   critical=0 是 Kafka 外表（不是 Iceberg），REFRESH 本来就报错，只 warn。
#   格式：表名|是否关键|刷新失败时的提示
LAKE_TABLES=(
  "lake.dwd.dwd_order_detail|1|水位(D2)、期望行数与逐行对称差都来自这张表，拿不到就拒绝执行"
  "lake.dwd.dwd_order_sku_detail|0|本次未直接使用"
  "lake.dwd.dwd_order_lifecycle|0|本次未直接使用"
  "lake.dim.dim_product_scd2|0|本次未直接使用"
  "lake.ods.ods_order_event|0|本次未直接使用"
  "lake.ods.ods_order_flat|0|Kafka 外表，本就不支持 REFRESH EXTERNAL TABLE（属正常）"
)

# 全区间物化后的验收基线（与湖侧同口径；只在请求区间 == 全区间时启用）
EXP_DWS_ROWS=18000
EXP_DWS_DAYS=90
EXP_OC=854844
EXP_PA="119749476.90"
EXP_RA="14069110.40"
EXP_ADS_ROWS=90
EXP_PC=726511
EXP_NA="105680366.50"
EXP_AOA_SUM="14834.44"
EXP_PR_SUM="76.4891"

usage() {
  cat <<'USAGE'
用法：bash scripts/run_dws_ads_from_lake.sh [--dry-run] [--auto] [--range D1 D2]
                                           [--lookback N] [--truncate]

  （不带参数）    【默认】自动增量：D1 = 仓侧 dws.MAX(dt) - (lookback-1)，D2 = 湖侧 dwd.MAX(dt)
  --auto          同默认（显式写法）
  --range D1 D2   手工指定区间（YYYY-MM-DD）
  --lookback N    自动增量时往前多算 N 天（默认 3，迟到事件回捞窗口）
  --truncate      全量重灌（先 TRUNCATE 两张仓表，再灌全区间）
  --dry-run, -n   只读校验，不写库
示例：
  bash scripts/run_dws_ads_from_lake.sh --dry-run          # 看它自己算出什么区间
  bash scripts/run_dws_ads_from_lake.sh                    # 日常增量（调度就用这条）
  bash scripts/run_dws_ads_from_lake.sh --lookback 3       # 多回捞几天
  bash scripts/run_dws_ads_from_lake.sh --range 2026-11-01 2026-11-03
  bash scripts/run_dws_ads_from_lake.sh --truncate         # 全量重灌（首次/去残留）
USAGE
}

log()  { printf '%s\n' "$*"; }
fail() { printf '[FAIL] %s\n' "$*" >&2; }
warn() { printf '[WARN] %s\n' "$*" >&2; }
die()  { printf '[ERROR] %s\n' "$*" >&2; exit 1; }

mysql_q()    { "${MYSQL_BASE[@]}" -e "$1"; }        # 只读单条 SQL
mysql_file() { "${MYSQL_BASE[@]}" < "$1"; }         # 从文件执行（写库走这里）

# 刷新湖侧外部表的元数据快照。
#   StarRocks 查 Iceberg 外部表走的是【缓存的元数据】：湖侧被 Spark 写过之后，
#   不刷新就会继续返回旧快照。本脚本的第一步就是读 lake.dwd 的水位和行数，
#   不刷新 = 用旧快照算区间、用旧快照做对称差自检（两边一起错，自检还全绿）。
#   表清单与「关键/非关键」分级见文件头的 LAKE_TABLES。
refresh_lake() {
  local item t critical note ok=0 bad=0
  for item in "${LAKE_TABLES[@]}"; do
    IFS='|' read -r t critical note <<< "$item"
    if mysql_q "REFRESH EXTERNAL TABLE $t" >/dev/null 2>&1; then
      ok=$((ok + 1))
      continue
    fi
    if [ "$critical" = 1 ]; then
      # 硬 gate：这张表拿不到新快照时，继续跑只会产出「看起来全绿」的错数据
      die "关键外部表刷新失败：$t
    —— $note
    REFRESH EXTERNAL TABLE 失败时 StarRocks 仍会返回【缓存里的旧快照】，
    于是 D2 水位、期望行数、逐行对称差全部基于旧数据，自检会「两边一起错」还显示通过。
    请先确认湖侧可读、external catalog lake 正常，再重跑：
      docker exec -i starrocks mysql -P9030 -h127.0.0.1 -uroot -e 'REFRESH EXTERNAL TABLE $t'"
    fi
    warn "刷新外部表失败（Kafka 外表属正常）：$t  —— $note"
    bad=$((bad + 1))
  done
  if [ "$bad" -eq 0 ]; then
    log "  湖侧外部表元数据已刷新（$ok/${#LAKE_TABLES[@]} 张）：避免读到湖侧写入前的旧快照"
  else
    log "  湖侧外部表已刷新 $ok/${#LAKE_TABLES[@]} 张，$bad 张失败（均为非关键表，见上方告警）"
  fi
}

# ------------------------------------------------------------------ 参数解析
DRY=0
MODE="auto"              # auto（默认，水位自算）| range（手工区间）| truncate（全量重灌）
LOOKBACK="$DEF_LOOKBACK"
POS=()
SAW_TRUNCATE=0
SAW_RANGE=0
while [ "$#" -gt 0 ]; do
  case "$1" in
    --dry-run|-n)  DRY=1 ;;
    --auto)        MODE="auto" ;;
    --truncate)    MODE="truncate"; SAW_TRUNCATE=1 ;;
    --incremental) MODE="auto" ;;           # 兼容旧写法 == --auto（水位自算、按天增量）
                                            #   注意：这里以前写的是 MODE="range"，但 range 要 POS，
                                            #   而它不塞 POS → :213 `POS[0]` 在 set -u 下必崩。
    --lookback)
        shift
        [ "$#" -gt 0 ] || { usage >&2; die "--lookback 后面要跟天数"; }
        LOOKBACK="$1"
        ;;
    --lookback=*)  LOOKBACK="${1#--lookback=}" ;;
    --range)
        shift
        [ "$#" -gt 0 ] || { usage >&2; die "--range 后面要跟 D1 [D2]"; }
        MODE="range"; SAW_RANGE=1; POS+=("$1")
        if [ "$#" -gt 1 ] && [[ "$2" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
          POS+=("$2"); shift
        fi
        ;;
    -h|--help)     usage; exit 0 ;;
    -*)            usage >&2; die "未知参数：$1" ;;
    *)             MODE="range"; SAW_RANGE=1; POS+=("$1") ;;   # 位置参数 = 手工区间（向后兼容）
  esac
  shift
done

# A2：--truncate 与 --range 语义互斥。以前两者同给时「后写的那个吃掉前一个」且不报警，
#     两种顺序分别是「2 天回刷 → 偷偷变成全量 TRUNCATE 重灌」和「以为清了场、其实没清」。
if [ "$SAW_TRUNCATE" = 1 ] && [ "$SAW_RANGE" = 1 ]; then
  usage >&2
  die "--truncate 与 --range（含位置参数）不能同时使用：
      --truncate = 清空两张仓表后按湖侧水位全量重灌（不看区间）；
      --range D1 D2 = 只回刷指定区间，其他日期原样保留。
      全量重灌：bash $0 --truncate
      回刷区间：bash $0 --range D1 D2"
fi

if [ "${#POS[@]}" -gt 2 ]; then
  usage >&2; die "最多接受两个日期参数（D1 D2），收到 ${#POS[@]} 个"
fi
if [ "${#POS[@]}" -gt 0 ] && [ "$MODE" = auto ]; then MODE="range"; fi
[[ "$LOOKBACK" =~ ^[0-9]+$ ]] || die "--lookback 必须是 >= 0 的整数：$LOOKBACK"
[ "$LOOKBACK" -ge 1 ] || die "--lookback 至少为 1（否则本次区间为空）"

[ -f "$SQL_SRC" ] || die "找不到占位符 SQL：$SQL_SRC"

# D1 / D2 由 resolve_range() 定：auto 靠水位自算，range 用手工值
D1=""
D2=""
FULL_RANGE=0

# -------------------------------------------------------------------- 渲染
render_sql() {
  local out="$1"
  # 只替换两个占位符；D1/D2 已被正则限定为数字与短横线，无 sed 注入面
  sed "s/\${D1}/$D1/g; s/\${D2}/$D2/g" "$SQL_SRC" > "$out"
  if grep -q '\${D' "$out"; then
    die "渲染后仍残留 \${D1}/\${D2} 占位符，请检查 $SQL_SRC"
  fi
}

# ------------------------------------------------- 区间解析（auto = 水位自算）
add_days() { date -d "$1 $2 days" +%F; }

resolve_range() {
  if [ "$MODE" = range ]; then
    D1="${POS[0]}"
    D2="${POS[1]:-${POS[0]}}"
  else
    # auto / truncate：D2 = 湖侧最新一天
    local lmax wmax
    lmax="$(mysql_q "SELECT COALESCE(CAST(MAX(dt) AS STRING),'') FROM lake.dwd.dwd_order_detail" | tr -d '[:space:]')"
    [ -n "$lmax" ] || die "湖侧 lake.dwd.dwd_order_detail 是空的（或不可读），无法自算区间；首次请显式 --range $FULL_MIN $FULL_MAX"
    wmax="$(mysql_q "SELECT COALESCE(CAST(MAX(dt) AS STRING),'') FROM dws.dws_user_order_day" | tr -d '[:space:]')"
    if [ "$MODE" = truncate ] || [ -z "$wmax" ]; then
      D1="$FULL_MIN"
    else
      D1="$(add_days "$wmax" "-$((LOOKBACK - 1))")"
      [ "$D1" '<' "$FULL_MIN" ] && D1="$FULL_MIN"
    fi
    D2="$lmax"
  fi

  for d in "$D1" "$D2"; do
    [[ "$d" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || die "日期格式必须是 YYYY-MM-DD：$d"
  done
  # ISO8601 字典序 == 时间序
  if [[ "$D1" > "$D2" ]]; then
    die "区间倒挂：D1($D1) > D2($D2)。湖侧还没到该重算的日期，或 --lookback 给太大；请检查后再跑"
  fi

  # 起点 gate：仓侧 dws 必须是一个「从 MIN(dt) 起连续」的区间（ads 依赖它）
  #   auto 模式：D1 早于 MIN 直接拒 —— 那会在仓侧留空洞，而两侧对称差自检看不出来
  #   range 模式：不拦（回刷历史是正常操作），但必须显式告警，让操作者自己确认
  local wmin
  wmin="$(mysql_q "SELECT COALESCE(CAST(MIN(dt) AS STRING),'') FROM dws.dws_user_order_day" | tr -d '[:space:]')"
  if [ -n "$wmin" ] && [ "$D1" '<' "$wmin" ]; then
    if [ "$MODE" = auto ]; then
      die "auto 模式拒绝执行：D1($D1) 早于仓侧 dws 现有 MIN(dt)($wmin)。
      —— 仓侧 dws 原本是[$wmin 起]的连续区间，往前回刷会在中间留空洞，
         而 ads 是从仓侧 dws 聚合的，两边会一起错、对称差自检看不出来。
         确实要回刷历史请显式 --range $D1 $D2，并自行确认与已有数据连续。"
    fi
    warn "区间起点 $D1 早于仓侧 dws 现有 MIN(dt)($wmin)："
    warn "  本次只物化 $D1 ~ $D2，[$D1, $wmin) 在仓侧仍为空 —— ads 是按天从仓侧 dws 聚合的，"
    warn "  这两天 ads 会算少（两侧对称差自检同样为 0，发现不了）。"
    warn "  若本意是回刷历史，请把 [$wmin, 原 MAX] 一并纳入区间，做连续回刷。"
  fi

  FULL_RANGE=0
  if [ "$D1" = "$FULL_MIN" ] && [ "$D2" = "$FULL_MAX" ]; then FULL_RANGE=1; fi
}

# ------------------------------------------------------------------ 只读探活
preflight() {
  log "===== 预检（只读）====="
  mysql_q "SELECT 1 FROM lake.dwd.dwd_order_detail LIMIT 1" >/dev/null 2>&1 \
    || die "读不到 lake.dwd.dwd_order_detail（external catalog lake 是否可用？）"
  for t in dws.dws_user_order_day ads.ads_daily_sales; do
    mysql_q "SELECT 1 FROM $t LIMIT 1" >/dev/null 2>&1 \
      || die "读不到 $t；请先执行 sql/dws_ads/dws_user_order_day.sql / sql/dws_ads/ads_daily_sales.sql 建表"
  done
  log "  lake.dwd.dwd_order_detail / dws.dws_user_order_day / ads.ads_daily_sales  均可读"
  log "  区间：$D1 ~ $D2$([ "$FULL_RANGE" = 1 ] && echo '（全区间）')"
  log "  写库模式：$MODE$([ "$MODE" = truncate ] && echo '（先 TRUNCATE 两张仓表，再灌数）' || echo '（只按区间 DELETE + INSERT）')"
  log ""
}

# --------------------------------------------------------------- dry-run 校验
dry_run() {
  log "########## DRY-RUN（只读，不写库） ##########"
  render_sql "$DRY_SQL"
  log "-- 渲染后的可执行语句（占位符已替换；注释已过滤）--"
  grep -vE '^[[:space:]]*--' "$DRY_SQL" | grep -vE '^[[:space:]]*$' | sed 's/^/    /'
  if [ "$MODE" = truncate ]; then
    log "    TRUNCATE TABLE dws.dws_user_order_day;   -- 由 runner 在写库前执行（清探针残留）"
    log "    TRUNCATE TABLE ads.ads_daily_sales;"
  fi
  log ""

  # 聚合值一律 COALESCE(CAST(... AS STRING),'NULL')：CONCAT 遇 NULL 会整行变 NULL，
  # 那会让「区间无数据」的诊断行消失（实测：lake_dwd_range 行直接不见）。
  cat > "$CHK_SQL" <<SQL
SELECT CONCAT('lake_dwd_range|', COUNT(*), '|', COUNT(DISTINCT dt), '|',
              COALESCE(CAST(MIN(dt) AS STRING), 'NULL'), '|',
              COALESCE(CAST(MAX(dt) AS STRING), 'NULL'), '|',
              COALESCE(CAST(SUM(amount) AS STRING), 'NULL'))
  FROM lake.dwd.dwd_order_detail WHERE dt >= '$D1' AND dt <= '$D2';
SELECT CONCAT('expected_days|', DATEDIFF('$D2', '$D1') + 1);
SELECT CONCAT('wh_dws_range|', COUNT(*), '|', COUNT(DISTINCT dt), '|',
              COALESCE(CAST(SUM(order_cnt) AS STRING), 'NULL'), '|',
              COALESCE(CAST(SUM(paid_amount) AS STRING), 'NULL'), '|',
              COALESCE(CAST(SUM(refund_amount) AS STRING), 'NULL'))
  FROM dws.dws_user_order_day WHERE dt >= '$D1' AND dt <= '$D2';
SELECT CONCAT('wh_ads_range|', COUNT(*), '|', COUNT(DISTINCT dt), '|',
              COALESCE(CAST(SUM(order_cnt) AS STRING), 'NULL'), '|',
              COALESCE(CAST(SUM(paid_amount) AS STRING), 'NULL'), '|',
              COALESCE(CAST(SUM(refund_amount) AS STRING), 'NULL'), '|',
              COALESCE(CAST(SUM(net_amount) AS STRING), 'NULL'))
  FROM ads.ads_daily_sales WHERE dt >= '$D1' AND dt <= '$D2';
SQL

  local out lake_line exp_days l_rows l_days l_min l_max l_amt rc=0
  out="$(mysql_file "$CHK_SQL")"
  log "-- 湖侧 / 仓侧 区间现状 --"
  printf '%s\n' "$out" | sed 's/^/    /'
  log ""

  lake_line="$(printf '%s\n' "$out" | grep '^lake_dwd_range|' | head -n1 || true)"
  [ -n "$lake_line" ] || { fail "取不到湖侧区间统计"; return 1; }
  IFS='|' read -r _ l_rows l_days l_min l_max l_amt <<< "$lake_line"
  exp_days="$(printf '%s\n' "$out" | grep '^expected_days|' | head -n1 | cut -d'|' -f2 || true)"

  log "-- 校验 --"
  if [ "$l_rows" -gt 0 ] 2>/dev/null; then
    log "    [OK]   湖侧区间行数 > 0：$l_rows"
  else
    fail "湖侧区间行数为 0：lake.dwd.dwd_order_detail 在 $D1 ~ $D2 没有数据"; rc=1
  fi
  if [ "$l_days" = "$exp_days" ]; then
    log "    [OK]   湖侧区间天数 = 期望天数：$l_days"
  else
    fail "湖侧区间天数 $l_days != 期望 $exp_days（区间内有日期空洞）"; rc=1
  fi
  if [ "$l_min" = "$D1" ] && [ "$l_max" = "$D2" ]; then
    log "    [OK]   湖侧 dt 边界与请求区间一致：$l_min ~ $l_max"
  else
    log "    [WARN] 湖侧 dt 实际边界 $l_min ~ $l_max 与请求区间 $D1 ~ $D2 不完全一致（端点当天无数据属正常）"
  fi
  log "    湖侧区间 sum(amount) = $l_amt"

  if [ "$FULL_RANGE" = 1 ]; then
    if [ "$l_rows" = "$FULL_ROWS" ]; then
      log "    [OK]   全区间湖侧行数 == 基线 $FULL_ROWS"
    else
      fail "全区间湖侧行数 $l_rows != 基线 $FULL_ROWS —— 湖侧数据与预期不符，禁止写库"; rc=1
    fi
  fi
  # （原 EXPECTED_ROWS 钩子已删：全脚本从无赋值点，是改造前遗留的死分支）

  if [ "$rc" = 0 ]; then
    log ""
    log "########## DRY-RUN OK：只读校验通过，未写库 ##########"
  else
    log ""
    fail "DRY-RUN 校验未通过"
  fi
  return "$rc"
}

# ------------------------------------------------- 窗口外漂移探测（只读诊断）
# 为什么需要：dws_mismatch / ads_mismatch 只比 [D1, D2]，窗口外的漂移一条都看不见，
# 于是「自检全部通过」可能只是「窗口内没问题」（复核实测：破坏 12-10/12-11 后跑默认
# 增量，两个 mismatch 都是 0、EXIT=0，而漂移原封不动）。
# 这里把审计区间扩到 [FULL_MIN, D1-1]（即仓侧全部历史里、本次不回刷的那一段），
# 数出漂移组数；非 0 就显式告警并给出可复制的修复命令。
detect_out_of_window_drift() {
  if [ "$MODE" = range ] || [ "$FULL_RANGE" = 1 ]; then
    return 0
  fi
  local lo hi
  hi="$(add_days "$D1" -1)"
  if [ "$hi" '<' "$FULL_MIN" ]; then
    return 0
  fi
  lo="$FULL_MIN"

  local audit="$CHK_SQL.drift.sql"
  cat > "$audit" <<SQL
SELECT CONCAT('out_drift_dws|', COUNT(*)) FROM (
  SELECT 1 FROM (
      SELECT user_id, dt, order_cnt, paid_cnt, paid_amount, refund_amount
        FROM dws.dws_user_order_day WHERE dt >= '$lo' AND dt <= '$hi'
      UNION ALL
      SELECT user_id, dt,
             COUNT(*) AS order_cnt,
             COUNT(CASE WHEN status = 'paid' THEN 1 END) AS paid_cnt,
             SUM(CASE WHEN status = 'paid' THEN amount ELSE 0 END) AS paid_amount,
             SUM(CASE WHEN status = 'refund' THEN amount ELSE 0 END) AS refund_amount
        FROM lake.dwd.dwd_order_detail WHERE dt >= '$lo' AND dt <= '$hi'
       GROUP BY user_id, dt
  ) t
  GROUP BY user_id, dt, order_cnt, paid_cnt, paid_amount, refund_amount
  HAVING COUNT(*) <> 2
) z;
SELECT CONCAT('out_drift_ads|', COUNT(*)) FROM (
  SELECT 1 FROM (
      SELECT dt, order_cnt, paid_cnt, paid_amount, refund_amount, net_amount, avg_order_amount, pay_rate
        FROM ads.ads_daily_sales WHERE dt >= '$lo' AND dt <= '$hi'
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
                FROM lake.dwd.dwd_order_detail WHERE dt >= '$lo' AND dt <= '$hi'
               GROUP BY user_id, dt
        ) u
       GROUP BY dt
  ) t
  GROUP BY dt, order_cnt, paid_cnt, paid_amount, refund_amount, net_amount, avg_order_amount, pay_rate
  HAVING COUNT(*) <> 2
) z;
SQL

  local out v nd na rc=0
  out="$(mysql_file "$audit")" || rc=$?
  if [ "$rc" != 0 ]; then
    warn "窗口外漂移探测本身执行失败（exit=$rc）——本次报告不含窗口外结论，请手工核对"
    return 0
  fi
  v="$(printf '%s\n' "$out" | grep '^out_drift_dws|' | head -n1 || true)"; nd="${v#*|}"
  v="$(printf '%s\n' "$out" | grep '^out_drift_ads|' | head -n1 || true)"; na="${v#*|}"
  [ -n "$nd" ] || nd="?"
  [ -n "$na" ] || na="?"

  log ""
  log "-- C2. 窗口外漂移探测（$lo ~ $hi，即本次不回刷的那段）--"
  if [ "$nd" = "0" ] && [ "$na" = "0" ]; then
    log "    [OK]   窗口外无漂移：整段历史与湖侧现算逐行一致"
  else
    warn "窗口外仍有漂移：dws $nd 组 / ads $na 组（区段 $lo ~ $hi）"
    warn "  —— 本次自检只覆盖 $D1 ~ $D2，上面的「自检全部通过」**不代表全表一致**。"
    warn "     要把它一起修掉，二选一："
    warn "       显式回刷： bash $0 --range $lo $D2"
    warn "       扩大窗口： bash $0 --lookback N   （N 需 >= 距最新日的天数 + 1）"
  fi
  return 0
}

# ------------------------------------------------------- 清场（TRUNCATE 模式）
clean_warehouse() {
  if [ "$MODE" != "truncate" ]; then
    log "  清场模式：区间 DELETE + INSERT（不 TRUNCATE，$D1 ~ $D2 之外的日期原样保留）"
    return 0
  fi
  # 以前这里 warn「TRUNCATE + 区间 != 全区间」——但 MODE=truncate 时区间恒等于全区间，
  # 这条永远不触发，而且它建议用的 --incremental 当时是必崩的。真正的风险是：
  # 用户同时给了 --range，以为只刷几天，实际是全表清空重灌（现在已在参数解析处直接拒绝）。
  warn "TRUNCATE 模式：两张仓表将被整表清空后按全区间重灌（不看任何 --range）"
  cat > "$CLEAN_SQL" <<'SQL'
TRUNCATE TABLE dws.dws_user_order_day;
TRUNCATE TABLE ads.ads_daily_sales;
SQL
  log "  清场：TRUNCATE dws.dws_user_order_day / ads.ads_daily_sales（去 PK 表 upsert 删不掉的探针残留）"
  local st
  mysql_file "$CLEAN_SQL" || { st=$?; die "TRUNCATE 清场失败（exit=$st）"; }
  log "  清场完成"
}

# ------------------------------------------------------------------ 写库执行
run_materialize() {
  log "########## 正式物化（写库） ##########"
  clean_warehouse
  render_sql "$SQL_TMP"
  log "  渲染 SQL -> $SQL_TMP"
  log "  执行：docker exec -i $CT mysql -P9030 -h127.0.0.1 -uroot < $SQL_TMP"
  local t0 t1 st
  t0=$(date +%s)
  mysql_file "$SQL_TMP" || { st=$?; die "SQL 执行失败（exit=$st），已中止，不继续自检"; }
  t1=$(date +%s)
  log "  SQL 执行完成，退出码 0，耗时 $((t1 - t0))s"
  log ""
}

# ------------------------------------------------------------------ 自检对账
build_check_sql() {
  : > "$CHK_SQL"

  # A. 整表自检（等价于任务要求的两条 SQL，用 CONCAT 便于逐项解析）
  cat >> "$CHK_SQL" <<SQL
SELECT CONCAT('wh_dws_whole|', COUNT(*), '|', COUNT(DISTINCT dt), '|', SUM(order_cnt), '|', SUM(paid_cnt), '|', SUM(paid_amount), '|', SUM(refund_amount))
  FROM dws.dws_user_order_day;
SELECT CONCAT('wh_ads_whole|', COUNT(*), '|', COUNT(DISTINCT dt), '|', SUM(order_cnt), '|', SUM(paid_cnt), '|', SUM(paid_amount), '|', SUM(refund_amount), '|', SUM(net_amount), '|', SUM(avg_order_amount), '|', SUM(pay_rate))
  FROM ads.ads_daily_sales;

-- B. 区间聚合（仓侧 vs 湖侧对照，逐字段）
SELECT CONCAT('wh_dws_range|', COUNT(*), '|', COUNT(DISTINCT dt), '|', SUM(order_cnt), '|', SUM(paid_cnt), '|', SUM(paid_amount), '|', SUM(refund_amount))
  FROM dws.dws_user_order_day WHERE dt >= '$D1' AND dt <= '$D2';
SELECT CONCAT('wh_ads_range|', COUNT(*), '|', COUNT(DISTINCT dt), '|', SUM(order_cnt), '|', SUM(paid_cnt), '|', SUM(paid_amount), '|', SUM(refund_amount), '|', SUM(net_amount), '|', SUM(avg_order_amount), '|', SUM(pay_rate))
  FROM ads.ads_daily_sales WHERE dt >= '$D1' AND dt <= '$D2';

-- B0. 行数硬 gate：只用 COUNT，绝不可能是 NULL，因此不会像 CONCAT 那样整行消失。
--     （若区间为空、或物化一行没写进去，对称差同样会是 0，光靠对称差会静默通过。）
SELECT CONCAT('exp_dws_rows_range|', COUNT(*)) FROM (
    SELECT user_id, dt FROM lake.dwd.dwd_order_detail
     WHERE dt >= '$D1' AND dt <= '$D2' GROUP BY user_id, dt
) g;
SELECT CONCAT('exp_ads_rows_range|', COUNT(DISTINCT dt))
  FROM lake.dwd.dwd_order_detail WHERE dt >= '$D1' AND dt <= '$D2';
SELECT CONCAT('wh_dws_rows_range|', COUNT(*))
  FROM dws.dws_user_order_day WHERE dt >= '$D1' AND dt <= '$D2';
SELECT CONCAT('wh_ads_rows_range|', COUNT(*))
  FROM ads.ads_daily_sales WHERE dt >= '$D1' AND dt <= '$D2';

-- B1. 区间覆盖 gate：仓侧「已物化的连续区间」必须完整盖住 [D1, D2]
--     ① MAX(dt) 必须正好 = D2：否则湖侧新到的那几天没物化，而主 gate 只看区间内，发现不了
--     ② MIN(dt) 不允许 > D1：否则 [D1, MIN) 这段在仓侧是空的，而 ads 是从仓侧 dws 聚合的
SELECT CONCAT('wh_dws_cov|', COALESCE(CAST(MIN(dt) AS STRING),'-'), '|', COALESCE(CAST(MAX(dt) AS STRING),'-'))
  FROM dws.dws_user_order_day;

-- C. 权威 gate 1：区间内整行对称差（仓侧 vs 由 lake.dwd 现算）
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
SQL

}

check_baseline() {
  local out="$1" expect="$2" label="$3"
  local key want got
  key="${expect%%|*}"
  want="${expect#*|}"
  got="$(printf '%s\n' "$out" | grep "^$key|" | head -n1 || true)"
  got="${got#*|}"
  if [ "$got" = "$want" ]; then
    log "    [OK]   $label 基线一致：$got"
    return 0
  fi
  fail "$label 基线不一致"
  log "          期望：$want"
  log "          实际：$got"
  return 1
}

# 任务指定两条整表自检，原样执行、原样打印（含列头）
print_required_selfcheck() {
  cat > "$REQ_SQL" <<'SQL'
SELECT count(*) rows_, count(DISTINCT dt) days_, sum(order_cnt) oc, sum(paid_amount) pa, sum(refund_amount) ra FROM dws.dws_user_order_day;
SELECT count(*) rows_, count(DISTINCT dt) days_, sum(order_cnt) oc, sum(paid_amount) pa, sum(refund_amount) ra, sum(net_amount) na FROM ads.ads_daily_sales;
SQL
  docker exec -i "$CT" mysql -P9030 -h127.0.0.1 -uroot --table < "$REQ_SQL" || true
}

self_check() {
  log "########## 自检 / 与湖侧对账 ##########"

  # 湖侧 lake.dws / lake.ads 是 2026-10-14 之前那套「湖侧建 dws/ads」的遗留表，
  # 自 cfab3e0 起已无人更新 —— 拿它们当对照会「今天恰好相等、明天必然漂移」，
  # 所以对照基准统一改成「由 lake.dwd 现算」（B0 / C 段）。
  build_check_sql
  local out v n w rc=0
  out="$(mysql_file "$CHK_SQL")"

  log ""
  log "-- A. 整表自检（rows|days|oc|pc|pa|ra|na|aoa_sum|pr_sum）--"
  printf '%s\n' "$out" | grep -E '^wh_(dws|ads)_whole\|' | sed 's/^/    /' || true

  log ""
  log "-- B. 区间聚合（仓侧）--"
  printf '%s\n' "$out" | grep -E '^wh_(dws|ads)_range\|' | sed 's/^/    /' || true

  log ""
  log "-- C. 整行对称差 gate（必须全为 0）--"
  printf '%s\n' "$out" | grep -E '_mismatch\|' | sed 's/^/    /' || true

  # C. 对称差 gate（dws/ads 均已与「由 lake.dwd 现算」的期望逐行比过）
  #    ⚠️ 这两个 gate 只覆盖 [D1, D2]；窗口外的漂移由 C2 的「窗口外漂移探测」负责。
  for k in dws_mismatch ads_mismatch; do
    v="$(printf '%s\n' "$out" | grep "^$k|" | head -n1 || true)"
    [ -n "$v" ] || continue
    n="${v#*|}"
    if [ "$n" = "0" ]; then
      log "    [OK]   $k = 0"
    else
      fail "$k = $n（期望 0：仓侧与湖侧存在 $n 组不一致的行）"
      rc=1
    fi
  done

  # C0. 行数硬 gate（对称差为 0 也可能是「两边都空」，所以必须单独数行数）
  for pair in "dws|wh_dws_rows_range|exp_dws_rows_range" "ads|wh_ads_rows_range|exp_ads_rows_range"; do
    IFS='|' read -r kind wk ek <<< "$pair"
    w="$(printf '%s\n' "$out" | grep "^$wk|" | head -n1 || true)"
    v="$(printf '%s\n' "$out" | grep "^$ek|" | head -n1 || true)"
    if [ -z "$w" ] || [ -z "$v" ]; then
      fail "$kind 区间行数 gate 取不到计数行（wh='${w}' exp='${v}'），拒绝放行"
      rc=1
      continue
    fi
    wn="${w#*|}"; vn="${v#*|}"
    if [ "$wn" != "$vn" ]; then
      fail "$kind 区间行数不一致：仓侧 $wn 行 != 湖侧期望 $vn 行"
      rc=1
    elif [ "$vn" -le 0 ] 2>/dev/null; then
      fail "$kind 区间行数为 0（湖侧期望也是 0），该区间没有可物化的数据"
      rc=1
    else
      log "    [OK]   $kind 区间行数 = $wn（== 湖侧期望）"
    fi
  done

  # C1. 区间覆盖 gate（auto 模式的核心不变量：连续区间 + 已追平湖侧最新）
  #     「MAX(dt) 必须 == D2」只在 auto 下校验：auto 的语义就是「追平湖侧最新」。
  #     手工 --range 回刷某几天时 MAX(dt) 本来就会 > D2，那不是缺陷而是设计。
  local cov wmin wmax
  cov="$(printf '%s\n' "$out" | grep '^wh_dws_cov|' | head -n1 || true)"
  if [ -z "$cov" ]; then
    fail "取不到 wh_dws_cov（仓侧 dws 可能为空）；拒绝放行"
    rc=1
  else
    IFS='|' read -r _ wmin wmax <<< "$cov"
    if [ "$MODE" = auto ]; then
      if [ "$wmax" != "$D2" ]; then
        fail "覆盖 gate(auto)：仓侧 dws MAX(dt)=$wmax != D2=$D2 —— 本次没把湖侧最新一天物化进来"
        rc=1
      else
        log "    [OK]   覆盖 gate(auto)：仓侧 dws MAX(dt) == D2（$D2，已追平湖侧最新）"
      fi
    elif [ "$wmax" = "-" ]; then
      fail "覆盖 gate：仓侧 dws 为空，但本次区间是 $D1 ~ $D2 —— 写库没生效？"
      rc=1
    elif [ "$wmax" '<' "$D2" ]; then
      fail "覆盖 gate：仓侧 dws MAX(dt)=$wmax < D2=$D2 —— 本次区间上沿没物化进去"
      rc=1
    else
      log "    [OK]   覆盖 gate($MODE)：仓侧 dws MAX(dt)=$wmax >= D2（$D2），区间上沿未回退"
    fi
    if [ "$wmin" != "-" ] && [ "$wmin" '>' "$D1" ]; then
      fail "覆盖 gate：仓侧 dws MIN(dt)=$wmin > D1=$D1 —— [$D1, $wmin) 在仓侧是空的，ads 会少算"
      rc=1
    else
      log "    [OK]   覆盖 gate：仓侧 dws MIN(dt)=$wmin <= D1（区间下沿无空洞）"
    fi
    if [ "$MODE" = truncate ]; then
      log "    [INFO] TRUNCATE 模式：两张仓表已整表清空后重灌（含区间外日期）"
    fi
  fi

  # C2. 窗口外漂移探测：把「自检全绿但其实窗口外有漂移」这个假绿消掉
  detect_out_of_window_drift || true

  # 全区间时，额外对照硬基线
  log ""
  if [ "$FULL_RANGE" = 1 ]; then
    log "-- D. 全区间验收基线 --"
    check_baseline "$out" "wh_dws_whole|$EXP_DWS_ROWS|$EXP_DWS_DAYS|$EXP_OC|$EXP_PC|$EXP_PA|$EXP_RA" "dws 整表" || rc=1
    check_baseline "$out" "wh_ads_whole|$EXP_ADS_ROWS|$EXP_DWS_DAYS|$EXP_OC|$EXP_PC|$EXP_PA|$EXP_RA|$EXP_NA|$EXP_AOA_SUM|$EXP_PR_SUM" "ads 整表" || rc=1
  else
    log "-- D. 区间 != 全区间，跳过整表硬基线（整表数字见上方 A 段）--"
  fi

  log ""
  log "-- E. 任务指定的两条整表自检（原样输出）--"
  print_required_selfcheck

  log ""
  if [ "$FULL_RANGE" = 1 ]; then
    log "  ℹ️ 本次校验范围：全区间 $D1 ~ $D2（含整表硬基线）"
  else
    log "  ℹ️ 本次逐行对称差只校验了 $D1 ~ $D2；窗口外的情况见上方 C2 段"
  fi
  log ""
  if [ "$rc" = 0 ]; then
    log "########## 自检全部通过 ##########"
  else
    fail "自检不一致，请核对上方差异"
  fi
  return "$rc"
}

# ---------------------------------------------------------------------- main
log "=============================================================="
log " 仓侧 DWS/ADS 物化（lake.dwd -> dws -> ads，StarRocks 侧聚合）"
log " 开始时间：$(date '+%F %T')"
log " PROJ      : $PROJ"
log " SQL_SRC   : $SQL_SRC"
log " 参数      : MODE=$MODE LOOKBACK=$LOOKBACK$([ "${#POS[@]}" -gt 0 ] && echo " POS=${POS[*]}" || true)"
log "=============================================================="
log ""

# ★ 先刷新湖侧外部表元数据：否则水位和后续对账都可能基于旧快照
refresh_lake

# 区间必须在 preflight 之前定好（preflight / dry_run / 写库 / 自检都依赖 D1、D2）
resolve_range
log " 解析区间  : $D1 ~ $D2$([ "$FULL_RANGE" = 1 ] && echo '（正好是全区间）' || true)"
log " 写库模式  : $([ "$DRY" = 1 ] && echo 'DRY-RUN（只读，不写库）' || { [ "$MODE" = truncate ] && echo 'TRUNCATE 全量重灌' || echo '区间 DELETE + INSERT（增量）'; })"
log ""

preflight

if [ "$DRY" = 1 ]; then
  if dry_run; then exit 0; else exit 2; fi
fi

run_materialize
if self_check; then exit 0; else exit 2; fi
