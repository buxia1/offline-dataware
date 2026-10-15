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
#   bash scripts/run_dws_ads_from_lake.sh [--dry-run] [--incremental] [D1] [D2]
#     D1 / D2        日期，格式 YYYY-MM-DD；默认 2026-09-20 2026-12-18（全区间）
#     只给 D1 时 D2 取同一天（单日）
#     --dry-run      只做【只读】校验：湖侧区间行数/天数/金额、仓侧 dws/ads 现状；
#                    不 TRUNCATE、不 DELETE、不 INSERT，不写库
#     --truncate     写库前先 TRUNCATE 两张仓表再灌数（【默认】；见下方「清场」说明）
#     --incremental  不 TRUNCATE，只按区间 DELETE 再 INSERT（按天回刷用）
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
# 【清场：为什么默认 TRUNCATE 而不是只按区间 DELETE】
#   仓侧这两张都是 PRIMARY KEY 模型。PK 模型是 upsert 语义：DELETE 只能删掉
#   「本次计算能产出的键」，删不掉「旧表里有、但本次结果里已经没有」的键 ——
#   冒烟/探针残留行（如 user_id >= 90000000 的测试行）会永久留在表里，
#   最后表现为「一半新一半旧」。所以默认先 TRUNCATE 两张表做全量重算，
#   这两张表本来就是「每日全量重算」语义。
#   ⚠️ TRUNCATE 是整表清空：若区间 != 全区间，清场后表里只会剩该区间的数据。
#      只回刷某几天请显式加 --incremental（区间 DELETE + INSERT，不动其他天）。
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
SQL_TMP="/tmp/dws_ads_from_lake.sql"                # sed 渲染后的待执行 SQL
DRY_SQL="/tmp/dws_ads_from_lake.dryrun.sql"         # dry-run 的渲染预览
CLEAN_SQL="/tmp/dws_ads_from_lake.clean.sql"        # TRUNCATE 清场语句
CHK_SQL="/tmp/dws_ads_from_lake.check.sql"          # 自检 / 对照 SQL
REQ_SQL="/tmp/dws_ads_from_lake.required.sql"       # 任务指定的两条整表自检

CT="starrocks"
MYSQL_BASE=(docker exec -i "$CT" mysql -P9030 -h127.0.0.1 -uroot --batch --raw --skip-column-names)

# 湖侧已知全量基线（用于 dry-run 的期望行数对账）
FULL_MIN="2026-09-20"
FULL_MAX="2026-12-18"
FULL_ROWS="854844"

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
用法：bash scripts/run_dws_ads_from_lake.sh [--dry-run] [--incremental] [D1] [D2]
  D1/D2          日期 YYYY-MM-DD（默认 2026-09-20 2026-12-18）
  --dry-run      只读校验：湖侧区间行数/天数/金额 + 仓侧 dws/ads 现状；不写库
  --truncate     写库前 TRUNCATE 两张仓表（默认模式，去探针残留）
  --incremental  不 TRUNCATE，只按区间 DELETE + INSERT（按天回刷）
示例：
  bash scripts/run_dws_ads_from_lake.sh --dry-run
  bash scripts/run_dws_ads_from_lake.sh --incremental 2026-09-20 2026-09-20
  bash scripts/run_dws_ads_from_lake.sh                      # 全区间 + TRUNCATE
USAGE
}

log()  { printf '%s\n' "$*"; }
fail() { printf '[FAIL] %s\n' "$*" >&2; }
warn() { printf '[WARN] %s\n' "$*" >&2; }
die()  { printf '[ERROR] %s\n' "$*" >&2; exit 1; }

mysql_q()    { "${MYSQL_BASE[@]}" -e "$1"; }        # 只读单条 SQL
mysql_file() { "${MYSQL_BASE[@]}" < "$1"; }         # 从文件执行（写库走这里）

# ------------------------------------------------------------------ 参数解析
DRY=0
MODE="truncate"          # truncate | incremental
POS=()
for arg in "$@"; do
  case "$arg" in
    --dry-run|-n)  DRY=1 ;;
    --truncate)    MODE="truncate" ;;
    --incremental) MODE="incremental" ;;
    -h|--help)     usage; exit 0 ;;
    -*)            usage >&2; die "未知参数：$arg" ;;
    *)             POS+=("$arg") ;;
  esac
done

if [ "${#POS[@]}" -gt 2 ]; then
  usage >&2; die "最多接受两个日期参数（D1 D2），收到 ${#POS[@]} 个"
fi
D1="${POS[0]:-2026-09-20}"
D2="${POS[1]:-${POS[0]:-2026-12-18}}"

for d in "$D1" "$D2"; do
  [[ "$d" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || die "日期格式必须是 YYYY-MM-DD：$d"
done
# ISO8601 字典序 == 时间序
if [[ "$D1" > "$D2" ]]; then die "D1($D1) 必须 <= D2($D2)"; fi
[ -f "$SQL_SRC" ] || die "找不到占位符 SQL：$SQL_SRC"

FULL_RANGE=0
if [ "$D1" = "$FULL_MIN" ] && [ "$D2" = "$FULL_MAX" ]; then FULL_RANGE=1; fi

# -------------------------------------------------------------------- 渲染
render_sql() {
  local out="$1"
  # 只替换两个占位符；D1/D2 已被正则限定为数字与短横线，无 sed 注入面
  sed "s/\${D1}/$D1/g; s/\${D2}/$D2/g" "$SQL_SRC" > "$out"
  if grep -q '\${D' "$out"; then
    die "渲染后仍残留 \${D1}/\${D2} 占位符，请检查 $SQL_SRC"
  fi
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
  if [ -n "${EXPECTED_ROWS:-}" ]; then
    if [ "$l_rows" = "$EXPECTED_ROWS" ]; then
      log "    [OK]   湖侧行数 == EXPECTED_ROWS=$EXPECTED_ROWS"
    else
      fail "湖侧行数 $l_rows != EXPECTED_ROWS=$EXPECTED_ROWS"; rc=1
    fi
  fi

  if [ "$rc" = 0 ]; then
    log ""
    log "########## DRY-RUN OK：只读校验通过，未写库 ##########"
  else
    log ""
    fail "DRY-RUN 校验未通过"
  fi
  return "$rc"
}

# ------------------------------------------------------- 清场（TRUNCATE 模式）
clean_warehouse() {
  if [ "$MODE" != "truncate" ]; then
    log "  清场模式：incremental（不 TRUNCATE，交由 SQL 内区间 DELETE 处理）"
    return 0
  fi
  if [ "$FULL_RANGE" != 1 ]; then
    warn "TRUNCATE 模式 + 区间 != 全区间：清场后表内只会剩 $D1 ~ $D2 的数据；只回刷几天请用 --incremental"
  fi
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
  local has_lake_dws="$1" has_lake_ads="$2"
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

  # C. 权威 gate 2：与湖侧已物化对照表逐日整行比（若存在）
  if [ "$has_lake_dws" = 1 ]; then
    cat >> "$CHK_SQL" <<SQL
SELECT CONCAT('lake_dws_range|', COUNT(*), '|', COUNT(DISTINCT dt), '|', SUM(order_cnt), '|', SUM(paid_cnt), '|', SUM(paid_amount), '|', SUM(refund_amount))
  FROM lake.dws.dws_user_order_day WHERE dt >= '$D1' AND dt <= '$D2';
SELECT CONCAT('dws_ref_mismatch|', COUNT(*)) FROM (
  SELECT 1 FROM (
      SELECT user_id, dt, order_cnt, paid_cnt, paid_amount, refund_amount
        FROM dws.dws_user_order_day WHERE dt >= '$D1' AND dt <= '$D2'
      UNION ALL
      SELECT user_id, dt, order_cnt, paid_cnt, paid_amount, refund_amount
        FROM lake.dws.dws_user_order_day WHERE dt >= '$D1' AND dt <= '$D2'
  ) t
  GROUP BY user_id, dt, order_cnt, paid_cnt, paid_amount, refund_amount
  HAVING COUNT(*) <> 2
) z;
SQL
  fi
  if [ "$has_lake_ads" = 1 ]; then
    cat >> "$CHK_SQL" <<SQL
SELECT CONCAT('lake_ads_range|', COUNT(*), '|', COUNT(DISTINCT dt), '|', SUM(order_cnt), '|', SUM(paid_cnt), '|', SUM(paid_amount), '|', SUM(refund_amount), '|', SUM(net_amount), '|', SUM(avg_order_amount), '|', SUM(pay_rate))
  FROM lake.ads.ads_daily_sales WHERE dt >= '$D1' AND dt <= '$D2';
SELECT CONCAT('ads_ref_mismatch|', COUNT(*)) FROM (
  SELECT 1 FROM (
      SELECT dt, order_cnt, paid_cnt, paid_amount, refund_amount, net_amount, avg_order_amount, pay_rate
        FROM ads.ads_daily_sales WHERE dt >= '$D1' AND dt <= '$D2'
      UNION ALL
      SELECT dt, order_cnt, paid_cnt, paid_amount, refund_amount, net_amount, avg_order_amount, pay_rate
        FROM lake.ads.ads_daily_sales WHERE dt >= '$D1' AND dt <= '$D2'
  ) t
  GROUP BY dt, order_cnt, paid_cnt, paid_amount, refund_amount, net_amount, avg_order_amount, pay_rate
  HAVING COUNT(*) <> 2
) z;
SQL
  fi
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

  local has_lake_dws=0 has_lake_ads=0
  mysql_q "SELECT 1 FROM lake.dws.dws_user_order_day LIMIT 1" >/dev/null 2>&1 && has_lake_dws=1
  mysql_q "SELECT 1 FROM lake.ads.ads_daily_sales LIMIT 1"     >/dev/null 2>&1 && has_lake_ads=1
  log "  湖侧对照表可用性：lake.dws.dws_user_order_day=$has_lake_dws  lake.ads.ads_daily_sales=$has_lake_ads"

  build_check_sql "$has_lake_dws" "$has_lake_ads"
  local out v n w rc=0
  out="$(mysql_file "$CHK_SQL")"

  log ""
  log "-- A. 整表自检（rows|days|oc|pc|pa|ra|na|aoa_sum|pr_sum）--"
  printf '%s\n' "$out" | grep -E '^wh_(dws|ads)_whole\|' | sed 's/^/    /' || true

  log ""
  log "-- B. 区间聚合 --"
  printf '%s\n' "$out" | grep -E '^(wh|lake)_(dws|ads)_range\|' | sed 's/^/    /' || true

  log ""
  log "-- C. 整行对称差 gate（必须全为 0）--"
  printf '%s\n' "$out" | grep -E '_mismatch\|' | sed 's/^/    /' || true

  # B. 逐字段对照（仓侧 vs 湖侧对照表）
  for pair in "dws:wh_dws_range:lake_dws_range" "ads:wh_ads_range:lake_ads_range"; do
    IFS=':' read -r kind whk lkk <<< "$pair"
    w="$(printf '%s\n' "$out" | grep "^$whk|" | head -n1 || true)"
    v="$(printf '%s\n' "$out" | grep "^$lkk|" | head -n1 || true)"
    if [ -n "$v" ]; then
      if [ "${w#*|}" = "${v#*|}" ]; then
        log "    [OK]   仓侧 $kind 区间聚合 == 湖侧对照表（逐字段一致）"
      else
        fail "仓侧 $kind 区间聚合 != 湖侧对照表"
        log "          仓侧：${w#*|}"
        log "          湖侧：${v#*|}"
        rc=1
      fi
    fi
  done

  # C. 对称差 gate
  for k in dws_mismatch ads_mismatch dws_ref_mismatch ads_ref_mismatch; do
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
log " 区间      : $D1 ~ $D2   模式：$([ "$DRY" = 1 ] && echo 'DRY-RUN（只读）' || echo "写库/$MODE")"
log "=============================================================="
log ""

preflight

if [ "$DRY" = 1 ]; then
  if dry_run; then exit 0; else exit 2; fi
fi

run_materialize
if self_check; then exit 0; else exit 2; fi
