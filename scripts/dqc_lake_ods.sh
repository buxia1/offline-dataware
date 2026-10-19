#!/usr/bin/env bash
# ============================================================
# dqc_lake_ods.sh —— 湖侧 ODS 入口质量闸门（6 项）
#
# 为什么需要它：新链路唯一的数据入口是 Kafka → lake.ods（重放节点即使读到 0 条
# 也 exit 0）。而下游所有 DQC / 对账检查的都是「两个实体互相一致」的相对关系，
# 整天数据缺失时两边一起缺，照样全绿。本脚本给的是「绝对断言」：这一天必须有
# 数据、量级不能断崖、主键唯一、枚举合法、重复率可控、近 7 天无缺口。
#
# 用法：bash scripts/dqc_lake_ods.sh [YYYYMMDD]     # 缺省=昨天
# 退出码：0 通过 / 1 不通过或脚本异常
# 环境变量：SR_CT（StarRocks 容器名，默认 starrocks）
# ============================================================
set -uo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"
SQL_FILE="$ROOT/sql/dqc/dqc_lake_ods.sql"
RESET_SH="$ROOT/scripts/lake/sr_catalog_reset.sh"
SR_CT="${SR_CT:-starrocks}"

BIZ="${1:-}"
if [ -z "$BIZ" ]; then
  BIZ="$(date -d yesterday +%Y%m%d)"
fi
if ! [[ "$BIZ" =~ ^[0-9]{8}$ ]]; then
  echo "用法：bash $0 [YYYYMMDD]" >&2
  exit 1
fi
DT="${BIZ:0:4}-${BIZ:4:2}-${BIZ:6:2}"

sr() { docker exec "$SR_CT" mysql -h127.0.0.1 -P9030 -uroot -N -B -e "$1"; }

echo "=== DQC 湖侧 ODS  dt=$DT"

# ---------- 0) 刷 FE 元数据缓存（坑 31：FE 按路径缓存 Iceberg 元数据） ----------
REFRESHED=0
for _ in 1 2 3; do
  if sr "REFRESH EXTERNAL TABLE lake.ods.ods_order; REFRESH EXTERNAL TABLE lake.ods.ods_order_event" >/dev/null 2>&1; then
    REFRESHED=1
    break
  fi
  sleep 1
done
if [ "$REFRESHED" -eq 0 ]; then
  echo "  ⚠️ REFRESH 连续失败，尝试重置外部 catalog"
  if [ -x "$RESET_SH" ] || [ -f "$RESET_SH" ]; then
    bash "$RESET_SH" || { echo "!!! catalog 重置失败"; exit 1; }
  else
    echo "!!! REFRESH 失败且找不到 $RESET_SH"
    exit 1
  fi
fi

# ---------- 1) 动态阈值：前 7 天（含当日）日均 × 0.7 ----------
# 注意：StarRocks 对「一个分区都没匹配上」的外部表查询会返回 NULL，这里统一兜成 0
num() { case "${1:-}" in ''|*[!0-9]*) echo 0 ;; *) echo "$1" ;; esac; }
THR_ORDER="$(num "$(sr "SELECT CAST(avg(c) * 0.7 AS INT) FROM (SELECT dt, count(*) AS c FROM lake.ods.ods_order       WHERE dt > date_sub('$DT', 7) AND dt <= '$DT' GROUP BY dt) t")")"
THR_EVENT="$(num "$(sr "SELECT CAST(avg(c) * 0.7 AS INT) FROM (SELECT dt, count(*) AS c FROM lake.ods.ods_order_event WHERE dt > date_sub('$DT', 7) AND dt <= '$DT' GROUP BY dt) t")")"
echo "  阈值（前 7 天日均 ×0.7）：orders ≥ $THR_ORDER，events ≥ $THR_EVENT（无历史时为 0，由 C1 兜底）"

# ---------- 2) 跑 6 项检查 ----------
OUT="$(mktemp)"
sed -e "s/\${DT}/$DT/g" -e "s/\${THR_ORDER}/$THR_ORDER/g" -e "s/\${THR_EVENT}/$THR_EVENT/g" "$SQL_FILE" \
  | docker exec -i "$SR_CT" mysql -h127.0.0.1 -P9030 -uroot -N -B > "$OUT" 2>&1
SQL_RC=$?

if [ "$SQL_RC" -ne 0 ]; then
  echo "!!! 检查 SQL 执行失败（rc=$SQL_RC）："
  sed 's/^/    /' "$OUT"
  rm -f "$OUT"
  exit 1
fi

# ---------- 3) 逐条判定 ----------
CHECKS=0
FAIL=0
SUMMARY=""
while IFS=$'\t' read -r CID CNAME OKV DETAIL; do
  [ -z "${CID:-}" ] && continue
  CHECKS=$((CHECKS + 1))
  if [ "${OKV:-}" = "1" ]; then
    echo "  ✅ $CID $CNAME  ($DETAIL)"
    SUMMARY="$SUMMARY|$CID=1"
  else
    echo "  ❌ $CID $CNAME  ($DETAIL)"
    SUMMARY="$SUMMARY|$CID=0"
    FAIL=$((FAIL + 1))
  fi
done < "$OUT"
rm -f "$OUT"

if [ "$CHECKS" -ne 6 ]; then
  echo "!!! 检查项数量异常：$CHECKS（期望 6），疑似 SQL 占位符或语法问题"
  exit 1
fi

if [ "$FAIL" -gt 0 ]; then
  echo "!!! 湖侧 ODS 质量检查失败：$FAIL/$CHECKS 项不达标"
  exit 1
fi

echo "DQC_RESULT|OK|checks=$CHECKS|dt=$DT$SUMMARY"
echo "✅ 全部通过"
exit 0
