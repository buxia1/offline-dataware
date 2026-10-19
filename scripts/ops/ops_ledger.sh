#!/usr/bin/env bash
# ============================================================================
# 湖仓每日巡检台账：把「当天这次跑的结果」追加一行到 CSV
#
# 用法（DS 第 10 个节点 lake_ops_ledger，Shell 任务，超时 5 min）：
#   bash /opt/offline-dw/scripts/ops/ops_ledger.sh ${system.biz.date}
#   手动：bash scripts/ops/ops_ledger.sh 20261018
#
# 台账文件：容器内 /opt/dolphinscheduler/logs/ops/ledger.csv（26 列，2026-10-19 起含湖侧 ODS 闸门 5 列）
#          宿主机 ds/logs/ops/ledger.csv（logs 是 rw 挂载，容器重启不丢）
#
# 设计约束（DS 容器里只有这些能用）：
#   · 只有 docker.sock（可 docker exec），没有挂载湖仓盘 → 湖表行数/快照数
#     从【本实例 lake_maintain 节点的日志】解析，不重跑 Spark、零额外开销
#   · 节点统计**排除台账节点自己**：它是流程最后一个节点，读的时候自己还在 RUNNING，
#     否则会被算成 nodes_fail=1（2026-10-19 实例 398 实测踩到）
#   · 读的时候流程本身也还在 RUNNING（end_time 为空）→ flow_state 用「其余节点是否全绿」
#     推断（全绿=7 / 否则=6），duration 取「启动至今」
#   · 退出码：0 写入成功；1 取不到实例（流程没跑 / DS 元数据库连不上）
# ============================================================================
set -uo pipefail

RAW="${1:-}"
if [[ "$RAW" =~ ^[0-9]{8}$ ]]; then
  DT="${RAW:0:4}-${RAW:4:2}-${RAW:6:2}"
else
  DT="$(date -d 'yesterday' +%Y-%m-%d)"
fi

FLOW_NAME="offline_dataware_lake"
MYSQL_CT="mysql"
KAFKA_CT="kafka"
SPARK_CT="spark"
LOG_ROOT="/opt/dolphinscheduler/logs"
OUT_DIR="${LOG_ROOT}/ops"
OUT="${LEDGER_OUT:-${OUT_DIR}/ledger.csv}"   # 可用 LEDGER_OUT 覆盖（自测/回放用）

ds_sql() { docker exec "$MYSQL_CT" mysql -uroot -proot123 -N -B dolphinscheduler -e "$1" 2>/dev/null; }
kafka_msgs() {  # $1=topic → 各分区 latest offset 之和（= 主题里现存消息条数）
  docker exec "$KAFKA_CT" /opt/kafka/bin/kafka-get-offsets.sh \
      --bootstrap-server localhost:9092 --topic "$1" --time -1 2>/dev/null \
    | awk -F: '{s+=$3} END{printf "%d", s+0}'
}
NOTES=()

# ---------- 1) 定位本实例：数据日 DT 的次日凌晨那次跑；手动补跑则退回「今天最近一次」 ----------
FLOW_CODE="$(ds_sql "SELECT code FROM t_ds_process_definition
  WHERE name='${FLOW_NAME}' ORDER BY version DESC LIMIT 1;")"
if [[ -z "${FLOW_CODE:-}" ]]; then
  echo "!!! 取不到 ${FLOW_NAME} 的 code（DS 元数据库连不上？）" >&2; exit 1
fi

INST="$(ds_sql "SELECT id FROM t_ds_process_instance
  WHERE process_definition_code=${FLOW_CODE} AND DATE(start_time)=DATE_ADD('${DT}', INTERVAL 1 DAY)
  ORDER BY id DESC LIMIT 1;")"
[[ -z "${INST:-}" ]] && INST="$(ds_sql "SELECT id FROM t_ds_process_instance
  WHERE process_definition_code=${FLOW_CODE} AND DATE(start_time)=CURDATE()
  ORDER BY id DESC LIMIT 1;")"
[[ -z "${INST:-}" ]] && INST="$(ds_sql "SELECT id FROM t_ds_process_instance
  WHERE process_definition_code=${FLOW_CODE} ORDER BY id DESC LIMIT 1;")"
if [[ -z "${INST:-}" ]]; then echo "!!! 找不到 ${FLOW_NAME} 的实例" >&2; exit 1; fi

read -r FLOW_STATE DUR_S RUN_DATE < <(ds_sql "SELECT state,
    COALESCE(TIMESTAMPDIFF(SECOND, start_time, end_time), -1),
    DATE_FORMAT(start_time, '%Y-%m-%d')
  FROM t_ds_process_instance WHERE id=${INST};" | awk '{print $1, $2, $3}')
# 节点统计排除台账节点自己（它读的时候还在 RUNNING），否则会误报 nodes_fail=1
read -r NODES_OK NODES_FAIL < <(ds_sql "SELECT COALESCE(SUM(state=7),0), COALESCE(SUM(state<>7),0)
  FROM t_ds_task_instance WHERE process_instance_id=${INST}
    AND REPLACE(name,'\t','') <> 'lake_ops_ledger';" | awk '{print $1, $2}')

# 台账是流程最后一个节点 → 流程本身还在 RUNNING、end_time 为空：
#   flow_state 用「其余节点是否全绿」推断；duration 取「启动至今」
[[ "${LEDGER_FORCE_RUNNING:-0}" == "1" ]] && DUR_S="-1"   # 自测：模拟「流程还在跑」
if [[ "${DUR_S}" == "-1" ]]; then
  DUR_S="$(ds_sql "SELECT TIMESTAMPDIFF(SECOND, start_time, NOW())
    FROM t_ds_process_instance WHERE id=${INST};")"
  DUR_S="${DUR_S:-0}"
  if [[ "${NODES_FAIL}" == "0" ]]; then FLOW_STATE=7; else FLOW_STATE=6; fi
fi

# 节点名去前导 Tab 后建 name=state 字典
NODE_STATES="$(ds_sql "SELECT REPLACE(name, '\t', ''), state FROM t_ds_task_instance
  WHERE process_instance_id=${INST};" | awk '{print $1"=" $2}')"
node_state() { echo "$NODE_STATES" | awk -F= -v k="$1" '$1==k{print $2; exit}'; }
DQC_ORDER="$(node_state dqc_order_chain)"
DQC_DIM="$(node_state dqc_dim_product)"
RECON_STATE="$(node_state recon_lake_wh)"
MAINTAIN_STATE="$(node_state lake_maintain)"

# 本实例的日志目录（.../logs/<yyyymmdd>/<flowCode>/<version>/<实例id>/<任务实例id>.log）
INST_LOG_DIR="$(find "${LOG_ROOT}" -maxdepth 6 -type d -name "${INST}" -path "*${FLOW_CODE}*" 2>/dev/null | head -1)"

# ---------- 2) 从 lake_maintain 日志解析湖表行数 / 快照数（零额外开销）----------
MAINTAIN_ID="$(ds_sql "SELECT id FROM t_ds_task_instance
  WHERE process_instance_id=${INST} AND REPLACE(name,'\t','')='lake_maintain' LIMIT 1;")"
SNAPSHOTS_MAX=0; ODS_ORDER=0; ODS_EVENT=0; DWD_DETAIL=0; DWD_LIFECYCLE=0
MAINTAIN_LOG="${INST_LOG_DIR}/${MAINTAIN_ID}.log"
if [[ -n "${INST_LOG_DIR:-}" && -f "${MAINTAIN_LOG}" ]]; then
  # 每行形如：lake.ods.ods_order   快照 5  数据文件 90 (7.8 MB)  清单 12 (0.1 MB)  行数 900000
  INV="$(sed -n '/维护前/,$p' "${MAINTAIN_LOG}" | grep -E '^[[:space:]]+lake\.' | tail -8 |
         awk '{snap=0;rows=0; for(i=2;i<=NF;i++){if($i=="快照")snap=$(i+1); if($i=="行数")rows=$(i+1)} print $1, snap, rows}')"
  while read -r TBL SNAP ROWS; do
    [[ -z "${TBL:-}" ]] && continue
    (( SNAP > SNAPSHOTS_MAX )) && SNAPSHOTS_MAX=$SNAP
    case "$TBL" in
      lake.ods.ods_order)           ODS_ORDER=$ROWS ;;
      lake.ods.ods_order_event)     ODS_EVENT=$ROWS ;;
      lake.dwd.dwd_order_detail)    DWD_DETAIL=$ROWS ;;
      lake.dwd.dwd_order_lifecycle) DWD_LIFECYCLE=$ROWS ;;
    esac
  done <<< "$INV"
  [[ "$SNAPSHOTS_MAX" == 0 ]] && NOTES+=("maintain-log-unparsed")
else
  NOTES+=("no-maintain-log")
fi

# ---------- 2.5) 湖侧 ODS 闸门（dqc_lake_ods 节点日志）----------
# 日志形如：DQC_RESULT|OK|checks=6|dt=2026-10-18|C1=1|...
#          ✅ C1 业务日非空  (orders=10000 events=33622)
#          ✅ C5 事件重复率≤1.10  (ratio=1.0000)
DQ_ODS_ID="$(ds_sql "SELECT id FROM t_ds_task_instance
  WHERE process_instance_id=${INST} AND REPLACE(name,'\t','')='dqc_lake_ods' LIMIT 1;")"
DQ_ODS_STATE="$(node_state dqc_lake_ods)"
DQ_ODS_RESULT=""; DQ_ODS_CHECKS=""; DQ_ODS_ORDERS=""; DQ_ODS_EVENTS=""; DQ_ODS_DUP=""
DQ_ODS_LOG="${INST_LOG_DIR}/${DQ_ODS_ID}.log"
if [[ -n "${INST_LOG_DIR:-}" && -f "${DQ_ODS_LOG}" ]]; then
  DQ_ODS_RESULT="$(grep -o 'DQC_RESULT|[A-Z]*' "${DQ_ODS_LOG}" | tail -1 | cut -d'|' -f2)"
  DQ_ODS_CHECKS="$(grep -o 'checks=[0-9]*' "${DQ_ODS_LOG}" | tail -1 | cut -d= -f2)"
  DQ_ODS_ORDERS="$(grep -o 'orders=[0-9][0-9]*' "${DQ_ODS_LOG}" | head -1 | cut -d= -f2)"
  DQ_ODS_EVENTS="$(grep -o 'events=[0-9][0-9]*' "${DQ_ODS_LOG}" | head -1 | cut -d= -f2)"
  DQ_ODS_DUP="$(grep -o 'ratio=[0-9.]*' "${DQ_ODS_LOG}" | tail -1 | cut -d= -f2)"
fi
if [[ -z "${DQ_ODS_RESULT}" ]]; then
  [[ "${DQ_ODS_STATE:-}" == 7 ]] && DQ_ODS_RESULT="OK(from-state)" || DQ_ODS_RESULT="UNKNOWN"
  NOTES+=("dqc-ods-log-missing")
fi

# ---------- 3) 对账结果（recon 节点日志里的 RECON_RESULT|OK|checks=26|range=...）----------
RECON_ID="$(ds_sql "SELECT id FROM t_ds_task_instance
  WHERE process_instance_id=${INST} AND REPLACE(name,'\t','')='recon_lake_wh' LIMIT 1;")"
RECON_RESULT=""; RECON_CHECKS=""
RECON_LOG="${INST_LOG_DIR}/${RECON_ID}.log"
if [[ -n "${INST_LOG_DIR:-}" && -f "${RECON_LOG}" ]]; then
  RECON_RESULT="$(grep -o 'RECON_RESULT|[A-Z]*' "${RECON_LOG}" | tail -1 | cut -d'|' -f2)"
  RECON_CHECKS="$(grep -o 'checks=[0-9]*' "${RECON_LOG}" | tail -1 | cut -d= -f2)"
fi
if [[ -z "$RECON_RESULT" ]]; then
  [[ "${RECON_STATE:-}" == 7 ]] && RECON_RESULT="OK(from-state)" || RECON_RESULT="UNKNOWN"
  NOTES+=("recon-log-missing")
fi

# ---------- 4) Kafka 现存消息数 + 仓库体积 ----------
K_ORDER="$(kafka_msgs ods_order)"
K_EVENT="$(kafka_msgs ods_order_event)"
WH_MB="$(docker exec "$SPARK_CT" du -sb /opt/offline-dw/lake/warehouse 2>/dev/null | cut -f1 |
        awk '{printf "%.1f", $1/1048576}')"
[[ -z "${WH_MB:-}" ]] && { WH_MB="?"; NOTES+=("du-failed"); }

# ---------- 5) 追加一行（表头只写一次）----------
mkdir -p "$OUT_DIR"
if [[ ! -s "$OUT" ]]; then
  echo "dt,run_date,instance_id,flow_state,duration_s,nodes_ok,nodes_fail,dqc_order_state,dqc_dim_state,recon_state,recon_result,recon_checks,ods_order_rows,ods_event_rows,dwd_detail_rows,dwd_lifecycle_rows,snapshots_max,kafka_order_msgs,kafka_event_msgs,warehouse_mb,dq_ods_state,dq_ods_result,dq_ods_orders,dq_ods_events,dq_ods_dup,notes" > "$OUT"
fi
NOTE_STR="$(IFS=';'; echo "${NOTES[*]:-}")"
echo "${DT},${RUN_DATE},${INST},${FLOW_STATE},${DUR_S},${NODES_OK},${NODES_FAIL},${DQC_ORDER:-?},${DQC_DIM:-?},${RECON_STATE:-?},${RECON_RESULT},${RECON_CHECKS:-?},${ODS_ORDER},${ODS_EVENT},${DWD_DETAIL},${DWD_LIFECYCLE},${SNAPSHOTS_MAX},${K_ORDER},${K_EVENT},${WH_MB},${DQ_ODS_STATE:-?},${DQ_ODS_RESULT},${DQ_ODS_ORDERS:--},${DQ_ODS_EVENTS:--},${DQ_ODS_DUP:--},${NOTE_STR}" >> "$OUT"

# ---------- 6) 摘要（进 DS 节点日志）----------
echo "=========================================================================="
echo "巡检台账 ${DT}  写入 ${OUT}"
echo "  实例 ${INST}  state=${FLOW_STATE}  耗时 ${DUR_S}s  节点 ${NODES_OK} 绿 / ${NODES_FAIL} 红"
echo "  DQC 订单=${DQC_ORDER:-?}  商品=${DQC_DIM:-?}   对账=${RECON_STATE:-?} (${RECON_RESULT}, checks=${RECON_CHECKS:-?})   维护=${MAINTAIN_STATE:-?}"
echo "  湖表行数  ods_order=${ODS_ORDER}  ods_event=${ODS_EVENT}  dwd_detail=${DWD_DETAIL}  lifecycle=${DWD_LIFECYCLE}  最大快照数=${SNAPSHOTS_MAX}"
echo "  湖侧 ODS 闸门 state=${DQ_ODS_STATE:-?} ${DQ_ODS_RESULT} checks=${DQ_ODS_CHECKS:-?} orders=${DQ_ODS_ORDERS:--} events=${DQ_ODS_EVENTS:--} dup=${DQ_ODS_DUP:--}"
echo "  Kafka 现存消息 order=${K_ORDER} event=${K_EVENT}   仓库=${WH_MB} MB   备注=${NOTE_STR:-无}"
echo "--------------------------------------------------------------------------"
tail -3 "$OUT"
echo "=========================================================================="
exit 0
