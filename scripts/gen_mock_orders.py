# -*- coding: utf-8 -*-
"""模拟订单「事件流」生成器 —— 累积快照事实表 dwd.dwd_order_lifecycle 的数据源

把一个订单的生命周期拆成一个个事件，发到 Kafka：

    order → pay → ship → finish      正常单（normal_fast / normal_slow）
    order → pay → ship               stuck_finish（永不完成）
    order → pay                      stuck_ship  （永不发货）
    order                             stuck_pay   （永不支付）
    order → pay → cancel             cancel      （先支付再取消）

用法：
    python3 scripts/gen_mock_orders.py --date 2026-09-20 --dry-run   # 只打印，不连 Kafka
    python3 scripts/gen_mock_orders.py --date 2026-09-20             # 真发 Kafka

【方案：按天产生事件】
    --date 只发「到期日 == 这一天」的事件：
      · 这天新下的单                          → 发 order 事件
      · 存量订单里，里程碑到期日正好是这天的   → 发对应事件
    → 必须【从最早一天开始按天连续推进】。跳过某天 = 那天的事件永远不补发。
      脚本为此做两类漏发检查，命中就打印告警并返回退出码 1：
        · overdue      到期日已过、却仍未发（跳天的最直接证据；连最后一个里程碑也能抓到）
        · missing_prev 同一批里前置里程碑没发（级联保护）

【状态从 ODS 读，不维护状态文件】
    「下一个待发的里程碑」= ods.ods_order_event 里第一个 event_time 为 NULL 的里程碑。
    不用 LEAST/MIN 判空：LEAST(NULL, x) 返回 NULL 会静默算错；一律显式判 NULL。

【可重放】
    order_id 由日期推导、订单属性由 random.Random(order_id) 决定
    → 同一天重跑产出逐字节相同。Kafka 允许重复（ODS 是追加层、Spark 用 earliest
      全量重读），装载侧用 MAX/MIN(event_time) 聚合 → 天然幂等。

【order_id 自描述】
    order_id = int(YYYYMMDD) * 1000 + i        (i = 0..99)
    → order_id % 1000 = 当天序号 i（类别也只需这一个数）
    → order_id % 100  = 类别区间（沿用原设计 order_id % 100 的判定）

【和旧生成器的区别】
    · 旧：一次发 N 条「订单宽消息」，status 是随机的字符串
    · 新：按天发「事件」，每个事件带 event_type + event_time（里程碑时间戳）
    · 旧脚本（scripts/gen_mock_orders.py 的老版本）不在本文件里，见 git 历史
    · 本脚本写的是新 topic ods_order_event，旧 topic ods_order 完全不动
"""
import argparse
import json
import random
import subprocess
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta

# ============ 可调参数 ============
TOPIC = "ods_order_event"
KAFKA_SERVERS = "localhost:9092"

# 每天固定造 100 个新订单：i 必须覆盖 0..99，6 个类别才会都出现
NEW_ORDERS_PER_DAY = 100

NULL_USER_RATE = 0.05     # 5% 的记录 user_id 为 None（脏数据）
NEG_AMOUNT_RATE = 0.03    # 3% 的记录金额为负数（脏数据）

# 商品单价表：沿用老公式，保证金额口径和 ods_order 对得上
PRICE_TABLE = {pid: round(9.9 + (pid % 10) * 10, 2) for pid in range(1, 51)}

# 类别区间：按 order_id % 100 判定
CATEGORY_RANGES = [
    ("normal_fast",   0, 29),
    ("normal_slow",  30, 59),
    ("stuck_pay",    60, 69),
    ("stuck_ship",   70, 79),
    ("stuck_finish", 80, 89),
    ("cancel",       90, 99),
]

# 每类的里程碑偏移（秒）。None = 永不发生（这就是「卡单」）
H = 3600
D = 86400
CATEGORY_PLAN = {
    "normal_fast":  {"pay": 2 * H, "ship": 2 * D, "finish": 4 * D},
    "normal_slow":  {"pay": 6 * H, "ship": 4 * D, "finish": 8 * D},
    "stuck_pay":    {"pay": None},
    "stuck_ship":   {"pay": 3 * H, "ship": None},
    "stuck_finish": {"pay": 4 * H, "ship": 2 * D, "finish": None},
    "cancel":       {"pay": 4 * H, "cancel": 1 * D},
}

# 事件的定义顺序：同一天多个里程碑到期时，按这个顺序产出／回填
EVENT_SEQ = {"order": 0, "pay": 1, "ship": 2, "finish": 3, "cancel": 4}
PHASE2_ORDER = ("pay", "ship", "finish", "cancel")

FMT = "%Y-%m-%d %H:%M:%S"
FMT_DATE = "%Y-%m-%d"
# =================================

def category_of(residue):
    """order_id % 100 → 类别名"""
    for name, lo, hi in CATEGORY_RANGES:
        if lo <= residue <= hi:
            return name
    raise ValueError("没有类别覆盖 %r" % residue)

def make_order_time(order_id, day):
    """同一个 order_id 永远得到同一个下单时刻（下单日 = day）"""
    rnd = random.Random("%d:time" % order_id)          # 字符串种子 → sha512，跨进程稳定
    sec = rnd.randrange(0, 24 * 60 * 60)
    return datetime(day.year, day.month, day.day) + timedelta(seconds=sec)

def make_attributes(order_id):
    """订单级属性：只由 order_id 决定 → 同一订单的每个事件都带同一份属性"""
    rnd = random.Random("%d:attr" % order_id)
    product_id = rnd.randint(1, 50)
    quantity = rnd.randint(1, 5)
    amount = round(PRICE_TABLE[product_id] * quantity, 2)
    user_id = rnd.randint(1, 200)
    if rnd.random() < NULL_USER_RATE:                  # 脏数据 1：user_id 置空
        user_id = None
    if rnd.random() < NEG_AMOUNT_RATE:                 # 脏数据 2：金额变负
        amount = -amount
    return {
        "order_id": order_id,
        "user_id": user_id,
        "product_id": product_id,
        "amount": amount,
    }

# 从 ODS 读「每个订单已发到哪一步」。
# MAX(CASE WHEN ...) 而不是 LEAST/MIN：LEAST(NULL, x) = NULL 会静默算错。
ODS_QUERY = """
SELECT order_id,
       DATE_FORMAT(MIN(order_time), '%Y-%m-%d %H:%i:%s')                AS order_time,
       MAX(CASE WHEN event_type = 'order'  THEN event_time END)         AS t_order,
       MAX(CASE WHEN event_type = 'pay'    THEN event_time END)         AS t_pay,
       MAX(CASE WHEN event_type = 'ship'   THEN event_time END)         AS t_ship,
       MAX(CASE WHEN event_type = 'finish' THEN event_time END)         AS t_finish,
       MAX(CASE WHEN event_type = 'cancel' THEN event_time END)         AS t_cancel
FROM ods.ods_order_event
GROUP BY order_id
"""

def load_existing():
    """返回 {order_id: {"order_time": str, "emitted": {event_type: str}}}

    表还没建时不要崩：按「无存量订单」处理（第一次跑就是这种情况）。
    """
    cmd = ["docker", "exec", "starrocks", "mysql", "-P9030", "-h127.0.0.1",
           "-uroot", "-N", "-B", "-e", ODS_QUERY]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, check=True)
    except FileNotFoundError:
        print("⚠️  找不到 docker 命令 → 按「无存量订单」处理")
        return {}
    except subprocess.CalledProcessError as exc:
        msg = (exc.stderr or "").strip().splitlines()
        print("⚠️  读 ods.ods_order_event 失败（表还没建？）→ 按「无存量订单」处理")
        if msg:
            print("    " + msg[-1][:200])
        return {}

    existing = {}
    for line in proc.stdout.splitlines():
        parts = line.split("\t")
        if len(parts) < 7 or not parts[0].isdigit():
            continue
        emitted = {}
        for idx, etype in enumerate(("order", "pay", "ship", "finish", "cancel"), start=2):
            if parts[idx] != "NULL":                   # mysql -B 把 NULL 打成字符串 NULL
                emitted[etype] = parts[idx]
        existing[int(parts[0])] = {"order_time": parts[1], "emitted": emitted}
    return existing

def generate(day, existing):
    """纯函数：给定「日期」和「ODS 现状」，返回 (事件列表, 告警列表)

    事件元组 = (event_time(datetime), order_id, event_type, payload(dict), category)
    不碰 Kafka、不碰库 → 可以脱库单测（见 t_check_events.py）
    """
    events = []
    warnings = []

    # 在内存里推进一份状态副本：新订单当天就能产出「同日到期」的后续事件
    state = {oid: {"order_time": st["order_time"], "emitted": dict(st["emitted"])}
             for oid, st in existing.items()}

    # ---------- 阶段一：这天新下的单 → order 事件 ----------
    base = int(day.strftime("%Y%m%d")) * 1000
    for i in range(NEW_ORDERS_PER_DAY):
        order_id = base + i
        if order_id in state:
            continue                                   # 重跑时该单已存在 → 幂等
        order_time = make_order_time(order_id, day)
        payload = make_attributes(order_id)
        payload["order_time"] = order_time.strftime(FMT)
        payload["event_type"] = "order"
        payload["event_time"] = payload["order_time"]
        events.append((order_time, order_id, "order", payload, category_of(order_id % 100)))
        state[order_id] = {"order_time": payload["order_time"],
                           "emitted": {"order": payload["order_time"]}}

    # ---------- 阶段二：存量（含刚建的）订单里，今天到期的里程碑 ----------
    for order_id in sorted(state):
        st = state[order_id]
        plan = CATEGORY_PLAN[category_of(order_id % 100)]
        t0 = datetime.strptime(st["order_time"], FMT)
        done_today = set()
        for ev_name in PHASE2_ORDER:
            if ev_name not in plan:
                continue
            offset = plan[ev_name]
            if offset is None:                         # 永不 → 卡单，不发
                continue
            if ev_name in st["emitted"]:               # 已经发过
                continue
            due = t0 + timedelta(seconds=offset)
            if due.date() < day:
                # 到期日已经过去却还没发 → 说明那天没跑（跳天）。
                # 按设计不补发，但必须吼出来，否则缺口是静默的。
                # 这个检查连「最后一个里程碑被跳过」也能抓到（前置检查抓不到）。
                warnings.append({"kind": "overdue", "order_id": order_id,
                                 "event_type": ev_name, "missing": [],
                                 "due": due.strftime(FMT)})
                continue
            if due.date() != day:                      # 还没到期
                continue
            # 前置检查：同一天里更早的里程碑没发（级联）→ 不发并告警
            missing = [p for p in plan
                       if plan[p] is not None
                       and EVENT_SEQ[p] < EVENT_SEQ[ev_name]
                       and p not in st["emitted"]
                       and p not in done_today]
            if missing:
                warnings.append({"kind": "missing_prev", "order_id": order_id,
                                 "event_type": ev_name, "missing": missing,
                                 "due": due.strftime(FMT)})
                continue
            payload = make_attributes(order_id)
            payload["order_time"] = st["order_time"]
            payload["event_type"] = ev_name
            payload["event_time"] = due.strftime(FMT)
            events.append((due, order_id, ev_name, payload,
                           category_of(order_id % 100)))
            done_today.add(ev_name)

    # 同一天多个事件时，按「时间 → order_id → 定义顺序」稳定排序
    events.sort(key=lambda e: (e[0], e[1], EVENT_SEQ[e[2]]))
    return events, warnings

def print_table(events):
    head = ("%-10s %-14s %-4s %-13s %-10s %-19s %-8s %-10s %10s"
            % ("date", "order_id", "i", "category", "event_type",
               "event_time", "user_id", "product_id", "amount"))
    print(head)
    print("-" * len(head))
    for _t, order_id, ev_name, p, cat in events:
        print("%-10s %-14d %-4d %-13s %-10s %-19s %-8s %-10d %10.2f"
              % (p["event_time"][:10], order_id, order_id % 1000, cat, ev_name,
                 p["event_time"], ("NULL" if p["user_id"] is None else p["user_id"]),
                 p["product_id"], p["amount"]))

def self_check(events, warnings):
    """返回 (ok, 明细行列表)。三条断言都直接来自事件列表，一眼可判。"""
    lines = []

    # 断言 1：(order_id, event_type) 不重复
    seen = Counter((oid, ev) for _t, oid, ev, _p, _c in events)
    dups = [k for k, v in seen.items() if v > 1]
    lines.append(("无重复 (order_id,event_type)", not dups, "重复 %d 组" % len(dups)))

    # 断言 2：同一个订单的事件按 event_time 严格递增
    per_order = defaultdict(list)
    for t, oid, ev, _p, _c in events:
        per_order[oid].append((t, ev))
    bad = []
    for oid, lst in per_order.items():
        times = [t for t, _e in lst]
        if times != sorted(times) or len(set(times)) != len(times):
            bad.append(oid)
    lines.append(("每单 event_time 严格递增", not bad, "异常 %d 单" % len(bad)))

    # 断言 3：没有「过期未发 / 前置缺失」的静默缺口
    overdue = [w for w in warnings if w["kind"] == "overdue"]
    cascade = [w for w in warnings if w["kind"] == "missing_prev"]
    lines.append(("逾期未发(跳天)", not overdue, "%d 条" % len(overdue)))
    lines.append(("前置里程碑缺失(级联)", not cascade, "%d 条" % len(cascade)))
    return lines

def print_summary(day, existing, events, warnings):
    cat = Counter(c for _t, _o, _e, _p, c in events)
    typ = Counter(e for _t, _o, e, _p, _c in events)
    orders = set(o for _t, o, _e, _p, _c in events)
    order_ev = typ.get("order", 0)

    print("\n---------------- 汇总 ----------------")
    print("主题                %s" % TOPIC)
    print("日期                %s" % day.strftime(FMT_DATE))
    print("存量订单(读自 ODS)   %d" % len(existing))
    print("事件合计            %d" % len(events))
    print("  其中 order 事件    %d   (= 这天新下单数)" % order_ev)
    print("  其中里程碑事件     %d" % (len(events) - order_ev))
    print("涉及订单数           %d" % len(orders))
    print("类别分布(按事件)     %s" % " ".join("%s=%d" % kv for kv in sorted(cat.items())))
    print("事件类型分布         %s" % " ".join("%s=%d" % kv for kv in sorted(typ.items())))
    for name, ok, detail in self_check(events, warnings):
        print("自检 %-24s %s  %s" % (name, "✅" if ok else "❌", detail))
    if warnings:
        print("⚠️  漏发告警（说明跳过了某天；必须从最早一天连续回放，脚本不补发）：")
        for w in warnings[:10]:
            if w["kind"] == "overdue":
                print("    order_id=%d  %s 应在 %s 发出，已逾期仍未发（那天没跑）"
                      % (w["order_id"], w["event_type"], w["due"]))
            else:
                print("    order_id=%d  %s 到期 %s，但前置 %s 还没发"
                      % (w["order_id"], w["event_type"], w["due"], "/".join(w["missing"])))
        if len(warnings) > 10:
            print("    …… 其余 %d 条省略" % (len(warnings) - 10))

def build_producer():
    """延迟导入：--dry-run 时根本不需要 kafka 库"""
    from kafka import KafkaProducer
    return KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    )

def main():
    ap = argparse.ArgumentParser(
        description="按天产生订单事件流，发到 Kafka %s" % TOPIC)
    ap.add_argument("--date", required=True, metavar="YYYY-MM-DD",
                    help="只发「到期日 == 这一天」的事件")
    ap.add_argument("--dry-run", action="store_true",
                    help="只打印，不连 Kafka、不发消息")
    args = ap.parse_args()

    try:
        day = datetime.strptime(args.date, FMT_DATE).date()
    except ValueError:
        print("❌ --date 必须是 YYYY-MM-DD，例如 2026-09-20")
        return 2

    existing = load_existing()
    events, warnings = generate(day, existing)

    print_table(events)
    print_summary(day, existing, events, warnings)

    if args.dry_run:
        print("\n--dry-run：未连接 Kafka，未发送任何消息")
        return 1 if warnings else 0

    if not events:
        print("\n没有到期事件，不发送")
        return 1 if warnings else 0

    producer = build_producer()
    for _t, _oid, _ev, payload, _c in events:
        producer.send(TOPIC, value=payload)
    producer.flush()
    producer.close()
    print("\n已发送 %d 条到 topic %s" % (len(events), TOPIC))
    return 1 if warnings else 0

if __name__ == "__main__":
    sys.exit(main())
