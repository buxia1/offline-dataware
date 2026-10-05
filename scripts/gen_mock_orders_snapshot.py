# -*- coding: utf-8 -*-
"""老链路（订单快照）生成器 —— 按天产生「那天下单」的订单宽消息

发给 topic ods_order。消息 schema 和 scripts/ods_order_to_starrocks.py 完全对齐：
    order_id, user_id, product_id, amount, order_time, status

用法：
    python3 scripts/gen_mock_orders_snapshot.py --date 2026-09-20 --dry-run
    python3 scripts/gen_mock_orders_snapshot.py --date 2026-09-20

【它和 scripts/gen_mock_orders.py 的区别（两个生成器，两条链路）】
    gen_mock_orders.py            事件流：一个订单多个事件 → 累积快照 dwd_order_lifecycle
    gen_mock_orders_snapshot.py   订单快照：一个订单一条   → 订单链路 dwd_order_detail
    两条链路互相独立，【号段也不重叠】：
        事件流      order_id = int(YYYYMMDD) * 1000 +   0 ~  99
        订单快照    order_id = int(YYYYMMDD) * 1000 + 500 ~ 599
    （错开号段是为了将来做"一致性维度共享"时一眼能分清来源）

【为什么改成按天产生（老版本的问题）】
    老版本用 datetime.now() 往前推 7 天：
        moment = datetime.now() - timedelta(seconds=random.randint(0, 7*24*3600))
    两个后果：
      ① 日期跟着"今天"漂 —— 今天跑和明天跑，同一批订单落到不同的 dt
      ② random 没有种子 —— 重跑得到完全不同的订单
    → 出事后【无法复现、无法重建】。这正是 2026-10-05 那次事故救不回来的根本原因
      （Kafka 消息过期 + 生成器不可重放 = 数据永久丢失）。

    改成 --date 之后：
      · order_id 由日期推导            → 同一天重跑，订单号完全一样
      · 属性由 random.Random(order_id) → 同一天重跑，逐字节相同
      · 于是整条链路可重放：删库后逐天重跑就能重建

【幂等靠四层保证（本次改造的核心）】
    ① 生成器       同一天重跑产出相同消息（可重放）
    ② ODS 表       ods.ods_order 已改 PRIMARY KEY(order_id) → INSERT 即 UPSERT
                   → 即使 Spark 用 earliest 全量重读、消息重复 N 遍，表里也只留一份
    ③ DWD          dwd_overwrite.sh 按天 INSERT OVERWRITE PARTITION → 当天整体替换
    ④ DWS / ADS    本来就是 PRIMARY KEY 模型 → INSERT 即 UPSERT

【为什么不再注入"复用已出现 order_id"这类脏数据】
    老版本有 DUP_ORDER_RATE = 2% 复用 order_id。主键模型下那会变成"覆盖"，
    语义怪异；而且"订单号唯一"是业务约束，不该造假。
    保留了另外两条脏数据：5% user_id 为空、3% 金额为负 —— 下游 dwd_overwrite.sh
    用 ABS(amount) 和 IS NOT NULL 过滤，正是拿它们练手的。
"""
import argparse
import json
import random
import sys
from collections import Counter
from datetime import datetime, timedelta

# ============ 可调参数 ============
TOPIC = "ods_order"
KAFKA_SERVERS = "localhost:9092"

NEW_ORDERS_PER_DAY = 100     # 每天新下的单
ID_OFFSET = 500              # 号段起点：和新链路事件流的 0~99 错开

NULL_USER_RATE = 0.05        # 5% 的记录 user_id 为 None（脏数据）
NEG_AMOUNT_RATE = 0.03       # 3% 的记录金额为负数（脏数据）

STATUS_WEIGHTS = [("paid", 85), ("refund", 10), ("cancel", 5)]

# 商品单价表：和事件流生成器、gen_mock_products.py 用同一个公式，保证金额口径一致
PRICE_TABLE = {pid: round(9.9 + (pid % 10) * 10, 2) for pid in range(1, 51)}

FMT = "%Y-%m-%d %H:%M:%S"
FMT_DATE = "%Y-%m-%d"
# =================================

def make_order(order_id, day):
    """同一个 order_id 永远得到同一条订单（这是"可重放"的全部秘密）"""
    # ---- 商品与金额：只由 order_id 决定 ----
    rnd_attr = random.Random("%d:attr" % order_id)
    product_id = rnd_attr.randint(1, 50)
    quantity = rnd_attr.randint(1, 5)
    amount = round(PRICE_TABLE[product_id] * quantity, 2)
    user_id = rnd_attr.randint(1, 200)

    if rnd_attr.random() < NULL_USER_RATE:            # 脏数据 1：user_id 置空
        user_id = None
    if rnd_attr.random() < NEG_AMOUNT_RATE:           # 脏数据 2：金额变负
        amount = -amount

    # ---- 下单时刻：固定落在 day 这一天之内 ----
    rnd_time = random.Random("%d:time" % order_id)
    order_time = (datetime(day.year, day.month, day.day)
                  + timedelta(seconds=rnd_time.randrange(24 * 60 * 60)))

    # ---- 状态：按权重抽，但结果只由 order_id 决定 ----
    rnd_status = random.Random("%d:status" % order_id)
    names = [s for s, _ in STATUS_WEIGHTS]
    weights = [w for _, w in STATUS_WEIGHTS]
    status = rnd_status.choices(names, weights=weights)[0]

    return {
        "order_id": order_id,
        "user_id": user_id,
        "product_id": product_id,
        "amount": amount,
        "order_time": order_time.strftime(FMT),
        "status": status,
    }

def generate(day):
    """纯函数：给定日期，返回这一天的订单列表（按 order_time 排序）

    不碰 Kafka、不碰库 → 可以脱库单测（见 t_check_snapshot.py）
    """
    base = int(day.strftime("%Y%m%d")) * 1000 + ID_OFFSET
    orders = [make_order(base + i, day) for i in range(NEW_ORDERS_PER_DAY)]
    orders.sort(key=lambda o: (o["order_time"], o["order_id"]))
    return orders

def print_table(orders, day):
    head = ("%-10s %-14s %-8s %-10s %-19s %-8s %10s"
            % ("date", "order_id", "user_id", "product_id", "order_time",
               "status", "amount"))
    print(head)
    print("-" * len(head))
    for o in orders:
        print("%-10s %-14d %-8s %-10d %-19s %-8s %10.2f"
              % (day.strftime(FMT_DATE), o["order_id"],
                 ("NULL" if o["user_id"] is None else o["user_id"]),
                 o["product_id"], o["order_time"], o["status"], o["amount"]))

def self_check(orders, day):
    """返回 [(检查项, 通过?, 明细)]，全是"一眼可判"的"""
    lines = []

    ids = [o["order_id"] for o in orders]
    base = int(day.strftime("%Y%m%d")) * 1000 + ID_OFFSET
    lo, hi = base, base + NEW_ORDERS_PER_DAY - 1

    dups = [k for k, v in Counter(ids).items() if v > 1]
    lines.append(("order_id 唯一", not dups, "重复 %d 组" % len(dups)))
    lines.append(("号段正确 %d~%d" % (lo, hi),
                  min(ids) >= lo and max(ids) <= hi,
                  "实际 %d~%d" % (min(ids), max(ids))))

    off_day = [o for o in orders
               if not o["order_time"].startswith(day.strftime(FMT_DATE))]
    lines.append(("order_time 都落在指定日期", not off_day,
                  "越界 %d 条" % len(off_day)))

    # 和新链路（事件流）的号段不能重叠：事件流是 base 的 0~99
    ev_lo, ev_hi = base - ID_OFFSET, base - ID_OFFSET + 99
    overlap = [i for i in ids if ev_lo <= i <= ev_hi]
    lines.append(("与新链路事件流号段不重叠", not overlap,
                  "重叠 %d 条（事件流占 %d~%d）" % (len(overlap), ev_lo, ev_hi)))

    lines.append(("订单数 = %d" % NEW_ORDERS_PER_DAY, len(orders) == NEW_ORDERS_PER_DAY,
                  "实际 %d" % len(orders)))
    return lines

def print_summary(orders, day):
    base = int(day.strftime("%Y%m%d")) * 1000 + ID_OFFSET
    status = Counter(o["status"] for o in orders)
    null_user = sum(1 for o in orders if o["user_id"] is None)
    neg_amount = sum(1 for o in orders if o["amount"] < 0)
    times = sorted(o["order_time"] for o in orders)

    print("\n---------------- 汇总 ----------------")
    print("主题                %s" % TOPIC)
    print("日期                %s" % day.strftime(FMT_DATE))
    print("订单数              %d" % len(orders))
    print("号段                %d ~ %d" % (base, base + NEW_ORDERS_PER_DAY - 1))
    print("下单单时刻范围      %s ~ %s" % (times[0], times[-1]))
    print("status 分布         %s" % " ".join("%s=%d" % kv for kv in sorted(status.items())))
    print("user_id 为空        %d  (%.1f%%)" % (null_user, 100.0 * null_user / len(orders)))
    print("金额为负            %d  (%.1f%%)" % (neg_amount, 100.0 * neg_amount / len(orders)))
    for name, ok, detail in self_check(orders, day):
        print("自检 %-26s %s  %s" % (name, "✅" if ok else "❌", detail))

def build_producer():
    """延迟导入：--dry-run 时根本不需要 kafka 库"""
    from kafka import KafkaProducer
    return KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    )

def main():
    ap = argparse.ArgumentParser(
        description="按天产生订单快照，发到 Kafka %s" % TOPIC)
    ap.add_argument("--date", required=True, metavar="YYYY-MM-DD",
                    help="只发「这一天新下单」的订单")
    ap.add_argument("--dry-run", action="store_true",
                    help="只打印，不连 Kafka、不发消息")
    args = ap.parse_args()

    try:
        day = datetime.strptime(args.date, FMT_DATE).date()
    except ValueError:
        print("❌ --date 必须是 YYYY-MM-DD，例如 2026-09-20")
        return 2

    orders = generate(day)
    print_table(orders, day)
    print_summary(orders, day)

    if args.dry_run:
        print("\n--dry-run：未连接 Kafka，未发送任何消息")
        return 0

    producer = build_producer()
    for order in orders:
        producer.send(TOPIC, value=order)
    producer.flush()
    producer.close()
    print("\n已发送 %d 条到 topic %s" % (len(orders), TOPIC))
    return 0

if __name__ == "__main__":
    sys.exit(main())
