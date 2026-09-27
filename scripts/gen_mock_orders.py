# -*- coding: utf-8 -*-
"""模拟订单数据生成器：生成带脏数据的订单，发送到 Kafka ods_order 主题

用法：
    python3 gen_mock_orders.py          # 默认 1000 条
    python3 gen_mock_orders.py 5000     # 生成 5000 条
"""
import json
import random
import sys
from datetime import datetime, timedelta

from kafka import KafkaProducer

# ============ 可调参数 ============
TOPIC = "ods_order"
KAFKA_SERVERS = "localhost:9092"

NULL_USER_RATE = 0.05    # 5% 的记录 user_id 为 None
NEG_AMOUNT_RATE = 0.03   # 3% 的记录金额为负数
DUP_ORDER_RATE = 0.02    # 2% 的记录复用已出现过的 order_id

STATUS_WEIGHTS = [("paid", 85), ("refund", 10), ("cancel", 5)]
# =================================


def build_price_table():
    """构造固定的商品单价表：商品 1~50 号，单价必须每次运行都一样"""
    table = {}
    for pid in range(1, 51):
        table[pid] = round(9.9 + (pid % 10) * 10, 2)
    return table


PRICE_TABLE = build_price_table()


def random_order_time():
    """最近 7 天内随机一个时刻"""
    seconds_back = random.randint(0, 7 * 24 * 3600)
    moment = datetime.now() - timedelta(seconds=seconds_back)
    return moment.strftime("%Y-%m-%d %H:%M:%S")


def random_status():
    """按权重随机一个订单状态"""
    names = [s for s, _ in STATUS_WEIGHTS]
    weights = [w for _, w in STATUS_WEIGHTS]
    return random.choices(names, weights=weights)[0]


def make_order(order_id, seen_ids):
    """生成一条订单记录"""
    user_id = random.randint(1, 200)
    product_id = random.randint(1, 50)
    quantity = random.randint(1, 5)
    amount = round(PRICE_TABLE[product_id] * quantity, 2)

    # 脏数据 1：user_id 置空
    if random.random() < NULL_USER_RATE:
        user_id = None

    # 脏数据 2：金额变负
    if random.random() < NEG_AMOUNT_RATE:
        amount = -amount

    # 脏数据 3：复用已经出现过的 order_id
    if seen_ids and random.random() < DUP_ORDER_RATE:
        order_id = random.choice(seen_ids)

    seen_ids.append(order_id)

    return {
        "order_id": order_id,
        "user_id": user_id,
        "product_id": product_id,
        "amount": amount,
        "order_time": random_order_time(),
        "status": random_status(),
    }


def build_producer():
    """创建 Kafka 生产者"""
    return KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    )


def main():
    total = int(sys.argv[1]) if len(sys.argv) > 1 else 1000

    producer = build_producer()
    seen_ids = []

    for i in range(total):
        order = make_order(1000 + i, seen_ids)
        producer.send(TOPIC, value=order)

    producer.flush()
    producer.close()
    print("发送完成，共 %d 条" % total)


if __name__ == "__main__":
    main()
