# -*- coding: utf-8 -*-
"""商品维度快照生成器：产出某一天的 50 个商品全量快照 CSV

用法：
    python3 gen_mock_products.py                    # 生成"今天"的快照
    python3 gen_mock_products.py --date 2026-09-20  # 生成指定日期的快照（造历史数据用）

产物：data/dim/product_snapshot_YYYYMMDD.csv

【为什么商品走文件、订单走 Kafka】
    订单是"事件流"，一条一条持续产生 → Kafka 合适
    商品是"实体状态"，每天一份全量 → 文件同步合适
    真实业务里商品也是整表导出走 DataX，不走消息队列
"""
import argparse
import csv
import os
from datetime import date, datetime

# ============ 可调参数 ============
PRODUCT_COUNT = 50
OUT_DIR = "data/dim"
USE_SAMPLE_TIME = "2026-09-01"   # 不上传真实数据时，用这个当"上传时间"，保证可复现

CATEGORIES = ["家电", "图书", "服饰", "食品", "数码"]
BRANDS = ["华为", "小米", "苹果"]
STATUSES = ["on_sale", "off_shelf"]
# =================================


def base_price(pid):
    """基准价：沿用 gen_mock_orders.py 里的老公式，保证金额口径能对上"""
    return round(9.9 + (pid % 10) * 10, 2)


def build_product(pid, today):
    """构造一个商品的快照行，返回 dict"""
    # ---- 品类：pid % 5 当索引，从 5 个品类里取 ----
    # 再叠加"时间偏移"，让品类会随日期变化（SCD2 要用）
    category = CATEGORIES[(pid + today.toordinal()) % len(CATEGORIES)]

    # ---- 品牌：同套路，3 个品牌 ----
    brand = BRANDS[pid % len(BRANDS)]

    # ---- 价格：基准价，加上"涨价模拟" ----
    price = base_price(pid)
    if pid % 7 == 0:                      # 商品 7,14,21,...,49 涨价
        price = round(price + 10, 2)

    # ---- 状态：二选一，用 if / else ----
    if pid % 11 == 0:                     # 商品 11,22,33,44 下架
        status = STATUSES[1]
    else:
        status = STATUSES[0]

    return {
        "product_id": pid,                                   # 直接就是 pid
        "product_name": f"商品-{pid:02d}",                    # :02d = 补零到 2 位
        "category": category,
        "brand": brand,
        "price": price,
        "status": status,
        "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=None, help="快照日期，格式 YYYY-MM-DD，默认今天")
    args = parser.parse_args()

    if args.date:
        today = date.fromisoformat(args.date)
    else:
        today = date.today()

    # 确保目录存在
    os.makedirs(OUT_DIR, exist_ok=True)
    out_path = os.path.join(OUT_DIR, "product_snapshot_%s.csv" % today.strftime("%Y%m%d"))

    # 7 个字段的顺序，必须和表头完全一致
    fieldnames = ["product_id", "product_name", "category",
                  "brand", "price", "status", "update_time"]

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()                       # 自动写逗号分隔的表头
        for pid in range(1, PRODUCT_COUNT + 1):
            writer.writerow(build_product(pid, today))

    print("写入完成: %s （%d 行）" % (out_path, PRODUCT_COUNT))


if __name__ == "__main__":
    main()
