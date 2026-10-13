#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
湖层 ODS 灌数：Kafka → Iceberg（Spark）

用法（由 lake_ods_replay.sh 调用，一般不直接跑）：
    spark-submit lake_ods_replay.py --date 2026-09-20 [--dry-run]

与仓侧 scripts/ods_order_event_to_starrocks.py 的关系：
    · 读 Kafka 的方式【一样】（显式 schema、earliest）
    · 时间字段【必须】cast 成 timestamp_ntz（仓侧是 timestamp）
    · 写入方式【不同】：Iceberg 用 INSERT OVERWRITE + dynamic，不用位点表
"""
import argparse
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, LongType, DecimalType, StringType,
)

TOPIC_ORDER = "ods_order"
TOPIC_EVENT = "ods_order_event"
KAFKA_BOOTSTRAP = "kafka:29092"

LAKE_WH = "file:///opt/offline-dw/lake/warehouse"

# ⚠️ 时间字段声明为 StringType（不是 TimestampType/TimestampNTZType）：
#    from_json 的 schema DSL 不认 "TimestampNTZ"；写 "Timestamp" 则是 LTZ，会被
#    会话时区来回换算导致落库偏 8 小时。保持 StringType + 下游显式 cast("timestamp_ntz")
SCHEMA_ORDER = StructType([
    StructField("order_id", LongType()),
    StructField("user_id", LongType()),
    StructField("product_id", LongType()),
    StructField("amount", DecimalType(10, 2)),
    StructField("order_time", StringType()),
    StructField("status", StringType()),
])

SCHEMA_EVENT = StructType([
    StructField("order_id", LongType()),
    StructField("user_id", LongType()),
    StructField("product_id", LongType()),
    StructField("amount", DecimalType(10, 2)),
    StructField("order_time", StringType()),
    StructField("event_type", StringType()),
    StructField("event_time", StringType()),
])


def build_spark():
    return (SparkSession.builder
            .appName("lake_ods_replay")
            .config("spark.sql.session.timeZone", "Asia/Shanghai")
            # ★ 头号开关：没有它 INSERT OVERWRITE 会清空整表
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.lake.type", "hadoop")
            .config("spark.sql.catalog.lake.warehouse", LAKE_WH)
            .config("spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .getOrCreate())


def read_topic(spark, topic, schema):
    """读 Kafka 全量（earliest）。消息体的 value 是 JSON 字符串。"""
    raw = (spark.read.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
           .option("subscribe", topic)
           .option("startingOffsets", "earliest")
           # 若 Kafka 已按 retention 删掉旧段，别让作业直接挂
           .option("failOnDataLoss", "false")
           .load())
    return (raw.select(F.from_json(F.col("value").cast("string"), schema).alias("j"))
            .select("j.*"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="目标日 YYYY-MM-DD")
    ap.add_argument("--dry-run", action="store_true", help="只报数，不写入")
    args = ap.parse_args()

    target = args.date
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # ---------- 订单 ----------
    order = read_topic(spark, TOPIC_ORDER, SCHEMA_ORDER)
    order = (order
             .withColumn("order_time", F.col("order_time").cast("timestamp_ntz"))
             .withColumn("dt", F.to_date("order_time"))
             .filter(F.col("dt") == F.lit(target).cast("date")))

    # ---------- 事件 ----------
    ev = read_topic(spark, TOPIC_EVENT, SCHEMA_EVENT)
    ev = (ev
          .withColumn("order_time", F.col("order_time").cast("timestamp_ntz"))
          .withColumn("event_time", F.col("event_time").cast("timestamp_ntz"))
          .withColumn("dt", F.to_date("event_time"))
          .filter(F.col("dt") == F.lit(target).cast("date")))

    # 去重（与仓侧 order_event_chain 的口径一致）
    ev_dedup = ev.dropDuplicates(["order_id", "event_type"])

    n_ord = order.count()
    n_ev_raw = ev.count()
    n_ev = ev_dedup.count()
    n_null = ev_dedup.filter(F.col("user_id").isNull()).count()
    n_neg = ev_dedup.filter(F.col("amount") < F.lit(0)).count()

    print("=" * 60)
    print(f"LAKE_DATE       = {target}")
    print(f"LAKE_ORDER_N    = {n_ord}")
    print(f"LAKE_EVENT_RAW  = {n_ev_raw}")
    print(f"LAKE_EVENT_N    = {n_ev}")
    print(f"LAKE_EVENT_NULL = {n_null}")
    print(f"LAKE_EVENT_NEG  = {n_neg}")
    print(f"DUP_DROPPED     = {n_ev_raw - n_ev}")
    print("=" * 60)

    if args.dry_run:
        print("DRY_RUN_ONLY  -> 未写入任何数据")
        spark.stop()
        return 0

    order.createOrReplaceTempView("_tmp_order")
    ev_dedup.createOrReplaceTempView("_tmp_event")

    cols_ord = ("order_id, user_id, product_id, amount, order_time, status, dt")
    cols_ev = ("order_id, user_id, product_id, amount, order_time, "
               "event_type, event_time, dt")

    # ★ 不带 PARTITION 子句；靠 dynamic 只替换 dt == 目标日 的那一片
    spark.sql(f"INSERT OVERWRITE lake.ods.ods_order "
              f"SELECT {cols_ord} FROM _tmp_order")
    spark.sql(f"INSERT OVERWRITE lake.ods.ods_order_event "
              f"SELECT {cols_ev} FROM _tmp_event")
    print("WRITE_DONE")

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
