# -*- coding: utf-8 -*-
"""从 Kafka 读 ods_order_event，解析 JSON，写入 StarRocks ods.ods_order_event

和 scripts/ods_order_to_starrocks.py 是同一套写法，只改了 4 处：
    ① TOPIC    : ods_order                    → ods_order_event
    ② dbtable  : ods_order                    → ods_order_event
    ③ schema   : 加 event_type / event_time   （其余字段一致）
    ④ dt 口径  : to_date(order_time)          → to_date(event_time)
       ← 第 ④ 处容易被漏掉。新表是独立链路，dt 的语义就是「事件发生日」；
         写成 order_time 的话，整个订单的 pay/ship/finish 会被塞进下单日那一个 dt，
         装载按 dt 过滤时静默取错集合，且不报错。

老脚本（ods_order_to_starrocks.py）一行不动，继续跑订单快照。

运行方式（和旧脚本一样，在 spark 容器里）：
    docker exec spark /opt/spark/bin/spark-submit --master 'local[2]' \
      --conf spark.jars.ivy=/tmp/.ivy2 \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.mysql:mysql-connector-j:8.4.0 \
      /opt/offline-dw/scripts/ods_order_event_to_starrocks.py
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date
from pyspark.sql.types import (
    DecimalType, LongType, StringType, StructField, StructType,
)

KAFKA_BOOTSTRAP = "kafka:29092"
TOPIC = "ods_order_event"
SR_URL = ("jdbc:mysql://starrocks:9030/ods"
          "?useSSL=false&allowPublicKeyRetrieval=true"
          "&serverTimezone=Asia/Shanghai&rewriteBatchedStatements=true")

spark = (
    SparkSession.builder
    .appName("ods_order_event")
    .config("spark.sql.session.timeZone", "Asia/Shanghai")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# 只改了这里：多两个字段
schema = StructType([
    StructField("order_id", LongType()),
    StructField("user_id", LongType()),
    StructField("product_id", LongType()),
    StructField("amount", DecimalType(10, 2)),
    StructField("order_time", StringType()),
    StructField("event_type", StringType()),
    StructField("event_time", StringType()),
])

raw = (
    spark.read.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
)

rows = (
    raw.select(from_json(col("value").cast("string"), schema).alias("j"))
    .select("j.*")
    .withColumn("order_time", col("order_time").cast("timestamp"))
    .withColumn("event_time", col("event_time").cast("timestamp"))
    # 只改了这里：dt = 事件发生日（不是下单日）
    .withColumn("dt", to_date(col("event_time")))
)

rows.cache()
print("=" * 60)
print(u"从 Kafka 读到的行数: %d" % rows.count())
rows.show(truncate=False)
print("=" * 60)

(
    rows.write.format("jdbc")
    .option("url", SR_URL)
    .option("dbtable", "ods_order_event")
    .option("user", "root")
    .option("password", "")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .mode("append")
    .save()
)

print(u"写入 StarRocks 完成")
spark.stop()

