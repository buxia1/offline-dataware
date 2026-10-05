# -*- coding: utf-8 -*-
"""从 Kafka 读 ods_order，解析 JSON，写入 StarRocks ods.ods_order"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date
from pyspark.sql.types import (
    DecimalType, LongType, StringType, StructField, StructType,
)
import sys

KAFKA_BOOTSTRAP = "kafka:29092"
TOPIC = "ods_order"
SR_URL = ("jdbc:mysql://starrocks:9030/ods"
          "?useSSL=false&allowPublicKeyRetrieval=true"
          "&serverTimezone=Asia/Shanghai&rewriteBatchedStatements=true")

spark = (
    SparkSession.builder
    .appName("ods_order")
    .config("spark.sql.session.timeZone", "Asia/Shanghai")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("order_id", LongType()),
    StructField("user_id", LongType()),
    StructField("product_id", LongType()),
    StructField("amount", DecimalType(10, 2)),
    StructField("order_time", StringType()),
    StructField("status", StringType()),
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
    .withColumn("dt", to_date(col("order_time")))
)

rows.cache()
n = rows.count()
print("=" * 60)
print(u"从 Kafka 读到的行数: %d" % n)
rows.show(truncate=False)
print("=" * 60)

# ★ 新增：读到 0 行必须失败退出
#   上游节点是 truncate_ods（先清表再灌）→ 读到 0 行 = 表被清空却报成功
#   = 静默清库。必须让它变红。
if n == 0:
    print(u"❌ 从 Kafka 读到 0 行 —— 拒绝以成功收场")
    print(u"   排查：1) topic retention 是否已过期  2) topic 是否为空")
    spark.stop()
    sys.exit(1)

(
    rows.write.format("jdbc")
    .option("url", SR_URL)
    .option("dbtable", "ods_order")
    .option("user", "root")
    .option("password", "")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .mode("append")
    .save()
)

print(u"写入 StarRocks 完成")
spark.stop()
