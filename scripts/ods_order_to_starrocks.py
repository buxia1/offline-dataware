# -*- coding: utf-8 -*-
"""从 Kafka 读 ods_order，解析 JSON，写入 StarRocks ods.ods_order"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_date
from pyspark.sql.types import (
    DecimalType, LongType, StringType, StructField, StructType,
)

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
print("=" * 60)
print(u"从 Kafka 读到的行数: %d" % rows.count())
rows.show(truncate=False)
print("=" * 60)

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
