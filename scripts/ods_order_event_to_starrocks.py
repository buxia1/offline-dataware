# -*- coding: utf-8 -*-
"""从 Kafka 增量读 ods_order_event，解析 JSON，写入 StarRocks ods.ods_order_event

【2026-10-07 改成增量】
    原来：startingOffsets=earliest 每次全量重读 + append
          → 表里已有数据时，每跑一次就把全部历史重写一遍（不幂等）
    现在：位点表 ods.ods_kafka_offset 记住"读到哪了"，每次只读新消息
          → Kafka 的 retention 不再能伤害我们（永远不重读历史）

【位点表为空 = 从头读】
    存的是 next_offset（已消费 + 1），所以空表天然等价于 earliest，
    不需要额外的"有没有记录"判断。回补 = TRUNCATE 位点表 + TRUNCATE 事件表。

【⚠️ 关键设计：写库的 DataFrame 和算位点的 DataFrame 必须分开】
    StarRocks + Spark JDBC 写入【不允许 DataFrame 里有目标表没有的列】
    （实测：8 列与表一致 → 成功；7 列（子集）→ 成功；8 列 + k_part → 失败
      `AnalysisException: Column k_part not found in schema`）
    所以 Kafka 的 partition/offset 元数据列**不能和业务列混在一张 DataFrame 里**：
      · biz     —— 只有 8 个业务列，用于写 ODS
      · offsets —— 只有 partition/offset，用于算新位点（不写 ODS）

【为什么 batch 内还要按 (order_id,event_type) 去重】
    零成本的保险：万一位点表被回退，它能挡住重复写入。

运行方式（和旧脚本一样，在 spark 容器里）：
    docker exec spark /opt/spark/bin/spark-submit --master 'local[2]' \
      --conf spark.jars.ivy=/tmp/.ivy2 \
      --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,com.mysql:mysql-connector-j:8.4.0 \
      /opt/offline-dw/scripts/ods_order_event_to_starrocks.py
"""
import json
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, row_number, to_date
from pyspark.sql.types import (
    DecimalType, IntegerType, LongType, StringType, StructField, StructType,
    TimestampType,
)
from pyspark.sql.window import Window

KAFKA_BOOTSTRAP = "kafka:29092"
TOPIC = "ods_order_event"
SR_URL = ("jdbc:mysql://starrocks:9030/ods"
          "?useSSL=false&allowPublicKeyRetrieval=true"
          "&serverTimezone=Asia/Shanghai&rewriteBatchedStatements=true")
OFFSET_TABLE = "ods_kafka_offset"
EVENT_TABLE = "ods_order_event"

_BASE_OPTS = dict(url=SR_URL, user="root", password="",
                  driver="com.mysql.cj.jdbc.Driver")

spark = (
    SparkSession.builder
    .appName("ods_order_event")
    .config("spark.sql.session.timeZone", "Asia/Shanghai")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# 表里的 8 个业务列（写库的 DataFrame 只能有这些）
BIZ_COLS = ["order_id", "user_id", "product_id", "amount",
            "order_time", "event_type", "event_time", "dt"]
# 其中 dt 是【派生列】（= to_date(event_time)），不在 Kafka 的 JSON 里
JSON_COLS = ["order_id", "user_id", "product_id", "amount",
             "order_time", "event_type", "event_time"]

schema = StructType([
    StructField("order_id", LongType()),
    StructField("user_id", LongType()),
    StructField("product_id", LongType()),
    StructField("amount", DecimalType(10, 2)),
    StructField("order_time", StringType()),
    StructField("event_type", StringType()),
    StructField("event_time", StringType()),
])

# =============================================================================
# ① 读位点表 → 拼 startingOffsets
#    ⚠️ JSON 必须带 topic 外层：{"ods_order_event": {"0": 197, "1": 187}}
#       传 {"0": 197} 会报 IllegalArgumentException
# =============================================================================
off_rows = (spark.read.format("jdbc").option("dbtable", OFFSET_TABLE)
            .options(**_BASE_OPTS).load().collect())

offsets = {int(r["partition_id"]): int(r["next_offset"]) for r in off_rows}
if offsets:
    starting = json.dumps({TOPIC: {str(p): o for p, o in offsets.items()}})
else:
    starting = "earliest"          # 位点表为空 = 从头读
print(u"位点表起始位置: %s" % starting)

# =============================================================================
# ② 只读新消息
# =============================================================================
raw = (spark.read.format("kafka")
       .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
       .option("subscribe", TOPIC)
       .option("startingOffsets", starting)
       .option("endingOffsets", "latest")
       .load())

# ---- 业务 DataFrame：只保留 8 个业务列（用于写 ODS）----
# 注意：dt 不在 JSON 里，必须由 event_time 派生，所以放在 select 之后
biz = (raw.select(from_json(col("value").cast("string"), schema).alias("j"))
       .select([col("j." + c) for c in JSON_COLS])
       .withColumn("order_time", col("order_time").cast("timestamp"))
       .withColumn("event_time", col("event_time").cast("timestamp"))
       .withColumn("dt", to_date(col("event_time")))
       .select(BIZ_COLS))

# ---- 位点 DataFrame：只保留 partition/offset（用于算新位点，绝不写 ODS）----
offsets_df = (raw.select(col("partition").alias("k_part"),
                         col("offset").alias("k_off"))
              .withColumn("k_part", col("k_part").cast("int"))
              .withColumn("k_off", col("k_off").cast("long")))

# 批内去重（保险，见文件头说明）
before = biz.count()
w = Window.partitionBy("order_id", "event_type").orderBy(
    col("event_time").asc(), col("order_time").asc())
biz = (biz.withColumn("_rn", row_number().over(w))
       .filter(col("_rn") == 1).drop("_rn"))
after = biz.count()

print("=" * 60)
print(u"本次从 Kafka 读到 %d 行，按 (order_id,event_type) 去重后 %d 行" % (before, after))
if before != after:
    print(u"⚠️  读到 %d 行重复消息 —— 已折叠（位点表被回退过？）" % (before - after))
print(u"写库 DataFrame 列: %s" % [f.name for f in biz.schema.fields])
print("=" * 60)

# ---- 防线：读到 0 行 → 什么都不做，明确退出（别把"没新消息"和"脚本坏了"混为一谈）----
if after == 0:
    print(u"没有新消息，退出（未写入、未改位点）")
    spark.stop()
    raise SystemExit(0)

# =============================================================================
# ③ 写 ODS
#    append —— StarRocks + Spark JDBC 只有 append 可用（overwrite 会 DROP 表，
#    且生成的 MySQL 方言 DDL 在 StarRocks 上语法错误，见 PITFALLS §4.3）
#    ⚠️ 这里传进去的必须是 biz（只有 8 个业务列）
# =============================================================================
biz.cache()
biz.show(truncate=False)
(biz.write.format("jdbc").option("dbtable", EVENT_TABLE)
 .option("columns", ",".join(BIZ_COLS))
 .options(**_BASE_OPTS).mode("append").save())
print(u"写入 StarRocks 完成")

# =============================================================================
# ④ 算本次各分区的新位点（max(offset) + 1，+1 不能漏）
# =============================================================================
offsets_df.cache()
new_offsets = {}
for m in offsets_df.groupBy("k_part").max("k_off").collect():
    new_offsets[int(m["k_part"])] = int(m["max(k_off)"]) + 1
print(u"本次各分区新位点: %s" % sorted(new_offsets.items()))

# ---- retention 防线（尽力而为）----
# Spark 容器里没有 kafka-python，这里会走 ImportError 分支跳过；
# 真正的防线在 shell 外壳 scripts/ods_order_event_ingest.sh（用 kafka-get-offsets.sh）
try:
    from kafka import KafkaConsumer
    from kafka.structs import TopicPartition
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP, enable_auto_commit=False)
    parts = [TopicPartition(TOPIC, p) for p in sorted(new_offsets)]
    for p, begin in consumer.beginning_offsets(parts).items():
        nxt = new_offsets.get(p.partition)
        if nxt is not None and nxt < begin:
            print(u"❌ 分区 %d 的位点 %d < Kafka 现存最早 %d —— 消息已被 retention 删除"
                  % (p.partition, nxt, begin))
            consumer.close()
            spark.stop()
            raise SystemExit(1)
    consumer.close()
    print(u"✅ retention 防线通过")
except ImportError:
    print(u"提示：spark 容器无 kafka-python，retention 防线由外壳脚本负责")

# =============================================================================
# ⑤ 回写位点表（PK 表 → append 即 UPSERT，天然幂等）
#    ⚠️ 必须在 ODS 写成功【之后】再更新，顺序反了会丢消息
# =============================================================================
off_schema = StructType([
    StructField("topic", StringType()),
    StructField("partition_id", IntegerType()),
    StructField("next_offset", LongType()),
    StructField("update_time", TimestampType()),
])
now = datetime.now()
data = [(TOPIC, p, o, now) for p, o in sorted(new_offsets.items())]
(spark.createDataFrame(data, off_schema)
 .write.format("jdbc").option("dbtable", OFFSET_TABLE)
 .options(**_BASE_OPTS).mode("append").save())
print(u"位点表已更新: %s" % sorted(new_offsets.items()))

spark.stop()
