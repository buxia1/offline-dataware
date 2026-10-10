from pyspark.sql import SparkSession

s = (SparkSession.builder.appName("kafka-conn-probe")
     .config("spark.sql.shuffle.partitions", "3")
     .getOrCreate())
s.sparkContext.setLogLevel("ERROR")

J = "/opt/offline-dw/jars"
df = (s.read.format("kafka")
      .option("kafka.bootstrap.servers", "kafka:29092")   # ← 必须 kafka:29092
      .option("subscribe", "ods_order_event")
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .load())
n = df.count()
print("KAFKA_ROWS=%d" % n)
df.selectExpr("CAST(value AS STRING) AS v").show(3, truncate=120)
s.stop()
