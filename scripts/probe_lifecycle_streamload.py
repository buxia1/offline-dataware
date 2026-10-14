import base64, json, urllib.request, urllib.error
from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder.appName("dbg")
         .config("spark.sql.session.timeZone", "Asia/Shanghai")
         .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
         .config("spark.sql.catalog.lake.type", "hadoop")
         .config("spark.sql.catalog.lake.warehouse", "file:///opt/offline-dw/lake/warehouse")
         .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

EPOCH = "1970-01-01 00:00:00"
ev = spark.read.format("iceberg").load("lake.ods.ods_order_event")
agg = (ev.groupBy("order_id").agg(
    F.max("user_id").alias("user_id"),
    F.max("product_id").alias("product_id"),
    F.max("amount").alias("amount"),
    F.max(F.when(F.col("event_type") == "order", F.col("event_time"))).alias("order_time"),
    F.max(F.when(F.col("event_type") == "pay", F.col("event_time"))).alias("pay_time"),
    F.max(F.when(F.col("event_type") == "ship", F.col("event_time"))).alias("ship_time"),
    F.max(F.when(F.col("event_type") == "finish", F.col("event_time"))).alias("finish_time"),
    F.max(F.when(F.col("event_type") == "cancel", F.col("event_time"))).alias("cancel_time"),
)).limit(200)

d = (agg
     .withColumn("last_event_time", F.greatest(*[F.coalesce(F.col(c), F.lit(EPOCH).cast("timestamp"))
                  for c in ("order_time", "pay_time", "ship_time", "finish_time", "cancel_time")]))
     .withColumn("current_stage",
                 F.when(F.col("cancel_time") == F.col("last_event_time"), F.lit("cancel"))
                  .when(F.col("finish_time") == F.col("last_event_time"), F.lit("finish"))
                  .when(F.col("ship_time") == F.col("last_event_time"), F.lit("ship"))
                  .when(F.col("pay_time") == F.col("last_event_time"), F.lit("pay"))
                  .otherwise(F.lit("order")))
     .withColumn("pay_lag_hours",
                 F.when(F.col("pay_time").isNull() | F.col("order_time").isNull(), F.lit(None))
                  .otherwise(F.floor((F.unix_timestamp("pay_time") - F.unix_timestamp("order_time")) / 3600)))
     .withColumn("ship_lag_days", F.datediff(F.col("ship_time"), F.col("order_time")))
     .withColumn("finish_lag_days", F.datediff(F.col("finish_time"), F.col("order_time")))
     .withColumn("update_time", F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss")))

# 故意取【含 NULL 里程碑】的行 + 普通行
sel = d.select("order_id", "user_id", "product_id", "amount",
               "order_time", "pay_time", "ship_time", "finish_time", "cancel_time",
               "current_stage", "pay_lag_hours", "ship_lag_days", "finish_lag_days",
               "last_event_time", "update_time")
rows = sel.collect()
print("SAMPLE_ROWS =", len(rows))
n_null_order = sum(1 for r in rows if r["order_time"] is None)
print("rows with order_time IS NULL =", n_null_order)

for r in rows[:3]:
    print("RAW:", {k: r[k] for k in r.asDict()})

COLS = ",".join(sel.columns)


def render(r, null_token):
    f = lambda v: null_token if v is None else str(v)
    return "\t".join(f(r[c]) for c in sel.columns)


def push(rows, null_token, tag):
    txt = "\n".join(render(r, null_token) for r in rows) + "\n"
    headers = {"Authorization": "Basic " + base64.b64encode(b"root:").decode(),
               "Expect": "100-continue", "format": "csv", "column_separator": "\t",
               "skip_header": "0", "strict_mode": "true", "max_filter_ratio": "0",
               "columns": COLS, "label": "lcdbg_%s" % tag}
    print("--- 变体 %s: null_token=%r 字节=%d ---" % (tag, null_token, len(txt.encode())))
    req = urllib.request.Request("http://starrocks:8040/api/dwd/dwd_order_lifecycle/_stream_load",
                                 data=txt.encode(), method="PUT", headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=300) as resp:
            body = resp.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "replace")
    j = json.loads(body)
    print("  Status=%s Loaded=%s Filtered=%s Msg=%s" % (
        j.get("Status"), j.get("NumberLoadedRows"), j.get("NumberFilteredRows"), j.get("Message")))
    if j.get("ErrorURL"):
        print("  ErrorURL=%s" % j["ErrorURL"])
        try:
            with urllib.request.urlopen(urllib.request.Request(
                    j["ErrorURL"], headers={"Authorization": headers["Authorization"]})) as r2:
                print("  --- 错误样本 ---")
                print(r2.read().decode("utf-8", "replace")[:1500])
        except Exception as ex:
            print("  (取错误样本失败: %s)" % ex)


push(rows, "\\N", "backslash_N")
push(rows, "", "empty")
spark.stop()
