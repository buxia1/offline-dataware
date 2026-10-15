#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""二分定位 dwd_order_lifecycle Stream Load 被过滤的行。

用与 dwd_order_lifecycle_streamload.py 【完全相同的】拼行逻辑写出一个 part 文件，
然后按行数二分推给 BE，直到定位到最小失败集合。
"""
import base64
import json
import os
import shutil
import sys
import tempfile
import urllib.error
import urllib.request

from pyspark.sql import SparkSession, functions as F

EPOCH = "1970-01-01 00:00:00"
TAB = "\t"
NUL = "\\N"
CSV_COLS = ["order_id", "user_id", "product_id", "amount",
            "order_time", "pay_time", "ship_time", "finish_time", "cancel_time",
            "current_stage", "pay_lag_hours", "ship_lag_days", "finish_lag_days",
            "last_event_time", "update_time"]
COLS_HEADER = ",".join(CSV_COLS)
AUTH = {"Authorization": "Basic " + base64.b64encode(b"root:").decode()}


def push_lines(lines, tag):
    txt = "\n".join(lines) + "\n"
    headers = dict(AUTH)
    headers.update({"Expect": "100-continue", "format": "csv", "column_separator": TAB,
                    "skip_header": "0", "strict_mode": "true", "max_filter_ratio": "0",
                    "columns": COLS_HEADER, "label": "lcbisect_%s" % tag})
    req = urllib.request.Request("http://starrocks:8040/api/dwd/dwd_order_lifecycle/_stream_load",
                                 data=txt.encode(), method="PUT", headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=600) as resp:
            body = resp.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "replace")
    j = json.loads(body)
    ok = j.get("Status") == "Success"
    print("  [%s] n=%d Status=%s Loaded=%s Filtered=%s" % (
        tag, len(lines), j.get("Status"), j.get("NumberLoadedRows"), j.get("NumberFilteredRows")))
    return ok


def main():
    spark = (SparkSession.builder.appName("lc_bisect")
             .config("spark.sql.session.timeZone", "Asia/Shanghai")
             .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
             .config("spark.sql.catalog.lake.type", "hadoop")
             .config("spark.sql.catalog.lake.warehouse", "file:///opt/offline-dw/lake/warehouse")
             .config("spark.sql.extensions",
                     "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

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
    ))
    d = (agg
         .withColumn("last_event_time", F.greatest(*[
             F.coalesce(F.col(c), F.lit(EPOCH).cast("timestamp"))
             for c in ("order_time", "pay_time", "ship_time", "finish_time", "cancel_time")]))
         .withColumn("current_stage",
                     F.when(F.col("cancel_time") == F.col("last_event_time"), F.lit("cancel"))
                      .when(F.col("finish_time") == F.col("last_event_time"), F.lit("finish"))
                      .when(F.col("ship_time") == F.col("last_event_time"), F.lit("ship"))
                      .when(F.col("pay_time") == F.col("last_event_time"), F.lit("pay"))
                      .otherwise(F.lit("order")))
         .withColumn("pay_lag_hours",
                     F.when(F.col("pay_time").isNull() | F.col("order_time").isNull(), F.lit(None))
                      .otherwise(F.floor((F.unix_timestamp("pay_time")
                                          - F.unix_timestamp("order_time")) / 3600)))
         .withColumn("ship_lag_days", F.datediff(F.col("ship_time"), F.col("order_time")))
         .withColumn("finish_lag_days", F.datediff(F.col("finish_time"), F.col("order_time")))
         .withColumn("update_time", F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss")))

    def S(col, fmt=None):
        c = F.col(col)
        c = F.date_format(c, fmt) if fmt else c.cast("string")
        return F.coalesce(c, F.lit(NUL))

    line = F.concat_ws(TAB,
                       S("order_id"), S("user_id"), S("product_id"),
                       F.coalesce(F.format_number("amount", 2), F.lit(NUL)),
                       S("order_time", "yyyy-MM-dd HH:mm:ss"),
                       S("pay_time", "yyyy-MM-dd HH:mm:ss"),
                       S("ship_time", "yyyy-MM-dd HH:mm:ss"),
                       S("finish_time", "yyyy-MM-dd HH:mm:ss"),
                       S("cancel_time", "yyyy-MM-dd HH:mm:ss"),
                       S("current_stage"), S("pay_lag_hours"), S("ship_lag_days"),
                       S("finish_lag_days"),
                       S("last_event_time", "yyyy-MM-dd HH:mm:ss"),
                       S("update_time"))

    tmp = tempfile.mkdtemp(prefix="lcb_")
    out_dir = os.path.join(tmp, "out")
    (d.select(line.alias("value")).repartition(4)
     .write.mode("overwrite").option("compression", "none").text(out_dir))
    parts = sorted(os.path.join(out_dir, f) for f in os.listdir(out_dir) if f.startswith("part-"))
    print("PARTS =", [os.path.basename(p) for p in parts])

    lines = []
    for p in parts:
        with open(p, "r", encoding="utf-8") as fh:
            lines.extend([ln.rstrip("\n") for ln in fh if ln.strip() != ""])
    print("TOTAL_LINES =", len(lines))

    print("--- 小样验证（前 200 行）---")
    if not push_lines(lines[:200], "head200"):
        print("  ！！前 200 行就失败 —— 少量样本能过说明问题与数据内容相关")
        # 逐行找
        bad = None
        for i, ln in enumerate(lines[:200]):
            if not push_lines([ln], "one%d" % i):
                bad = i
                print("  >>> 第一处失败行 index=%d" % i)
                print("  >>> 内容: %r" % ln)
                print("  >>> 字段数: %d" % (ln.count(TAB) + 1))
                break
        if bad is None:
            print("  （单行都过 —— 失败是批量触发的，需按分片逼近）")
        shutil.rmtree(tmp, ignore_errors=True)
        spark.stop()
        return

    print("--- 二分需要行数（找失败分片）---")
    lo, hi = 200, len(lines)
    while lo < hi:
        mid = (lo + hi) // 2
        if push_lines(lines[:mid], "bis%d" % mid):
            lo = mid + 1
        else:
            hi = mid
    print(">>> 最小失败行数 = %d（第 %d 行是触发者）" % (lo, lo))
    idx = lo - 1
    print(">>> 触发行 index=%d 字段数=%d" % (idx, lines[idx].count(TAB) + 1))
    print(">>> 内容: %r" % lines[idx])
    print(">>> 上下文:")
    for k in range(max(0, idx - 2), min(len(lines), idx + 2)):
        print("   %6d 字段数=%2d %r" % (k, lines[k].count(TAB) + 1, lines[k][:160]))

    shutil.rmtree(tmp, ignore_errors=True)
    spark.stop()


if __name__ == "__main__":
    main()
