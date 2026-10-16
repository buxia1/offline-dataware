#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
累积快照事实表 dwd.dwd_order_lifecycle 的 Spark 版（湖层事件流 → 仓侧）

【为什么从湖层读事件表】
  老作业 scripts/legacy_warehouse/dwd_order_lifecycle_load.sh 是「StarRocks 内部读 ods.ods_order_event
  → 视图 dwd.v_order_lifecycle_expected 聚合 → INSERT 回表」。
  仓侧事件表 3,120,421 行里有 150,842 行是同日完全重复（生成器 DUP_EVENT_RATE 注入），
  而湖侧是按 (dt, order_id, event_type) 去重后的 2,969,579 行。两者【聚合结果等价】：
  Milestone 取的是 MAX(event_time)，同一事件类型的重复副本不会改变 MAX。
  所以下游口径一致，但湖侧扫描量更小、且没有重复放大。

【口径逐字照搬 sql/dwd/dwd_order_lifecycle.sql 的视图 dwd.v_order_lifecycle_expected】
    agg = GROUP BY order_id:
        user_id      = MAX(user_id)        （注意：MAX 忽略 NULL）
        product_id   = MAX(product_id)
        amount       = MAX(amount)
        order_time   = MAX(CASE event_type='order'  THEN event_time END)
        pay_time     = MAX(CASE event_type='pay'    ...)
        ship_time    = MAX(CASE event_type='ship'   ...)
        finish_time  = MAX(CASE event_type='finish' ...)
        cancel_time  = MAX(CASE event_type='cancel' ...)
    last_event_time = GREATEST(COALESCE(五个里程碑, '1970-01-01 00:00:00'))
    current_stage   = cancel > finish > ship > pay > order（同时刻并列取终态）
    pay_lag_hours   = TIMESTAMPDIFF(HOUR, order_time, pay_time)
    ship_lag_days   = DATEDIFF(ship_time,   order_time)
    finish_lag_days = DATEDIFF(finish_time, order_time)

  ★ GREATEST 必须用 COALESCE 兜底：StarRocks 的 GREATEST(NULL, x) 返回 NULL，
    只要有一个里程碑为空，current_stage 就会整列退化成 'order'。Spark 同理
    （F.greatest 遇 NULL 返回 NULL），所以这里也用 coalesce 兜底。

【幂等性】
  dwd_order_lifecycle 是 PRIMARY KEY(order_id) 模型，Stream Load 是 upsert 语义。
  但【全量重建】的语义是"表里不该有已不存在的订单"，upsert 做不到删除，
  所以外壳脚本按 --full 语义先 TRUNCATE，再一次性灌入当天全量快照。
  增量补数可以用 --dates 只灌某天之后有事件的订单（同样是 upsert，天然幂等）。
"""
import argparse
import base64
import json
import os
import shutil
import sys
import tempfile
import urllib.error
import urllib.request

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType

WAREHOUSE = "file:///opt/offline-dw/lake/warehouse"
JDBC_JAR = "/opt/spark/jars/mysql-connector-java-8.0.30.jar"

SR_HOST = os.environ.get("SR_HOST", "starrocks")
SR_PORT = int(os.environ.get("SR_PORT", "8040"))
SR_AUTH = "Basic " + base64.b64encode(b"root:").decode()

TAB = "\t"
CSV_COLS = ["order_id", "user_id", "product_id", "amount",
            "order_time", "pay_time", "ship_time", "finish_time", "cancel_time",
            "current_stage", "pay_lag_hours", "ship_lag_days", "finish_lag_days",
            "last_event_time", "update_time"]
COLS_HEADER = ",".join(CSV_COLS)

EPOCH = "1970-01-01 00:00:00"
ROWS_PER_BATCH = 150000


def stream_load(csv_path, rows_expected, label, dry_run=False):
    if dry_run:
        print("      [dry-run] 跳过写入（%d 行）" % rows_expected)
        return rows_expected, []
    url = "http://%s:%d/api/dwd/dwd_order_lifecycle/_stream_load" % (SR_HOST, SR_PORT)
    headers = {
        "Authorization": SR_AUTH,
        "Expect": "100-continue",
        "format": "csv",
        "column_separator": TAB,
        "skip_header": "0",
        "strict_mode": "true",
        "max_filter_ratio": "0",
        "columns": COLS_HEADER,
        "label": label,
    }
    with open(csv_path, "rb") as fh:
        data = fh.read()
    req = urllib.request.Request(url, data=data, method="PUT", headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=1800) as resp:
            body = resp.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "replace")
        print("      HTTP %s: %s" % (e.code, body[:400]))
        sys.exit(1)
    j = json.loads(body)
    status = j.get("Status")
    loaded = int(j.get("NumberLoadedRows") or 0)
    errs = []
    if j.get("ErrorURL"):
        errs.append(j["ErrorURL"])
    if status != "Success":
        print("      ❌ Stream Load 失败 Status=%s Msg=%s" % (status, j.get("Message")))
        sys.exit(1)
    return loaded, errs


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dates", default=None,
                    help="逗号分隔的散天：只回填这些天【及之后】有事件的订单（增量语义）")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--label-suffix", default="full")
    args = ap.parse_args()

    spark = (SparkSession.builder
             .appName("dwd_order_lifecycle_streamload")
             .config("spark.sql.session.timeZone", "Asia/Shanghai")
             .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
             .config("spark.sql.catalog.lake.type", "hadoop")
             .config("spark.sql.catalog.lake.warehouse", WAREHOUSE)
             .config("spark.sql.extensions",
                     "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    ev = spark.read.format("iceberg").load("lake.ods.ods_order_event")
    # ★ 只留在 ODS 表里有 dt 的事件（与仓侧一致：仓侧也是按 dt 过滤）
    ev = ev.select("order_id", "event_type", "event_time", "dt",
                   "user_id", "product_id", "amount").cache()
    n_ev = ev.count()
    print("LAKE_EVENT_ROWS = %d" % n_ev)

    # 增量：scope = 在指定日期及之后发生过事件的订单（dt >= 最早那天）
    scope_ids = None
    if args.dates:
        days = [d.strip() for d in args.dates.split(",") if d.strip()]
        frm = min(days)
        scope_ids = (ev.filter(F.col("dt") >= F.lit(frm))
                     .select("order_id").distinct().cache())
        n_scope = scope_ids.count()
        print("SCOPE_FROM = %s  范围内订单数 = %d" % (frm, n_scope))

    agg = (ev.groupBy("order_id").agg(
        F.max("user_id").alias("user_id"),
        F.max("product_id").alias("product_id"),
        F.max("amount").alias("amount"),
        F.max(F.when(F.col("event_type") == F.lit("order"),
                     F.col("event_time"))).alias("order_time"),
        F.max(F.when(F.col("event_type") == F.lit("pay"),
                     F.col("event_time"))).alias("pay_time"),
        F.max(F.when(F.col("event_type") == F.lit("ship"),
                     F.col("event_time"))).alias("ship_time"),
        F.max(F.when(F.col("event_type") == F.lit("finish"),
                     F.col("event_time"))).alias("finish_time"),
        F.max(F.when(F.col("event_type") == F.lit("cancel"),
                     F.col("event_time"))).alias("cancel_time"),
    ))

    if scope_ids is not None:
        agg = agg.join(F.broadcast(scope_ids), on="order_id", how="inner")

    d = (agg
         .withColumn("last_event_time", F.greatest(
             F.coalesce(F.col("order_time"), F.lit(EPOCH).cast("timestamp")),
             F.coalesce(F.col("pay_time"), F.lit(EPOCH).cast("timestamp")),
             F.coalesce(F.col("ship_time"), F.lit(EPOCH).cast("timestamp")),
             F.coalesce(F.col("finish_time"), F.lit(EPOCH).cast("timestamp")),
             F.coalesce(F.col("cancel_time"), F.lit(EPOCH).cast("timestamp"))))
         .withColumn("current_stage",
                     F.when(F.col("cancel_time") == F.col("last_event_time"), F.lit("cancel"))
                      .when(F.col("finish_time") == F.col("last_event_time"), F.lit("finish"))
                      .when(F.col("ship_time") == F.col("last_event_time"), F.lit("ship"))
                      .when(F.col("pay_time") == F.col("last_event_time"), F.lit("pay"))
                      .otherwise(F.lit("order")))
         # TIMESTAMPDIFF(HOUR, order_time, pay_time)：Spark 无同名函数，用秒差/3600。
         # ★ 不能用 col.cast("long")：Iceberg 读出来的类型是 TIMESTAMP_NTZ，
         #   直接 CAST 到 BIGINT 会报 DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION。
         #   用 unix_timestamp() 拿秒数（负数时间差下 Spark 的 floor 与
         #   StarRocks TIMESTAMPDIFF 的「向下取整」一致）。
         .withColumn("pay_lag_hours",
                     F.when(F.col("pay_time").isNull() | F.col("order_time").isNull(), F.lit(None))
                      .otherwise(F.floor(
                          (F.unix_timestamp("pay_time") - F.unix_timestamp("order_time")) / 3600))
                      .cast("int"))
         # DATEDIFF(a, b) = date(a) - date(b)，与 StarRocks 的 DATEDIFF 一致
         .withColumn("ship_lag_days", F.datediff(F.col("ship_time"), F.col("order_time")).cast("int"))
         .withColumn("finish_lag_days", F.datediff(F.col("finish_time"), F.col("order_time")).cast("int"))
         .withColumn("update_time", F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss")))

    total = d.count()
    print("LIFECYCLE_ROWS = %d" % total)

    d = d.repartition(max(1, (total + ROWS_PER_BATCH - 1) // ROWS_PER_BATCH))

    # ★ concat_ws 会【跳过 NULL】，那样字段会整列错位，所以每个可空列都要
    #   先用 coalesce 换成 StarRocks 认的 NULL 字面量 \N（反斜杠 N）。
    NUL = "\\N"

    def S(col, fmt=None):
        c = F.col(col)
        c = F.date_format(c, fmt) if fmt else c.cast("string")
        return F.coalesce(c, F.lit(NUL))

    line = F.concat_ws(
        TAB,
        S("order_id"),
        S("user_id"),
        S("product_id"),
        # ★★ 千万别用 F.format_number()：它走 java.text.DecimalFormat，
        #    千分位会写出 '2,995.00'，逗号让整行被切成两段 →
        #    BE 报 `too many filtered rows`（实测 2026-10-14，二分定位到
        #    order_id=2026110400184、amount=2995.00）。DECIMAL→string 的
        #    cast 不带千分位，直接用。
        S("amount"),
        S("order_time", "yyyy-MM-dd HH:mm:ss"),
        S("pay_time", "yyyy-MM-dd HH:mm:ss"),
        S("ship_time", "yyyy-MM-dd HH:mm:ss"),
        S("finish_time", "yyyy-MM-dd HH:mm:ss"),
        S("cancel_time", "yyyy-MM-dd HH:mm:ss"),
        # current_stage 有 otherwise 兜底，last_event_time 有 epoch 兜底，不会为空
        S("current_stage"),
        S("pay_lag_hours"),
        S("ship_lag_days"),
        S("finish_lag_days"),
        S("last_event_time", "yyyy-MM-dd HH:mm:ss"),
        S("update_time"))

    tmp = tempfile.mkdtemp(prefix="lc_")
    out_dir = os.path.join(tmp, "out")
    (d.select(line.alias("value"))
     .write.mode("overwrite").option("compression", "none").text(out_dir))

    parts = sorted(os.path.join(out_dir, f) for f in os.listdir(out_dir)
                   if f.startswith("part-"))
    print("PART_FILES = %d" % len(parts))

    # ★ 落盘自检：每个可空字段都用 \N 占位，所以每行必须是 14 个 TAB / 15 列。
    #   少一列说明 concat_ws 跳过了 NULL；多一列说明字段里混进了 TAB。
    #   实测教训：F.format_number 的千分位逗号虽然不破坏列数，但会被 BE 切错，
    #   所以这里同时检查有没有半角逗号（本表所有字段都不该出现逗号）。
    bad_cols, bad_comma, checked = 0, 0, 0
    for p in parts:
        with open(p, "r", encoding="utf-8") as fh:
            for ln in fh:
                ln = ln.rstrip("\n")
                if not ln:
                    continue
                checked += 1
                if ln.count(TAB) != len(CSV_COLS) - 1:
                    bad_cols += 1
                    if bad_cols <= 3:
                        print("      ⚠️ 列数异常(%d): %r" % (ln.count(TAB) + 1, ln[:160]))
                elif "," in ln:
                    bad_comma += 1
                    if bad_comma <= 3:
                        print("      ⚠️ 含逗号: %r" % ln[:160])
    print("SELFCHECK lines=%d bad_cols=%d bad_comma=%d" % (checked, bad_cols, bad_comma))
    if bad_cols or bad_comma:
        print("❌ 落盘自检不通过，拒绝加载")
        sys.exit(2)

    if args.dry_run:
        for p in parts:
            print("      [dry-run] %s（%d 字节）" % (os.path.basename(p), os.path.getsize(p)))
        spark.stop()
        return

    # 先 TRUNCATE：全量重建语义（upsert 删不掉已不在快照里的订单）
    # —— 但容器里没有 docker，所以 TRUNCATE 由外壳脚本在提交前完成，这里只灌。
    grand = 0
    for i, p in enumerate(parts):
        label = "lc_%s_%03d" % (args.label_suffix, i)
        got = stream_load(p, 0, label)
        grand += got[0]
        print("  PART %03d %s loaded=%d" % (i, os.path.basename(p), got[0]))

    print("LIFECYCLE_LOADED = %d" % grand)
    shutil.rmtree(tmp, ignore_errors=True)
    spark.stop()


if __name__ == "__main__":
    main()
