#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
湖侧 DWD 层构建：lake.ods.* -> lake.dwd.*

三张表的口径**逐字照搬**仓侧实现（只把事实来源从仓侧换成湖侧 Iceberg）：

  dwd_order_detail      <- scripts/dwd_order_detail_streamload.py（口径来自 sql/dwd_order_detail.sql
                           + scripts/dwd_overwrite.sh 的清洗段）
  dwd_order_sku_detail  <- scripts/dwd_order_sku_streamload.py（口径来自 sql/dwd_order_sku_detail_load.sql）
  dwd_order_lifecycle   <- scripts/dwd_order_lifecycle_streamload.py（口径来自 sql/dwd_order_lifecycle.sql
                           的视图 dwd.v_order_lifecycle_expected）

用法：
  spark-submit build.py --table dwd_order_detail --mode rebuild
  spark-submit build.py --table all --mode rebuild
  spark-submit build.py --table dwd_order_detail --mode append --start 2026-09-20 --end 2026-09-30
  spark-submit build.py --table dwd_order_detail --mode dry      # 只算不写
"""
import argparse
import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

LAKE = "lake"
SRC_ORDER = "lake.ods.ods_order"
SRC_EVENT = "lake.ods.ods_order_event"
SRC_SCD2 = "lake.dim.dim_product_scd2"

T_DETAIL = "lake.dwd.dwd_order_detail"
T_SKU = "lake.dwd.dwd_order_sku_detail"
T_LIFE = "lake.dwd.dwd_order_lifecycle"

# 三张目标表的列顺序（与 sql/lake_dwd.sql 的 CREATE TABLE 严格一致）
COLS_DETAIL = ["order_id", "dt", "user_id", "product_id", "amount", "order_time", "status"]
COLS_SKU = ["order_id", "dt", "user_id", "product_id", "amount", "order_time", "status",
            "category", "brand", "sku_price", "valid_from", "valid_to"]
COLS_LIFE = ["order_id", "user_id", "product_id", "amount", "order_time", "pay_time",
             "ship_time", "finish_time", "cancel_time", "current_stage", "pay_lag_hours",
             "ship_lag_days", "finish_lag_days", "last_event_time", "update_time"]

EPOCH = "1970-01-01 00:00:00"


def build_spark():
    return (SparkSession.builder
            .appName("lake_dwd_build")
            .config("spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.lake.type", "hadoop")
            .config("spark.sql.catalog.lake.warehouse",
                    "file:///opt/offline-dw/lake/warehouse")
            .config("spark.sql.session.timeZone", "Asia/Shanghai")
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .getOrCreate())


def read_orders(spark, d1=None, d2=None):
    df = spark.read.format("iceberg").load(SRC_ORDER)
    if d1:
        df = df.filter((F.col("dt") >= F.lit(d1)) & (F.col("dt") <= F.lit(d2)))
    return df


def read_events(spark, d1=None, d2=None):
    df = spark.read.format("iceberg").load(SRC_EVENT)
    if d1:
        df = df.filter((F.col("dt") >= F.lit(d1)) & (F.col("dt") <= F.lit(d2)))
    return df


# --------------------------------------------------------------------------
# 1) dwd_order_detail
#    仓侧清洗：user_id IS NOT NULL / ABS(amount) / 按 (dt, order_id) 取 ABS(amount) 最大的一行
#    ★ 与仓侧脚本一致：先按天去重（湖侧 ODS 已按天去重，这里保留 ROW_NUMBER 以防重跑叠数据）
# --------------------------------------------------------------------------
def build_detail(spark, d1=None, d2=None):
    src = read_orders(spark, d1, d2)
    w = Window.partitionBy("dt", "order_id").orderBy(F.col("amount").desc())
    df = (src
          .filter(F.col("user_id").isNotNull())
          .withColumn("amount", F.abs(F.col("amount")))
          .withColumn("_rn", F.row_number().over(w))
          .filter(F.col("_rn") == 1)
          .select(*[F.col(c) for c in COLS_DETAIL]))
    return df


# --------------------------------------------------------------------------
# 2) dwd_order_sku_detail
#    仓侧口径：o.dt BETWEEN s.valid_from AND s.valid_to（SCD2 时点关联，inner join）
#    维度侧从仓侧 dim_product_scd2 换成湖侧（湖仓主存迁湖后的必然）
# --------------------------------------------------------------------------
def build_sku(spark, d1=None, d2=None):
    fact = build_detail(spark, d1, d2)
    # ★ 维度侧给 product_id 起别名：两张表都有 product_id，join 后再 select 会报
    #   [AMBIGUOUS_REFERENCE] Reference `product_id` is ambiguous
    dim = (spark.read.format("iceberg").load(SRC_SCD2)
           .select(F.col("product_id").alias("_dim_pid"), "valid_from", "valid_to",
                   "category", "brand", F.col("price").alias("sku_price")))
    joined = (fact.join(F.broadcast(dim),
                        (fact.product_id == dim._dim_pid) &
                        (fact.dt >= dim.valid_from) & (fact.dt <= dim.valid_to),
                        "inner")
              .select(*[F.col(c) for c in COLS_SKU]))
    return joined


# --------------------------------------------------------------------------
# 3) dwd_order_lifecycle
#    仓侧视图 dwd.v_order_lifecycle_expected 的逐字移植：
#    五个里程碑 MAX(CASE WHEN event_type=... THEN event_time END)
#    ★ GREATEST 必须用 COALESCE 兜底（任一里程碑 NULL 会让整列退化）
# --------------------------------------------------------------------------
def build_lifecycle(spark, d1=None, d2=None):
    ev = read_events(spark, d1, d2)
    # 增量：只重算 dt >= 起点的那些订单（与仓侧 --inc 的 scope_orders 语义一致）
    if d1:
        scope = ev.filter(F.col("dt") >= F.lit(d1)).select("order_id").distinct()
    else:
        scope = None

    ev_all = read_events(spark) if scope is not None else ev
    agg = (ev_all.groupBy("order_id").agg(
        F.max("user_id").alias("user_id"),
        F.max("product_id").alias("product_id"),
        F.max("amount").alias("amount"),
        F.max(F.when(F.col("event_type") == "order", F.col("event_time"))).alias("order_time"),
        F.max(F.when(F.col("event_type") == "pay", F.col("event_time"))).alias("pay_time"),
        F.max(F.when(F.col("event_type") == "ship", F.col("event_time"))).alias("ship_time"),
        F.max(F.when(F.col("event_type") == "finish", F.col("event_time"))).alias("finish_time"),
        F.max(F.when(F.col("event_type") == "cancel", F.col("event_time"))).alias("cancel_time"),
    ))
    if scope is not None:
        agg = agg.join(F.broadcast(scope), "order_id", "inner")

    ts = F.lit(EPOCH).cast("timestamp")
    last_ev = F.greatest(
        F.coalesce(F.col("order_time"), ts), F.coalesce(F.col("pay_time"), ts),
        F.coalesce(F.col("ship_time"), ts), F.coalesce(F.col("finish_time"), ts),
        F.coalesce(F.col("cancel_time"), ts))

    df = (agg
          .withColumn("last_event_time", last_ev)
          .withColumn("current_stage", F.when(F.col("cancel_time") == F.col("last_event_time"), "cancel")
                      .when(F.col("finish_time") == F.col("last_event_time"), "finish")
                      .when(F.col("ship_time") == F.col("last_event_time"), "ship")
                      .when(F.col("pay_time") == F.col("last_event_time"), "pay")
                      .otherwise("order"))
          # TIMESTAMPDIFF(HOUR, order_time, pay_time)：Spark 无该函数。
          # ★ TIMESTAMP_NTZ 不能直接 cast("long")（报 Cannot resolve CAST(pay_time AS BIGINT)），
          #   必须走 unix_timestamp（秒）；整除 3600 与 TIMESTAMPDIFF(HOUR) 同义（向下取整）
          .withColumn("pay_lag_hours",
                      F.when(F.col("pay_time").isNull() | F.col("order_time").isNull(), F.lit(None).cast("int"))
                       .otherwise((F.unix_timestamp("pay_time") - F.unix_timestamp("order_time")) / 3600)
                       .cast("int"))
          .withColumn("ship_lag_days", F.datediff("ship_time", "order_time"))
          .withColumn("finish_lag_days", F.datediff("finish_time", "order_time"))
          .withColumn("update_time", F.current_timestamp())
          .select(*[F.col(c) for c in COLS_LIFE]))
    return df


BUILDERS = {
    "dwd_order_detail": (build_detail, T_DETAIL),
    "dwd_order_sku_detail": (build_sku, T_SKU),
    "dwd_order_lifecycle": (build_lifecycle, T_LIFE),
}

ORDER_OF_EXEC = ["dwd_order_detail", "dwd_order_sku_detail", "dwd_order_lifecycle"]


def run_one(spark, name, mode, d1, d2):
    fn, tgt = BUILDERS[name]
    print("=" * 70)
    print("BUILD %s -> %s mode=%s range=%s..%s" % (name, tgt, mode, d1, d2))
    before = spark.sql("SELECT count(*) FROM %s" % tgt).collect()[0][0]
    print("  当前行数 = %d" % before)

    df = fn(spark, d1, d2).cache()
    n = df.count()
    print("  算出 %d 行" % n)
    if n == 0:
        print("  !! 0 行，跳过")
        return 0

    if mode == "dry":
        print("  [dry-run] 不写")
        df.show(3, truncate=False)
        return n

    if mode == "rebuild":
        # 全量重建：先清空（Iceberg 没有 TRUNCATE，用 DELETE）
        spark.sql("DELETE FROM %s" % tgt)
        print("  已清空目标表")
        df.writeTo(tgt).append()
    else:
        # 增量：先把范围内已有的行删掉再写（幂等），ODS 是 append-only 不代表 DWD 也是
        if d1:
            spark.sql("DELETE FROM %s WHERE dt >= '%s' AND dt <= '%s'" % (tgt, d1, d2))
        df.writeTo(tgt).append()

    after = spark.sql("SELECT count(*) FROM %s" % tgt).collect()[0][0]
    print("  RESULT %s before=%d written=%d after=%d" % (name, before, n, after))
    if mode == "rebuild" and after != n:
        print("  !! 重建后行数 %d != 算出的 %d" % (after, n))
    return n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", required=True, choices=sorted(BUILDERS) + ["all"])
    ap.add_argument("--mode", default="rebuild", choices=["rebuild", "append", "dry"])
    ap.add_argument("--start", default=None)
    ap.add_argument("--end", default=None)
    args = ap.parse_args()

    d1, d2 = args.start, (args.end or args.start)
    spark = build_spark()
    total = 0
    tables = ORDER_OF_EXEC if args.table == "all" else [args.table]
    for t in tables:
        total += run_one(spark, t, args.mode, d1, d2)
        print("")
    print("==========")
    print("DWD_TOTAL = %d" % total)
    spark.stop()
    print("EXIT=0")
    return 0


if __name__ == "__main__":
    sys.exit(main())
