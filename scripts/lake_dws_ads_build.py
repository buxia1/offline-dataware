#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
湖侧 DWS / ADS 层构建：lake.dwd.dwd_order_detail -> lake.dws.* -> lake.ads.*

口径**逐字照搬**仓侧（只把事实来源从仓侧 dwd.dwd_order_detail 换成湖侧 lake.dwd.dwd_order_detail）：

  DWS  <- sql/dws_user_order_day_insert.sql
          GROUP BY user_id, dt；COUNT(*) / COUNT(CASE status='paid') /
          SUM(CASE status='paid') / SUM(CASE status='refund')

  ADS  <- sql/ads_daily_sales_insert.sql
          再按 dt 聚合；net/avg/pay_rate 三个派生指标必须"先各自 SUM 再相除"

用法：
  spark-submit lake_dws_ads_build.py --table all --mode rebuild
  spark-submit lake_dws_ads_build.py --table dws_user_order_day --mode rebuild \
      --start 2026-09-20 --end 2026-09-30
  spark-submit lake_dws_ads_build.py --table all --mode dry
"""
import argparse
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SRC_DETAIL = "lake.dwd.dwd_order_detail"

T_DWS = "lake.dws.dws_user_order_day"
T_ADS = "lake.ads.ads_daily_sales"

COLS_DWS = ["user_id", "dt", "order_cnt", "paid_cnt", "paid_amount", "refund_amount"]
COLS_ADS = ["dt", "order_cnt", "paid_cnt", "paid_amount", "refund_amount",
            "net_amount", "avg_order_amount", "pay_rate"]


def build_spark():
    return (SparkSession.builder
            .appName("lake_dws_ads_build")
            .config("spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.lake.type", "hadoop")
            .config("spark.sql.catalog.lake.warehouse",
                    "file:///opt/offline-dw/lake/warehouse")
            .config("spark.sql.session.timeZone", "Asia/Shanghai")
            .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
            .getOrCreate())


def read_detail(spark, d1=None, d2=None):
    df = spark.read.format("iceberg").load(SRC_DETAIL)
    if d1:
        df = df.filter((F.col("dt") >= F.lit(d1)) & (F.col("dt") <= F.lit(d2)))
    return df


# --------------------------------------------------------------------------
# 1) DWS：user_id × dt
#    仓侧 SQL 里 SUM(CASE WHEN paid THEN amount ELSE 0 END) 的 ELSE 0 不能省 ——
#    省掉会得到 NULL，而仓侧口径是 0。
# --------------------------------------------------------------------------
def build_dws(spark, d1=None, d2=None):
    df = (read_detail(spark, d1, d2)
          .groupBy("user_id", "dt")
          .agg(
              F.count(F.lit(1)).alias("order_cnt"),
              F.count(F.when(F.col("status") == "paid", 1)).alias("paid_cnt"),
              F.sum(F.when(F.col("status") == "paid", F.col("amount")).otherwise(F.lit(0)))
               .cast("decimal(18,2)").alias("paid_amount"),
              F.sum(F.when(F.col("status") == "refund", F.col("amount")).otherwise(F.lit(0)))
               .cast("decimal(18,2)").alias("refund_amount"),
          )
          .select(*[F.col(c) for c in COLS_DWS]))
    return df


# --------------------------------------------------------------------------
# 2) ADS：dt
#    ★ 派生指标：SUM(paid_amount)/SUM(paid_cnt)，不能先算比率再平均。
#    ★ NULLIF(x,0) 除零保护：某天零支付时结果是 NULL 而不是崩。
# --------------------------------------------------------------------------
def build_ads(spark, d1=None, d2=None):
    # 直接从明细算，不经过 DWS —— 与仓侧 ads SQL「读 dws」在数值上等价
    # （DWS 只是 user×dt 的再聚合，SUM 可加），但少一次全量 shuffle。
    # 若要与仓侧完全同路径，可改成读 T_DWS；两者实测结果一致。
    src = read_detail(spark, d1, d2)
    df = (src
          .groupBy("dt")
          .agg(
              F.count(F.lit(1)).alias("order_cnt"),
              F.count(F.when(F.col("status") == "paid", 1)).alias("paid_cnt"),
              F.sum(F.when(F.col("status") == "paid", F.col("amount")).otherwise(F.lit(0)))
               .alias("paid_amount"),
              F.sum(F.when(F.col("status") == "refund", F.col("amount")).otherwise(F.lit(0)))
               .alias("refund_amount"),
          )
          .withColumn("net_amount", (F.col("paid_amount") - F.col("refund_amount"))
                      .cast("decimal(18,2)"))
          .withColumn("avg_order_amount",
                      F.when(F.col("paid_cnt") == 0, F.lit(None).cast("decimal(18,2)"))
                       .otherwise(F.round(F.col("paid_amount") / F.col("paid_cnt"), 2))
                       .cast("decimal(18,2)"))
          .withColumn("pay_rate",
                      F.when(F.col("order_cnt") == 0, F.lit(None).cast("decimal(10,4)"))
                       .otherwise(F.round(F.col("paid_cnt") / F.col("order_cnt"), 4))
                       .cast("decimal(10,4)"))
          .select(*[F.col(c) for c in COLS_ADS]))
    return df


BUILDERS = {
    "dws_user_order_day": (build_dws, T_DWS),
    "ads_daily_sales": (build_ads, T_ADS),
}
ORDER_OF_EXEC = ["dws_user_order_day", "ads_daily_sales"]


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
        df.orderBy("dt").show(3, truncate=False)
        df.orderBy(F.col("dt").desc()).show(3, truncate=False)
        return n

    if mode == "rebuild":
        spark.sql("DELETE FROM %s" % tgt)
        print("  已清空目标表")
        df.writeTo(tgt).append()
    else:
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
    print("DWS_ADS_TOTAL = %d" % total)
    spark.stop()
    print("EXIT=0")
    return 0


if __name__ == "__main__":
    sys.exit(main())
