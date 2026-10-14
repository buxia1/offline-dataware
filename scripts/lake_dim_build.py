#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
湖侧商品维度链构建：lake.ods.ods_product -> lake.dim.dim_product_scd2 -> lake.dim.dim_product

口径逐字照搬：
  · SCD2  <- sql/dim_product_scd2_load.sql
            （TRUNCATE 后从快照表完整重推：按 product_id 分区、
              LAG(category/price/status) 标变更点、LEAD(snapshot_date)-1 天定 valid_to、
              末版 valid_to = 9999-12-31）
  · 当前版 <- sql/dim_product_load.sql
            （ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY snapshot_date DESC) 取 rn=1）

用法：
  spark-submit build.py --table scd2
  spark-submit build.py --table all
  spark-submit build.py --table all --mode dry
"""
import argparse
import sys

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

SRC = "lake.ods.ods_product"
T_SCD2 = "lake.dim.dim_product_scd2"
T_CUR = "lake.dim.dim_product"

SENTINEL = "9999-12-31"

# 快照表里参与"变更判定"的属性（与 sql/dim_product_scd2_load.sql 一致）
TRACKED = ["product_name", "category", "brand", "price", "status"]


def build_spark():
    return (SparkSession.builder
            .appName("lake_dim_build")
            .config("spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.lake.type", "hadoop")
            .config("spark.sql.catalog.lake.warehouse",
                    "file:///opt/offline-dw/lake/warehouse")
            .config("spark.sql.session.timeZone", "Asia/Shanghai")
            .getOrCreate())


def build_scd2(spark):
    src = (spark.read.format("iceberg").load(SRC)
           .select("product_id", "snapshot_date", *TRACKED)
           .orderBy("product_id", "snapshot_date"))

    w = Window.partitionBy("product_id").orderBy("snapshot_date")
    w_next = Window.partitionBy("product_id").orderBy("snapshot_date").rowsBetween(1, 1)

    # 是否变更点：首版必是；之后任一被跟踪列与上一版不同
    changed = None
    for c in TRACKED:
        prev = F.lag(F.col(c)).over(w)
        cond = ~(F.col(c).eqNullSafe(prev))
        changed = cond if changed is None else (changed | cond)
    is_change = F.when(F.row_number().over(w) == 1, F.lit(True)).otherwise(changed)

    ev = (src
          .withColumn("_is_change", is_change)
          .filter(F.col("_is_change"))
          .withColumn("valid_from", F.col("snapshot_date")))
    # valid_to = 下一个变更点的前一天；末版用哨兵（与仓侧同）
    ev = (ev
          .withColumn("_next_from", F.lead("valid_from").over(Window.partitionBy("product_id").orderBy("valid_from")))
          .withColumn("valid_to",
                      F.when(F.col("_next_from").isNull(), F.lit(SENTINEL).cast("date"))
                       .otherwise(F.date_sub(F.col("_next_from"), 1)))
          .withColumn("is_current", F.when(F.col("valid_to") == F.lit(SENTINEL).cast("date"), 1).otherwise(0).cast("int"))
          .withColumn("update_time", F.current_timestamp())
          .select("product_id", "valid_from", "valid_to", "is_current",
                  "product_name", "category", "brand", "price", "status", "update_time"))
    return ev


def build_current(spark):
    scd2 = spark.read.format("iceberg").load(T_SCD2)
    w = Window.partitionBy("product_id").orderBy(F.col("valid_from").desc())
    return (scd2
            .withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .select("product_id", "product_name", "category", "brand", "price", "status", "update_time"))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", default="all", choices=["scd2", "current", "all"])
    ap.add_argument("--mode", default="rebuild", choices=["rebuild", "dry"])
    args = ap.parse_args()

    spark = build_spark()
    if args.table in ("scd2", "all"):
        df = build_scd2(spark).cache()
        n = df.count()
        print("SCD2 算出 %d 行" % n)
        if args.mode == "dry":
            df.orderBy("product_id", "valid_from").show(12, truncate=False)
        else:
            spark.sql("DELETE FROM %s" % T_SCD2)
            df.writeTo(T_SCD2).append()
            print("SCD2 写入后 = %d" % spark.sql("SELECT count(*) FROM %s" % T_SCD2).collect()[0][0])
    if args.table in ("current", "all"):
        df = build_current(spark).cache()
        n = df.count()
        print("CURRENT 算出 %d 行" % n)
        if args.mode == "dry":
            df.orderBy("product_id").show(5, truncate=False)
        else:
            spark.sql("DELETE FROM %s" % T_CUR)
            df.writeTo(T_CUR).append()
            print("CURRENT 写入后 = %d" % spark.sql("SELECT count(*) FROM %s" % T_CUR).collect()[0][0])
    spark.stop()
    print("EXIT=0")
    return 0


if __name__ == "__main__":
    sys.exit(main())
