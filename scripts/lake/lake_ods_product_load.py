#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
湖侧商品快照导入：data/dim/product_snapshot_YYYYMMDD.csv -> lake.ods.ods_product

为什么要有它：
  湖侧维度链（lake_dim_build.py）的源是 lake.ods.ods_product，但仓库原先既没有它的
  建表 SQL、也没有任何往它写数的脚本（现有的 4500 行是手工灌的，换台机器就没了）。
  本脚本把这条链路补成可复现的一条命令。

用法（由 scripts/lake/run_ods_product.sh 包装，一般不用直接调）：
  spark-submit ... lake_ods_product_load.py --all
  spark-submit ... lake_ods_product_load.py --start 2026-12-01 --end 2026-12-18
  spark-submit ... lake_ods_product_load.py --file /opt/offline-dw/data/dim/product_snapshot_20261218.csv
  spark-submit ... lake_ods_product_load.py --all --mode dry        # 只解析、不写

幂等：每天先 DELETE FROM ... WHERE snapshot_date = DATE'...' 再 append（与 lake_dwd_build.py 同做法）。
      snapshot_date 不在 CSV 里，由文件名解析——这是本表最容易写错的一处。
"""
import argparse
import glob
import os
import re
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DATA_DIR = "/opt/offline-dw/data/dim"
PATTERN = "product_snapshot_*.csv"
T = "lake.ods.ods_product"
FNAME_RE = re.compile(r"product_snapshot_(\d{4})(\d{2})(\d{2})\.csv$")

# CSV 列 → 表列类型（与 data/dim/*.csv 表头逐列对齐）
CSV_COLS = [("product_id", "bigint"), ("product_name", "string"), ("category", "string"),
            ("brand", "string"), ("price", "decimal(10,2)"), ("status", "string"),
            ("update_time", "timestamp")]


def build_spark():
    return (SparkSession.builder
            .appName("lake_ods_product_load")
            .config("spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.lake.type", "hadoop")
            .config("spark.sql.catalog.lake.warehouse",
                    "file:///opt/offline-dw/lake/warehouse")
            .config("spark.sql.session.timeZone", "Asia/Shanghai")
            .getOrCreate())


def date_of(path):
    m = FNAME_RE.search(os.path.basename(path))
    if not m:
        raise SystemExit("文件名解析不出日期：%s" % path)
    return "%s-%s-%s" % m.groups()


def pick_files(args):
    if args.file:
        if not os.path.exists(args.file):
            raise SystemExit("文件不存在：%s" % args.file)
        return [args.file]
    files = sorted(glob.glob(os.path.join(args.dir, PATTERN)))
    if not files:
        raise SystemExit("没找到 %s/%s" % (args.dir, PATTERN))
    if args.all:
        return files
    if not (args.start and args.end):
        raise SystemExit("要么 --all，要么 --start + --end，要么 --file")
    return [f for f in files if args.start <= date_of(f) <= args.end]


def read_csv(spark, path, d):
    df = spark.read.option("header", True).option("mode", "FAILFAST").csv(path)
    missing = [c for c, _ in CSV_COLS if c not in df.columns]
    if missing:
        raise SystemExit("%s 缺列：%s（实际表头 %s）" % (path, missing, df.columns))
    return (df.select([F.col(c).cast(t).alias(c) for c, t in CSV_COLS])
              .withColumn("snapshot_date", F.lit(d).cast("date")))


def day_count(spark, d):
    return spark.sql("SELECT count(*) FROM %s WHERE snapshot_date = DATE'%s'" % (T, d)).collect()[0][0]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--all", action="store_true", help="data/dim 下全部快照")
    ap.add_argument("--start", help="起始快照日 YYYY-MM-DD（含）")
    ap.add_argument("--end", help="结束快照日 YYYY-MM-DD（含）")
    ap.add_argument("--file", help="单个 CSV 绝对路径")
    ap.add_argument("--dir", default=DATA_DIR)
    ap.add_argument("--mode", default="append", choices=["append", "dry"])
    args = ap.parse_args()

    files = pick_files(args)
    print("待处理 %d 个文件：%s ... %s" % (len(files), date_of(files[0]), date_of(files[-1])))

    spark = build_spark()
    for p in files:
        d = date_of(p)
        df = read_csv(spark, p, d).cache()
        n = df.count()
        if args.mode == "dry":
            print("DRY  %s snapshot_date=%s csv_rows=%d" % (os.path.basename(p), d, n))
            df.orderBy("product_id").show(3, truncate=False)
            df.unpersist()
            continue
        before = day_count(spark, d)
        spark.sql("DELETE FROM %s WHERE snapshot_date = DATE'%s'" % (T, d))
        df.writeTo(T).append()
        after = day_count(spark, d)
        df.unpersist()
        ok = (after == n)
        print("RESULT %s snapshot_date=%s before=%d csv=%d after=%d %s"
              % (os.path.basename(p), d, before, n, after, "OK" if ok else "!!! 行数不符"))
        if not ok:
            print("!!! %s 写入行数与 CSV 不符，中止" % d)
            spark.stop()
            return 2

    if args.mode == "append":
        t = spark.sql("SELECT count(*) n, count(DISTINCT snapshot_date) days, "
                      "cast(max(snapshot_date) as string) mx FROM %s" % T).collect()[0]
        print("ods_product TOTAL = %d 行 / %d 天 / 最新 %s" % (t["n"], t["days"], t["mx"]))
    spark.stop()
    print("EXIT=0")
    return 0


if __name__ == "__main__":
    sys.exit(main())
