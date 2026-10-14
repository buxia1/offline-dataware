#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Iceberg 湖表维护探针
用法：spark-submit lake_maintain_probe.py [--expire]

实测踩坑（都改过了，别再犯）：
  1. Spark SQL 解析器不接受中文列别名 -> PARSE_SYNTAX_ERROR at '快'
  2. Iceberg `manifests` 元数据表没有 file_size_in_bytes 列，只有 length
     （`files` 表才有 file_size_in_bytes）
  3. expire_snapshots 用 older_than + retain_last 时必须保证 older_than
     确实早于要清理的快照，否则静默不删
"""
import argparse
import sys
import time

from pyspark.sql import SparkSession

LAKE_WH = "file:///opt/offline-dw/lake/warehouse"


def build_spark():
    return (SparkSession.builder
            .appName("lake_maintain_probe")
            .config("spark.sql.session.timeZone", "Asia/Shanghai")
            .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.lake.type", "hadoop")
            .config("spark.sql.catalog.lake.warehouse", LAKE_WH)
            .config("spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .getOrCreate())


def snapshot_of(spark, t):
    fq = f"lake.ods.{t}"
    print(f"--- {fq} ---")
    spark.sql(f"SELECT count(*) AS snapshots FROM {fq}.snapshots").show(truncate=False)
    spark.sql(f"SELECT count(*) AS data_files, "
              f"round(sum(file_size_in_bytes)/1024/1024,1) AS mb "
              f"FROM {fq}.files").show(truncate=False)
    # ⚠️ manifests 表用的是 length，不是 file_size_in_bytes
    spark.sql(f"SELECT count(*) AS manifests, "
              f"round(sum(length)/1024/1024,1) AS mb "
              f"FROM {fq}.manifests").show(truncate=False)
    spark.sql(f"SELECT count(*) AS all_entries, "
              f"round(sum(file_size_in_bytes)/1024/1024,1) AS mb "
              f"FROM {fq}.all_data_files").show(truncate=False)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--expire", action="store_true")
    ap.add_argument("--keep", type=int, default=5)
    args = ap.parse_args()

    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")

    print("=" * 70)
    print("维护前：快照 / 数据文件 / 清单")
    for t in ("ods_order", "ods_order_event"):
        snapshot_of(spark, t)

    if not args.expire:
        print("（只读模式，未做任何清理。加 --expire 才执行 expire_snapshots）")
        spark.stop()
        return 0

    # 关键：older_than 取"当前时间 -1 小时"，保证早于所有要清的历史快照
    older = spark.sql(
        "SELECT date_format(now() - INTERVAL 1 HOUR, 'yyyy-MM-dd HH:mm:ss') AS t"
    ).collect()[0]["t"]
    print(f"\nolder_than = {older}   retain_last = {args.keep}")

    t0 = time.time()
    for t in ("ods_order", "ods_order_event"):
        fq = f"lake.ods.{t}"
        print(f"\n>>> expire_snapshots({fq})")
        spark.sql(f"CALL lake.system.expire_snapshots("
                  f"table => '{fq}', "
                  f"older_than => TIMESTAMP '{older}', "
                  f"retain_last => {args.keep})")
        print(f"    耗时 {time.time()-t0:.1f}s")

    print("\n" + "=" * 70)
    print("维护后：快照 / 数据文件 / 清单")
    for t in ("ods_order", "ods_order_event"):
        snapshot_of(spark, t)

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
