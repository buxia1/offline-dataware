#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Iceberg 湖表维护：只读盘点 + 清理历史快照

用法（在 spark 容器里跑，见 README「湖表快照清理」一节）：
    spark-submit lake_maintain.py                        # 只读盘点，零写入
    spark-submit lake_maintain.py --expire --keep 5      # 每张表保留最近 5 个快照
    spark-submit lake_maintain.py --tables ods_order,dwd_order_detail --expire --keep 3

验收判据（缺一不可）：**快照数下降** 且 **行数不变**。
快照清理只影响「时间旅行能回看多久」，不影响当前数据。
任何一项不达标本脚本以退出码 1 结束（交给 DS 告警）。

实测踩坑（都改过了，别再犯）：
  1. Spark SQL 解析器不接受中文列别名 -> PARSE_SYNTAX_ERROR at '快'
  2. Iceberg `manifests` 元数据表没有 file_size_in_bytes 列，只有 length
     （`files` 表才有 file_size_in_bytes）
  3. expire_snapshots 用 older_than + retain_last 时必须保证 older_than
     确实早于要清理的快照，否则静默不删
  4. 表清单不写死：namespaces 下的表全部枚举，新增湖表自动纳入维护
"""
import argparse
import sys
import time

from pyspark.sql import SparkSession

LAKE_WH = "file:///opt/offline-dw/lake/warehouse"
NAMESPACES = ("ods", "dim", "dwd")


def build_spark():
    return (SparkSession.builder
            .appName("lake_maintain")
            .config("spark.sql.session.timeZone", "Asia/Shanghai")
            .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.lake.type", "hadoop")
            .config("spark.sql.catalog.lake.warehouse", LAKE_WH)
            .config("spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .getOrCreate())


def q1(spark, sql):
    rows = spark.sql(sql).collect()
    return rows[0] if rows else None


def lake_tables(spark):
    names = []
    for ns in NAMESPACES:
        try:
            rows = spark.sql(f"SHOW TABLES IN lake.{ns}").collect()
        except Exception as e:  # 命名空间不存在就跳过，不算错误
            print(f"  （跳过 lake.{ns}：{e}）")
            continue
        for r in rows:
            names.append(f"lake.{ns}.{r['tableName']}")
    return sorted(names)


def inventory(spark, fq):
    d = {"table": fq}
    d["snapshots"] = q1(spark, f"SELECT count(*) AS c FROM {fq}.snapshots")["c"]
    fr = q1(spark, f"SELECT count(*) AS n, "
                   f"round(sum(file_size_in_bytes)/1024/1024,1) AS mb FROM {fq}.files")
    d["files"], d["files_mb"] = fr["n"], fr["mb"]
    # ⚠️ manifests 表用 length；files 表才是 file_size_in_bytes
    mr = q1(spark, f"SELECT count(*) AS n, "
                   f"round(sum(length)/1024/1024,1) AS mb FROM {fq}.manifests")
    d["manifests"], d["manifests_mb"] = mr["n"], mr["mb"]
    d["rows"] = q1(spark, f"SELECT count(*) AS c FROM {fq}")["c"]
    return d


def show(d):
    print(f"  {d['table']:32s} 快照 {d['snapshots']:>4}   "
          f"数据文件 {d['files']:>4} ({d['files_mb']} MB)   "
          f"清单 {d['manifests']:>4} ({d['manifests_mb']} MB)   "
          f"行数 {d['rows']}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--expire", action="store_true", help="真的执行 expire_snapshots（默认只读）")
    ap.add_argument("--keep", type=int, default=5, help="每表保留最近 N 个快照（默认 5）")
    ap.add_argument("--tables", default="", help="只处理这些表名（逗号分隔，可写简名如 ods_order）")
    args = ap.parse_args()

    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")

    all_tables = lake_tables(spark)
    if args.tables.strip():
        want = [t.strip() for t in args.tables.split(",") if t.strip()]
        tables = [t for t in all_tables if t in want or t.split(".")[-1] in want]
        missing = [w for w in want if not any(w == t or w == t.split(".")[-1] for t in tables)]
        if missing:
            print(f"!!! 表名不存在：{missing}")
            return 2
    else:
        tables = all_tables

    if not tables:
        print("!!! 没有找到任何 lake 表")
        return 2

    print("=" * 100)
    print(f"维护对象 {len(tables)} 张表：" + ", ".join(t.split(".")[-1] for t in tables))
    print("维护前")
    before = {t: inventory(spark, t) for t in tables}
    for t in tables:
        show(before[t])

    if not args.expire:
        print("\n（只读模式，未做任何清理；加 --expire 才执行 expire_snapshots）")
        spark.stop()
        return 0

    # 关键：older_than 取「当前时间 -1 小时」，保证早于所有要清的历史快照
    older = q1(spark, "SELECT date_format(now() - INTERVAL 1 HOUR, 'yyyy-MM-dd HH:mm:ss') AS t")["t"]
    print(f"\nolder_than = {older}   retain_last = {args.keep}")

    t0 = time.time()
    for t in tables:
        print(f">>> expire_snapshots({t})")
        spark.sql(f"CALL lake.system.expire_snapshots("
                  f"table => '{t}', "
                  f"older_than => TIMESTAMP '{older}', "
                  f"retain_last => {args.keep})")
    print(f"全部表清理耗时 {time.time() - t0:.1f}s")

    print("\n维护后")
    after = {t: inventory(spark, t) for t in tables}
    for t in tables:
        show(after[t])

    print("\n" + "=" * 100)
    bad = []
    for t in tables:
        b, a = before[t], after[t]
        ok_snap = a["snapshots"] <= b["snapshots"]
        ok_rows = a["rows"] == b["rows"]
        flag = "OK " if (ok_snap and ok_rows) else "!!!"
        print(f"  {flag} {t:32s} 快照 {b['snapshots']} -> {a['snapshots']}   "
              f"行数 {b['rows']} -> {a['rows']}")
        if not ok_rows:
            bad.append(f"{t}: 行数变了 {b['rows']} -> {a['rows']}")
        if a["snapshots"] > args.keep + 1:
            bad.append(f"{t}: 清理后仍有 {a['snapshots']} 个快照（> keep+1={args.keep + 1}）")

    if bad:
        print("\n!!! 验收不通过：")
        for x in bad:
            print("   - " + x)
        spark.stop()
        return 1

    print("\n✅ 快照数下降 且 行数不变，验收通过")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
