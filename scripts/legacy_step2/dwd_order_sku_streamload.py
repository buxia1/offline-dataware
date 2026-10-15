#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SKU 明细链路：湖层 Iceberg(订单明细) ⋈ SCD2 商品维度(仓侧) → 仓侧 dwd_order_sku_detail

【为什么从湖层读订单明细】
  和 dwd_order_detail_streamload.py 完全相同的理由：湖层已有 90 天数据，
  Spark 直读 Iceberg 零 JDBC 抽取。仓侧的 dwd_order_detail 就是同一份口径算出来的
  （逐日行数 + sum(amount) 已 90/90 天对账通过），读湖层读仓侧结果一样，
  但读湖省掉那趟 JDBC。

【为什么商品维度反而要 JDBC 读仓侧】
  dim_product_scd2 在仓侧由 sql/dim_product_scd2_load.sql 从 ods_product 重推，
  90 天快照重建后是 4,500 行、50 个商品、覆盖 2026-09-20 ~ 2026-12-18。
  商品链路并没有进湖（湖层只有 ods_order / ods_order_event 两张表），
  4,500 行 JDBC 读一次是毫秒级 —— 【只读维度、不读事实】是关键：
  慢的是写事实、不是读小维度。

【口径逐字照搬 sql/dwd_order_sku_detail_load.sql】
    FROM dwd.dwd_order_detail o
    JOIN dim.dim_product_scd2 s
      ON  o.product_id = s.product_id
     AND  o.dt BETWEEN s.valid_from AND s.valid_to
    WHERE o.dt = '<day>'
  只把来源从仓侧 dwd_order_detail 换成湖侧同口径明细。

【幂等】dwd_order_sku_detail 是 DUPLICATE KEY + 分区表（和 dwd_order_detail 一样），
  所以每天写入前必须先 DELETE 掉该分区 —— 否则每跑一次就翻一倍。
  按 15 天一批 DELETE，不是 90 天一次删（避免大事务）。
"""
import argparse
import base64
import csv
import os
import shutil
import sys
import tempfile
import time
import urllib.error
import urllib.request

from pyspark.sql import SparkSession, functions as F

WAREHOUSE = "file:///opt/offline-dw/lake/warehouse"
ICEBERG_JAR = "/opt/offline-dw/jars/iceberg-spark-runtime-3.5_2.12-1.9.0.jar"
JDBC_JAR = "/opt/spark/jars/mysql-connector-java-8.0.30.jar"

SR_HOST = os.environ.get("SR_HOST", "starrocks")
SR_PORT = int(os.environ.get("SR_PORT", "8040"))
SR_AUTH = "Basic " + base64.b64encode(b"root:").decode()

TAB = "\t"
CSV_COLS = ["order_id", "dt", "user_id", "product_id", "amount", "order_time",
            "status", "category", "brand", "sku_price", "valid_from", "valid_to"]
# ★ Stream Load 的 columns 头用【逗号】分隔（不是制表符），数据才是制表符分隔
COLS_HEADER = ",".join(CSV_COLS)

BATCH_DAYS = 15

# ★★ 每次运行必须换 label（2026-10-14 实测踩到）
#   StarRocks 的 Stream Load label 一旦用过（无论成功还是失败）就【永久占用】
#   （得等 label_keep_max_second 才回收，默认 3 天），复用它会被拒：
#     begin transaction failed, errmsg=Label [xxx] has already been used.
#   而 BE 处理被拒的请求时会在读完 body 之前就关连接 → 客户端只看到
#     urllib.error.URLError: <urlopen error [Errno 32] Broken pipe>
#   （BE 日志里才有真正原因，客户端这个报错完全误导）。
#   所以 label 一律带一个「本次运行」的时间戳后缀。
RUN_ID = os.environ.get("SKU_RUN_ID") or time.strftime("%H%M%S")


def make_label(first, last):
    return "dwd_sku_%s_%s_%s" % (first.replace("-", ""), last.replace("-", ""), RUN_ID)


def stream_load(day, csv_path, rows_expected, label, dry_run=False):
    """把一个 CSV 推给 BE 的 dwd_order_sku_detail。返回 (loaded, filtered)."""
    if dry_run:
        print("      [dry-run] 跳过写入 %s（%d 行）" % (day, rows_expected))
        return rows_expected, 0
    url = "http://%s:%d/api/dwd/dwd_order_sku_detail/_stream_load" % (SR_HOST, SR_PORT)
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
    except Exception as e:  # BrokenPipeError / URLError 等
        # ★ BE 在「读完 body 之前」拒绝请求（最典型的就是 label 重复）时，
        #   客户端只会看到一个完全误导的 Broken pipe。这里把它翻译成人话。
        print("      ❌ Stream Load 连接异常 %s: %s" % (type(e).__name__, e))
        print("      label=%s  size=%d bytes" % (label, os.path.getsize(csv_path)))
        print("      常见原因：label 已被用过（StarRocks 标签永久占用）→ 换 RUN_ID 重跑")
        print("      真因在 BE 日志：docker exec starrocks grep '%s' "
              "/data/deploy/starrocks/be/log/be.WARNING | tail -3" % label)
        sys.exit(1)
    try:
        import json
        j = json.loads(body)
    except Exception:
        print("      非 JSON 响应: %s" % body[:400])
        sys.exit(1)
    status = j.get("Status")
    loaded = int(j.get("NumberLoadedRows") or 0)
    filtered = int(j.get("NumberFilteredRows") or 0)
    if status != "Success":
        print("      ❌ Stream Load 失败 Status=%s Msg=%s" % (status, j.get("Message")))
        sys.exit(1)
    return loaded, filtered


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default=None)
    ap.add_argument("--end", default=None)
    ap.add_argument("--dates", default=None, help="逗号分隔的散天")
    ap.add_argument("--jdbc-url", default="jdbc:mysql://starrocks:9030")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    spark = (SparkSession.builder
             .appName("dwd_order_sku_detail_streamload")
             .config("spark.sql.session.timeZone", "Asia/Shanghai")
             .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
             .config("spark.sql.catalog.lake.type", "hadoop")
             .config("spark.sql.catalog.lake.warehouse", WAREHOUSE)
             .config("spark.sql.extensions",
                     "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    det = spark.read.format("iceberg").load("lake.ods.ods_order")
    print("TIME %.1fs 湖层明细读取器就绪" % 0)

    # ---- 时间边界 ----
    if args.dates:
        days = [d.strip() for d in args.dates.split(",") if d.strip()]
    else:
        s, e = args.start, args.end
        days = [r.dt.strftime("%Y-%m-%d")
                for r in det.select("dt").distinct().orderBy("dt")
                .filter((F.col("dt") >= F.lit(s)) & (F.col("dt") <= F.lit(e)))
                .collect()]
    print("PLAN_DAYS = %d  %s .. %s" % (len(days), days[0] if days else "-",
                                        days[-1] if days else "-"))

    # ---- 商品维度（SCD2，仓侧，只读、很小）----
    scd2 = (spark.read.format("jdbc")
            .option("url", args.jdbc_url)
            .option("query", "SELECT product_id, valid_from, valid_to, category, brand, price "
                             "FROM dim.dim_product_scd2")
            .option("user", "root")
            .option("password", "")
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .load()
            .select(
                F.col("product_id").cast("long").alias("product_id"),
                F.to_date("valid_from").alias("valid_from"),
                F.to_date("valid_to").alias("valid_to"),
                F.col("category"), F.col("brand"),
                F.col("price").cast("decimal(10,2)").alias("sku_price"))
            .cache())
    n_scd2 = scd2.count()
    print("SCD2_DIM_ROWS = %d" % n_scd2)

    # ---- 湖层明细按老口径清洗 ----
    filtered = det.filter(F.col("user_id").isNotNull())

    total_rows = 0
    total_loaded = 0
    total_filtered = 0
    covered = []
    for bi in range(0, len(days), BATCH_DAYS):
        chunk = days[bi:bi + BATCH_DAYS]
        part = (filtered.filter(F.col("dt").isin(*chunk))
                # ★★ 必须取绝对值（2026-10-14 实测踩到）：
                #   源表 lake.ods.ods_order 里有 3% 的负金额（生成器的 NEG_AMOUNT_RATE），
                #   而仓侧口径（sql/dwd_order_sku_detail_load.sql 读的是 dwd_order_detail）
                #   拿到的是【已经 ABS 过】的明细。少这一步 → 25,534 行金额为负、
                #   sum(amount) 比 detail 少 8,476,266.40（仓侧当年就是这么错的）。
                #   行数完全一致、只有金额错 —— 只看行数的对账发现不了。
                .withColumn("amount", F.abs(F.col("amount")))
                .select("order_id", "dt", "user_id", "product_id", "amount",
                        "order_time", "status"))
        part.cache()
        exp = part.count()
        if exp == 0:
            part.unpersist()
            continue

        joined = (part.join(F.broadcast(scd2),
                            (part.product_id == scd2.product_id)
                            & (part.dt >= scd2.valid_from)
                            & (part.dt <= scd2.valid_to), "inner")
                  .select(part.order_id, part.dt, part.user_id, part.product_id,
                          part.amount, part.order_time, part.status,
                          scd2.category, scd2.brand, scd2.sku_price,
                          scd2.valid_from, scd2.valid_to))
        got = joined.count()

        # ★ 不能直接 .select(多列).write.text()：text() 数据源只收单列
        #   （报 "Text data source supports only a single column, and you have 12 columns"），
        #   并且 text() 写出来是【目录】不是单个文件。所以先在 Spark SQL 内部
        #   concat_ws 拼成单列，写完再合并 part-*。（同一个坑在 dws_ads 那支已经踩过）
        tmp = tempfile.mkdtemp(prefix="sku_")
        out_dir = os.path.join(tmp, "out")
        line = F.concat_ws(
            TAB,
            F.col("order_id").cast("string"),
            F.date_format("dt", "yyyy-MM-dd"),
            F.col("user_id").cast("string"),
            F.col("product_id").cast("string"),
            # ★ 不要用 F.format_number(col,2)：它会加千分位逗号（2,995.00），
            #   逗号把 CSV 行切断 → BE 报 "too many filtered rows"（同一个坑在
            #   累积快照那支作业上踩过）。cast("string") 不带千分位。
            F.col("amount").cast("string"),
            F.date_format("order_time", "yyyy-MM-dd HH:mm:ss"),
            F.col("status"),
            F.col("category"),
            F.col("brand"),
            F.col("sku_price").cast("string"),
            F.date_format("valid_from", "yyyy-MM-dd"),
            F.date_format("valid_to", "yyyy-MM-dd"))
        (joined.select(line.alias("value"))
         .coalesce(1)
         .write.mode("overwrite").option("compression", "none").text(out_dir))

        parts = [os.path.join(out_dir, f) for f in os.listdir(out_dir) if f.startswith("part-")]
        if len(parts) != 1:
            print("❌ 期望 1 个 part 文件，实际 %d" % len(parts))
            sys.exit(1)
        csv_path = os.path.join(tmp, "sku.tsv")
        shutil.move(parts[0], csv_path)

        first, last = chunk[0], chunk[-1]
        label = make_label(first, last)

        # ★ 幂等：dwd_order_sku_detail 是 DUPLICATE KEY 分区表，写入前必须清掉该分区，
        #   否则重跑翻倍。但【不能在容器里调 docker exec】—— 容器内没有 docker。
        #   所以清理由【宿主侧的外壳脚本】在提交本作业之前完成（见 run_dwd_sku_full.sh）。
        #   这里只做写入。

        loaded, flt = stream_load(last, csv_path, got, label, args.dry_run)
        if not args.dry_run:
            covered.append((first, last))

        total_rows += exp
        total_loaded += loaded
        total_filtered += flt
        print("  BATCH %03d %s~%s days=%d exp=%d got=%d loaded=%d verify=%s"
              % (bi // BATCH_DAYS, first, last, len(chunk), exp, got, loaded,
                 "OK" if loaded == got else "MISMATCH"))

        joined.unpersist()
        part.unpersist()
        shutil.rmtree(tmp, ignore_errors=True)

    first_cov = covered[0][0] if covered else "-"
    last_cov = covered[-1][1] if covered else "-"
    print("DET_TOTAL_IN_RANGE = %d" % total_rows)
    print("SKU_TOTAL_IN_RANGE = %d" % total_loaded)
    print("FILTERED_TOTAL = %d" % total_filtered)
    print("COVERED = %s .. %s" % (first_cov, last_cov))
    spark.stop()


if __name__ == "__main__":
    main()
